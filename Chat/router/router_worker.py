import json
import logging
import os
import threading
import time

from typing import Any, Dict
from kafka import KafkaConsumer, KafkaProducer
from pythonjsonlogger import jsonlogger
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from storage.cassandra_store import CassandraStore
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

resource = Resource(attributes={
    "service.name": "chat4all-router-worker"
})

provider = TracerProvider(
    resource=resource,
    sampler=TraceIdRatioBased(0.01)
)

# Logging estruturado (JSON)
_log_handler = logging.StreamHandler()
_log_formatter = jsonlogger.JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s"
)
_log_handler.setFormatter(_log_formatter)

root_logger = logging.getLogger()
root_logger.handlers = [_log_handler]
root_logger.setLevel(logging.INFO)

logger = logging.getLogger("RouterWorker")

# Tracing distribuído com OpenTelemetry
resource = Resource(attributes={"service.name": "chat4all-router-worker"})
provider = TracerProvider(resource=resource)

otel_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")

exporter = OTLPSpanExporter(endpoint=otel_endpoint, insecure=True)
processor = BatchSpanProcessor(exporter)
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)

tracer = trace.get_tracer(__name__)

# Configuração Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

INCOMING_TOPIC = "chat-incoming"       # mensagens vindas da API/gRPC
STATUS_TOPIC = "chat-status"           # callbacks de status (mocks, connectors reais)
EVENTS_TOPIC = "chat-events"           # eventos internos para clientes conectados

WHATSAPP_TOPIC = "whatsapp-out"
INSTAGRAM_TOPIC = "instagram-out"
TELEGRAM_TOPIC = "telegram-out"

def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks="all",
    )

def make_consumer(topic: str, group_id: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

# RouterWorker
class RouterWorker:
    """
    Worker responsável por:
      - Consumir mensagens de chat-incoming
      - Persistir no Cassandra
      - Fazer fan-out para canais internos (chat-events) e externos
        (whatsapp-out, instagram-out, telegram-out)
      - Consumir status em chat-status
      - Atualizar status no Cassandra e propagar para chat-events
    """

    def __init__(self) -> None:
        self.store = CassandraStore()

        self.producer = make_producer()
        self.consumer_incoming = make_consumer(INCOMING_TOPIC, group_id="chat-router")
        self.consumer_status = make_consumer(STATUS_TOPIC, group_id="chat-router-status")

        logger.info(
            "RouterWorker inicializado",
            extra={
                "kafka_bootstrap": KAFKA_BOOTSTRAP,
                "incoming_topic": INCOMING_TOPIC,
                "status_topic": STATUS_TOPIC,
                "events_topic": EVENTS_TOPIC,
            },
        )

    # Loop principal
    def run(self) -> None:
        """
        Inicia as threads que consomem incoming e status.
        """
        t_in = threading.Thread(target=self._loop_incoming, daemon=True)
        t_status = threading.Thread(target=self._loop_status, daemon=True)

        t_in.start()
        t_status.start()

        logger.info("RouterWorker em execução (loops incoming/status iniciados)")

        # Mantém o processo vivo
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            logger.info("Encerrando RouterWorker por KeyboardInterrupt...")

    # Loop de mensagens novas
    def _loop_incoming(self) -> None:
        logger.info("Loop de consumo de %s iniciado", INCOMING_TOPIC)
        for msg in self.consumer_incoming:
            event = msg.value
            with tracer.start_as_current_span("process_message") as span:
                span.set_attribute("event.type", "incoming")
                span.set_attribute("message.id", event.get("message_id", ""))
                span.set_attribute("conversation.id", event.get("conversation_id", ""))
                span.set_attribute("channels", ",".join(event.get("channels", [])))

                logger.info("Mensagem recebida de chat-incoming", extra={"event": event})

                try:
                    self._process_incoming_event(event)
                except Exception as e:
                    logger.exception("Erro ao processar mensagem incoming: %r", e)
                    span.record_exception(e)

    # Loop de status (DELIVERED, READ, etc.)
    def _loop_status(self) -> None:
        logger.info("Loop de consumo de %s iniciado", STATUS_TOPIC)
        for msg in self.consumer_status:
            status_event = msg.value
            with tracer.start_as_current_span("process_status") as span:
                span.set_attribute("event.type", "status")
                span.set_attribute("message.id", status_event.get("message_id", ""))
                span.set_attribute("status", status_event.get("status", ""))

                logger.info("Evento de status recebido", extra={"event": status_event})

                try:
                    self._process_status_event(status_event)
                except Exception as e:
                    logger.exception("Erro ao processar evento de status: %r", e)
                    span.record_exception(e)

    # Processamento de mensagem incoming
    def _process_incoming_event(self, event: Dict[str, Any]) -> None:
        msg_id = event.get("message_id")
        conv_id = event.get("conversation_id")
        sender = event.get("from_user")
        to = event.get("to", [])
        channels = event.get("channels", ["internal"])

        target = to[0] if to else ""
        is_group = bool(target and self.store.is_group(target))

        if is_group and not self.store.is_user_in_group(sender, target):
            logger.warning(
                "Usuário tentou enviar mensagem para grupo sem participação",
                extra={"sender": sender, "group": target},
            )
            return

        logger.info(
            "Processando incoming",
            extra={
                "message_id": msg_id,
                "conversation_id": conv_id,
                "from_user": sender,
                "to": to,
                "channels": channels,
            },
        )

        # Persistir no Cassandra
        self.store.save_message(event)

        # Enviar evento interno para chat-events
        internal_event = dict(event)
        internal_event.setdefault("status", "SENT")

        key = self._key_for_event(internal_event)
        self.producer.send(EVENTS_TOPIC, key=key, value=internal_event)

        # Fan-out externo
        self._fanout_external(event, channels)

    # Processamento de evento de status
    def _process_status_event(self, status_event: Dict[str, Any]) -> None:
        """
        Processa um evento de status vindo de um mock/conector real.
        Exemplo de status_event:
            {
                "message_id": "...",
                "status": "DELIVERED" | "READ",
                "channel": "whatsapp" | "instagram" | "telegram",
                "metadata": {...}
            }
        """
        msg_id = status_event.get("message_id")
        status = status_event.get("status")
        channel = status_event.get("channel")
        conv_id = status_event.get("conversation_id")
        ts_unix_ms = status_event.get("ts_unix_ms")

        if not conv_id or ts_unix_ms is None:
            logger.warning("Evento de status sem conversation_id ou ts_unix_ms: %s", status_event)
        else:
            try:
                self.store.update_message_status(conv_id, ts_unix_ms, msg_id, status)
            except Exception as e:
                logger.exception("Erro ao atualizar status no Cassandra: %r", e)

        # Atualizar status no Cassandra
        try:
            self.store.update_message_status(msg_id, status, channel=channel)
        except Exception as e:
            logger.exception("Erro ao atualizar status no Cassandra: %r", e)

        # Propagar atualização de status para clientes internos via chat-events
        key = self._key_for_event(status_event)
        try:
            self.producer.send(EVENTS_TOPIC, key=key, value=status_event)
        except Exception as e:
            logger.exception("Erro ao publicar evento de status em %s: %r", EVENTS_TOPIC, e)

    # Fan-out externo
    def _fanout_external(self, event: Dict[str, Any], channels: Any) -> None:
        """
        Envia a mensagem para os tópicos externos corretos (WhatsApp/Instagram/Telegram).
        """
        if not channels:
            return

        for ch in channels:
            if ch == "whatsapp":
                self._send_external(WHATSAPP_TOPIC, event, channel="whatsapp")
            elif ch == "instagram":
                self._send_external(INSTAGRAM_TOPIC, event, channel="instagram")
            elif ch == "telegram":
                self._send_external(TELEGRAM_TOPIC, event, channel="telegram")

    def _send_external(self, topic: str, event: Dict[str, Any], channel: str) -> None:
        outgoing = dict(event)
        outgoing.setdefault("channel", channel)

        key = self._key_for_event(outgoing)

        logger.info(
            "Enviando para canal externo",
            extra={"topic": topic, "channel": channel, "message_id": event.get("message_id")},
        )

        try:
            self.producer.send(topic, key=key, value=outgoing)
        except Exception as e:
            logger.exception("Erro ao enviar para tópico externo %s: %r", topic, e)

    # Helpers
    def _key_for_event(self, event: Dict[str, Any]) -> bytes:
        """
        Gera uma key para particionamento no Kafka baseada na conversation_id.
        """
        conv_id = event.get("conversation_id") or event.get("message_id") or ""
        return str(conv_id).encode("utf-8")

if __name__ == "__main__":
    worker = RouterWorker()
    worker.run()
