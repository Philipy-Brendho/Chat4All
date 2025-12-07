# whatsapp_mock.py

import json
import logging
import time
from typing import Any, Dict

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WhatsAppMock")

KAFKA_BOOTSTRAP = "localhost:9092"
WHATSAPP_TOPIC = "whatsapp-out"
STATUS_TOPIC = "chat-status"


class WhatsAppMockConnector:
    """
    Connector mock de WhatsApp para a Semana 5–6.

    - Consome mensagens do tópico 'whatsapp-out'.
    - Simula envio com logs.
    - Publica callbacks de status em 'chat-status':
        - DELIVERED
        - READ
    """

    def __init__(self):
        self.consumer = KafkaConsumer(
            WHATSAPP_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="whatsapp-mock",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def run(self):
        logger.info("WhatsAppMock iniciado. Consumindo de '%s'.", WHATSAPP_TOPIC)
        for message in self.consumer:
            event = message.value
            try:
                self._handle_message(event)
            except Exception as e:
                logger.exception("Erro no WhatsAppMock: %r", e)

    def _handle_message(self, event: Dict[str, Any]):
        msg_id = event.get("message_id")
        conv_id = event.get("conversation_id")
        to = event.get("to", [])
        payload = event.get("payload", {})

        logger.info(
            "[WhatsApp] Simulando envio msg_id=%s conv=%s para=%s payload=%s",
            msg_id,
            conv_id,
            to,
            payload,
        )

        now_ms = int(time.time() * 1000)

        # 1) DELIVERED
        delivered_event = {
            "message_id": msg_id,
            "conversation_id": conv_id,
            "status": "DELIVERED",
            "channel": "whatsapp",
            "ts_unix_ms": now_ms,
            "extra": {"info": "entregue no dispositivo WhatsApp mock"},
        }
        self._send_status(delivered_event)

        # 2) READ (simulado após pequeno delay)
        time.sleep(0.5)
        read_event = {
            "message_id": msg_id,
            "conversation_id": conv_id,
            "status": "READ",
            "channel": "whatsapp",
            "ts_unix_ms": int(time.time() * 1000),
            "extra": {"info": "lido no dispositivo WhatsApp mock"},
        }
        self._send_status(read_event)

    def _send_status(self, status_event: Dict[str, Any]):
        logger.info(
            "[WhatsApp] Callback status=%s msg_id=%s conv=%s",
            status_event.get("status"),
            status_event.get("message_id"),
            status_event.get("conversation_id"),
        )
        self.producer.send(STATUS_TOPIC, value=status_event)


if __name__ == "__main__":
    WhatsAppMockConnector().run()
