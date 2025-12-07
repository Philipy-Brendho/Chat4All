import json
import threading
import time
from typing import Callable, Dict

from kafka import KafkaProducer, KafkaConsumer

class KafkaBroker:
    """
    Broker baseado em Kafka.

    Tópicos usados:
    - chat-incoming : mensagens recém-enviadas pela API.
    - chat-events   : eventos para os usuários (mensagens + avisos de sistema + estados).

    Estratégia para offline (backlog):
    - Cada usuário consome de 'chat-events' com group_id fixo: chat-user-<username>.
    - Kafka guarda o offset por group_id.
    - Com auto_offset_reset="earliest":
        * se nunca houve offset para aquele grupo, começa do início;
        * depois, sempre retoma do ponto onde parou.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self._user_consumers: Dict[str, bool] = {}
        self._lock = threading.Lock()

    #  Pub
    def publish_incoming(self, event: dict) -> None:
        """
        API -> Router: nova mensagem entrando no sistema.
        """
        self.producer.send("chat-incoming", event)
        self.producer.flush()

    def publish_delivery_to_user(self, username: str, event: dict) -> None:
        """
        Router/serviços -> usuários: evento que será filtrado depois por subscribe_user.
        """
        self.producer.send("chat-events", event)
        self.producer.flush()

    def broadcast_system(self, content: str) -> None:
        """
        Evento de sistema visível para todos.
        """
        event = {
            "ts_unix_ms": int(time.time() * 1000),
            "system": {"content": content},
        }
        self.producer.send("chat-events", event)
        self.producer.flush()

    #  Sub
    def subscribe_user(self, username: str, callback: Callable[[dict], None]) -> None:
        """
        Cria uma thread consumindo 'chat-events' para um usuário específico.

        - Usa group_id = chat-user-<username> (fixo).
        - Usa auto_offset_reset="earliest" para pegar backlog quando não há offset.
        - Filtra:
            * eventos de sistema -> sempre entrega
            * eventos kind="state" -> entrega
            * mensagens privadas/grupo -> entrega só se username estiver em 'to'
        """
        with self._lock:
            if self._user_consumers.get(username):
                return
            self._user_consumers[username] = True

        def worker():
            consumer = KafkaConsumer(
                "chat-events",
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=f"chat-user-{username}",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )

            for msg in consumer:
                data = msg.value or {}

                if "system" in data:
                    callback(data)
                    continue

                if data.get("kind") == "state":
                    callback(data)
                    continue

                to = data.get("to") or []
                if username in to:
                    callback(data)

        t = threading.Thread(target=worker, daemon=True)
        t.start()
