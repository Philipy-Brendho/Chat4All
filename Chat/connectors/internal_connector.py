import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

from .base import BaseConnector
from broker.kafka_broker import KafkaBroker

class InternalConnector(BaseConnector):
    """
    Conector que entrega mensagens para usuários internos
    via Kafka → tópico 'chat-events'.
    """

    def __init__(self, broker: KafkaBroker):
        self.broker = broker

    def send(self, event: dict):
        # A lógica de filtro por usuário é feita na subscribe_user
        # (cada usuário pega apenas o que é destinado a ele).
        # Aqui apenas publicamos o evento.
        self.broker.publish_delivery_to_user("", event)
