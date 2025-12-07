import os
import json
import logging
import uuid
from datetime import datetime, timezone
from threading import Thread

from dotenv import load_dotenv
from kafka import KafkaProducer, KafkaConsumer
from telegram import Update
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [telegram_connector] %(message)s",
)
logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Tópicos usados
TOPIC_INCOMING = "chat-incoming"    # Telegram -> Chat4All
TOPIC_OUTGOING = "telegram-out"     # Chat4All -> Telegram

def new_message_id() -> str:
    return str(uuid.uuid4())

def build_event_from_telegram(update: Update) -> dict:
    """
    Converte uma mensagem de texto do Telegram em um evento interno do Chat4All.
    Aqui é o ponto em que você adapta para o formato que o seu Router espera.
    """
    msg = update.effective_message
    user = update.effective_user

    text = msg.text or ""

    # conversation_id: pode ser o chat_id do Telegram
    conversation_id = f"telegram:{msg.chat_id}"

    event = {
        "message_id": new_message_id(),
        "conversation_id": conversation_id,
        "from_user": f"telegram:{user.id}",
        "from_display": user.username or user.full_name,
        "to": [],  # vai ser decidido pelo router
        # IMPORTANTE: aqui pedimos cross-channel para INSTAGRAM
        "channels": ["instagram"],  # cross-channel real: Telegram -> mock Instagram
        "payload": {
            "type": "text",
            "text": text,
        },
        "metadata": {
            "source": "telegram",
            "telegram_chat_id": msg.chat_id,
            "telegram_message_id": msg.id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    }
    return event


def kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

def kafka_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_OUTGOING,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="telegram-connector",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

async def handle_telegram_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler chamado sempre que alguém manda mensagem para o bot no Telegram.
    Ele empacota em evento e publica no Kafka -> chat-incoming.
    """
    if not update.effective_message or not update.effective_user:
        return

    producer = context.bot_data.get("kafka_producer")
    if producer is None:
        logger.error("Kafka producer não encontrado no contexto")
        return

    event = build_event_from_telegram(update)
    logger.info("Recebido do Telegram: %s", event)

    producer.send(TOPIC_INCOMING, event)
    producer.flush()


def start_telegram_polling(producer: KafkaProducer):
    """
    Thread que roda o bot Telegram (polling).
    """
    application = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # guardamos o producer no contexto do bot
    application.bot_data["kafka_producer"] = producer

    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_telegram_message))

    logger.info("Iniciando polling do Telegram...")
    application.run_polling()


def start_outgoing_consumer():
    """
    Thread que consome telegram-out e envia mensagens via Telegram.
    Espera eventos no formato:
      {
        "telegram_chat_id": ...,
        "payload": {"type": "text", "text": "..."}
      }
    """
    consumer = kafka_consumer()

    # como o telegram bot está em outra thread, vamos criar um app simples só para enviar
    # Usamos diretamente o HTTP do bot via python-telegram-bot (contexto simplificado):
    from telegram import Bot

    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    logger.info("Consumidor telegram-out iniciado...")
    for msg in consumer:
        event = msg.value
        logger.info("Enviando para Telegram (telegram-out): %s", event)

        meta = event.get("metadata", {})
        chat_id = meta.get("telegram_chat_id")
        payload = event.get("payload", {})
        if not chat_id:
            logger.warning("Evento sem telegram_chat_id; ignorando.")
            continue

        if payload.get("type") == "text":
            text = payload.get("text", "")
            try:
                bot.send_message(chat_id=chat_id, text=text)
            except Exception as e:
                logger.exception("Erro ao enviar mensagem para Telegram: %r", e)

def main():
    producer = kafka_producer()

    t_poll = Thread(target=start_telegram_polling, args=(producer,), daemon=True)
    t_poll.start()

    # consumidor para 'telegram-out'
    start_outgoing_consumer()

if __name__ == "__main__":
    main()