import json
import logging
from logging.handlers import RotatingFileHandler

from .base import BaseConnector

class MockTelegramConnector(BaseConnector):
    def __init__(self, logfile: str = "mock_telegram.log"):
        self.logger = logging.getLogger("mock_telegram")
        self.logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(logfile, maxBytes=1024 * 1024, backupCount=3)
        self.logger.addHandler(handler)

    def send(self, event: dict):
        safe_event = {
            "message_id": event["message_id"],
            "to": event["to"],
            "ts_unix_ms": event["ts_unix_ms"],
            "has_text": bool(event["payload"].get("text")),
            "has_file": bool(event["payload"].get("file_id")),
        }
        self.logger.info(json.dumps(safe_event, ensure_ascii=False))
