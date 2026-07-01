from datetime import datetime, timezone
from typing import Optional

from core.kafka_client import KafkaClient
from core.config import settings


class DeadLetterHandler:
    def __init__(self, kafka: KafkaClient):
        self._kafka = kafka

    def send(
        self,
        original_message: dict,
        error: str,
        topic: Optional[str] = None,
    ) -> None:
        dlq_message = {
            "original_message": original_message,
            "error": error,
            "failed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._kafka.produce(
            topic or settings.dead_letter_topic,
            value=dlq_message,
            key=original_message.get("id"),
        )
