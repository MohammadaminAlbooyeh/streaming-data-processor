from datetime import datetime, timezone

from core.config import settings
from core.dead_letter_handler import DeadLetterHandler
from core.kafka_client import KafkaClient
from consumers.base_consumer import BaseConsumer
from models.database import SessionLocal, EventRecord, AggregateRecord, FraudAlertRecord, init_db


class StorageConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            topic=settings.raw_events_topic,
            group_id="storage-consumer",
        )
        self._dlq = DeadLetterHandler(KafkaClient())
        init_db()

    def process(self, message: dict) -> None:
        db = SessionLocal()
        try:
            record = EventRecord(
                id=message.get("id", ""),
                event_type=message.get("event_type", ""),
                source=message.get("source", ""),
                timestamp=self._parse_timestamp(message.get("timestamp")),
                data=message.get("data", {}),
                extra_metadata=message.get("metadata", {}),
            )
            db.add(record)
            db.commit()
            print(f"StorageConsumer: Stored event {record.id}")
        except Exception as e:
            db.rollback()
            self._dlq.send(message, str(e))
            print(f"StorageConsumer: Error storing event: {e}")
        finally:
            db.close()

    def _parse_timestamp(self, ts) -> datetime:
        if isinstance(ts, datetime):
            return ts
        if isinstance(ts, str):
            return datetime.fromisoformat(ts)
        return datetime.now(timezone.utc)


if __name__ == "__main__":
    consumer = StorageConsumer()
    consumer.run()
