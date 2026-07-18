from datetime import datetime, timezone

from core.config import settings
from core.dead_letter_handler import DeadLetterHandler
from core.kafka_client import KafkaClient
from consumers.base_consumer import BaseConsumer
from models.database import SessionLocal, EventRecord, FraudAlertRecord, init_db


class StorageConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            topic=settings.raw_events_topic,
            group_id="storage-consumer",
        )
        self._kafka = KafkaClient()
        self._dlq = DeadLetterHandler(self._kafka)
        init_db()

    def process(self, message: dict) -> None:
        if "rule" in message and "severity" in message:
            self._store_fraud_alert(message)
        else:
            self._store_event(message)

    def run(self) -> None:
        self._kafka.consume_multiple([
            (settings.raw_events_topic, "storage-consumer", self._handle_with_errors),
            (settings.fraud_alerts_topic, "storage-consumer", self._handle_with_errors),
        ])

    def _store_event(self, message: dict) -> None:
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

    def _store_fraud_alert(self, message: dict) -> None:
        db = SessionLocal()
        try:
            record = FraudAlertRecord(
                id=message.get("id", ""),
                event_id=message.get("event_id", ""),
                rule=message.get("rule", ""),
                severity=message.get("severity", ""),
                details=message.get("details", {}),
                created_at=self._parse_timestamp(message.get("created_at")),
            )
            db.add(record)
            db.commit()
            print(f"StorageConsumer: Stored fraud alert {record.id}")
        except Exception as e:
            db.rollback()
            self._dlq.send(message, str(e))
            print(f"StorageConsumer: Error storing fraud alert: {e}")
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
