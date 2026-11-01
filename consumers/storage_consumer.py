from datetime import datetime, timezone

from core.config import settings
from core.dead_letter_handler import DeadLetterHandler
from core.kafka_client import KafkaClient
from consumers.base_consumer import BaseConsumer
from models.database import SessionLocal, EventRecord, FraudAlertRecord, AggregateRecord, init_db


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
        elif "window_key" in message and "count" in message:
            self._store_aggregate(message)
        else:
            self._store_event(message)

    def run(self) -> None:
        self._kafka.consume_multiple([
            (settings.raw_events_topic, "storage-consumer", self._handle_with_errors),
            (settings.enriched_events_topic, "storage-consumer", self._handle_with_errors),
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
            self.logger.info("event_stored", event_id=record.id)
        except Exception as e:
            db.rollback()
            self._dlq.send(message, str(e))
            self.logger.error("event_store_failed", error=str(e), event_id=message.get("id"))
        finally:
            db.close()

    def _store_aggregate(self, message: dict) -> None:
        db = SessionLocal()
        try:
            record = AggregateRecord(
                id=message.get("id", ""),
                window_key=message.get("window_key", ""),
                window_start=self._parse_timestamp(message.get("window_start")),
                window_end=self._parse_timestamp(message.get("window_end")),
                event_type=message.get("event_type", ""),
                count=message.get("count", 0),
                sum=message.get("sum", 0.0),
                avg=message.get("avg", 0.0),
                min=message.get("min", 0.0),
                max=message.get("max", 0.0),
                last_updated=self._parse_timestamp(message.get("last_updated")),
            )
            db.merge(record)
            db.commit()
            self.logger.info("aggregate_stored", window_key=record.window_key)
        except Exception as e:
            db.rollback()
            self._dlq.send(message, str(e))
            self.logger.error("aggregate_store_failed", error=str(e), window_key=message.get("window_key"))
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
            self.logger.info("fraud_alert_stored", alert_id=record.id, rule=record.rule)
        except Exception as e:
            db.rollback()
            self._dlq.send(message, str(e))
            self.logger.error("fraud_alert_store_failed", error=str(e), alert_id=message.get("id"))
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
