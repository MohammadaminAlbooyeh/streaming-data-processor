from datetime import datetime, timezone
from typing import List, Dict

from core.config import settings
from core.kafka_client import KafkaClient
from core.redis_client import RedisClient
from core.dead_letter_handler import DeadLetterHandler
from consumers.base_consumer import BaseConsumer
from models.event import Event


FRAUD_RULES = [
    "high_value_transaction",
    "rapid_fire_events",
    "unusual_source_activity",
]

HIGH_VALUE_THRESHOLD = 10000.0
RAPID_FIRE_LIMIT = 10
RAPID_FIRE_WINDOW = 60
UNUSUAL_EVENT_LIMIT = 50
UNUSUAL_EVENT_WINDOW = 300


class FraudConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            topic=settings.raw_events_topic,
            group_id="fraud-consumer",
        )
        self._redis = RedisClient()
        self._kafka = KafkaClient()
        self._dlq = DeadLetterHandler(self._kafka)

    def process(self, message: dict) -> None:
        event = Event(**message)
        alerts = []

        if rule := self._check_high_value(event):
            alerts.append(rule)

        if rule := self._check_rapid_fire(event):
            alerts.append(rule)

        if rule := self._check_unusual_activity(event):
            alerts.append(rule)

        for alert in alerts:
            self._kafka.produce(
                settings.fraud_alerts_topic,
                value=alert,
                key=alert.get("id"),
            )
            print(f"FraudConsumer: Alert triggered - {alert['rule']} for event {event.id}")

    def _check_high_value(self, event: Event) -> Dict | None:
        value = event.data.get("amount", 0)
        if isinstance(value, (int, float)) and value >= HIGH_VALUE_THRESHOLD:
            return {
                "id": f"fraud-{event.id}",
                "event_id": event.id,
                "rule": "high_value_transaction",
                "severity": "medium",
                "details": {"amount": value, "source": event.source},
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        return None

    def _check_rapid_fire(self, event: Event) -> Dict | None:
        key = f"fraud:rapid:{event.source}"
        count = self._redis.incr(key)
        self._redis.expire(key, RAPID_FIRE_WINDOW)

        if count >= RAPID_FIRE_LIMIT:
            return {
                "id": f"fraud-{event.id}",
                "event_id": event.id,
                "rule": "rapid_fire_events",
                "severity": "high",
                "details": {
                    "source": event.source,
                    "event_count": count,
                    "window_seconds": RAPID_FIRE_WINDOW,
                },
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        return None

    def _check_unusual_activity(self, event: Event) -> Dict | None:
        key = f"fraud:unusual:{event.source}:{event.event_type}"
        count = self._redis.incr(key)
        self._redis.expire(key, UNUSUAL_EVENT_WINDOW)

        if count >= UNUSUAL_EVENT_LIMIT:
            return {
                "id": f"fraud-{event.id}",
                "event_id": event.id,
                "rule": "unusual_source_activity",
                "severity": "medium",
                "details": {
                    "source": event.source,
                    "event_type": event.event_type,
                    "event_count": count,
                    "window_seconds": UNUSUAL_EVENT_WINDOW,
                },
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        return None


if __name__ == "__main__":
    consumer = FraudConsumer()
    consumer.run()
