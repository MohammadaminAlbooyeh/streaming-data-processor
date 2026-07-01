from core.config import settings
from core.kafka_client import KafkaClient
from core.redis_client import RedisClient
from core.deduplication import DeduplicationService
from ingestion.schemas import IncomingEvent
from models.event import Event


class EventProducer:
    def __init__(self, kafka: KafkaClient, redis: RedisClient):
        self._kafka = kafka
        self._dedup = DeduplicationService(redis)

    def publish(self, incoming: IncomingEvent) -> Event:
        event = Event(
            event_type=incoming.event_type,
            source=incoming.source,
            timestamp=incoming.timestamp or Event.model_fields["timestamp"].default_factory(),
            data=incoming.data,
            metadata=incoming.metadata,
        )

        if self._dedup.is_duplicate(event.id):
            raise ValueError(f"Duplicate event detected: {event.id}")

        self._kafka.produce(
            settings.raw_events_topic,
            value=event.model_dump(mode="json"),
            key=event.id,
        )

        return event
