from core.config import settings
from core.redis_client import RedisClient
from core.windowing import WindowService
from core.dead_letter_handler import DeadLetterHandler
from core.kafka_client import KafkaClient
from consumers.base_consumer import BaseConsumer
from models.event import Event


class AggregationConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            topic=settings.raw_events_topic,
            group_id="aggregation-consumer",
        )
        self._redis = RedisClient()
        self._window_service = WindowService(self._redis)
        self._dlq = DeadLetterHandler(KafkaClient())

    def process(self, message: dict) -> None:
        try:
            event = Event(**message)
            self._window_service.update_window(event)
            print(f"AggregationConsumer: Updated window for event {event.id}")
        except Exception as e:
            self._dlq.send(message, str(e))
            print(f"AggregationConsumer: Sent to DLQ: {e}")


if __name__ == "__main__":
    consumer = AggregationConsumer()
    consumer.run()
