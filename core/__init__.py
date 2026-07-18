from core.config import settings
from core.kafka_client import KafkaClient
from core.redis_client import RedisClient
from core.deduplication import DeduplicationService
from core.windowing import WindowService
from core.dead_letter_handler import DeadLetterHandler

__all__ = [
    "settings",
    "KafkaClient",
    "RedisClient",
    "DeduplicationService",
    "WindowService",
    "DeadLetterHandler",
]

