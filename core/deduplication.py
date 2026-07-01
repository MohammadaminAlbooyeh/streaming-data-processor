from core.config import settings
from core.redis_client import RedisClient


class DeduplicationService:
    def __init__(self, redis: RedisClient):
        self._redis = redis

    def is_duplicate(self, event_id: str) -> bool:
        key = f"dedup:{event_id}"
        if self._redis.exists(key):
            return True
        self._redis.set(key, 1, ttl=settings.dedup_ttl_seconds)
        return False
