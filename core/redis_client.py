import json
from typing import Any, Optional
import redis as sync_redis

from core.config import settings


class RedisClient:
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, db: Optional[int] = None):
        self._client = sync_redis.Redis(
            host=host or settings.redis_host,
            port=port or settings.redis_port,
            db=db or settings.redis_db,
            decode_responses=True,
        )

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        serialized = json.dumps(value, default=str)
        self._client.set(key, serialized, ex=ttl)

    def get(self, key: str) -> Optional[Any]:
        value = self._client.get(key)
        if value is not None:
            return json.loads(value)
        return None

    def delete(self, key: str) -> None:
        self._client.delete(key)

    def exists(self, key: str) -> bool:
        return bool(self._client.exists(key))

    def incr(self, key: str) -> int:
        return self._client.incr(key)

    def expire(self, key: str, ttl: int) -> None:
        self._client.expire(key, ttl)

    def close(self) -> None:
        self._client.close()
