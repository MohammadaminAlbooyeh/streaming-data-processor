from datetime import datetime, timedelta, timezone
from typing import Optional

from core.config import settings
from core.redis_client import RedisClient
from models.aggregate import Aggregate
from models.event import Event


class WindowService:
    def __init__(self, redis: RedisClient):
        self._redis = redis

    def get_window_key(self, event: Event, window_size_seconds: Optional[int] = None) -> str:
        size = window_size_seconds or settings.window_size_seconds
        window_start = self._get_window_start(event.timestamp, size)
        return f"{event.event_type}:{size}:{int(window_start.timestamp())}"

    def _get_window_start(self, timestamp: datetime, window_size_seconds: int) -> datetime:
        ts = int(timestamp.timestamp())
        window_ts = (ts // window_size_seconds) * window_size_seconds
        return datetime.fromtimestamp(window_ts)

    def update_window(self, event: Event, value_field: str = "value") -> Aggregate:
        window_key = self.get_window_key(event)
        window_size = settings.window_size_seconds
        window_start = self._get_window_start(event.timestamp, window_size)
        window_end = window_start + timedelta(seconds=window_size)

        redis_key = f"window:{window_key}"
        existing = self._redis.get(redis_key)

        value = event.data.get(value_field, 0)
        if isinstance(value, (int, float)):
            value = float(value)
        else:
            value = 0.0

        if existing:
            agg = Aggregate(**existing)
            agg.count += 1
            agg.sum += value
            agg.avg = agg.sum / agg.count if agg.count > 0 else 0.0
            agg.min = min(agg.min, value)
            agg.max = max(agg.max, value)
            agg.last_updated = datetime.now(timezone.utc)
        else:
            agg = Aggregate(
                window_key=window_key,
                window_start=window_start,
                window_end=window_end,
                event_type=event.event_type,
                count=1,
                sum=value,
                avg=value,
                min=value,
                max=value,
            )

        self._redis.set(redis_key, agg.model_dump())
        return agg

    def get_window(self, window_key: str) -> Optional[Aggregate]:
        redis_key = f"window:{window_key}"
        data = self._redis.get(redis_key)
        if data:
            return Aggregate(**data)
        return None
