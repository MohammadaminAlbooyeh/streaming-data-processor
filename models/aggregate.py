from datetime import datetime, timezone
from uuid import uuid4
from pydantic import BaseModel, Field


def _utcnow():
    return datetime.now(timezone.utc)


class Aggregate(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    window_key: str
    window_start: datetime
    window_end: datetime
    event_type: str
    count: int = 0
    sum: float = 0.0
    avg: float = 0.0
    min: float = 0.0
    max: float = 0.0
    last_updated: datetime = Field(default_factory=_utcnow)
