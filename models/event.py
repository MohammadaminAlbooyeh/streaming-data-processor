from datetime import datetime, timezone
from uuid import uuid4
from pydantic import BaseModel, Field


def _utcnow():
    return datetime.now(timezone.utc)


class Event(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str
    source: str
    timestamp: datetime = Field(default_factory=_utcnow)
    data: dict = Field(default_factory=dict)
    metadata: dict = Field(default_factory=dict)
