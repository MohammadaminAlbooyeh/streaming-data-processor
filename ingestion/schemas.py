from datetime import datetime
from pydantic import BaseModel, Field


class IncomingEvent(BaseModel):
    event_type: str
    source: str
    timestamp: datetime | None = None
    data: dict = Field(default_factory=dict)
    metadata: dict = Field(default_factory=dict)
