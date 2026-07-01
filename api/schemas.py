from datetime import datetime
from pydantic import BaseModel


class EventResponse(BaseModel):
    id: str
    event_type: str
    source: str
    timestamp: datetime
    data: dict
    extra_metadata: dict
    created_at: datetime | None = None


class AggregateResponse(BaseModel):
    id: str
    window_key: str
    window_start: datetime
    window_end: datetime
    event_type: str
    count: int
    sum: float
    avg: float
    min: float
    max: float
    last_updated: datetime


class FraudAlertResponse(BaseModel):
    id: str
    event_id: str
    rule: str
    severity: str
    details: dict
    created_at: datetime
