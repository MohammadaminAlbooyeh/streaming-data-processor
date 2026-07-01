import re
from typing import List, Tuple

from ingestion.schemas import IncomingEvent


VALID_EVENT_TYPES = {"transaction", "click", "pageview", "login", "logout", "purchase", "refund"}
VALID_SOURCES_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+$")


def validate_event(event: IncomingEvent) -> List[Tuple[str, str]]:
    errors: List[Tuple[str, str]] = []

    if not event.event_type:
        errors.append(("event_type", "Event type is required"))
    elif event.event_type not in VALID_EVENT_TYPES:
        errors.append(("event_type", f"Invalid event type '{event.event_type}'. Valid types: {', '.join(sorted(VALID_EVENT_TYPES))}"))

    if not event.source:
        errors.append(("source", "Source is required"))
    elif not VALID_SOURCES_PATTERN.match(event.source):
        errors.append(("source", "Source must match pattern [a-zA-Z0-9_-]+"))

    if event.data and not isinstance(event.data, dict):
        errors.append(("data", "Data must be a dictionary"))

    if event.metadata and not isinstance(event.metadata, dict):
        errors.append(("metadata", "Metadata must be a dictionary"))

    return errors
