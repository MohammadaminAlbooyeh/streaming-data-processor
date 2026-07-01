import pytest
from datetime import datetime

from ingestion.schemas import IncomingEvent
from ingestion.validators import validate_event, VALID_EVENT_TYPES
from models.event import Event


class TestSchemas:
    def test_incoming_event_valid(self):
        event = IncomingEvent(event_type="transaction", source="web-app", data={"amount": 100})
        assert event.event_type == "transaction"
        assert event.source == "web-app"

    def test_incoming_event_defaults(self):
        event = IncomingEvent(event_type="click", source="mobile")
        assert event.data == {}
        assert event.metadata == {}
        assert event.timestamp is None

    def test_event_model_defaults(self):
        event = Event(event_type="login", source="api-gateway")
        assert event.id is not None
        assert isinstance(event.timestamp, datetime)


class TestValidators:
    def test_valid_event_passes(self):
        event = IncomingEvent(event_type="transaction", source="web-app", data={"amount": 50})
        errors = validate_event(event)
        assert errors == []

    def test_invalid_event_type(self):
        event = IncomingEvent(event_type="invalid_type", source="web-app")
        errors = validate_event(event)
        assert any(field == "event_type" for field, _ in errors)

    def test_empty_source(self):
        event = IncomingEvent(event_type="click", source="")
        errors = validate_event(event)
        assert any(field == "source" for field, _ in errors)

    def test_source_with_spaces(self):
        event = IncomingEvent(event_type="click", source="bad source!")
        errors = validate_event(event)
        assert any(field == "source" for field, _ in errors)

    def test_non_dict_data_type_error(self):
        with pytest.raises((TypeError, ValueError)):
            IncomingEvent(event_type="click", source="app", data=123)

    def test_all_valid_event_types_pass(self):
        for et in VALID_EVENT_TYPES:
            event = IncomingEvent(event_type=et, source="test-source")
            errors = validate_event(event)
            assert errors == [], f"Failed for event_type: {et}"
