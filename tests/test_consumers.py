"""Tests for consumers: base, enrichment, storage, and aggregation."""

import pytest
from unittest.mock import Mock, patch

from consumers.base_consumer import BaseConsumer
from consumers.enrichment_consumer import EnrichmentConsumer
from consumers.storage_consumer import StorageConsumer


class TestBaseConsumer:
    def test_base_consumer_cannot_instantiate(self):
        with pytest.raises(TypeError):
            BaseConsumer(topic="test", group_id="test-group")

    def test_handle_with_errors_catches_exceptions(self):
        class TestConsumer(BaseConsumer):
            def process(self, message):
                raise ValueError("test error")

        consumer = TestConsumer(topic="test", group_id="test-group", use_dlq=False)
        consumer._kafka = Mock()
        consumer._kafka.consume.side_effect = lambda topic, group_id, handler: handler({"id": "1"})

        consumer.run()

    def test_handle_with_errors_sends_to_dlq(self):
        class TestConsumer(BaseConsumer):
            def process(self, message):
                raise ValueError("DLQ test error")

        consumer = TestConsumer(topic="test", group_id="test-group", use_dlq=True)
        consumer._kafka = Mock()
        consumer._dlq = Mock()
        consumer._kafka.consume.side_effect = lambda topic, group_id, handler: handler({"id": "1"})

        consumer.run()
        consumer._dlq.send.assert_called_once_with({"id": "1"}, "DLQ test error")


class TestEnrichmentConsumer:
    def setup_method(self):
        self.consumer = EnrichmentConsumer()
        self.consumer._kafka = Mock()
        self.consumer._dlq = Mock()

    def test_enrichment_adds_fields(self):
        message = {
            "id": "test-1",
            "event_type": "click",
            "source": "web",
            "timestamp": "2024-01-15T12:00:00",
            "data": {},
            "metadata": {},
        }
        self.consumer.process(message)

        call_args = self.consumer._kafka.produce.call_args
        assert call_args is not None
        kwargs = call_args[1]
        enriched = kwargs["value"]
        assert enriched["metadata"]["enriched"] is True
        assert "country" in enriched["metadata"]
        assert "device" in enriched["metadata"]
        assert "browser" in enriched["metadata"]


class TestStorageConsumer:
    def setup_method(self):
        self.consumer = StorageConsumer()
        self.consumer._dlq = Mock()

    @patch("consumers.storage_consumer.SessionLocal")
    def test_stores_event(self, mock_session_local):
        mock_db = Mock()
        mock_session_local.return_value = mock_db

        message = {
            "id": "test-1",
            "event_type": "pageview",
            "source": "web-app",
            "timestamp": "2024-01-15T12:00:00",
            "data": {"url": "/home"},
            "metadata": {},
        }
        self.consumer.process(message)

        assert mock_db.add.called
        assert mock_db.commit.called
        assert mock_db.close.called

    @patch("consumers.storage_consumer.SessionLocal")
    def test_stores_fraud_alert(self, mock_session_local):
        mock_db = Mock()
        mock_session_local.return_value = mock_db

        message = {
            "id": "fraud-1",
            "event_id": "event-1",
            "rule": "rapid_fire_events",
            "severity": "high",
            "details": {"source": "bot", "event_count": 10},
            "created_at": "2024-01-15T12:00:00",
        }
        self.consumer.process(message)

        assert mock_db.add.called
        assert mock_db.commit.called

    @patch("consumers.storage_consumer.SessionLocal")
    def test_rollback_on_db_error(self, mock_session_local):
        mock_db = Mock()
        mock_db.commit.side_effect = Exception("DB error")
        mock_session_local.return_value = mock_db

        message = {
            "id": "test-1",
            "event_type": "pageview",
            "source": "web-app",
            "timestamp": "2024-01-15T12:00:00",
            "data": {},
            "metadata": {},
        }
        self.consumer.process(message)

        assert mock_db.rollback.called
        assert self.consumer._dlq.send.called

    def test_parse_timestamp_datetime(self):
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        result = self.consumer._parse_timestamp(now)
        assert result == now

    def test_parse_timestamp_string(self):
        from datetime import datetime

        result = self.consumer._parse_timestamp("2024-01-15T12:00:00")
        assert result == datetime(2024, 1, 15, 12, 0, 0)

    def test_parse_timestamp_none(self):
        from datetime import datetime

        result = self.consumer._parse_timestamp(None)
        assert isinstance(result, datetime)
