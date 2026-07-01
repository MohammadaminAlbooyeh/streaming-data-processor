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

        consumer = TestConsumer(topic="test", group_id="test-group")
        consumer._kafka = Mock()
        consumer._kafka.consume.side_effect = lambda topic, group_id, handler: handler({"id": "1"})

        consumer.run()
        # Should not raise


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
        topic, kwargs = call_args[0], call_args[1]
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
