"""Tests for the ingestion event producer."""

from unittest.mock import Mock, patch

import pytest

from ingestion.producer import EventProducer
from ingestion.schemas import IncomingEvent
from core.kafka_client import KafkaClient
from core.redis_client import RedisClient


class TestEventProducer:
    def setup_method(self):
        self.kafka = Mock(spec=KafkaClient)
        self.redis = Mock(spec=RedisClient)
        self.producer = EventProducer(self.kafka, self.redis)

    @patch("ingestion.producer.DeduplicationService")
    def test_publish_creates_event_and_produces(self, mock_dedup_cls):
        mock_dedup = Mock()
        mock_dedup.is_duplicate.return_value = False
        mock_dedup_cls.return_value = mock_dedup
        producer = EventProducer(self.kafka, self.redis)

        incoming = IncomingEvent(
            event_type="transaction",
            source="web-app",
            data={"amount": 99.99},
        )
        event = producer.publish(incoming)

        assert event.event_type == "transaction"
        assert event.source == "web-app"
        assert event.data == {"amount": 99.99}
        self.kafka.produce.assert_called_once()
        args, kwargs = self.kafka.produce.call_args
        assert kwargs["value"]["event_type"] == "transaction"
        assert kwargs["key"] == event.id

    @patch("ingestion.producer.DeduplicationService")
    def test_publish_raises_on_duplicate(self, mock_dedup_cls):
        mock_dedup = Mock()
        mock_dedup.is_duplicate.return_value = True
        mock_dedup_cls.return_value = mock_dedup
        producer = EventProducer(self.kafka, self.redis)

        incoming = IncomingEvent(event_type="click", source="web")
        with pytest.raises(ValueError, match="Duplicate event detected"):
            producer.publish(incoming)

    @patch("ingestion.producer.DeduplicationService")
    def test_publish_uses_provided_timestamp(self, mock_dedup_cls):
        from datetime import datetime, timezone

        mock_dedup = Mock()
        mock_dedup.is_duplicate.return_value = False
        mock_dedup_cls.return_value = mock_dedup
        producer = EventProducer(self.kafka, self.redis)

        ts = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
        incoming = IncomingEvent(
            event_type="purchase",
            source="mobile",
            timestamp=ts,
        )
        event = producer.publish(incoming)

        assert event.timestamp == ts

    @patch("ingestion.producer.DeduplicationService")
    def test_publish_includes_metadata(self, mock_dedup_cls):
        mock_dedup = Mock()
        mock_dedup.is_duplicate.return_value = False
        mock_dedup_cls.return_value = mock_dedup
        producer = EventProducer(self.kafka, self.redis)

        incoming = IncomingEvent(
            event_type="login",
            source="api",
            metadata={"ip": "10.0.0.1", "user_agent": "curl"},
        )
        event = producer.publish(incoming)

        assert event.metadata == {"ip": "10.0.0.1", "user_agent": "curl"}