"""Tests for core services: deduplication, dead letter handler, config."""

from unittest.mock import Mock, patch

import pytest
from datetime import datetime, timezone

from core.deduplication import DeduplicationService
from core.dead_letter_handler import DeadLetterHandler
from core.config import settings


class TestDeduplicationService:
    def setup_method(self):
        self.mock_redis = Mock()
        self.service = DeduplicationService(self.mock_redis)

    def test_is_duplicate_returns_false_for_new_event(self):
        self.mock_redis.exists.return_value = False
        result = self.service.is_duplicate("new-event-id")
        assert result is False
        self.mock_redis.set.assert_called_once()

    def test_is_duplicate_returns_true_for_existing_event(self):
        self.mock_redis.exists.return_value = True
        result = self.service.is_duplicate("existing-event-id")
        assert result is True
        self.mock_redis.set.assert_not_called()

    def test_is_duplicate_sets_ttl(self):
        self.mock_redis.exists.return_value = False
        self.service.is_duplicate("event-with-ttl")
        args, kwargs = self.mock_redis.set.call_args
        assert kwargs.get("ttl") == settings.dedup_ttl_seconds


class TestDeadLetterHandler:
    def setup_method(self):
        self.mock_kafka = Mock()
        self.handler = DeadLetterHandler(self.mock_kafka)

    def test_send_produces_to_dead_letter_topic(self):
        message = {"id": "failed-event", "event_type": "test"}
        self.handler.send(message, "Test error")
        self.mock_kafka.produce.assert_called_once()
        args, kwargs = self.mock_kafka.produce.call_args
        # topic is first positional arg
        assert args[0] == settings.dead_letter_topic
        assert kwargs["key"] == "failed-event"

    def test_send_includes_error_info(self):
        message = {"id": "failed-event"}
        self.handler.send(message, "Connection timeout")
        args, kwargs = self.mock_kafka.produce.call_args
        dlq_message = kwargs["value"]
        assert dlq_message["original_message"] == message
        assert dlq_message["error"] == "Connection timeout"
        assert "failed_at" in dlq_message

    def test_send_to_custom_topic(self):
        message = {"id": "test"}
        self.handler.send(message, "error", topic="custom-dlq")
        args, kwargs = self.mock_kafka.produce.call_args
        assert args[0] == "custom-dlq"


class TestConfig:
    def test_settings_have_defaults(self):
        assert settings.kafka_bootstrap_servers == "localhost:9092"
        assert settings.redis_host == "localhost"
        assert settings.redis_port == 6379
        assert settings.dedup_ttl_seconds == 3600
        assert settings.window_size_seconds == 60
