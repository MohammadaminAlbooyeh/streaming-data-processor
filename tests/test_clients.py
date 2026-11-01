"""Tests for Kafka and Redis clients (mocked)."""

from unittest.mock import Mock, MagicMock, patch

import pytest
from kafka.errors import KafkaError

from core.kafka_client import KafkaClient
from core.redis_client import RedisClient


class TestKafkaClient:
    def setup_method(self):
        self.client = KafkaClient(bootstrap_servers="localhost:9092")

    @patch("core.kafka_client.KafkaProducer")
    def test_producer_lazy_initialization(self, mock_kafka_producer):
        assert self.client._producer is None
        _ = self.client.producer
        assert self.client._producer is not None
        mock_kafka_producer.assert_called_once()

    @patch("core.kafka_client.KafkaProducer")
    def test_produce_sends_message(self, mock_kafka_producer):
        mock_producer = Mock()
        mock_kafka_producer.return_value = mock_producer

        self.client.produce("test-topic", {"key": "value"}, key="msg-key")

        mock_producer.send.assert_called_once()
        args, kwargs = mock_producer.send.call_args
        assert args[0] == "test-topic"
        assert kwargs["value"] == {"key": "value"}
        assert kwargs["key"] == "msg-key"

    @patch("core.kafka_client.KafkaProducer")
    def test_produce_raises_on_kafka_error(self, mock_kafka_producer):
        mock_producer = Mock()
        # Raise a KafkaError (not a generic Exception) to match the catch in kafka_client
        mock_producer.send.side_effect = KafkaError("Kafka unavailable")
        mock_kafka_producer.return_value = mock_producer

        # Force producer init
        self.client._producer = mock_producer

        with pytest.raises(RuntimeError, match="Failed to produce message"):
            self.client.produce("test-topic", {"data": "test"})

    @patch("core.kafka_client.KafkaConsumer")
    def test_consume_calls_handler(self, mock_kafka_consumer):
        mock_message = MagicMock()
        mock_message.value = {"id": "test-1", "event_type": "click"}

        mock_consumer = MagicMock()
        mock_consumer.__iter__.return_value = [mock_message]
        mock_kafka_consumer.return_value = mock_consumer

        handler = Mock()
        self.client.consume("test-topic", "test-group", handler)

        handler.assert_called_once_with({"id": "test-1", "event_type": "click"})

    @patch("core.kafka_client.KafkaConsumer")
    def test_consume_multiple_routes_correctly(self, mock_kafka_consumer):
        msg1 = MagicMock()
        msg1.topic = "topic-a"
        msg1.value = {"id": "a1"}
        msg2 = MagicMock()
        msg2.topic = "topic-b"
        msg2.value = {"id": "b1"}

        mock_consumer = MagicMock()
        mock_consumer.__iter__.return_value = [msg1, msg2]
        mock_kafka_consumer.return_value = mock_consumer

        handler_a = Mock()
        handler_b = Mock()

        self.client.consume_multiple([
            ("topic-a", "group", handler_a),
            ("topic-b", "group", handler_b),
        ])

        handler_a.assert_called_once_with({"id": "a1"})
        handler_b.assert_called_once_with({"id": "b1"})


class TestRedisClient:
    @patch("core.redis_client.sync_redis.Redis")
    def test_set_value(self, mock_redis):
        mock_instance = Mock()
        mock_redis.return_value = mock_instance

        client = RedisClient(host="localhost", port=6379, db=0)
        client.set("test-key", {"value": 42}, ttl=60)

        mock_instance.set.assert_called_once()
        args, kwargs = mock_instance.set.call_args
        assert args[0] == "test-key"
        assert "42" in args[1]

    @patch("core.redis_client.sync_redis.Redis")
    def test_get_value(self, mock_redis):
        mock_instance = Mock()
        mock_instance.get.return_value = '{"value": 42}'
        mock_redis.return_value = mock_instance

        client = RedisClient(host="localhost", port=6379, db=0)
        result = client.get("test-key")
        assert result == {"value": 42}

    @patch("core.redis_client.sync_redis.Redis")
    def test_get_nonexistent(self, mock_redis):
        mock_instance = Mock()
        mock_instance.get.return_value = None
        mock_redis.return_value = mock_instance

        client = RedisClient(host="localhost", port=6379, db=0)
        result = client.get("nonexistent")
        assert result is None

    @patch("core.redis_client.sync_redis.Redis")
    def test_exists(self, mock_redis):
        mock_instance = Mock()
        mock_instance.exists.return_value = 1
        mock_redis.return_value = mock_instance

        client = RedisClient(host="localhost", port=6379, db=0)
        assert client.exists("existing-key") is True

    @patch("core.redis_client.sync_redis.Redis")
    def test_incr(self, mock_redis):
        mock_instance = Mock()
        mock_instance.incr.return_value = 5
        mock_redis.return_value = mock_instance

        client = RedisClient(host="localhost", port=6379, db=0)
        result = client.incr("counter-key")
        assert result == 5

    @patch("core.redis_client.sync_redis.Redis")
    def test_delete(self, mock_redis):
        mock_instance = Mock()
        mock_redis.return_value = mock_instance
        client = RedisClient(host="localhost", port=6379, db=0)
        client.delete("test-key")
        mock_instance.delete.assert_called_once_with("test-key")

    @patch("core.redis_client.sync_redis.Redis")
    def test_expire(self, mock_redis):
        mock_instance = Mock()
        mock_redis.return_value = mock_instance
        client = RedisClient(host="localhost", port=6379, db=0)
        client.expire("test-key", 60)
        mock_instance.expire.assert_called_once_with("test-key", 60)

    @patch("core.redis_client.sync_redis.Redis")
    def test_close(self, mock_redis):
        mock_instance = Mock()
        mock_redis.return_value = mock_instance
        client = RedisClient(host="localhost", port=6379, db=0)
        client.close()
        mock_instance.close.assert_called_once()


class TestKafkaClientExtended:
    @patch("core.kafka_client.KafkaProducer")
    def test_flush(self, mock_kafka_producer):
        mock_producer = Mock()
        mock_kafka_producer.return_value = mock_producer
        client = KafkaClient(bootstrap_servers="localhost:9092")
        client._producer = mock_producer
        client.flush()
        mock_producer.flush.assert_called_once()

    @patch("core.kafka_client.KafkaProducer")
    def test_close_flushes_and_closes(self, mock_kafka_producer):
        mock_producer = Mock()
        mock_kafka_producer.return_value = mock_producer
        client = KafkaClient(bootstrap_servers="localhost:9092")
        client._producer = mock_producer
        client.close()
        mock_producer.flush.assert_called_once()
        mock_producer.close.assert_called_once()

    @patch("core.kafka_client.KafkaProducer")
    def test_close_noop_when_no_producer(self, mock_kafka_producer):
        client = KafkaClient(bootstrap_servers="localhost:9092")
        client.close()  # should not raise

    @patch("core.kafka_client.KafkaProducer")
    def test_consume_multiple_empty_topics(self, mock_kafka_producer):
        client = KafkaClient(bootstrap_servers="localhost:9092")
        # Should not raise
        with pytest.raises(Exception):
            # Will fail because consumer needs at least one topic
            client.consume_multiple([])
