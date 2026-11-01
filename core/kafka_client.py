import json
import time
from typing import Callable, Optional
from kafka import KafkaProducer, KafkaConsumer, errors

from core.config import settings


class KafkaClient:
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self._producer: Optional[KafkaProducer] = None
        self._last_flush = time.time()
        self._flush_interval = 0.1  # flush every 100ms

    @property
    def producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=10,  # batch messages for 10ms
                batch_size=16384,
                buffer_memory=33554432,
            )
        return self._producer

    def produce(self, topic: str, value: dict, key: Optional[str] = None) -> None:
        try:
            self.producer.send(topic, value=value, key=key)
            # Periodic flush instead of per-message flush
            now = time.time()
            if now - self._last_flush > self._flush_interval:
                self.producer.flush()
                self._last_flush = now
        except errors.KafkaError as e:
            raise RuntimeError(f"Failed to produce message to {topic}: {e}") from e

    def flush(self) -> None:
        if self._producer is not None:
            self.producer.flush()
            self._last_flush = time.time()

    def consume(
        self,
        topic: str,
        group_id: str,
        handler: Callable[[dict], None],
        timeout_ms: int = 1000,
    ) -> None:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        try:
            for message in consumer:
                handler(message.value)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    def consume_multiple(
        self,
        topic_handlers: list[tuple[str, str, Callable[[dict], None]]],
        timeout_ms: int = 1000,
    ) -> None:
        """Consume from multiple topics, routing each message to the appropriate handler.

        Args:
            topic_handlers: List of (topic, group_id, handler) tuples.
            timeout_ms: Consumer poll timeout in milliseconds.
        """
        topics = [th[0] for th in topic_handlers]
        group_id = topic_handlers[0][1] if topic_handlers else "multi-consumer"

        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        handler_map = {th[0]: th[2] for th in topic_handlers}

        try:
            for message in consumer:
                handler = handler_map.get(message.topic)
                if handler:
                    handler(message.value)
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()

