import json
from typing import Callable, Optional
from kafka import KafkaProducer, KafkaConsumer, errors

from core.config import settings


class KafkaClient:
    def __init__(self, bootstrap_servers: Optional[str] = None):
        self.bootstrap_servers = bootstrap_servers or settings.kafka_bootstrap_servers
        self._producer: Optional[KafkaProducer] = None

    @property
    def producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
        return self._producer

    def produce(self, topic: str, value: dict, key: Optional[str] = None) -> None:
        try:
            self.producer.send(topic, value=value, key=key)
            self.producer.flush()
        except errors.KafkaError as e:
            raise RuntimeError(f"Failed to produce message to {topic}: {e}") from e

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

    def close(self) -> None:
        if self._producer is not None:
            self._producer.close()
