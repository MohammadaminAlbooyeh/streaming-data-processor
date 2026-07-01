import signal
import sys
from abc import ABC, abstractmethod
from typing import Optional

from core.config import settings
from core.kafka_client import KafkaClient


class BaseConsumer(ABC):
    def __init__(self, topic: str, group_id: str, bootstrap_servers: Optional[str] = None):
        self._kafka = KafkaClient(bootstrap_servers=bootstrap_servers)
        self._topic = topic
        self._group_id = group_id
        self._running = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    @abstractmethod
    def process(self, message: dict) -> None:
        ...

    def _signal_handler(self, signum, frame) -> None:
        self._running = False

    def run(self) -> None:
        self._running = True
        self._kafka.consume(
            topic=self._topic,
            group_id=self._group_id,
            handler=self._handle_with_errors,
        )

    def _handle_with_errors(self, message: dict) -> None:
        try:
            self.process(message)
        except Exception as e:
            print(f"Error processing message {message.get('id', 'unknown')}: {e}", file=sys.stderr)
