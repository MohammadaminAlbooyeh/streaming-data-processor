import random
import logging

from core.config import settings
from core.kafka_client import KafkaClient
from core.dead_letter_handler import DeadLetterHandler
from consumers.base_consumer import BaseConsumer


ENRICHMENT_SOURCES = ["geo_ip", "user_agent_parser", "device_lookup"]
COUNTRIES = ["US", "GB", "DE", "FR", "JP", "AU", "CA", "BR", "IN"]
DEVICES = ["desktop", "mobile", "tablet", "unknown"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge", "Unknown"]


class EnrichmentConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(
            topic=settings.raw_events_topic,
            group_id="enrichment-consumer",
        )
        self._kafka = KafkaClient()
        self._dlq = DeadLetterHandler(self._kafka)

    def process(self, message: dict) -> None:
        try:
            enriched = dict(message)
            enriched["metadata"]["enriched"] = True
            enriched["metadata"]["enrichment_source"] = random.choice(ENRICHMENT_SOURCES)
            enriched["metadata"]["country"] = random.choice(COUNTRIES)
            enriched["metadata"]["device"] = random.choice(DEVICES)
            enriched["metadata"]["browser"] = random.choice(BROWSERS)

            self._kafka.produce(
                settings.enriched_events_topic,
                value=enriched,
                key=enriched.get("id"),
            )
            self.logger.info("event_enriched", event_id=message.get("id"))
        except Exception as e:
            self._dlq.send(message, str(e))
            self.logger.error("enrichment_failed", error=str(e), event_id=message.get("id"))


if __name__ == "__main__":
    consumer = EnrichmentConsumer()
    consumer.run()