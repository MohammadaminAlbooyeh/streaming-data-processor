"""
Example: Run a single consumer manually.

Usage:
    python -m examples.consumer_example aggregation
    python -m examples.consumer_example enrichment
    python -m examples.consumer_example fraud
    python -m examples.consumer_example storage
"""

import sys

from consumers.aggregation_consumer import AggregationConsumer
from consumers.enrichment_consumer import EnrichmentConsumer
from consumers.fraud_consumer import FraudConsumer
from consumers.storage_consumer import StorageConsumer

CONSUMERS = {
    "aggregation": AggregationConsumer,
    "enrichment": EnrichmentConsumer,
    "fraud": FraudConsumer,
    "storage": StorageConsumer,
}


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python -m examples.consumer_example <consumer_type>")
        print(f"Available: {', '.join(CONSUMERS.keys())}")
        sys.exit(1)

    consumer_type = sys.argv[1]
    if consumer_type not in CONSUMERS:
        print(f"Unknown consumer: {consumer_type}")
        sys.exit(1)

    consumer = CONSUMERS[consumer_type]()
    print(f"Starting {consumer_type} consumer...")
    consumer.run()


if __name__ == "__main__":
    main()
