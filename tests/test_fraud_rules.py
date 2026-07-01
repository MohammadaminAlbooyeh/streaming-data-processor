import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone

from consumers.fraud_consumer import FraudConsumer, HIGH_VALUE_THRESHOLD
from models.event import Event


class TestFraudRules:
    def setup_method(self):
        self.consumer = FraudConsumer()
        self.consumer._redis = Mock()
        self.consumer._kafka = Mock()

    def test_high_value_transaction_triggers_alert(self):
        event = Event(
            event_type="transaction",
            source="web-app",
            data={"amount": HIGH_VALUE_THRESHOLD + 1},
        )
        alert = self.consumer._check_high_value(event)
        assert alert is not None
        assert alert["rule"] == "high_value_transaction"
        assert alert["severity"] == "medium"

    def test_low_value_transaction_no_alert(self):
        event = Event(
            event_type="transaction",
            source="web-app",
            data={"amount": 5.0},
        )
        alert = self.consumer._check_high_value(event)
        assert alert is None

    def test_rapid_fire_triggers_after_limit(self):
        event = Event(event_type="click", source="bot-user")
        self.consumer._redis.incr.side_effect = [10]
        self.consumer._redis.expire.return_value = None

        alert = self.consumer._check_rapid_fire(event)
        assert alert is not None
        assert alert["rule"] == "rapid_fire_events"

    def test_rapid_fire_below_limit(self):
        event = Event(event_type="click", source="normal-user")
        self.consumer._redis.incr.side_effect = [5]
        self.consumer._redis.expire.return_value = None

        alert = self.consumer._check_rapid_fire(event)
        assert alert is None

    def test_unusual_activity_triggers_after_limit(self):
        event = Event(event_type="login", source="suspicious-ip")
        self.consumer._redis.incr.side_effect = [50]
        self.consumer._redis.expire.return_value = None

        alert = self.consumer._check_unusual_activity(event)
        assert alert is not None
        assert alert["rule"] == "unusual_source_activity"

    def test_unusual_activity_below_limit(self):
        event = Event(event_type="login", source="normal-ip")
        self.consumer._redis.incr.side_effect = [10]
        self.consumer._redis.expire.return_value = None

        alert = self.consumer._check_unusual_activity(event)
        assert alert is None

    def test_process_calls_all_checks(self):
        message = {
            "id": "test-1",
            "event_type": "transaction",
            "source": "web",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": {"amount": HIGH_VALUE_THRESHOLD + 1},
            "metadata": {},
        }
        self.consumer._redis.incr.side_effect = [1, 1]
        self.consumer._redis.exists.return_value = False
        self.consumer._redis.expire.return_value = None

        self.consumer.process(message)

        # Should have produced at least one alert (high value)
        assert self.consumer._kafka.produce.called
