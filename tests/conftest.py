"""Shared test fixtures and configuration."""

from unittest.mock import Mock, patch

import pytest
from datetime import datetime, timezone

from models.event import Event
from models.aggregate import Aggregate


@pytest.fixture
def sample_event_data() -> dict:
    return {
        "id": "test-event-1",
        "event_type": "transaction",
        "source": "web-app",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {"amount": 150.0, "currency": "USD"},
        "metadata": {"ip": "192.168.1.1"},
    }


@pytest.fixture
def sample_fraud_alert_data() -> dict:
    return {
        "id": "fraud-test-1",
        "event_id": "test-event-1",
        "rule": "high_value_transaction",
        "severity": "medium",
        "details": {"amount": 15000.0},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


@pytest.fixture
def mock_redis():
    return Mock()


@pytest.fixture
def mock_kafka():
    return Mock()
