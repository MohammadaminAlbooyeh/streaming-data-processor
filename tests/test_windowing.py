import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from core.windowing import WindowService
from models.event import Event
from models.aggregate import Aggregate


class TestWindowing:
    def setup_method(self):
        self.mock_redis = Mock()
        self.service = WindowService(self.mock_redis)

    def test_get_window_key(self):
        event = Event(event_type="transaction", source="app", timestamp=datetime(2024, 1, 15, 12, 0, 30))
        key = self.service.get_window_key(event, window_size_seconds=60)
        assert "transaction" in key
        assert "60" in key

    def test_get_window_start(self):
        ts = datetime(2024, 1, 15, 12, 0, 35)
        result = self.service._get_window_start(ts, 60)
        assert result.minute == 0
        assert result.second == 0

    def test_update_window_new(self):
        self.mock_redis.get.return_value = None
        event = Event(event_type="click", source="web", data={"value": 42})

        agg = self.service.update_window(event, value_field="value")

        assert agg.count == 1
        assert agg.sum == 42.0
        assert agg.avg == 42.0
        assert agg.min == 42.0
        assert agg.max == 42.0

    def test_update_window_existing(self):
        existing_agg = Aggregate(
            window_key="click:60:12345",
            window_start=datetime(2024, 1, 1, 0, 0, 0),
            window_end=datetime(2024, 1, 1, 0, 1, 0),
            event_type="click",
            count=1, sum=10.0, avg=10.0, min=10.0, max=10.0,
        )
        self.mock_redis.get.return_value = existing_agg.model_dump()

        event = Event(event_type="click", source="web", data={"value": 20})

        agg = self.service.update_window(event, value_field="value")

        assert agg.count == 2
        assert agg.sum == 30.0
        assert agg.avg == 15.0
        assert agg.min == 10.0
        assert agg.max == 20.0

    def test_update_window_non_numeric_value(self):
        self.mock_redis.get.return_value = None
        event = Event(event_type="click", source="web", data={"value": "string"})

        agg = self.service.update_window(event, value_field="value")

        assert agg.count == 1
        assert agg.sum == 0.0

    def test_get_window_returns_aggregate(self):
        data = {
            "id": "agg-1",
            "window_key": "click:60:1705315200",
            "window_start": "2024-01-15T12:00:00",
            "window_end": "2024-01-15T12:01:00",
            "event_type": "click",
            "count": 5,
            "sum": 10.0,
            "avg": 2.0,
            "min": 1.0,
            "max": 3.0,
            "last_updated": "2024-01-15T12:00:30",
        }
        self.mock_redis.get.return_value = data

        agg = self.service.get_window("click:60:1705315200")
        assert agg is not None
        assert agg.count == 5
        assert agg.sum == 10.0

    def test_get_window_returns_none_when_missing(self):
        self.mock_redis.get.return_value = None
        agg = self.service.get_window("nonexistent:60:0")
        assert agg is None
