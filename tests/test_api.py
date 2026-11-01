"""Tests for the Query API routes."""

from unittest.mock import Mock

from datetime import datetime, timezone
from fastapi.testclient import TestClient

from api.main import app
from models.database import get_db, EventRecord


client = TestClient(app)


def _make_mock_db(records):
    """Create a mock DB session that returns the given records."""
    mock_db = Mock()
    mock_query = Mock()
    mock_db.query.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.offset.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = records
    mock_query.first.return_value = records[0] if records else None
    return mock_db


class TestQueryAPI:
    def test_health_endpoint(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "query-api"

    def test_list_events(self):
        mock_db = _make_mock_db([
            EventRecord(
                id="event-1",
                event_type="click",
                source="web",
                timestamp=datetime.now(timezone.utc),
                data={"element": "button"},
                extra_metadata={},
            )
        ])
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            response = client.get("/events")
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 1
            assert data[0]["id"] == "event-1"
            assert data[0]["event_type"] == "click"
        finally:
            app.dependency_overrides.clear()

    def test_list_events_with_filters(self):
        mock_db = _make_mock_db([])
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            response = client.get("/events?event_type=transaction&source=web-app")
            assert response.status_code == 200
        finally:
            app.dependency_overrides.clear()

    def test_get_event_found(self):
        mock_db = _make_mock_db([
            EventRecord(
                id="event-1",
                event_type="pageview",
                source="web",
                timestamp=datetime.now(timezone.utc),
                data={"url": "/home"},
                extra_metadata={},
            )
        ])
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            response = client.get("/events/event-1")
            assert response.status_code == 200
            assert response.json()["id"] == "event-1"
        finally:
            app.dependency_overrides.clear()

    def test_get_event_not_found(self):
        mock_db = _make_mock_db([])
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            response = client.get("/events/nonexistent")
            assert response.status_code == 404
            assert response.json()["detail"] == "Event not found"
        finally:
            app.dependency_overrides.clear()

    def test_metrics_endpoint(self):
        response = client.get("/metrics")
        assert response.status_code == 200
        # Prometheus metrics include python_info by default
        body = response.text
        assert "# HELP" in body
        assert "# TYPE" in body


class TestQueryAPIWebSocket:
    def test_websocket_endpoint_exists(self):
        """Verify the WebSocket route is registered."""
        routes = [r.path for r in app.routes]
        assert "/ws" in routes
