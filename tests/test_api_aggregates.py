"""Tests for Query API aggregates, fraud alerts, and ingestion API."""

from unittest.mock import Mock, patch

from datetime import datetime, timezone
from fastapi.testclient import TestClient

from api.main import app
from models.database import get_db, AggregateRecord, FraudAlertRecord


client = TestClient(app)


def _make_mock_db(records):
    mock_db = Mock()
    mock_query = Mock()
    mock_db.query.return_value = mock_query
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = records
    return mock_db


class TestQueryAPIAggregates:
    def test_list_aggregates(self):
        mock_db = _make_mock_db([
            AggregateRecord(
                id="agg-1",
                window_key="click:60:12345",
                window_start=datetime.now(timezone.utc),
                window_end=datetime.now(timezone.utc),
                event_type="click",
                count=5,
                sum=50.0,
                avg=10.0,
                min=5.0,
                max=15.0,
                last_updated=datetime.now(timezone.utc),
            )
        ])
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            response = client.get("/aggregates")
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 1
            assert data[0]["event_type"] == "click"
        finally:
            app.dependency_overrides.clear()

    def test_list_fraud_alerts(self):
        mock_db = _make_mock_db([
            FraudAlertRecord(
                id="alert-1",
                event_id="event-1",
                rule="high_value_transaction",
                severity="medium",
                details={"amount": 15000},
                created_at=datetime.now(timezone.utc),
            )
        ])
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            response = client.get("/fraud-alerts")
            assert response.status_code == 200
            data = response.json()
            assert len(data) == 1
            assert data[0]["rule"] == "high_value_transaction"
        finally:
            app.dependency_overrides.clear()

    def test_list_fraud_alerts_with_filters(self):
        mock_db = _make_mock_db([])
        app.dependency_overrides[get_db] = lambda: mock_db
        try:
            response = client.get(
                "/fraud-alerts?severity=high&rule=rapid_fire_events"
            )
            assert response.status_code == 200
        finally:
            app.dependency_overrides.clear()
