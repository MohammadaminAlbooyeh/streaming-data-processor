"""Tests for the Ingestion API endpoints."""

from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from ingestion.main import app


client = TestClient(app)


class TestIngestionAPI:
    @patch("ingestion.main.producer")
    def test_create_event_returns_202(self, mock_producer):
        mock_event = Mock()
        mock_event.id = "new-event-1"
        mock_producer.publish.return_value = mock_event

        response = client.post(
            "/events",
            json={
                "event_type": "transaction",
                "source": "web-app",
                "data": {"amount": 99.99},
            },
        )
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert data["event_id"] == "new-event-1"

    @patch("ingestion.main.producer")
    def test_create_event_duplicate_returns_409(self, mock_producer):
        mock_producer.publish.side_effect = ValueError("Duplicate event detected")

        response = client.post(
            "/events",
            json={"event_type": "click", "source": "web", "data": {}},
        )
        assert response.status_code == 409

    @patch("ingestion.main.producer")
    def test_create_event_kafka_failure_returns_503(self, mock_producer):
        mock_producer.publish.side_effect = RuntimeError("Kafka unreachable")

        response = client.post(
            "/events",
            json={"event_type": "click", "source": "web", "data": {}},
        )
        assert response.status_code == 503

    def test_create_event_invalid_type_returns_422(self):
        response = client.post(
            "/events",
            json={"event_type": "INVALID", "source": "web", "data": {}},
        )
        assert response.status_code == 422

    def test_create_event_missing_source_returns_422(self):
        response = client.post(
            "/events",
            json={"event_type": "click", "source": "", "data": {}},
        )
        assert response.status_code == 422

    def test_health_endpoint(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "ingestion"
