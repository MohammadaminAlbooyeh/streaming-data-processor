"""Tests for the WebSocket EventStreamManager."""

import pytest
from unittest.mock import Mock, AsyncMock, patch

from api.websocket import EventStreamManager


class TestEventStreamManagerSync:
    def setup_method(self):
        self.manager = EventStreamManager()

    def test_disconnect_removes_connection(self):
        ws = Mock()
        self.manager._connections.add(ws)
        self.manager.disconnect(ws)
        assert len(self.manager._connections) == 0

    def test_disconnect_silent_on_missing(self):
        self.manager.disconnect(Mock())


@pytest.mark.asyncio
class TestEventStreamManagerAsync:
    def setup_method(self):
        self.manager = EventStreamManager()

    async def test_connect_adds_connection(self):
        ws = AsyncMock()
        await self.manager.connect(ws)
        assert len(self.manager._connections) == 1
        ws.accept.assert_awaited_once()

    async def test_broadcast_sends_to_all(self):
        ws1 = AsyncMock()
        ws2 = AsyncMock()
        await self.manager.connect(ws1)
        await self.manager.connect(ws2)

        await self.manager.broadcast({"type": "test", "data": "hello"})

        ws1.send_json.assert_awaited_once_with(
            {"type": "test", "data": "hello"}
        )
        ws2.send_json.assert_awaited_once_with(
            {"type": "test", "data": "hello"}
        )

    async def test_broadcast_removes_dead_connections(self):
        ws1 = AsyncMock()
        ws2 = AsyncMock()
        ws2.send_json.side_effect = Exception("Connection closed")

        await self.manager.connect(ws1)
        await self.manager.connect(ws2)

        await self.manager.broadcast({"msg": "test"})

        assert ws1 in self.manager._connections
        assert ws2 not in self.manager._connections


@pytest.mark.asyncio
class TestWebSocketEndpoint:
    async def test_websocket_endpoint_connect_disconnect(self):
        from api.websocket import websocket_endpoint
        from fastapi import WebSocket, WebSocketDisconnect

        ws = AsyncMock(spec=WebSocket)
        ws.receive_text.side_effect = WebSocketDisconnect()

        await websocket_endpoint(ws)
        ws.accept.assert_awaited_once()


class TestKafkaEventForwarder:
    @patch("api.websocket.KafkaClient")
    def test_kafka_event_forwarder_not_imported_error(self, mock_kafka_cls):
        """Verifies kafka_event_forwarder is defined and importable."""
        from api.websocket import kafka_event_forwarder
        assert callable(kafka_event_forwarder)
