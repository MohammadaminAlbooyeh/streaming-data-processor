import json
import asyncio
from typing import Set

from fastapi import WebSocket, WebSocketDisconnect

from core.config import settings
from core.kafka_client import KafkaClient


class EventStreamManager:
    def __init__(self):
        self._connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self._connections.discard(websocket)

    async def broadcast(self, message: dict) -> None:
        dead = set()
        for ws in self._connections:
            try:
                await ws.send_json(message)
            except Exception:
                dead.add(ws)
        self._connections -= dead


manager = EventStreamManager()


async def kafka_event_forwarder():
    loop = asyncio.get_event_loop()
    kafka = KafkaClient()

    def handle_message(msg: dict):
        asyncio.run_coroutine_threadsafe(manager.broadcast(msg), loop)

    await loop.run_in_executor(
        None,
        lambda: kafka.consume(
            topic=settings.raw_events_topic,
            group_id="websocket-forwarder",
            handler=handle_message,
        ),
    )


async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
