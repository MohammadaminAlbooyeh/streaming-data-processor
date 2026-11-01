import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from prometheus_client import make_asgi_app

from models.database import init_db
from api.routes import router
from api.websocket import websocket_endpoint, kafka_event_forwarder


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    forwarder_task = asyncio.create_task(kafka_event_forwarder())
    yield
    forwarder_task.cancel()
    try:
        await forwarder_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Streaming Data Processor - Query API", lifespan=lifespan)
app.include_router(router)

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.websocket("/ws")
async def ws(websocket: WebSocket):
    await websocket_endpoint(websocket)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "query-api"}
