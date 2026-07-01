from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket
from prometheus_client import make_asgi_app

from models.database import init_db
from api.routes import router
from api.websocket import websocket_endpoint


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


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
