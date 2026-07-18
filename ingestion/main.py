from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from prometheus_client import make_asgi_app, Counter, Histogram

from core.config import settings
from core.kafka_client import KafkaClient
from core.redis_client import RedisClient
from ingestion.schemas import IncomingEvent
from ingestion.validators import validate_event
from ingestion.producer import EventProducer

app = FastAPI(title="Streaming Data Processor - Ingestion Service")

kafka = KafkaClient()
redis = RedisClient()
producer = EventProducer(kafka, redis)

events_received = Counter("ingestion_events_received_total", "Total events received")
events_validated = Counter("ingestion_events_validated_total", "Events that passed validation")
events_rejected = Counter("ingestion_events_rejected_total", "Events rejected by validation")
event_processing_time = Histogram("ingestion_event_processing_seconds", "Time spent processing events")

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.post("/events")
@event_processing_time.time()
async def create_event(incoming: IncomingEvent):
    events_received.inc()

    errors = validate_event(incoming)
    if errors:
        events_rejected.inc()
        raise HTTPException(
            status_code=422,
            detail=[{"field": f, "message": m} for f, m in errors],
        )

    try:
        event = producer.publish(incoming)
        events_validated.inc()
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"status": "accepted", "event_id": event.id},
        )
    except ValueError as e:
        events_rejected.inc()
        raise HTTPException(status_code=409, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "ingestion"}
