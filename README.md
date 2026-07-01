# Streaming Data Processor

Real-time streaming data processing pipeline built with Kafka, Redis, FastAPI, and Prometheus/Grafana monitoring.

## Architecture

```
Client → Ingestion API → Kafka → Consumers → Query API
                                     ├── Aggregation (windowed metrics)
                                     ├── Enrichment (geo, device, browser)
                                     ├── Fraud Detection (velocity, thresholds)
                                     └── Storage (SQLite persistence)
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Ingestion API | 8000 | Accepts and validates events, produces to Kafka |
| Query API | 8001 | REST API + WebSocket for querying events and aggregates |
| Aggregation Consumer | — | Computes tumbling window aggregations |
| Enrichment Consumer | — | Enriches events with simulated external data |
| Fraud Consumer | — | Detects fraud patterns (high value, rapid fire, unusual activity) |
| Storage Consumer | — | Persists events to SQLite |
| Kafka | 9092 | Message broker |
| Redis | 6379 | State store for dedup, windowing, fraud counters |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Dashboards (admin/admin) |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Start infrastructure
docker compose up -d kafka redis

# Start services
uvicorn ingestion.main:app --port 8000 &
uvicorn api.main:app --port 8001 &
python -m consumers.aggregation_consumer &
python -m consumers.enrichment_consumer &
python -m consumers.fraud_consumer &
python -m consumers.storage_consumer &

# Simulate traffic
python -m examples.simulate_traffic --events 500 --rate 20
```

## API Endpoints

### Ingestion
- `POST /events` — Submit an event
- `GET /health` — Health check
- `GET /metrics` — Prometheus metrics

### Query API
- `GET /events` — List events (filter by `event_type`, `source`)
- `GET /events/{id}` — Get event by ID
- `GET /aggregates` — List aggregates (filter by `event_type`)
- `GET /fraud-alerts` — List fraud alerts (filter by `severity`, `rule`)
- `WS /ws` — Real-time event stream
- `GET /health` — Health check
- `GET /metrics` — Prometheus metrics

## Event Schema

```json
{
  "event_type": "transaction",
  "source": "web-app",
  "data": { "amount": 99.99, "currency": "USD" }
}
```

Valid event types: `transaction`, `click`, `pageview`, `login`, `logout`, `purchase`, `refund`

## Testing

```bash
pytest -v
```

## Docker Compose (full stack)

```bash
docker compose up --build
```
