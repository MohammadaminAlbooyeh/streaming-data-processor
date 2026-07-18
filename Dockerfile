FROM python:3.11-slim

LABEL maintainer="Streaming Data Processor Team"
LABEL description="Streaming data processing pipeline service"

WORKDIR /app

# Install system dependencies for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl --fail http://localhost:8000/health || exit 1

CMD ["uvicorn", "ingestion.main:app", "--host", "0.0.0.0", "--port", "8000"]
