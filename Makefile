.PHONY: install install-dev test test-cov run-ingestion run-api run-consumers lint clean

# Installation
install:
	pip install -r requirements.txt

install-dev: install
	pip install -r requirements-dev.txt

# Testing
test:
	python3 -m pytest tests/ -v

test-cov:
	python3 -m pytest tests/ -v --cov=core --cov=ingestion --cov=consumers --cov=api --cov=models --cov-report=term-missing

# Running services
run-ingestion:
	uvicorn ingestion.main:app --host 0.0.0.0 --port 8000 --reload

run-api:
	uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload

run-aggregation:
	python3 -m consumers.aggregation_consumer

run-enrichment:
	python3 -m consumers.enrichment_consumer

run-fraud:
	python3 -m consumers.fraud_consumer

run-storage:
	python3 -m consumers.storage_consumer

run-consumers:
	$(MAKE) run-aggregation &
	$(MAKE) run-enrichment &
	$(MAKE) run-fraud &
	$(MAKE) run-storage &
	wait

# Infrastructure
infra-up:
	docker compose up -d kafka redis

infra-down:
	docker compose down

# Linting
lint:
	ruff check .

# Cleanup
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -f streaming.db
