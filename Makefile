.PHONY: help setup up down logs ps ui features stats bench dlq schemas compat test lint clean

help:
	@echo "up        build and start the full pipeline"
	@echo "down      stop the pipeline"
	@echo "logs      follow logs"
	@echo "ui        also start Kafka UI on :8080"
	@echo "features  show features for a user (U=u000001)"
	@echo "stats     streaming throughput + store size"
	@echo "bench     measure events/min and serving latency"
	@echo "dlq       peek at dead-lettered events"
	@echo "compat    run the schema contract gate against the live registry"
	@echo "test      run unit tests"
	@echo "clean     stop and delete all volumes"

setup:
	@test -f .env || cp .env.example .env

up: setup
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

ui:
	docker compose --profile ui up -d kafka-ui

U ?= u000001
features:
	curl -s http://localhost:8000/features/$(U) | python -m json.tool

stats:
	curl -s http://localhost:8000/stats | python -m json.tool

bench:
	docker compose exec -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 feature-api \
		python -m eddy.bench --seconds 60 --api http://localhost:8000

dlq:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic user-events-dlq --from-beginning --max-messages 5

compat:
	docker compose exec feature-api python -m eddy.compat_check --registry

test:
	pytest -q

lint:
	ruff check .

clean:
	docker compose down -v
