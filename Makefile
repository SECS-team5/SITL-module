include .env
export BROKER_BACKEND

.PHONY: up up-kafka up-mqtt down logs unit-test integration-test clean

up:
ifeq ($(BROKER_BACKEND),kafka)
	@echo "Starting with Kafka..."
	docker compose --profile kafka up -d --build
else ifeq ($(BROKER_BACKEND),mqtt)
	@echo "Starting with MQTT..."
	docker compose --profile mqtt up -d --build
else
	@echo "Error: BROKER_BACKEND must be 'kafka' or 'mqtt' (got '$(BROKER_BACKEND)')"
	@exit 1
endif

up-kafka:
	@echo "Starting with Kafka..."
	docker compose --profile kafka up -d --build

up-mqtt:
	@echo "Starting with MQTT..."
	docker compose --profile mqtt up -d --build

up-app:
	@echo "Starting only app components..."
	docker compose up -d --build sitl_verifier sitl_controller sitl_core sitl_messaging

down:
	docker compose down

logs:
	docker compose logs -f

unit-test:
	@echo "=== Running unit tests ==="
	docker compose run --rm --no-deps --entrypoint "" sitl_verifier sh -c "pip install -q kafka-python aiohttp pytest pytest-asyncio && python -m pytest tests/unit/ -v"

integration-test:
	@echo "=== Running integration tests ==="
	docker compose run --rm --no-deps --entrypoint "" sitl_verifier sh -c "pip install -q kafka-python aiohttp && python tests/integration/test_full_lifecycle.py"
	docker compose run --rm --no-deps --entrypoint "" sitl_controller sh -c "pip install -q kafka-python aiohttp && python tests/integration/test_home_position_integration.py"
	docker compose run --rm --no-deps --entrypoint "" sitl_messaging sh -c "pip install -q kafka-python aiohttp && python tests/integration/test_messaging_integration.py"

clean:
	docker compose down --volumes --remove-orphans
