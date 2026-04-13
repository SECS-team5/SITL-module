# ── Читаем BROKER_BACKEND из .env ────────────────────────────────
-include .env
export BROKER_BACKEND ?= mqtt

# Пакеты для тестов в зависимости от брокера
MQTT_DEPS = paho-mqtt
KAFKA_DEPS = kafka-python

.PHONY: up-kafka up-mqtt up-sitl up down logs \
        unit-test integration-test integration-test-mqtt integration-test-kafka \
        wait-mqtt wait-kafka clean init

# ── Инициализация ────────────────────────────────────────────────

init:
	@echo "=== Downloading dependencies to pip-cache/ ==="
	@if not exist pip-cache mkdir pip-cache
	pip download --timeout 60 --retries 3 -i https://pypi.tuna.tsinghua.edu.cn/simple -d pip-cache -r requirements.txt
	@echo "=== Dependencies downloaded ==="

# ── Инфраструктура ───────────────────────────────────────────────

up-kafka:
	@echo "Starting Zookeeper and Kafka..."
	docker compose up -d zookeeper kafka
	@echo "Waiting for Kafka to be ready..."
	@docker compose exec -T kafka bash -c 'for i in $$(seq 1 30); do kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1 && exit 0; sleep 2; done; exit 1'

up-mqtt:
	@echo "Starting Mosquitto MQTT broker..."
	docker compose up -d mosquitto

up-redis:
	@echo "Starting Redis..."
	docker compose up -d redis

up-sitl: up-redis
	@echo "Building and starting SITL components (BROKER_BACKEND=$(BROKER_BACKEND))..."
	docker compose up -d --build sitl_verifier sitl_controller sitl_core sitl_messaging

up: up-sitl
	@echo "SITL components started. BROKER_BACKEND=$(BROKER_BACKEND)"

down:
	docker compose down

logs:
	docker compose logs -f

# ── Тесты ────────────────────────────────────────────────────────

unit-test:
	@echo "=== Running unit tests ==="
	docker compose run --rm --no-deps --entrypoint "" sitl_verifier sh -c \
		"pip install -q kafka-python aiohttp pytest pytest-asyncio paho-mqtt && python -m pytest tests/unit/ -v"

integration-test: integration-test-mqtt

integration-test-mqtt: up-mqtt up-redis up-sitl
	@echo ""
	@echo "=== Integration tests (MQTT) ==="
	@echo "Waiting for components to be ready..."
	@timeout /t 15 || ping -n 15 127.0.0.1 >nul
	@docker compose run --rm --no-deps \
		-e BROKER_BACKEND=mqtt \
		-e MQTT_BROKER=mosquitto \
		-e MQTT_PORT=1883 \
		-e REDIS_URL=redis://redis:6379 \
		--entrypoint "" sitl_verifier sh -c \
		"pip install --timeout 60 --retries 3 -i https://pypi.tuna.tsinghua.edu.cn/simple -q -r requirements.txt 2>/dev/null; python tests/integration/test_full_lifecycle.py"
	@docker compose run --rm --no-deps \
		-e BROKER_BACKEND=mqtt \
		-e MQTT_BROKER=mosquitto \
		-e MQTT_PORT=1883 \
		-e REDIS_URL=redis://redis:6379 \
		--entrypoint "" sitl_verifier sh -c \
		"pip install --timeout 60 --retries 3 -i https://pypi.tuna.tsinghua.edu.cn/simple -q -r requirements.txt 2>/dev/null; python tests/integration/test_command_integration.py"
	@docker compose run --rm --no-deps \
		-e BROKER_BACKEND=mqtt \
		-e MQTT_BROKER=mosquitto \
		-e MQTT_PORT=1883 \
		-e REDIS_URL=redis://redis:6379 \
		--entrypoint "" sitl_verifier sh -c \
		"pip install --timeout 60 --retries 3 -i https://pypi.tuna.tsinghua.edu.cn/simple -q -r requirements.txt 2>/dev/null; python tests/integration/test_home_position_integration.py"
	@docker compose run --rm --no-deps \
		-e BROKER_BACKEND=mqtt \
		-e MQTT_BROKER=mosquitto \
		-e MQTT_PORT=1883 \
		-e REDIS_URL=redis://redis:6379 \
		--entrypoint "" sitl_verifier sh -c \
		"pip install --timeout 60 --retries 3 -i https://pypi.tuna.tsinghua.edu.cn/simple -q -r requirements.txt 2>/dev/null; python tests/integration/test_messaging_integration.py"
	@echo ""
	@echo "=== All integration tests (MQTT) passed ==="

integration-test-kafka: up-kafka up-redis up-sitl
	@echo ""
	@echo "=== Integration tests (Kafka) ==="
	@docker compose run --rm --no-deps \
		-e BROKER_BACKEND=kafka \
		-e KAFKA_SERVERS=kafka:29092 \
		-e REDIS_URL=redis://redis:6379 \
		--entrypoint "" sitl_verifier sh -c \
		"python -c 'import aiohttp, kafka' 2>/dev/null || pip install -q --retries 2 --timeout 10 kafka-python aiohttp 2>/dev/null; python tests/integration/test_full_lifecycle.py"
	@docker compose run --rm --no-deps \
		-e BROKER_BACKEND=kafka \
		-e KAFKA_SERVERS=kafka:29092 \
		-e REDIS_URL=redis://redis:6379 \
		--entrypoint "" sitl_verifier sh -c \
		"python -c 'import aiohttp, kafka' 2>/dev/null || pip install -q --retries 2 --timeout 10 kafka-python aiohttp 2>/dev/null; python tests/integration/test_command_integration.py"
	@docker compose run --rm --no-deps \
		-e BROKER_BACKEND=kafka \
		-e KAFKA_SERVERS=kafka:29092 \
		-e REDIS_URL=redis://redis:6379 \
		--entrypoint "" sitl_verifier sh -c \
		"python -c 'import aiohttp, kafka' 2>/dev/null || pip install -q --retries 2 --timeout 10 kafka-python aiohttp 2>/dev/null; python tests/integration/test_home_position_integration.py"
	@docker compose run --rm --no-deps \
		-e BROKER_BACKEND=kafka \
		-e KAFKA_SERVERS=kafka:29092 \
		-e REDIS_URL=redis://redis:6379 \
		--entrypoint "" sitl_verifier sh -c \
		"python -c 'import aiohttp, kafka' 2>/dev/null || pip install -q --retries 2 --timeout 10 kafka-python aiohttp 2>/dev/null; python tests/integration/test_messaging_integration.py"
	@echo ""
	@echo "=== All integration tests (Kafka) passed ==="

clean:
	docker compose down --volumes --remove-orphans
