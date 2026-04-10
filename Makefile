.PHONY: up down logs unit-test integration-test clean

up:
	@echo "Starting application components..."
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
