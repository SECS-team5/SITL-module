.PHONY: up up-kafka up-mqtt down logs test clean

BROKER_BACKEND ?= kafka
COMPOSE_PROFILES ?= $(BROKER_BACKEND)

up:
	BROKER_BACKEND=$(BROKER_BACKEND) COMPOSE_PROFILES=$(COMPOSE_PROFILES) docker compose up -d --build

up-kafka:
	BROKER_BACKEND=kafka COMPOSE_PROFILES=kafka docker compose up -d --build

up-mqtt:
	BROKER_BACKEND=mqtt COMPOSE_PROFILES=mqtt docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

test:
	BROKER_BACKEND=$(BROKER_BACKEND) COMPOSE_PROFILES=$(COMPOSE_PROFILES) docker compose run --rm --no-deps verifier pytest -q tests

clean:
	docker compose down --volumes --remove-orphans
