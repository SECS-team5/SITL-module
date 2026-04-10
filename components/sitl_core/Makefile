.PHONY: docker-build docker-up unit-test

docker-build:
	@docker build -t sitl_core -f docker/Dockerfile .

docker-up:
	@docker run --rm --name sitl_core -e COMPONENT_ID=sitl_core_standalone sitl_core

unit-test:
	@python -m pytest tests/ -v
