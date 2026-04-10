.PHONY: docker-build docker-up unit-test

docker-build:
	@docker build -t sitl_messaging -f docker/Dockerfile .

docker-up:
	@docker run --rm --name sitl_messaging -e COMPONENT_ID=sitl_messaging_standalone sitl_messaging

unit-test:
	@python -m pytest tests/ -v
