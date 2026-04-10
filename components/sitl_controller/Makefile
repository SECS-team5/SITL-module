.PHONY: docker-build docker-up unit-test

docker-build:
	@docker build -t sitl_controller -f docker/Dockerfile .

docker-up:
	@docker run --rm --name sitl_controller -e COMPONENT_ID=sitl_controller_standalone sitl_controller

unit-test:
	@python -m pytest tests/ -v
