.PHONY: docker-build docker-up unit-test

docker-build:
	@docker build -t sitl_verifier -f docker/Dockerfile .

docker-up:
	@docker run --rm --name sitl_verifier -e COMPONENT_ID=sitl_verifier_standalone sitl_verifier

unit-test:
	@python -m pytest tests/ -v
