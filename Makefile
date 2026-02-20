.PHONY: up down logs test clean

up:
	docker-compose up -d --build

down:
	docker-compose down

logs:
	docker-compose logs -f

test:
	docker exec $$(docker-compose psq kafka | tail -1 | awk '{print $$1}') \
		kafka-console-producer --bootstrap-server localhost:9092 --topic input-messages \
		<<< '{"id": "test", "drone_id": "SITL-001", "type": "HEARTBEAT"}'

clean:
	docker-compose down -v
	docker system prune -f
