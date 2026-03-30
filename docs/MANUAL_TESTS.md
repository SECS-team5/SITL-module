# Ручные тесты для Kafka и MQTT

Этот документ проверяет актуальный pipeline:

- `verifier` принимает raw-сообщения и публикует verified-сообщения
- `controller` обновляет состояние дрона в Redis
- `core` двигает дрон в Redis с частотой 10 Гц
- `messaging` отвечает текущей позицией в `reply_to`

Во всех сценариях используются текущие topic names:

- `sitl.commands`
- `sitl-drone-home`
- `sitl.verified-commands`
- `sitl.verified-home`
- `sitl.telemetry.request`
- `sitl.telemetry.response`

## Общая подготовка

Очистить стек:

```bash
docker compose down --volumes --remove-orphans
```

Проверить состояние контейнеров:

```bash
docker compose ps -a
```

Открыть логи приложений:

```bash
docker compose logs -f verifier controller core messaging
```

## Kafka-профиль

### Запуск

```bash
make up-kafka
```

Ожидаемый результат:

- `sitl-zookeeper` в статусе `Up (healthy)`
- `sitl-kafka` в статусе `Up (healthy)`
- `sitl-redis` в статусе `Up (healthy)`
- `sitl-verifier`, `sitl-controller`, `sitl-core`, `sitl-messaging` в статусе `Up`

### Тест 1. HOME проходит через verifier и попадает в Redis

Терминал 1:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic sitl.verified-home --consumer-property auto.offset.reset=latest
```

Терминал 2:

```bash
docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic sitl-drone-home
```

Отправить:

```json
{"drone_id":"drone_901","home_lat":59.9386,"home_lon":30.3141,"home_alt":100.0}
```

Проверка:

```bash
docker compose exec redis redis-cli HGETALL drone:drone_901:state
```

Ожидаемый результат:

- в `sitl.verified-home` появляется тот же JSON
- в Redis есть `status = ARMED`
- в Redis присутствуют `lat`, `lon`, `alt`, `home_lat`, `home_lon`, `home_alt`
- в логах `controller` есть `Stored HOME for drone_id=drone_901`

### Тест 2. Невалидный HOME отбрасывается

```bash
docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic sitl-drone-home
```

Отправить:

```json
{"drone_id":"drone_902","home_lat":59.9386,"home_lon":30.3141}
```

Проверка:

```bash
docker compose exec redis redis-cli HGETALL drone:drone_902:state
```

Ожидаемый результат:

- в Redis для `drone_902` пусто
- в логах `verifier` есть reject по schema validation

### Тест 3. COMMAND запускает движение

Терминал 1:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic sitl.verified-commands --consumer-property auto.offset.reset=latest
```

Терминал 2:

```bash
docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic sitl.commands
```

Отправить:

```json
{"drone_id":"drone_901","vx":3.0,"vy":1.0,"vz":0.0,"mag_heading":90.0}
```

Проверка:

```bash
docker compose exec redis redis-cli HGETALL drone:drone_901:state
```

Через 1-2 секунды повторить команду `HGETALL`.

Ожидаемый результат:

- в `sitl.verified-commands` появляется JSON команды
- в Redis `status = MOVING`
- в Redis `vx = 3.0`, `vy = 1.0`, `vz = 0.0`, `mag_heading = 90.0`
- значения `lat` и `lon` меняются между двумя чтениями

### Тест 4. COMMAND без HOME игнорируется

```bash
docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic sitl.commands
```

Отправить:

```json
{"drone_id":"drone_903","vx":2.0,"vy":0.0,"vz":0.0,"mag_heading":45.0}
```

Проверка:

```bash
docker compose exec redis redis-cli HGETALL drone:drone_903:state
```

Ожидаемый результат:

- в Redis для `drone_903` пусто
- в логах `controller` есть `Ignored COMMAND for drone_id=drone_903: HOME state is missing`

### Тест 5. messaging отвечает в reply_to

Создать демонстрационный reply topic:

```bash
docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic sitl.telemetry.response.demo1 --partitions 1 --replication-factor 1
```

Терминал 1:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic sitl.telemetry.response.demo1 --consumer-property auto.offset.reset=latest --property print.headers=true
```

Терминал 2:

```bash
docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic sitl.telemetry.request
```

Отправить:

```json
{"drone_id":"drone_901","correlation_id":"demo-req-901","reply_to":"sitl.telemetry.response.demo1"}
```

Ожидаемый результат:

- в `sitl.telemetry.response.demo1` приходит JSON с `lat`, `lon`, `alt`
- в payload есть `correlation_id`
- в headers ответа есть `correlation_id`
- в логах `messaging` есть `Returned position for drone_id=drone_901`

## MQTT-профиль

### Запуск

```bash
make up-mqtt
```

Ожидаемый результат:

- `sitl-mosquitto` в статусе `Up`
- `sitl-redis` в статусе `Up (healthy)`
- `sitl-verifier`, `sitl-controller`, `sitl-core`, `sitl-messaging` в статусе `Up`

### Тест 1. HOME проходит через verifier и попадает в Redis

Терминал 1:

```bash
docker compose exec mosquitto mosquitto_sub -h localhost -p 1883 -t sitl.verified-home
```

Терминал 2:

```bash
docker compose exec mosquitto mosquitto_pub -h localhost -p 1883 -t sitl-drone-home -m '{"drone_id":"drone_911","home_lat":59.9386,"home_lon":30.3141,"home_alt":100.0}'
```

Проверка:

```bash
docker compose exec redis redis-cli HGETALL drone:drone_911:state
```

Ожидаемый результат:

- в `sitl.verified-home` приходит тот же JSON
- в Redis есть `status = ARMED`

### Тест 2. COMMAND запускает движение

Терминал 1:

```bash
docker compose exec mosquitto mosquitto_sub -h localhost -p 1883 -t sitl.verified-commands
```

Терминал 2:

```bash
docker compose exec mosquitto mosquitto_pub -h localhost -p 1883 -t sitl.commands -m '{"drone_id":"drone_911","vx":3.0,"vy":1.0,"vz":0.0,"mag_heading":90.0}'
```

Проверка:

```bash
docker compose exec redis redis-cli HGETALL drone:drone_911:state
```

Через 1-2 секунды повторить `HGETALL`.

Ожидаемый результат:

- в `sitl.verified-commands` приходит JSON команды
- в Redis `status = MOVING`
- `lat` и `lon` меняются между чтениями

### Тест 3. messaging отвечает в MQTT reply_to

Терминал 1:

```bash
docker compose exec mosquitto mosquitto_sub -h localhost -p 1883 -t sitl.telemetry.response.demo2
```

Терминал 2:

```bash
docker compose exec mosquitto mosquitto_pub -h localhost -p 1883 -t sitl.telemetry.request -m '{"drone_id":"drone_911","correlation_id":"demo-req-911","reply_to":"sitl.telemetry.response.demo2"}'
```

Ожидаемый результат:

- в `sitl.telemetry.response.demo2` приходит JSON с `lat`, `lon`, `alt`
- в payload ответа есть `correlation_id = "demo-req-911"`
- в логах `messaging` есть `Returned position for drone_id=drone_911`

### Тест 4. messaging не отвечает для неизвестного дрона

Терминал 1:

```bash
docker compose exec mosquitto mosquitto_sub -h localhost -p 1883 -t sitl.telemetry.response.demo3 -W 5
```

Терминал 2:

```bash
docker compose exec mosquitto mosquitto_pub -h localhost -p 1883 -t sitl.telemetry.request -m '{"drone_id":"drone_999","correlation_id":"demo-req-999","reply_to":"sitl.telemetry.response.demo3"}'
```

Ожидаемый результат:

- в `sitl.telemetry.response.demo3` ответ не приходит
- в логах `messaging` есть reject с текстом `state not found`
