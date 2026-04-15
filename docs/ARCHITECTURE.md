# Архитектура New-SITL

## Кратко

SITL система для учебного проекта экономики дронов. Все компоненты работают через единый брокер сообщений (`SystemBus`) и наследуются от `BaseComponent`.

Система состоит из четырех компонентов:

- `sitl_verifier` — валидация входящих команд
- `sitl_controller` — обработка верифицированных команд
- `sitl_core` — обновление позиций дронов в Redis
- `sitl_messaging` — запросы/ответы позиций дронов

И двух инфраструктурных зависимостей:

- Redis для хранения текущего состояния дронов
- Брокер сообщений (Kafka или MQTT), выбирается через `BROKER_BACKEND=kafka|mqtt`

За один запуск активен ровно один брокер. Topic names одинаковы для Kafka и MQTT, меняется только транспорт.

## Архитектура

```
new-SITL/
├── broker/                          # Брокер сообщений (Kafka/MQTT)
│   ├── src/                         #   Исходный код SystemBus
│   ├── kafka/                       #   Реализация Kafka
│   ├── mqtt/                        #   Реализация MQTT
│   └── README.md
│
├── components/                      # Компоненты SITL-системы
│   ├── sitl_messaging/              #   Запросы/ответы позиций дронов
│   ├── sitl_core/                   #   Обновление позиций дронов в Redis
│   ├── sitl_controller/             #   Обработка верифицированных команд
│   └── sitl_verifier/               #   Валидация команд
│
├── shared/                          # Общие утилиты
│   ├── state.py                     #   Состояние дронов
│   ├── contracts.py                 #   Схемы и валидация
│   └── infopanel_client.py          #   Клиент инфопанели
│
├── schemas/                         # JSON-схемы для валидации
├── sdk/                             # SDK (BaseComponent, SystemBus)
├── new-SITL/                        # Компонент-шаблон
│
└── requirements.txt                 # Зависимости проекта
```

## Поток данных

### 1. Входные сообщения

В систему поступают два типа входных сообщений:

- `sitl.commands`
- `sitl-drone-home`

`sitl_verifier` читает эти сообщения из выбранного брокера, валидирует JSON по схемам из `schemas/` и публикует результат в:

- `sitl.verified-commands`
- `sitl.verified-home`

### 2. Состояние дрона

`sitl_controller` читает verified-topic'и из выбранного брокера и хранит состояние в Redis по ключу:

```text
drone:{drone_id}:state
```

Поведение:

- HOME создает или обновляет базовое состояние дрона
- COMMAND без HOME игнорируется
- COMMAND с ненулевой скоростью переводит дрон в `MOVING`
- COMMAND с нулевой скоростью возвращает дрон в `ARMED`

### 3. Обновление позиции

`sitl_core` не зависит от брокера. Он работает только с Redis:

- ищет ключи `drone:*:state`
- двигает только дроны со статусом `MOVING`
- обновляет `lat`, `lon`, `alt`
- продлевает TTL состояния

Расчет позиции упрощенный и работает по векторам `vx`, `vy`, `vz`.

### 4. Запрос позиции

`sitl_messaging` принимает request-сообщения из `sitl.telemetry.request` и отвечает в `reply_to`.

Запрос:

- обязательно содержит `drone_id`
- может содержать `reply_to`
- может содержать `correlation_id`

Ответ:

- содержит `lat`, `lon`, `alt`
- может содержать `correlation_id`

Особенность транспорта:

- в Kafka `correlation_id` дублируется и в payload, и в headers
- в MQTT transport metadata передается только в payload

## Выбор брокера

### Kafka

Для Kafka используются:

- `zookeeper`
- `kafka`

Основные env:

- `BROKER_BACKEND=kafka`
- `KAFKA_SERVERS=kafka:29092`

### MQTT

Для MQTT используется:

- `mosquitto`

Основные env:

- `BROKER_BACKEND=mqtt`
- `MQTT_HOST=mosquitto`
- `MQTT_PORT=1883`
- `MQTT_USERNAME`
- `MQTT_PASSWORD`
- `MQTT_QOS=1`

## Компоненты

### sitl_verifier

Ответственность:

- принять raw message
- определить тип сообщения по topic
- провалидировать payload по JSON Schema
- перепубликовать payload в verified-topic

### sitl_controller

Ответственность:

- принять validated message
- создать HOME state
- применить COMMAND к существующему state
- сохранить state в Redis

### sitl_core

Ответственность:

- периодически проходить по Redis state
- пересчитывать позицию движущихся дронов
- обновлять время последнего изменения

### sitl_messaging

Ответственность:

- обработать запрос позиции
- достать состояние из Redis
- сформировать ответ
- отправить ответ в `reply_to` или в `POSITION_RESPONSE_TOPIC`

## Конфигурация

Общие topic env:

- `COMMAND_TOPIC`
- `HOME_TOPIC`
- `VERIFIED_COMMAND_TOPIC`
- `VERIFIED_HOME_TOPIC`
- `POSITION_REQUEST_TOPIC`
- `POSITION_RESPONSE_TOPIC`

Общие runtime env:

- `REDIS_URL`
- `STATE_TTL_SEC`
- `UPDATE_FREQUENCY_HZ`

## Docker Compose

Compose использует профили:

- `COMPOSE_PROFILES=kafka` поднимает Kafka-стек
- `COMPOSE_PROFILES=mqtt` поднимает Mosquitto

Примеры запуска:

```bash
make up-kafka
make up-mqtt
```

Сами приложения не завязаны на `depends_on` брокера. Вместо этого они повторяют попытки подключения с backoff, пока выбранный брокер не станет доступен.

## Ключевые изменения относительно SITL-module

| SITL-module | New-SITL |
|---|---|
| Собственный `broker.py` | `SystemBus` из `broker/` |
| Прямые `asyncio.run(main())` | Компоненты на `BaseComponent` |
| Retry-логика в каждом сервисе | Обеспечивается `SystemBus` |
| Один `src/` на всё | Отдельные компоненты с полной структурой |

## Создание нового компонента

1. Скопировать любой компонент из `components/`
2. Переименовать и адаптировать `src/*.py`
3. Наследоваться от `BaseComponent`, реализовать `_register_handlers()`
4. Обновить `__main__.py`, `docker/Dockerfile`, `Makefile`
