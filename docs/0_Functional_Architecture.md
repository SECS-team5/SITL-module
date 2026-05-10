# Функциональная архитектура системы

## Описание системы

SITL-система для управления дронами. Все компоненты работают через единый брокер сообщений (SystemBus) и наследуются от BaseComponent.

Система состоит из четырёх компонентов:
- sitl_verifier — валидация входящих команд
- sitl_controller — обработка верифицированных команд
- sitl_core — обновление позиций дронов в Redis
- sitl_messaging — запросы и ответы позиций дронов

И двух инфраструктурных зависимостей:
- Redis для хранения состояния дронов
- Брокер сообщений (Kafka или MQTT)

---

## Функциональная архитектура

### Поток данных

Входные сообщения поступают в систему через два топика:
- sitl.commands — команды дронов
- sitl-drone-home — домашние позиции

sitl_verifier читает эти сообщения, валидирует JSON по схемам и публикует результат в:
- sitl.verified-commands
- sitl.verified-home

sitl_controller читает verified-топики и хранит состояние в Redis по ключу drone:{id}:state.

sitl_core работает только с Redis, ищет ключи drone:*:state и обновляет позиции движущихся дронов (статус MOVING) с частотой 10Hz.

sitl_messaging принимает запросы из sitl.telemetry.request и отвечает в reply_to или sitl.telemetry.response.

---

## Архитектура безопасности

### Уровни системы

L1 — Внешний периметр: Message Broker
L2 — Валидация: sitl_verifier
L3 — Обработка: sitl_controller
L4 — Хранилище: Redis
L5 — Потребители: sitl_core, sitl_messaging

### Границы доверия

Внешние системы и пилоты отправляют сообщения в брокер без доверия.

VERIFIER получает сообщения из брокера и выполняет валидацию — высокий уровень доверия.

CONTROLLER получает верифицированные сообщения и работает с Redis — высокий уровень доверия.

Redis хранит состояние дронов — критический уровень доверия.

CORE и MESSAGING работают с Redis — операционный уровень доверия.

---

## Компоненты

### sitl_verifier

Входные топики: sitl.commands, sitl-drone-home
Выходные топики: sitl.verified-commands, sitl.verified-home

Принимает raw-сообщения, определяет тип по топику, валидирует payload по JSON Schema и перепубликует в verified-топики.

### sitl_controller

Входные топики: sitl.verified-commands, sitl.verified-home
Выход: Redis state

Принимает validated-сообщения, создаёт или обновляет HOME state, применяет COMMAND к существующему state.

### sitl_core

Работает с Redis напрямую, не зависит от брокера.
Ищет ключи drone:*:state, двигает только дроны со статусом MOVING, обновляет lat, lon, alt, продлевает TTL.

### sitl_messaging

Входной топик: sitl.telemetry.request
Выходной топик: sitl.telemetry.response

Принимает запрос позиции, достаёт состояние из Redis, формирует ответ с координатами.