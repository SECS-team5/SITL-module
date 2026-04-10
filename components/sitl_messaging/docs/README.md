# SITL Messaging

Компонент для обработки запросов/ответов позиций дронов.

## Описание

Адаптирован из `SITL-module/messaging.py`. Использует `SystemBus` вместо собственного `broker.py`.

## Функционал

- Приём запросов на получение позиции дрона
- Чтение состояния из Redis
- Ответ с валидацией схемы

## Топики

- Request: `sitl.telemetry.request`
- Response: `sitl.telemetry.response`

## Запуск

```bash
make docker-up
```

## Тесты

```bash
make unit-test
```
