# SITL Controller

Компонент обработки верифицированных команд дронов.

## Описание

Адаптирован из `SITL-module/controller.py`. Принимает верифицированные команды и обновляет состояние дронов в Redis.

## Функционал

- Обработка команд движения (`vx`, `vy`, `vz`)
- Обработка HOME-сообщений
- Сохранение состояния в Redis с TTL

## Топики

- Commands: `sitl.verified-commands`
- Home: `sitl.verified-home`

## Запуск

```bash
make docker-up
```

## Тесты

```bash
make unit-test
```
