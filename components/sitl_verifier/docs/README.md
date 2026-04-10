# SITL Verifier

Компонент валидации команд перед отправкой.

## Описание

Адаптирован из `SITL-module/verifier.py`. Проверяет входящие команды по схемам и публикует верифицированные сообщения.

## Функционал

- Валидация команд по JSON-схемам
- Валидация HOME-сообщений
- Публикация верифицированных сообщений в отдельные топики

## Топики

- Input: `sitl.commands`, `sitl-drone-home`
- Output: `sitl.verified-commands`, `sitl.verified-home`

## Запуск

```bash
make docker-up
```

## Тесты

```bash
make unit-test
```
