# NewSITL - SITL-компонент для симуляции дронов

## Описание

Компонент для Software-In-The-Loop (SITL) симуляции в учебном проекте экономики дронов.

## Структура

```
new-SITL/
├── docs/           - Документация
├── src/            - Исходный код
├── resources/      - Ресурсы (конфигурации, данные)
├── tests/          - Тесты
│   ├── unit/       - Юнит-тесты
│   └── module/     - Модульные тесты
├── docker/         - Docker файлы
├── __main__.py     - Точка входа
├── Makefile        - Команды сборки
└── README.md       - Этот файл
```

## Запуск

### Standalone режим

```bash
python -m new_sitl
```

### С брокером сообщений

Через систему: скопировать компонент в `systems/my_system/src/new_sitl/` и добавить `topics.py`, `.env`, `__main__.py` с подключением к брокеру.

## Разработка

1. Наследоваться от `BaseComponent` в `src/new_sitl.py`
2. Реализовать `_register_handlers()` для обработки сообщений
3. Добавить тесты в `tests/unit/` и `tests/module/`
4. Собрать Docker образ: `make docker-build`
5. Запустить: `make docker-up`
6. Запустить тесты: `make unit-test`
