# New-SITL — SITL-система для экономики дронов

## Описание

SITL (Software-In-The-Loop) система для учебного проекта экономики дронов. Все компоненты работают через единый брокер сообщений (`SystemBus`) и наследуются от `BaseComponent`.

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

## Компоненты

### sitl_messaging
Обработка запросов позиций дронов. Читает состояние из Redis и возвращает координаты.

```bash
cd components/sitl_messaging
make docker-up
```

### sitl_core
Периодическое обновление позиций движущихся дронов в Redis.

```bash
cd components/sitl_core
make docker-up
```

### sitl_controller
Приём верифицированных команд и HOME-сообщений, сохранение состояния в Redis.

```bash
cd components/sitl_controller
make docker-up
```

### sitl_verifier
Валидация входящих команд по JSON-схемам, публикация верифицированных сообщений.

```bash
cd components/sitl_verifier
make docker-up
```

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

## Тесты

```bash
# Запустить тесты одного компонента
cd components/sitl_messaging && make unit-test

# Запустить все тесты
python -m pytest components/*/tests/ -v
```

## Зависимости

```bash
pip install -r requirements.txt
```
