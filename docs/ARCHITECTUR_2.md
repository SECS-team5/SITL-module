# Архитектура SITL-module

## 1. C4 Диаграммы

### 1.1 System Context (Уровень 1)



### 1.2 Container Diagram (Уровень 2)


## 2. Схемы данных (JSON Schema)

### 3.1 Команда управления (`sitl-commands.json`)



### 3.2 Домашняя позиция (`sitl-drone-home.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["drone_id", "home_lat", "home_lon", "home_alt"],
  "properties": {
    "drone_id": {
      "type": "string",
      "pattern": "^drone_[0-9]{3,4}$"
    },
    "home_lat": {
      "type": "number"
    },
    "home_lon": {
      "type": "number"
    },
    "home_alt": {
      "type": "number"
    }
  }
}
```

### 3.3 Запрос позиции (`sitl-position-request.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["drone_id"],
  "properties": {
    "drone_id": {
      "type": "string",
      "pattern": "^drone_[0-9]{3,4}$"
    },
    "correlation_id": {
      "type": "string",
      "minLength": 1
    },
    "reply_to": {
      "type": "string",
      "minLength": 1
    }
  }
}
```

### 3.4 Ответ с позицией (`sitl-position-response.json`)

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["lat", "lon", "alt"],
  "properties": {
    "lat": {
      "type": "number"
    },
    "lon": {
      "type": "number"
    },
    "alt": {
      "type": "number"
    },
    "correlation_id": {
      "type": "string",
      "minLength": 1
    }
  }
}
```

---

## 4. Структура данных в Redis

### Ключ: `drone:{drone_id}:state` (Hash)

| Поле | Тип | Откуда обновляется | Описание |
|------|-----|-------------------|----------|
| `status` | string | Controller | `IDLE` / `ARMED` / `MOVING` |
| `lat` | float | Core | Широта (градусы) |
| `lon` | float | Core | Долгота (градусы) |
| `alt` | float | Core | Высота (метры) |
| `vx` | float | Controller | Скорость по оси X (м/с) → восток-запад |
| `vy` | float | Controller | Скорость по оси Y (м/с) → север-юг |
| `vz` | float | Controller | Скорость по оси Z (м/с) → вверх-вниз |
| `mag_heading` | float | Controller | Магнитный курс (0-359.9°) |
| `speed_h_ms` | float | Controller | Горизонтальная скорость (кэш) |
| `speed_v_ms` | float | Controller | Вертикальная скорость (кэш) |
| `home_lat` | float | Controller | Домашняя широта |
| `home_lon` | float | Controller | Домашняя долгота |
| `home_alt` | float | Controller | Домашняя высота |
| `last_update` | string | Controller, Core | ISO timestamp последнего обновления |

### Дополнительные параметры

| Параметр | Значение | Описание |
|----------|----------|----------|
| TTL (Time To Live) | 7200 секунд | Автоматическое удаление состояния |
| Формат drone_id | `drone_XXX` или `drone_XXXX` | XXX/XXXX — цифры (3-4 символа) |


## 5. Потоки данных

### 5.1 Полный цикл команды управления


### 5.2 Запрос телеметрии (Request-Response)


## Топики брокера сообщений

| Топик (входной) | Назначение | Схема |
|-----------------|------------|-------|
| `sitl.commands` | Команды управления (скорости, курс) | `sitl-commands.json` |
| `sitl-drone-home` | Установка домашней позиции дрона | `sitl-drone-home.json` |
| `sitl.telemetry.request` | Запрос текущей позиции дрона | `sitl-position-request.json` |

| Топик (внутренний/выходной) | Назначение |
|-----------------------------|------------|
| `sitl.verified-commands` | Валидированные команды (Verifier → Controller) |
| `sitl.verified-home` | Валидированные HOME (Verifier → Controller) |
| `sitl.telemetry.response` | Ответ с координатами (Messaging → потребитель) |


**Примечание о системе координат:**
- `vx` → движение по долготе (восток-запад)
- `vy` → движение по широте (север-юг)
