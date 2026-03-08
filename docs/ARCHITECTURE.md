# Алгоритм работы SITL-адаптера

## 1. Схема жизненного цикла данных (Data Flow)
Дрон в SITL-адаптере проходит три стадии состояния:
IDLE (Ожидание): Дрон известен системе, но у него нет точки HOME. Команды движения игнорируются.
ARMED (Готов): Получено сообщение в sitl-drone-home. Координаты записаны в Redis. Адаптер начинает транслировать NMEA с нулевой скоростью.
MOVING (Движение): Получена команда в sitl/commands. Адаптер обновляет вектор скорости в Redis и в каждом тике (10 Гц) пересчитывает координаты.

## 2. Структура хранения в Redis (Data Schema)
Для обеспечения скорости 10 Гц на 100 дронов используйте тип данных Hash. Это позволит обновлять параметры мгновенно без перепарсинга JSON.
Ключ: drone:{drone_id}:state
Поля (Fields):
lat, lon, alt — текущие географические координаты (float).
vx, vy, vz — текущие скорости в м/с (извлекаются из команд привода).
heading — курс (degrees).
home_lat, home_lon, home_alt — зафиксированная точка взлета.
last_update — timestamp последнего изменения вектора скорости.

## 3. Алгоритм работы Адаптера (Main Loop)
Адаптер должен состоять из двух параллельных процессов (или асинхронных задач):
Поток А: Приемник (Listener)
Слушает MQTT топики sitl/commands и sitl-drone-home.
При получении команды — просто делает HSET в Redis для соответствующего drone_id. Никаких расчетов здесь не делается.
Поток Б: Вычислитель (Ticker 10 Hz)
Раз в 100 мс берет список всех ключей drone:*:state.
Для каждого дрона:
Читает текущие lat, lon, alt и vx, vy, vz.
Рассчитывает приращение: d=V*сек.
Обновляет координаты
Формирует выходной JSON.
Публикует его в MQTT.

# JSON схемы для сообщений SITL
## Общие принципы
Изоляция — каждая схема для конкретного топика Kafka/MQTT

Контроль взаимодействия — строгая валидация входных/выходных данных

Минимализация доверенной базы — stateless валидация в verificator.py

Контейнеризация — схемы в schemas/ как Volume в Docker

## Базовые принципы (интерпретация для SITL)
Взаимодействие только через брокер сообщений (sitl/commands, sitl-drone-home)

Выдача событий безопасности по запросу через REST API

Передача телеметрии в сервис аналитики (sitl/telemetry)

Передача событий безопасности в сервис аналитики (sitl/safety-events)

События записываются в журнал с msg_id + timestamp

# 1. Схема входных команд от Приводов
Топик: sitl/commands
```
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "SITL Command Message",
  "type": "object",
  "required": ["drone_id", "msg_id", "timestamp", "nmea", "derived", "actions"],
  "properties": {
    "drone_id": {
      "type": "string",
      "pattern": "^drone_[0-9]{3,4}$"
    },
    "msg_id": {
      "type": "string",
      "format": "uuid"
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "nmea": {
      "type": "object",
      "required": ["rmc", "gga"],
      "properties": {
        "rmc": {
          "type": "object",
          "required": ["talker_id", "time", "status", "latitude", "lat_dir", "longitude", "lon_dir", "speed_knots", "course_degrees", "date"],
          "properties": {
            "talker_id": {"type": "string", "enum": ["GN", "GP"]},
            "time": {"type": "string", "pattern": "^[0-2][0-9][0-5][0-9][0-5][0-9]\\.[0-9]{3}$"},
            "status": {"type": "string", "enum": ["A", "V"]},
            "latitude": {"type": "string", "pattern": "^[0-8][0-9][0-9][0-9]\\.[0-9]{4}$"},
            "lat_dir": {"type": "string", "enum": ["N", "S"]},
            "longitude": {"type": "string", "pattern": "^(0|[1-9][0-9]{0,2})[0-5][0-9]\\.[0-9]{4}$"},
            "lon_dir": {"type": "string", "enum": ["E", "W"]},
            "speed_knots": {"type": "number", "minimum": 0},
            "course_degrees": {"type": "number", "minimum": 0, "maximum": 359.9},
            "date": {"type": "string", "pattern": "^[0-3][0-9][0-1][0-9][0-9]{2}$"}
          }
        },
        "gga": {
          "type": "object",
          "required": ["talker_id", "time", "latitude", "lat_dir", "longitude", "lon_dir", "quality", "satellites"],
          "properties": {
            "talker_id": {"type": "string", "enum": ["GN", "GP"]},
            "time": {"type": "string", "pattern": "^[0-2][0-9][0-5][0-9][0-5][0-9]\\.[0-9]{3}$"},
            "latitude": {"type": "string", "pattern": "^[0-8][0-9][0-9][0-9]\\.[0-9]{4}$"},
            "lat_dir": {"type": "string", "enum": ["N", "S"]},
            "longitude": {"type": "string", "pattern": "^(0|[1-9][0-9]{0,2})[0-5][0-9]\\.[0-9]{4}$"},
            "lon_dir": {"type": "string", "enum": ["E", "W"]},
            "quality": {"type": "integer", "minimum": 0, "maximum": 5},
            "satellites": {"type": "integer", "minimum": 0, "maximum": 32},
            "hdop": {"type": "number", "minimum": 0, "maximum": 20}
          }
        }
      }
    },
    "derived": {
      "type": "object",
      "required": ["lat_decimal", "lon_decimal", "altitude_msl"],
      "properties": {
        "lat_decimal": {"type": "number", "minimum": -90, "maximum": 90},
        "lon_decimal": {"type": "number", "minimum": -180, "maximum": 180},
        "altitude_msl": {"type": "number", "minimum": 0},
        "speed_vertical_ms": {"type": "number"}
      }
    },
    "actions": {
      "type": "object",
      "required": ["drop", "emergency_landing"],
      "properties": {
        "drop": {"type": "boolean"},
        "emergency_landing": {"type": "boolean"}
      }
    }
  }
}
```

# 2. Схема HOME позиции (только trusted)
Топик: sitl-drone-home
```
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "SITL Home Position Message (TRUSTED)", 
  "type": "object",
  "required": ["drone_id", "msg_id", "timestamp", "nmea", "derived"],
  "properties": {
    "drone_id": {
      "type": "string",
      "pattern": "^drone_[0-9]{3,4}$"
    },
    "msg_id": {
      "type": "string",
      "format": "uuid"
    },
    "timestamp": {
      "type": "string",
      "format": "date-time"
    },
    "nmea": {
      "type": "object",
      "required": ["rmc", "gga"],
      "properties": {
        "rmc": {
          "type": "object",
          "required": ["talker_id", "time", "status", "latitude", "lat_dir", "longitude", "lon_dir", "speed_knots", "course_degrees", "date"],
          "properties": {
            "talker_id": {"type": "string", "enum": ["GN", "GP"]},
            "status": {"type": "string", "enum": ["A", "V"]},
            "speed_knots": {"type": "number", "minimum": 0, "maximum": 50},
            "course_degrees": {"type": "number", "minimum": 0, "maximum": 359.9}
          }
        },
        "gga": {
          "type": "object",
          "required": ["talker_id", "time", "latitude", "lat_dir", "longitude", "lon_dir", "quality", "satellites"],
          "properties": {
            "quality": {"type": "integer", "minimum": 1},
            "satellites": {"type": "integer", "minimum": 4}
          }
        }
      }
    },
    "derived": {
      "type": "object",
      "required": ["lat_decimal", "lon_decimal", "altitude_msl", "gps_valid", "satellites_used"],
      "properties": {
        "lat_decimal": {"type": "number", "minimum": -90, "maximum": 90},
        "lon_decimal": {"type": "number", "minimum": -180, "maximum": 180},
        "altitude_msl": {"type": "number", "minimum": 0},
        "heading_at_home": {"type": "number", "minimum": 0, "maximum": 359.9},
        "gps_valid": {"type": "boolean"},
        "satellites_used": {"type": "integer", "minimum": 4, "maximum": 32},
        "position_accuracy_hdop": {"type": "number", "minimum": 0, "maximum": 5},
        "coord_system": {"type": "string", "enum": ["WGS84", "PZ-90"]}
      }
    }
  }
}
```

# 3. Схема состояния в Redis (drone:{drone_id}:state)
Тип данных: Hash (HSET/HGETALL)
```
Ключ: drone:drone_001:state

Поля:
status: "IDLE" | "ARMED" | "MOVING" | "EMERGENCY"
lat: 59.938623 (float)
lon: 30.316534 (float) 
alt: 100.0 (float)
vx: 1.23 (float)  // скорость Восток м/с
vy: 0.87 (float)  // скорость Север м/с  
vz: 0.0 (float)   // вертикальная скорость м/с
heading: 25.8 (float)
home_lat: 55.703981 (float)
home_lon: 37.693438 (float)
home_alt: 153.4 (float)
last_update: "2026-03-08T16:40:00Z" (string)
```
# Архитектура
Каждая папка — отдельный контейнер:
```
sitl-adapter/
├── schemas/
│   ├── sitl-commands.json
│   └── sitl-drone-home.json
├── verificator.py
├── core.py
├── controller.py
└── api-server.py
```
