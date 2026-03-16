# Алгоритм работы SITL-адаптера

## 1. Схема жизненного цикла данных (Data Flow)
Дрон в SITL-адаптере проходит **три стадии состояния**:

**IDLE** (Ожидание): Дрон известен системе, но у него нет точки HOME. Команды движения игнорируются.  
**ARMED** (Готов): Получено сообщение в `sitl-drone-home`. Координаты записаны в Redis. Адаптер начинает транслировать NMEA с нулевой скоростью.  
**MOVING** (Движение): Получена команда в `sitl/commands`. Адаптер обновляет вектор скорости в Redis и в каждом тике (10 Гц) пересчитывает координаты.

 Как работает: Listener мониторит MQTT-топики. При sitl-drone-home делает HSET drone:001:state home_lat 59.9 home_lon 30.3 status "ARMED". При sitl/commands с скоростями vx²+vy²>0.01 меняет status "MOVING". Ticker проверяет статус: если MOVING — применяет физику, иначе статично транслирует HOME с v=0.
 
## 2. Структура хранения в Redis (Data Schema)
Для обеспечения скорости 10 Гц на 100 дронов используйте тип данных **Hash**. Это позволит обновлять параметры мгновенно без перепарсинга JSON.  
**Ключ**: `drone:{drone_id}:state`  
**Поля (Fields)**:
```
lat, lon, alt — текущие географические координаты (float)
vx, vy, vz — текущие скорости в м/с (извлекаются из команд привода)
speed_h_ms — горизонтальная скорость √(vx²+vy²) (float, вычисляется в ticker)
speed_v_ms — вертикальная скорость |vz| (float, вычисляется в ticker)
heading — курс (degrees)
home_lat, home_lon, home_alt — зафиксированная точка взлета
last_update — timestamp последнего изменения вектора скорости (string)
```
 Как работает: Listener использует атомарный HSET — 1 команда меняет 5 полей (vx,vy,vz,heading,last_update). Ticker читает HGETALL (15 полей за 0.1мс), вычисляет speed_h_ms=math.sqrt(vx*vx+vy*vy) и speed_v_ms=abs(vz), записывает обратно HSET pipeline (атомарно для 100 дронов).
 
## 3. Алгоритм работы Адаптера (Main Loop)
Адаптер должен состоять из **двух параллельных процессов (или асинхронных задач)**:

**Поток А: Приемник (Listener)**  
Слушает MQTT топики `sitl.commands` и `sitl-drone-home`.  
При получении команды — просто делает **HSET** в Redis для соответствующего `drone_id`. Никаких расчетов здесь не делается.

**Поток Б: Вычислитель (Ticker 10 Hz)**  
Каждые 100мс обновить координаты всех активных дронов по скоростям из Redis.

## JSON схемы для сообщений SITL

### Общие принципы
- Изоляция — каждая схема для конкретного топика Kafka/MQTT
- Контроль взаимодействия — строгая валидация входных/выходных данных
- Минимализация доверенной базы — stateless валидация в `verificator.py`
- Контейнеризация — схемы в `schemas/` как Volume в Docker

### Базовые принципы (интерпретация для SITL)
- Взаимодействие только через брокер сообщений (`sitl.commands`, `sitl-drone-home`)
- Выдача событий безопасности по запросу через REST API
- Передача телеметрии в сервис аналитики (`sitl.telemetry`)
- Передача событий безопасности в сервис аналитики (`sitl.safety-events`)
- События записываются в журнал с `msg_id + timestamp`

## 1. Схема входных команд от Приводов
**Топик**: `sitl.commands`
```json
{
  "type": "object",
  "required": ["drone_id", "vx", "vy", "vz", "mag_heading"],
  "properties": {
    "drone_id": {"type": "string", "pattern": "^drone_[0-9]{3,4}$"},
    "vx": {"type": "number", "minimum": -50, "maximum": 50},
    "vy": {"type": "number", "minimum": -50, "maximum": 50},
    "vz": {"type": "number", "minimum": -10, "maximum": 10},
    "mag_heading": {"type": "number", "minimum": 0, "maximum": 359.9}
  }
}
```

## 2. Схема HOME позиции (только trusted)
**Топик**: `sitl-drone-home`
```json
{
  "type": "object",
  "required": ["drone_id", "home_lat", "home_lon", "home_alt"],
  "properties": {
    "drone_id": {"type": "string", "pattern": "^drone_[0-9]{3,4}$"},
    "home_lat": {"type": "number"},
    "home_lon": {"type": "number"},
    "home_alt": {"type": "number"}
  }
}

```

## 3. Схема состояния в Redis (drone:{drone_id}:state)
**Тип данных**: Hash (HSET/HGETALL)
```
Ключ: drone:drone_001:state

status: "IDLE"           # IDLE|ARMED|MOVING|EMERGENCY
lat: 59.938623           # текущая широта (float)
lon: 30.316534           # текущая долгота (float) 
alt: 100.0               # высота м (float)
vx: 1.23                 # скорость Восток м/с ENU (float)
vy: 0.87                 # скорость Север м/с ENU (float)
vz: 0.0                  # скорость вверх м/с (float)
mag_heading: 25.8        # магнитный курс градусов (float)
home_lat: 59.938623      # HOME широта (float)
home_lon: 30.316534      # HOME долгота (float)
home_alt: 100.0          # HOME высота (float)
last_update: "2026-03-12T21:35:00Z"  # ISO timestamp (string)

```
 
## 4. SITL-адаптер отправляет:
Запрос (дрон -> SITL)
```
Content-Type: application.json

{
  "drone_id": ["drone_001"]
}

```
Ответ (SITL -> дрон)
```
{
    "lat": 59.938623,
    "lon": 30.316534,
    "alt": 100.2,
}

```

## 5. Архитектура
**Каждая папка — отдельный контейнер**:
```
sitl-adapter/
├── schemas/
│   ├── sitl-commands.json
│   └── sitl-drone-home.json
├── verificator.py          # Stateless JSON Schema валидация
├── core.py                 # Ticker: физика + NMEA генератор + speed_h_ms, speed_v_ms
├── controller.py           # Listener: MQTT → Redis HSET
└── api-server.py           # REST API для safety-events и состояния
```
