import asyncio
import json
import logging
import math
import os
from datetime import datetime, timezone

import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TOPIC_COMMANDS = os.getenv("TOPIC_COMMANDS", "sitl/verified/commands")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
VERIFIER_STAMP = "SITL-v1"
KEY_TTL = 7200
KNOTS_TO_MPS = 0.514444


# ── Геометрия ───────────────────────────────────────────────────────

def course_to_vector(course_degrees: float) -> tuple[float, float]:
    """Курс (0°=N, 90°=E) → (vx=sin, vy=cos)."""
    rad = math.radians(course_degrees)
    return math.sin(rad), math.cos(rad)


def calculate_new_position(
    lat: float, lon: float,
    vx: float, vy: float,
    speed_mps: float, dt: float,
) -> tuple[float, float]:
    dist = speed_mps * dt
    new_lat = lat + (vy * dist) / 111_111.0
    new_lon = lon + (vx * dist) / (111_111.0 * math.cos(math.radians(lat)))
    return round(new_lat, 7), round(new_lon, 7)


# ────────────────────────────────────────────────────────────────────
# Схема §1 — ARCHITECTURE.md
# Топик: sitl/verified/commands
#
# {
#   "drone_id": "drone_001",
#   "msg_id": "uuid",
#   "timestamp": "ISO-8601",
#   "nmea": {
#     "rmc": {
#       "talker_id": "GN",
#       "time": "123456.789",
#       "status": "A",
#       "latitude": "5545.1234",
#       "lat_dir": "N",
#       "longitude": "03737.5678",
#       "lon_dir": "E",
#       "speed_knots": 15.5,
#       "course_degrees": 270.0,
#       "date": "150625"
#     },
#     "gga": {
#       "talker_id": "GN",
#       "time": "123456.789",
#       "latitude": "5545.1234",
#       "lat_dir": "N",
#       "longitude": "03737.5678",
#       "lon_dir": "E",
#       "quality": 2,
#       "satellites": 10,
#       "hdop": 1.2              ← optional
#     }
#   },
#   "derived": {
#     "lat_decimal": 55.7558,
#     "lon_decimal": 37.6173,
#     "altitude_msl": 200.0,
#     "speed_vertical_ms": 2.5   ← optional
#   },
#   "actions": {
#     "drop": false,
#     "emergency_landing": false
#   }
# }
#
# Redis-ключ: SITL:{drone_id}:state
# ────────────────────────────────────────────────────────────────────


# ── Обработка команды из Kafka ──────────────────────────────────────

async def process_command(r: redis.Redis, data: dict) -> None:
    """Записывает команду в Redis по схеме §1."""
    drone_id = data["drone_id"]
    key = f"SITL:{drone_id}:state"

    nmea_rmc = data["nmea"]["rmc"]
    nmea_gga = data["nmea"]["gga"]
    derived = data["derived"]
    actions = data["actions"]

    record = {
        # ── top-level (required) ──
        "drone_id":  data["drone_id"],
        "msg_id":    data["msg_id"],
        "timestamp": data["timestamp"],

        # ── nmea (required) ──
        "nmea": {
            "rmc": {
                "talker_id":      nmea_rmc["talker_id"],
                "time":           nmea_rmc["time"],
                "status":         nmea_rmc["status"],
                "latitude":       nmea_rmc["latitude"],
                "lat_dir":        nmea_rmc["lat_dir"],
                "longitude":      nmea_rmc["longitude"],
                "lon_dir":        nmea_rmc["lon_dir"],
                "speed_knots":    nmea_rmc["speed_knots"],
                "course_degrees": nmea_rmc["course_degrees"],
                "date":           nmea_rmc["date"],
            },
            "gga": {
                "talker_id":  nmea_gga["talker_id"],
                "time":       nmea_gga["time"],
                "latitude":   nmea_gga["latitude"],
                "lat_dir":    nmea_gga["lat_dir"],
                "longitude":  nmea_gga["longitude"],
                "lon_dir":    nmea_gga["lon_dir"],
                "quality":    nmea_gga["quality"],
                "satellites": nmea_gga["satellites"],
            },
        },

        # ── derived (required) ──
        "derived": {
            "lat_decimal":  derived["lat_decimal"],
            "lon_decimal":  derived["lon_decimal"],
            "altitude_msl": derived["altitude_msl"],
        },

        # ── actions (required) ──
        "actions": {
            "drop":              actions["drop"],
            "emergency_landing": actions["emergency_landing"],
        },

        "stored_at": datetime.now(timezone.utc).isoformat(),
    }

    # optional
    if "hdop" in nmea_gga:
        record["nmea"]["gga"]["hdop"] = nmea_gga["hdop"]
    if "speed_vertical_ms" in derived:
        record["derived"]["speed_vertical_ms"] = derived["speed_vertical_ms"]

    await r.set(key, json.dumps(record), ex=KEY_TTL)
    log.info("📡 Command saved → %s", key)


# ── Обновление позиции одного дрона ─────────────────────────────────
#
# Читает из Redis JSON по схеме §1:
#   state["derived"]["lat_decimal"]         ← текущая широта
#   state["derived"]["lon_decimal"]         ← текущая долгота
#   state["nmea"]["rmc"]["speed_knots"]     ← скорость в узлах
#   state["nmea"]["rmc"]["course_degrees"]  ← курс в градусах
#
# Обновляет:
#   state["derived"]["lat_decimal"]         → новая широта
#   state["derived"]["lon_decimal"]         → новая долгота
# ────────────────────────────────────────────────────────────────────

async def update_drone_position(r: redis.Redis, key: str, dt: float) -> None:
    raw = await r.get(key)
    if raw is None:
        return

    try:
        state = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("Invalid JSON — %s", key)
        return

    derived = state.get("derived")
    rmc = state.get("nmea", {}).get("rmc")

    if not derived or not rmc:
        return

    lat = derived.get("lat_decimal")              # схема §1: derived.lat_decimal
    lon = derived.get("lon_decimal")              # схема §1: derived.lon_decimal
    speed_knots = rmc.get("speed_knots", 0.0)     # схема §1: nmea.rmc.speed_knots
    course = rmc.get("course_degrees", 0.0)       # схема §1: nmea.rmc.course_degrees

    if lat is None or lon is None:
        return

    speed_mps = speed_knots * KNOTS_TO_MPS
    vx, vy = course_to_vector(course)
    new_lat, new_lon = calculate_new_position(lat, lon, vx, vy, speed_mps, dt)

    # обновляем координаты в той же структуре схемы §1
    state["derived"]["lat_decimal"] = new_lat     # схема §1: derived.lat_decimal
    state["derived"]["lon_decimal"] = new_lon     # схема §1: derived.lon_decimal
    state["last_position_update"] = datetime.now(timezone.utc).isoformat()

    await r.set(key, json.dumps(state), ex=KEY_TTL)


# ── Фоновая задача: обновление позиций 10 Гц ───────────────────────

async def position_updater_task(r: redis.Redis, update_hz: float = 10.0) -> None:
    dt = 1.0 / update_hz
    log.info("🔄 Position updater — %.1f Hz", update_hz)

    while True:
        try:
            keys: list[str] = []
            cursor = 0
            while True:
                cursor, batch = await r.scan(cursor, match="SITL:*:state", count=100)
                keys.extend(batch)
                if cursor == 0:
                    break

            for key in keys:
                await update_drone_position(r, key, dt)

        except Exception:
            log.exception("💥 Position updater error")

        await asyncio.sleep(dt)


# ── Задача чтения команд из Kafka ──────────────────────────────────

async def command_listener_task(r: redis.Redis) -> None:
    consumer = AIOKafkaConsumer(
        TOPIC_COMMANDS,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="SITL-controller-v1",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode()),
    )
    await consumer.start()
    log.info("📥 Command listener started — [%s]", TOPIC_COMMANDS)

    try:
        async for msg in consumer:
            payload = msg.value

            if payload.get("verifier_stage") != VERIFIER_STAMP:
                log.warning("❌ No verifier stamp")
                continue

            data = payload.get("data")
            if not data:
                log.warning("❌ Empty data — skip")
                continue

            try:
                await process_command(r, data)
            except KeyError as e:
                log.error("❌ Missing required field %s", e)
            except Exception:
                log.exception("💥 Error processing command")

    finally:
        await consumer.stop()


# ── Main: две параллельные задачи ───────────────────────────────────

async def main() -> None:
    update_hz = float(os.getenv("UPDATE_FREQUENCY_HZ", "10.0"))

    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()
    log.info("🚁 Controller started")

    try:
        await asyncio.gather(
            command_listener_task(r),       # Kafka → Redis (схема §1)
            position_updater_task(r, update_hz),  # Redis → Redis (10 Гц)
        )
    finally:
        await r.aclose()
        log.info("🛑 Controller stopped")


if __name__ == "__main__":
    asyncio.run(main())