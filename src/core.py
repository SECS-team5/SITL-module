import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TOPIC_HOME = os.getenv("TOPIC_HOME", "sitl/verified/home")
KAFKA_SERVERS = os.getenv("KAFKA_SERVERS", "kafka:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
VERIFIER_STAMP = "SITL-v1"
KEY_TTL = 7200


# ────────────────────────────────────────────────────────────────────
# Схема §2 — ARCHITECTURE.md
# Топик: sitl/verified/home
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
#       "speed_knots": 0.0,
#       "course_degrees": 90.0,
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
#       "satellites": 10
#     }
#   },
#   "derived": {
#     "lat_decimal": 55.7558,
#     "lon_decimal": 37.6173,
#     "altitude_msl": 200.0,
#     "gps_valid": true,
#     "satellites_used": 10,
#     "heading_at_home": 90.0,             ← optional
#     "position_accuracy_hdop": 1.1,       ← optional
#     "coord_system": "WGS84"             ← optional
#   }
# }
#
# Redis-ключ: SITL:{drone_id}:home
# ────────────────────────────────────────────────────────────────────
async def process_home(r: redis.Redis, data: dict) -> None:
    drone_id = data["drone_id"]
    key = f"SITL:{drone_id}:home"

    nmea_rmc = data["nmea"]["rmc"]
    nmea_gga = data["nmea"]["gga"]
    derived = data["derived"]

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
            "lat_decimal":    derived["lat_decimal"],
            "lon_decimal":    derived["lon_decimal"],
            "altitude_msl":   derived["altitude_msl"],
            "gps_valid":      derived["gps_valid"],
            "satellites_used": derived["satellites_used"],
        },

        "stored_at": datetime.now(timezone.utc).isoformat(),
    }

    # optional
    if "heading_at_home" in derived:
        record["derived"]["heading_at_home"] = derived["heading_at_home"]
    if "position_accuracy_hdop" in derived:
        record["derived"]["position_accuracy_hdop"] = derived["position_accuracy_hdop"]
    if "coord_system" in derived:
        record["derived"]["coord_system"] = derived["coord_system"]

    await r.set(key, json.dumps(record), ex=KEY_TTL)
    log.info("🏠 Home saved → %s", key)


async def main() -> None:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()
    log.info("✅ Redis connected")

    consumer = AIOKafkaConsumer(
        TOPIC_HOME,
        bootstrap_servers=KAFKA_SERVERS,
        group_id="SITL-core-v1",
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode()),
    )
    await consumer.start()
    log.info("⚙️ Core started — [%s]", TOPIC_HOME)

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
                await process_home(r, data)
            except KeyError as e:
                log.error("❌ Missing required field %s", e)
            except Exception:
                log.exception("💥 Error processing home")

    finally:
        await consumer.stop()
        await r.aclose()
        log.info("🛑 Core stopped")


if __name__ == "__main__":
    asyncio.run(main())