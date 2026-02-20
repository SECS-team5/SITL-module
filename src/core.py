import asyncio
import json
import logging
import os
import redis.asyncio as redis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

async def main():
    r = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379"))
    producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka:29092"),
        value_serializer=lambda v: json.dumps(v).encode()
    )
    
    consumer = AIOKafkaConsumer(
        os.getenv("INPUT_TOPIC", "verified-messages"),
        bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka:29092"),
        group_id="SITL-core-v1",
        value_deserializer=lambda m: json.loads(m.decode())
    )
    
    await producer.start()
    await consumer.start()
    log.info("⚙️ Core started")
    
    try:
        async for msg in consumer:
            if msg.value.get("verifier_stage") != "SITL-v1":
                log.warning("❌ No verifier stamp")
                continue
                
            data = msg.value["data"]
            key = f"SITL:{data.get('drone_id', 'unknown')}"
            
            await r.set(key, json.dumps(msg.value), ex=7200)
            
            result = {"data": data, "core_stage": "SITL-v1"}
            await producer.send_and_wait(os.getenv("OUTPUT_TOPIC", "core-results"), result)
            log.info(f"📤 Processed: {data}")
            
    finally:
        await producer.stop()
        await consumer.stop()
        await r.aclose() 

if __name__ == "__main__":
    asyncio.run(main())
