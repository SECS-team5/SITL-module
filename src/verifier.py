import asyncio
import json
import logging
import os
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka:9092"),
        value_serializer=lambda v: json.dumps(v).encode()
    )
    
    consumer = AIOKafkaConsumer(
        os.getenv("INPUT_TOPIC", "input-messages"),
        bootstrap_servers=os.getenv("KAFKA_SERVERS", "kafka:9092"),
        group_id="SITL-verifier-v1",
        value_deserializer=lambda m: json.loads(m.decode())
    )
    
    await producer.start()
    await consumer.start()
    log.info("🚁 Verifier started")
    
    try:
        async for msg in consumer:
            verified = {
                "data": msg.value,
                "verifier_stage": "SITL-v1",
                "timestamp": asyncio.get_event_loop().time()
            }
            await producer.send_and_wait(os.getenv("OUTPUT_TOPIC", "verified-messages"), verified)
            log.info(f"✅ Verified: {msg.value}")
    finally:
        await producer.stop()
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
