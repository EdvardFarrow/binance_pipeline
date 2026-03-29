import asyncio
import logging
import orjson
from aiokafka import AIOKafkaConsumer

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.common.config import settings
from src.consumer.s3_writer import MinIOWriter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 5000 

async def main():
    logger.info("Starting Consumer and connecting to Redpanda...")
    writer = MinIOWriter()

    consumer = AIOKafkaConsumer(
        settings.raw_events_topic,
        bootstrap_servers=settings.redpanda_brokers,
        group_id="raw_zone_loader_group",
        auto_offset_reset="earliest", 
        enable_auto_commit=False,     
        value_deserializer=lambda x: orjson.loads(x)
    )

    await consumer.start()
    logger.info("Consumer started. Listening for messages...")

    batch = []
    try:
        async for msg in consumer:
            batch.append(msg.value)

            if len(batch) >= BATCH_SIZE:
                logger.info(f"Batch reached {BATCH_SIZE} events. Writing to MinIO...")
                
                path = writer.write_batch(batch)
                logger.info(f"Successfully wrote batch to {path}")

                await consumer.commit()
                logger.info("Offsets committed successfully.")

                batch.clear()

    except asyncio.CancelledError:
        logger.info("Shutdown signal received. Stopping gracefully...")
    finally:
        # Graceful shutdown
        if batch:
            logger.info(f"Flushing final batch of {len(batch)} events before shutdown...")
            writer.write_batch(batch)
            await consumer.commit()
            
        logger.info("Closing consumer...")
        await consumer.stop()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass