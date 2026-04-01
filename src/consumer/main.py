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

BATCH_SIZE = 3000 

async def main():
    logger.info("Starting Consumer and connecting to Redpanda...")
    writer = MinIOWriter()

    consumer = AIOKafkaConsumer(
        settings.raw_events_topic,
        bootstrap_servers=settings.redpanda_brokers,
        group_id="raw_zone_loader_group_v3",
        auto_offset_reset="latest", 
        enable_auto_commit=True,     
        value_deserializer=lambda x: orjson.loads(x),
        max_poll_records=5000,    
        fetch_max_bytes=5242880,  
        fetch_min_bytes=1024,
    )

    await consumer.start()
    logger.info("Consumer started. Listening for messages...")

    batch = []
    try:
        while True:
            result = await consumer.getmany(timeout_ms=1000, max_records=5000)
            
            for tp, messages in result.items():
                for msg in messages:
                    batch.append(msg.value)

            if len(batch) >= BATCH_SIZE:
                logger.info(f"Batch reached {len(batch)} events. Writing to MinIO...")
                
                path = await asyncio.to_thread(writer.write_batch, batch.copy())
                logger.info(f"Successfully wrote batch to {path}")

                await consumer.commit()
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