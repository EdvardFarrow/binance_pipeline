import asyncio
import logging
import signal
import orjson
import websockets
from aiokafka import AIOKafkaProducer


import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.common.config import settings
from src.common.schemas import BinanceTrade

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


async def main():
    logger.info(f"Connecting to Redpanda at {settings.redpanda_brokers}...")
    
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.redpanda_brokers,
        value_serializer=lambda v: orjson.dumps(v), 
        acks=1,                     # Waiting for confirmation from all replicas.
        linger_ms=10,               # Waiting 10ms
        compression_type=None
    )
    
    await producer.start()
    logger.info("Producer started successfully.")

    try:
        logger.info(f"Connecting to Binance WS: {settings.binance_ws_url}")
        async with websockets.connect(settings.binance_ws_url) as websocket:
            logger.info("Connected to Binance! Listening for trades...")
            
            async for message in websocket:
                try:
                    # Parse and validate the data
                    raw_data = orjson.loads(message)
                    
                    if "data" in raw_data:
                        trade_payload = raw_data["data"]
                    else:
                        trade_payload = raw_data
                    
                    trade = BinanceTrade(**trade_payload)
                    
                    # Send to the topic.
                    await producer.send(
                        topic=settings.raw_events_topic,
                        key=trade.symbol.encode('utf-8'),
                        value=trade.model_dump()
                    )
                    
                    #logger.info(f"Produced event: {trade.event_id[:8]}... | {trade.symbol} | Price: {trade.price}")

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

    except asyncio.CancelledError:
        logger.info("Shutdown signal received. Stopping gracefully...")
    finally:
        logger.info("Flushing and stopping Redpanda producer...")
        await producer.stop()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass