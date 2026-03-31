import asyncio
import decimal
import logging
import pickle
import numpy as np
import orjson
from decimal import Decimal
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
from sklearn.ensemble import IsolationForest

from sqlalchemy import create_engine, Column, Integer, String, Float, BigInteger, Numeric
from sqlalchemy.orm import declarative_base, sessionmaker

import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from src.common.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# PostgreSQL
engine = create_engine(settings.postgres_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class AnomalyLog(Base):
    __tablename__ = "anomaly_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(String, unique=True, index=True)
    symbol = Column(String, index=True)
    price = Column(Numeric(precision=20, scale=8))
    quantity = Column(Numeric(precision=20, scale=8))
    event_time_ms = Column(BigInteger) 
    anomaly_score = Column(Float)

Base.metadata.create_all(bind=engine)


model = None
model_file = Path(settings.model_path)

def create_mock_model():
    """Train the Isolation Forest stub on mock data and save it."""
    logger.info("Training mock Isolation Forest model...")
    clf = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
    
    # Generate normal trades and a couple of hard emissions
    dummy_data = np.array([[60000 + i, 0.1] for i in range(200)] + [[100000, 500.0], [10000, 0.001]])
    clf.fit(dummy_data)
    
    with open(model_file, "wb") as f:
        pickle.dump(clf, f)
    logger.info(f"Mock model trained and saved to {model_file}")

def load_model():
    """Loading the model into RAM."""
    global model
    if not model_file.exists():
        create_mock_model()
    
    with open(model_file, "rb") as f:
        model = pickle.load(f)
    logger.info("Model loaded into memory successfully.")


# Kafka Consumer
async def consume_and_predict():
    consumer = AIOKafkaConsumer(
        settings.raw_events_topic,
        bootstrap_servers=settings.redpanda_brokers,
        group_id="ml_inference_group",
        value_deserializer=lambda m: orjson.loads(m),
        auto_offset_reset="latest"
    )
    
    await consumer.start()
    logger.info(f"ML Consumer started. Listening to topic: {settings.raw_events_topic}")
    
    try:
        async for msg in consumer:
            trade = msg.value
            
            try:
                price = Numeric(trade["price"])
                qty = Numeric(trade["quantity"])
            except (KeyError, ValueError):
                continue

            features = np.array([[price, qty]])
            
            prediction = model.predict(features)[0] 
            score = model.score_samples(features)[0]
            
            if prediction == -1:
                logger.warning(f"❌ ANOMALY: {trade.get('symbol')} | P: {price} | Q: {qty} | Score: {score:.3f}")
                
                # Saving alert in PostgreSQL
                db = SessionLocal()
                try:
                    log_entry = AnomalyLog(
                        event_id=str(trade.get("event_id", "")),
                        symbol=str(trade.get("symbol", "")),
                        price=price,
                        quantity=qty,
                        event_time_ms=trade.get("event_time_ms", 0),
                        anomaly_score=float(score)
                    )
                    db.add(log_entry)
                    db.commit()
                except Exception as e:
                    db.rollback()
                    
                    if "UniqueViolation" not in str(e):
                        logger.error(f"DB Insert error: {e}")
                finally:
                    db.close()
                    
    except asyncio.CancelledError:
        logger.info("Stopping ML Consumer...")
    finally:
        await consumer.stop()


# FastAPI Lifecycle
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_model()
    consumer_task = asyncio.create_task(consume_and_predict())
    yield
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass

app = FastAPI(lifespan=lifespan, title="Crypto Real-time MLOps API")

@app.get("/health")
def health_check():
    return {"status": "ok", "model_loaded": model is not None}