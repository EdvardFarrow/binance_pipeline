from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Binance
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    # Redpanda
    redpanda_brokers: str = "localhost:19092"
    raw_events_topic: str = "binance_trades_raw"
    
    # MinIO (S3 Raw Zone)
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str
    minio_secret_key: str
    minio_raw_bucket: str

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()