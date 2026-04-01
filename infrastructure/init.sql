CREATE DATABASE IF NOT EXISTS binance;

CREATE TABLE IF NOT EXISTS binance.trades_s3_queue
(
    event_id String,
    event_time_ms Int64,
    symbol String,
    trade_id Int64,
    price Decimal64(8),
    quantity Decimal128(18),
    ingested_at Int64
)
ENGINE = S3Queue(
    '${S3_ENDPOINT}', 
    '${S3_ACCESS_KEY}', 
    '${S3_SECRET_KEY}', 
    'Parquet'
)
SETTINGS 
    mode = 'unordered',
    s3queue_processing_threads_num = 4;

CREATE TABLE IF NOT EXISTS binance.trades_raw
(
    event_id String,
    event_time_ms Int64,
    symbol String,
    trade_id Int64,
    price Decimal64(8),
    quantity Decimal128(18),
    ingested_at Int64
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (symbol, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS binance.trades_mv
TO binance.trades_raw
AS SELECT 
    event_id,
    event_time_ms,
    symbol,
    trade_id,
    price,
    quantity,
    ingested_at
FROM binance.trades_s3_queue;