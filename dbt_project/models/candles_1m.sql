{{ config(
    materialized='incremental',
    engine='ReplacingMergeTree(updated_at)',
    order_by='(symbol, window_start)'
) }}

SELECT
    symbol,
    toStartOfMinute(toDateTime(event_time_ms / 1000)) AS window_start,
    
    -- Open: price at minimum time in this minute
    argMin(price, event_time_ms) AS open_price,
    -- High: max price
    max(price) AS high_price,
    -- Low: min price
    min(price) AS low_price,
    -- Close: price at maximum time in this minute
    argMax(price, event_time_ms) AS close_price,
    
    -- Total trading volume per minute
    sum(quantity) AS total_volume,
    
    -- Showcase assembly time (for ReplacingMergeTree to work)
    now() AS updated_at
FROM binance.trades_raw

{% if is_incremental() %}
    WHERE toStartOfMinute(toDateTime(event_time_ms / 1000)) >= (SELECT coalesce(max(window_start), toDateTime('1970-01-01 00:00:00')) FROM {{ this }})
{% endif %}

GROUP BY 
    symbol, 
    window_start