#!/bin/bash

TABLE_NAME="binance.trades_raw"

echo "Connecting to ClickHouse..."

PREV=$(docker exec clickhouse clickhouse-client -q "SELECT count(*) FROM $TABLE_NAME;" | xargs)

while true; do
    sleep 2
    
    CURR=$(docker exec clickhouse clickhouse-client -q "SELECT count(*) FROM $TABLE_NAME;" | xargs)
    
    DIFF=$((CURR - PREV))
    RPS=$((DIFF / 2))
    PREV=$CURR
    
    clear
    echo "================================================="
    echo " END-TO-END PIPELINE MONITOR (CLICKHOUSE)"
    echo "================================================="
    echo " Speed of loading:   $RPS records/sec"
    echo " Total data in DB:   $CURR rows"
    echo "================================================="
    echo " Press Ctrl+C to stop"
done