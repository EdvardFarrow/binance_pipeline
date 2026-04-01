#!/bin/bash

set -a
source .env
set +a

curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d @- <<EOF
{
    "name": "ml-anomalies-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "${POSTGRES_USER}",
        "database.password": "${POSTGRES_PASSWORD}",
        "database.dbname" : "${POSTGRES_DB}",
        "database.server.name": "binance_server",
        "table.include.list": "public.anomalies",  
        "topic.prefix": "cdc",
        "decimal.handling.mode": "string"   
    }
}
EOF