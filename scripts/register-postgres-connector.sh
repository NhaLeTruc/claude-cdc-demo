#!/bin/bash
# Register Postgres Debezium connector

set -e

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost}"
DEBEZIUM_PORT="${DEBEZIUM_PORT:-8083}"
POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-cdcuser}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-cdcpass}"
POSTGRES_DB="${POSTGRES_DB:-cdcdb}"

echo "Registering Postgres Debezium connector..."

curl -i -X POST \
  "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "'"${POSTGRES_HOST}"'",
      "database.port": "'"${POSTGRES_PORT}"'",
      "database.user": "'"${POSTGRES_USER}"'",
      "database.password": "'"${POSTGRES_PASSWORD}"'",
      "database.dbname": "'"${POSTGRES_DB}"'",
      "database.server.name": "postgres",
      "table.include.list": "public.customers,public.orders",
      "plugin.name": "pgoutput",
      "publication.name": "dbz_publication",
      "slot.name": "debezium",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",
      "topic.prefix": "cdc.postgres",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false"
    }
  }'

echo ""
echo "Postgres connector registered successfully!"
echo "Check status: curl http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/postgres-connector/status"
