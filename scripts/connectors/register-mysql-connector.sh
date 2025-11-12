#!/bin/bash
# Register MySQL Debezium connector

set -e

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost}"
DEBEZIUM_PORT="${DEBEZIUM_PORT:-8083}"
MYSQL_HOST="${MYSQL_HOST:-mysql}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-cdcuser}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-cdcpass}"
MYSQL_DB="${MYSQL_DB:-cdcdb}"

echo "Registering MySQL Debezium connector..."

curl -i -X POST \
  "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "'"${MYSQL_HOST}"'",
      "database.port": "'"${MYSQL_PORT}"'",
      "database.user": "'"${MYSQL_USER}"'",
      "database.password": "'"${MYSQL_PASSWORD}"'",
      "database.server.id": "184054",
      "database.server.name": "mysql",
      "table.include.list": "'"${MYSQL_DB}"'.products,'"${MYSQL_DB}"'.inventory_transactions",
      "database.include.list": "'"${MYSQL_DB}"'",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",
      "topic.prefix": "debezium",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false",
      "transforms.unwrap.delete.handling.mode": "rewrite",
      "transforms.unwrap.add.fields": "op,db,table,ts_ms",
      "transforms.unwrap.add.fields.prefix": "__",
      "include.schema.changes": "false",
      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
      "schema.history.internal.kafka.topic": "schema-changes.debezium.mysql"
    }
  }'

echo ""
echo "MySQL connector registered successfully!"
echo "Check status: curl http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/mysql-connector/status"
