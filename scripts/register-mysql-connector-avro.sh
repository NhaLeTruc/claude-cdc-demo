#!/bin/bash
# Register MySQL Debezium connector with Avro and Schema Registry
# This configuration enables schema evolution tracking through Confluent Schema Registry

set -e

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost}"
DEBEZIUM_PORT="${DEBEZIUM_PORT:-8083}"
MYSQL_HOST="${MYSQL_HOST:-mysql}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:-cdcuser}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-cdcpass}"
MYSQL_DB="${MYSQL_DB:-cdcdb}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"

echo "Registering MySQL Debezium connector with Avro serialization..."
echo "Schema Registry: ${SCHEMA_REGISTRY_URL}"

curl -i -X POST \
  "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-connector-avro",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "'"${MYSQL_HOST}"'",
      "database.port": "'"${MYSQL_PORT}"'",
      "database.user": "'"${MYSQL_USER}"'",
      "database.password": "'"${MYSQL_PASSWORD}"'",
      "database.server.name": "debezium",
      "database.include.list": "'"${MYSQL_DB}"'",
      "table.include.list": "'"${MYSQL_DB}"'.*",

      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "'"${SCHEMA_REGISTRY_URL}"'",
      "key.converter.schemas.enable": "true",

      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "'"${SCHEMA_REGISTRY_URL}"'",
      "value.converter.schemas.enable": "true",

      "topic.prefix": "debezium",

      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
      "schema.history.internal.kafka.topic": "schema-changes.debezium.mysql",

      "include.schema.changes": "true",
      "snapshot.mode": "initial",

      "transforms": "route",
      "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
      "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
      "transforms.route.replacement": "$1.$2.$3"
    }
  }'

echo ""
echo "MySQL connector with Avro serialization registered successfully!"
echo "Schema Registry URL: ${SCHEMA_REGISTRY_URL}"
echo ""
echo "Check connector status:"
echo "  curl http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/mysql-connector-avro/status"
echo ""
echo "Check schemas in Schema Registry:"
echo "  curl ${SCHEMA_REGISTRY_URL}/subjects"
echo "  curl ${SCHEMA_REGISTRY_URL}/subjects/debezium.${MYSQL_DB}.<table_name>-value/versions/latest"
