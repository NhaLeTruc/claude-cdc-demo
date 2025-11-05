#!/bin/bash
# Register Postgres Debezium connector with Avro and Schema Registry
# This configuration enables schema evolution tracking through Confluent Schema Registry

set -e

DEBEZIUM_HOST="${DEBEZIUM_HOST:-localhost}"
DEBEZIUM_PORT="${DEBEZIUM_PORT:-8083}"
POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-cdcuser}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-cdcpass}"
POSTGRES_DB="${POSTGRES_DB:-cdcdb}"
SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}"

echo "Registering Postgres Debezium connector with Avro serialization..."
echo "Schema Registry: ${SCHEMA_REGISTRY_URL}"

curl -i -X POST \
  "http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-connector-avro",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "'"${POSTGRES_HOST}"'",
      "database.port": "'"${POSTGRES_PORT}"'",
      "database.user": "'"${POSTGRES_USER}"'",
      "database.password": "'"${POSTGRES_PASSWORD}"'",
      "database.dbname": "'"${POSTGRES_DB}"'",
      "database.server.name": "debezium",
      "table.include.list": "public.*",
      "plugin.name": "pgoutput",
      "publication.name": "dbz_publication",
      "slot.name": "debezium_avro",

      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "'"${SCHEMA_REGISTRY_URL}"'",
      "key.converter.schemas.enable": "true",

      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "'"${SCHEMA_REGISTRY_URL}"'",
      "value.converter.schemas.enable": "true",

      "topic.prefix": "debezium",

      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
      "schema.history.internal.kafka.topic": "schema-changes.debezium",

      "include.schema.changes": "true",
      "provide.transaction.metadata": "false",

      "transforms": "route",
      "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
      "transforms.route.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
      "transforms.route.replacement": "$1.$2.$3"
    }
  }'

echo ""
echo "Postgres connector with Avro serialization registered successfully!"
echo "Schema Registry URL: ${SCHEMA_REGISTRY_URL}"
echo ""
echo "Check connector status:"
echo "  curl http://${DEBEZIUM_HOST}:${DEBEZIUM_PORT}/connectors/postgres-connector-avro/status"
echo ""
echo "Check schemas in Schema Registry:"
echo "  curl ${SCHEMA_REGISTRY_URL}/subjects"
echo "  curl ${SCHEMA_REGISTRY_URL}/subjects/debezium.public.<table_name>-value/versions/latest"
