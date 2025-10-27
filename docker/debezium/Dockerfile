FROM debezium/connect:2.5

# Install additional connectors if needed
# The base image already includes Postgres and MySQL connectors

# Set environment variables for Kafka Connect
ENV CONNECT_REST_ADVERTISED_HOST_NAME=debezium \
    CONNECT_REST_PORT=8083 \
    CONNECT_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=false \
    CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false \
    CONNECT_INTERNAL_KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
    CONNECT_INTERNAL_VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter

EXPOSE 8083
