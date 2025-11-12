#!/bin/bash
set -e

echo "========================================"
echo "Spark Streaming CDC Pipeline Starting"
echo "========================================"

# Configuration from environment variables with defaults
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
KAFKA_TOPIC=${KAFKA_TOPIC:-debezium.public.customers}
ICEBERG_CATALOG=${ICEBERG_CATALOG:-demo_catalog}
ICEBERG_WAREHOUSE=${ICEBERG_WAREHOUSE:-s3a://warehouse/iceberg}
ICEBERG_NAMESPACE=${ICEBERG_NAMESPACE:-cdc}
ICEBERG_TABLE=${ICEBERG_TABLE:-customers}
CHECKPOINT_LOCATION=${CHECKPOINT_LOCATION:-/tmp/spark-checkpoints/kafka-to-iceberg}
S3_ENDPOINT=${S3_ENDPOINT:-http://minio:9000}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-minioadmin}
S3_SECRET_KEY=${S3_SECRET_KEY:-minioadmin}
SPARK_MASTER=${SPARK_MASTER:-local[*]}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-1g}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-1g}

echo "Configuration:"
echo "  Kafka Bootstrap Servers: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "  Kafka Topic: ${KAFKA_TOPIC}"
echo "  Iceberg Catalog: ${ICEBERG_CATALOG}"
echo "  Iceberg Warehouse: ${ICEBERG_WAREHOUSE}"
echo "  Iceberg Namespace: ${ICEBERG_NAMESPACE}"
echo "  Iceberg Table: ${ICEBERG_TABLE}"
echo "  Checkpoint Location: ${CHECKPOINT_LOCATION}"
echo "  S3 Endpoint: ${S3_ENDPOINT}"
echo "========================================"

# Wait for Kafka to be ready
echo "Waiting for Kafka at ${KAFKA_BOOTSTRAP_SERVERS}..."
KAFKA_HOST=$(echo ${KAFKA_BOOTSTRAP_SERVERS} | cut -d: -f1)
KAFKA_PORT=$(echo ${KAFKA_BOOTSTRAP_SERVERS} | cut -d: -f2)

MAX_WAIT=120
ELAPSED=0
while ! nc -z ${KAFKA_HOST} ${KAFKA_PORT}; do
    if [ ${ELAPSED} -ge ${MAX_WAIT} ]; then
        echo "ERROR: Kafka not ready after ${MAX_WAIT} seconds"
        exit 1
    fi
    echo "  Waiting for Kafka... (${ELAPSED}s)"
    sleep 2
    ELAPSED=$((ELAPSED + 2))
done

echo "Kafka is ready!"
echo "========================================"

# Start Spark Structured Streaming job
echo "Starting Spark Structured Streaming job..."
echo "Job: /opt/spark-jobs/kafka_to_iceberg.py"
echo "========================================"

# Set Ivy cache to writable location
export SPARK_HOME=/opt/spark
mkdir -p /tmp/.ivy2
export IVY2_HOME=/tmp/.ivy2

# Set AWS region to avoid SDK trying to detect it (we're using MinIO not AWS)
export AWS_REGION=us-east-1

exec /opt/spark/bin/spark-submit \
    --master ${SPARK_MASTER} \
    --driver-memory ${SPARK_DRIVER_MEMORY} \
    --executor-memory ${SPARK_EXECUTOR_MEMORY} \
    --conf "spark.jars.ivy=/tmp/.ivy2" \
    --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.18" \
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog" \
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.catalog-impl=org.apache.iceberg.rest.RESTCatalog" \
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.uri=http://iceberg-rest:8181" \
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${ICEBERG_WAREHOUSE}" \
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.s3.endpoint=${S3_ENDPOINT}" \
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.s3.access-key-id=${S3_ACCESS_KEY}" \
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.s3.secret-access-key=${S3_SECRET_KEY}" \
    --conf "spark.sql.catalog.${ICEBERG_CATALOG}.s3.path-style-access=true" \
    --conf "spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT}" \
    --conf "spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY}" \
    --conf "spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY}" \
    --conf "spark.hadoop.fs.s3a.path.style.access=true" \
    --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
    --conf "spark.hadoop.fs.s3a.connection.ssl.enabled=false" \
    --conf "spark.sql.streaming.checkpointLocation=${CHECKPOINT_LOCATION}" \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.shuffle.partitions=10" \
    --conf "spark.default.parallelism=10" \
    /opt/spark-jobs/kafka_to_iceberg.py
