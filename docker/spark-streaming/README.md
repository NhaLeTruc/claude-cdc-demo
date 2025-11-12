# Spark Streaming CDC Docker Image

This directory contains the Dockerfile and entrypoint script for the Spark Structured Streaming CDC pipeline.

## Features

- **Pre-cached Dependencies**: All Maven/Ivy dependencies are downloaded during image build, eliminating the 3-5 minute startup delay
- **Custom Entrypoint**: Intelligent startup script that waits for Kafka and configures Spark properly
- **Security**: Runs as non-root `spark` user
- **Health Checks**: Includes netcat for container health monitoring

## Building the Image

From the project root:

```bash
docker build -t cdc-spark-streaming:latest docker/spark-streaming/
```

Or using docker-compose (recommended):

```bash
docker-compose -f compose/docker-compose.yml build spark-streaming
```

## Pre-cached Dependencies

The following Maven artifacts are pre-downloaded during build:

- `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2` - Iceberg integration for Spark
- `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` - Kafka integration for Spark Structured Streaming
- `org.apache.hadoop:hadoop-aws:3.3.4` - AWS S3 filesystem support
- `software.amazon.awssdk:bundle:2.20.18` - AWS SDK for S3 operations

## Entrypoint Script

The [entrypoint.sh](entrypoint.sh:1) script handles:

1. **Configuration**: Reads environment variables with sensible defaults
2. **Service Dependencies**: Waits for Kafka to be ready (with timeout)
3. **Spark Submit**: Launches the streaming job with all necessary configurations
4. **Logging**: Provides detailed startup information

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `debezium.public.customers` | Topic to consume CDC events from |
| `ICEBERG_CATALOG` | `demo_catalog` | Iceberg catalog name |
| `ICEBERG_WAREHOUSE` | `s3a://warehouse/iceberg` | Iceberg warehouse path |
| `ICEBERG_NAMESPACE` | `cdc` | Iceberg namespace |
| `ICEBERG_TABLE` | `customers` | Iceberg table name |
| `CHECKPOINT_LOCATION` | `/tmp/spark-checkpoints/kafka-to-iceberg` | Streaming checkpoint directory |
| `S3_ENDPOINT` | `http://minio:9000` | S3/MinIO endpoint |
| `S3_ACCESS_KEY` | `minioadmin` | S3 access key |
| `S3_SECRET_KEY` | `minioadmin` | S3 secret key |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `SPARK_DRIVER_MEMORY` | `1g` | Driver memory |
| `SPARK_EXECUTOR_MEMORY` | `1g` | Executor memory |

## Running the Container

Using docker-compose (recommended):

```bash
docker-compose -f compose/docker-compose.yml up spark-streaming
```

Standalone:

```bash
docker run -d \
  --name cdc-spark-streaming \
  --network cdc-network \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_TOPIC=debezium.public.customers \
  -e S3_ENDPOINT=http://minio:9000 \
  -v $(pwd)/src/cdc_pipelines/streaming:/opt/spark-jobs \
  cdc-spark-streaming:latest
```

## Logs

View container logs:

```bash
docker logs -f cdc-spark-streaming
```

## Health Check

The container includes a health check that verifies the Spark streaming job is running:

```bash
docker inspect --format='{{.State.Health.Status}}' cdc-spark-streaming
```

## Troubleshooting

### Container exits immediately

Check logs for startup errors:
```bash
docker logs cdc-spark-streaming
```

### Kafka connection timeout

Ensure Kafka is running and accessible:
```bash
docker exec cdc-spark-streaming nc -zv kafka 9092
```

### S3/MinIO connection errors

Verify MinIO is running and credentials are correct:
```bash
docker exec cdc-spark-streaming curl http://minio:9000/minio/health/live
```

### Iceberg table not found

The job will automatically create the table on first run. Check logs for schema creation messages.

## Performance Tuning

Adjust memory and CPU limits in [spark-streaming.yml](../../compose/streaming/spark-streaming.yml:73-77):

```yaml
deploy:
  resources:
    limits:
      memory: 2g
      cpus: '2.0'
```

For higher throughput, increase:
- `SPARK_DRIVER_MEMORY` and `SPARK_EXECUTOR_MEMORY`
- `spark.sql.shuffle.partitions` (default: 10)
- `maxOffsetsPerTrigger` in the Python job

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Kafka     │────▶│    Spark     │────▶│   Iceberg   │
│   Topic     │     │  Streaming   │     │   Table     │
│ (CDC Events)│     │  (Transform) │     │  (MinIO/S3) │
└─────────────┘     └──────────────┘     └─────────────┘
```

The Spark job:
1. Consumes unwrapped Debezium CDC events from Kafka
2. Applies transformations (name concatenation, location derivation)
3. Writes to Iceberg with exactly-once semantics
4. Maintains checkpoint for fault tolerance
