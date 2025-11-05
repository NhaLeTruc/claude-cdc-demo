# Modular Docker Compose Configuration

This directory contains a modularized Docker Compose setup for the CDC Demo project. The original monolithic `docker-compose.yml` (399 lines) has been split into organized component files for better maintainability.

## Structure

```
compose/
├── docker-compose.yml          # Main orchestrator file with includes
├── base/
│   ├── networks.yml            # Network definitions
│   └── volumes.yml             # Volume definitions (13 volumes)
├── databases/
│   ├── postgres.yml            # PostgreSQL with CDC enabled
│   └── mysql.yml               # MySQL with binlog enabled
├── streaming/
│   ├── kafka.yml               # Kafka + Zookeeper
│   └── schema-registry.yml     # Confluent Schema Registry
├── cdc/
│   ├── debezium.yml            # Debezium Kafka Connect
│   └── cdc-init.yml            # CDC initialization service
├── storage/
│   ├── minio.yml               # MinIO + init container
│   ├── iceberg.yml             # Apache Iceberg REST catalog
│   └── spark.yml               # Apache Spark
└── observability/
    ├── prometheus.yml          # Prometheus
    ├── grafana.yml             # Grafana
    ├── loki.yml                # Loki log aggregation
    └── alertmanager.yml        # AlertManager
```

## Services

The configuration includes 15 services:
- **Databases**: postgres, mysql
- **Streaming**: zookeeper, kafka, schema-registry
- **CDC**: debezium, cdc-init
- **Storage**: minio, minio-init, iceberg-rest, spark
- **Observability**: prometheus, grafana, loki, alertmanager

## Usage

All commands use the modular setup via the Makefile:

```bash
# Start all services
make start

# Stop all services
make stop

# View service status
make status

# View logs (all or specific service)
make logs
make logs SERVICE=kafka

# Build images
make build

# Clean up
make clean
```

Or use docker compose directly (note: requires `--project-directory .` to resolve paths correctly):

```bash
docker compose -f compose/docker-compose.yml --project-directory . up -d
docker compose -f compose/docker-compose.yml --project-directory . ps
docker compose -f compose/docker-compose.yml --project-directory . down
```

## Configuration

All configuration values are sourced from `.env` in the project root. The compose files use environment variable substitution with sensible defaults.

## Benefits

1. **Modularity**: Easy to locate and update specific services
2. **Reusability**: Can start subsets of services by including only needed files
3. **Maintainability**: Clear separation of concerns by functional area
4. **Environment-driven**: All configs from `.env` with no hardcoded values
5. **Version control**: Easier to review changes to specific components

## Migration

The original `docker-compose.yml` has been backed up to `docker-compose.yml.backup`. After validating this setup works correctly, the backup can be removed.
