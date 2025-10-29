# Quickstart: Test Infrastructure Setup

**Feature**: Test Infrastructure Enhancement
**Date**: 2025-10-29

## Overview

This guide helps you set up the new test infrastructure (Apache Spark + Delta Lake, Prometheus AlertManager) to enable all CDC tests locally.

---

## Prerequisites

- **Docker Desktop** or **Docker Engine** + Docker Compose v2+
- **8GB RAM minimum** (16GB recommended)
- **Ports available**: 7077, 8080, 4040 (Spark), 9093, 9094 (AlertManager)
- **Disk space**: 5GB free for containers and Delta tables

---

## Quick Start (60 seconds)

```bash
# 1. Start all services including new infrastructure
docker-compose up -d

# 2. Wait for services to be healthy (~30 seconds)
docker-compose ps

# 3. Verify Spark Master is running
curl http://localhost:8080 | grep "Spark Master"

# 4. Verify AlertManager is running
curl http://localhost:9093/-/healthy

# 5. Run full test suite
make test
```

**Expected Results**:
- ✅ 0 failed tests (down from 12)
- ✅ < 20 skipped tests (down from 137)
- ✅ 148 passing tests
- ✅ Test completion in < 5 minutes

---

## Service URLs

### New Services

| Service | URL | Purpose |
|---------|-----|---------|
| **Spark Master UI** | http://localhost:8080 | Monitor Spark jobs, workers |
| **Spark Master RPC** | spark://localhost:7077 | Connect Spark clients |
| **Spark Job UI** | http://localhost:4040 | Active job details (when job running) |
| **AlertManager UI** | http://localhost:9093 | View/manage alerts and silences |
| **AlertManager API** | http://localhost:9093/api/v1 | Alert API endpoint |

### Existing Services (for reference)

| Service | URL | Purpose |
|---------|-----|---------|
| **Prometheus** | http://localhost:9090 | Metrics and alerting |
| **Grafana** | http://localhost:3000 | Dashboards (admin/admin) |
| **PostgreSQL** | localhost:5432 | CDC source |
| **MySQL** | localhost:3306 | CDC source |
| **Debezium** | http://localhost:8083 | Connector management |

---

## Detailed Setup Steps

### 1. Add Spark Service to Docker Compose

```yaml
# Add to docker-compose.yml
services:
  spark:
    image: bitnami/spark:3.5.2
    container_name: cdc-spark
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_HOST=spark
      - SPARK_DRIVER_MEMORY=1g
      - SPARK_EXECUTOR_MEMORY=1g
    ports:
      - "7077:7077"  # Spark master RPC
      - "8080:8080"  # Spark master UI
      - "4040:4040"  # Spark job UI
    volumes:
      - ./data/delta-tables:/opt/delta-tables
      - ./configs/spark:/opt/spark/conf:ro
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080"]
      interval: 30s
      timeout: 10s
      retries: 3
```

### 2. Add AlertManager Service to Docker Compose

```yaml
# Add to docker-compose.yml
services:
  alertmanager:
    image: prom/alertmanager:v0.26.0
    container_name: cdc-alertmanager
    command:
      - '--config.file=/etc/alertmanager/alertmanager.yml'
      - '--storage.path=/alertmanager'
    ports:
      - "9093:9093"  # UI and API
      - "9094:9094"  # Cluster port
    volumes:
      - ./configs/alertmanager:/etc/alertmanager:ro
      - alertmanager-data:/alertmanager
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:9093/-/healthy"]
      interval: 10s
      timeout: 5s
      retries: 3
```

### 3. Create Spark Configuration

```bash
# Create config directory
mkdir -p configs/spark

# Create spark-defaults.conf
cat > configs/spark/spark-defaults.conf <<EOF
# Delta Lake Configuration
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Delta Lake JAR
spark.jars.packages=io.delta:delta-core_2.12:3.0.0

# Warehouse location
spark.sql.warehouse.dir=/opt/delta-tables

# Logging
spark.eventLog.enabled=false
EOF
```

### 4. Create AlertManager Configuration

```bash
# Create config directory
mkdir -p configs/alertmanager

# Create alertmanager.yml
cat > configs/alertmanager/alertmanager.yml <<EOF
global:
  resolve_timeout: 5m

route:
  receiver: 'test-receiver'
  group_by: ['alertname', 'severity']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 1h

receivers:
  - name: 'test-receiver'
    webhook_configs:
      - url: 'http://localhost:5001/webhook'
        send_resolved: true
EOF
```

### 5. Update Prometheus to Connect to AlertManager

```yaml
# Update configs/prometheus/prometheus.yml
alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']
```

### 6. Install Python MySQL Connector

```bash
# Add dependency
poetry add mysql-connector-python@^8.0.33

# Or with pip
pip install mysql-connector-python==8.0.33
```

### 7. Start Services

```bash
# Start all services
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f spark alertmanager
```

---

## Running Tests

### Run Full Test Suite

```bash
make test
```

### Run Specific Test Categories

```bash
# Delta Lake tests only
pytest tests/unit/test_cdc_pipelines/test_delta_writer.py -v

# AlertManager tests only
pytest tests/integration/test_alerting.py::TestPrometheusAlerting -v

# MySQL CDC tests only
pytest tests/integration/test_mysql_cdc.py -v
```

### Run with Verbose Output

```bash
pytest tests/ -v --tb=short
```

---

## Verification Checklist

After setup, verify each component:

- [ ] **Spark Master UI** accessible at http://localhost:8080
  - Should show "Spark Master at spark://spark:7077"
  - Status: ALIVE
  - Workers: 0 (expected, standalone mode)

- [ ] **Spark can create Delta tables**
  ```bash
  # Should succeed without errors
  python -c "from pyspark.sql import SparkSession; \
    spark = SparkSession.builder.master('spark://localhost:7077') \
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.jars.packages', 'io.delta:delta-core_2.12:3.0.0') \
    .getOrCreate(); \
    print('Spark session created:', spark.version)"
  ```

- [ ] **AlertManager UI** accessible at http://localhost:9093
  - Should show AlertManager UI
  - Config loaded successfully

- [ ] **AlertManager API** responds
  ```bash
  curl http://localhost:9093/api/v1/status | jq .
  ```

- [ ] **MySQL connector** installed
  ```bash
  python -c "import mysql.connector; print('MySQL connector version:', mysql.connector.__version__)"
  ```

- [ ] **All tests pass**
  ```bash
  make test
  # Expected: 148 passed, 12 failed → 0, <20 skipped
  ```

---

## Troubleshooting

### Spark Master Won't Start

**Symptoms**: Container exits immediately or health check fails

**Solutions**:
```bash
# 1. Check logs
docker-compose logs spark

# 2. Verify memory settings
docker stats spark

# 3. Reduce memory if needed (edit docker-compose.yml)
SPARK_DRIVER_MEMORY=512m
SPARK_EXECUTOR_MEMORY=512m

# 4. Check port conflicts
lsof -i :7077
lsof -i :8080
```

### Delta Lake JAR Not Found

**Symptoms**: `ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension`

**Solutions**:
```bash
# 1. Verify spark-defaults.conf exists
cat configs/spark/spark-defaults.conf

# 2. Check volume mount
docker exec cdc-spark cat /opt/spark/conf/spark-defaults.conf

# 3. Manually download Delta JAR (if needed)
docker exec cdc-spark wget \
  https://repo1.maven.org/maven2/io/delta/delta-core_2.12/3.0.0/delta-core_2.12-3.0.0.jar \
  -P /opt/spark/jars/
```

### AlertManager Not Receiving Alerts

**Symptoms**: Tests fail with "Alert not found"

**Solutions**:
```bash
# 1. Check AlertManager logs
docker-compose logs alertmanager

# 2. Verify Prometheus connection
curl http://localhost:9090/api/v1/alertmanagers | jq .

# 3. Manually send test alert
curl -X POST http://localhost:9093/api/v1/alerts \
  -H 'Content-Type: application/json' \
  -d '[{
    "labels": {"alertname": "TestAlert", "severity": "info"},
    "annotations": {"summary": "Test"},
    "startsAt": "'$(date -u +%Y-%m-%dT%H:%M:%S.000Z)'"
  }]'

# 4. Check alerts were received
curl http://localhost:9093/api/v1/alerts | jq .
```

### MySQL Connector Import Error

**Symptoms**: `ModuleNotFoundError: No module named 'mysql'`

**Solutions**:
```bash
# 1. Verify installation
poetry show mysql-connector-python

# 2. Reinstall if needed
poetry remove mysql-connector-python
poetry add mysql-connector-python@^8.0.33

# 3. Check for conflicts
poetry check
```

### Tests Still Skipping

**Symptoms**: Tests show "skipped" status

**Solutions**:
```bash
# 1. Check skip reason
pytest tests/unit/test_cdc_pipelines/test_delta_writer.py -v -rs

# 2. Verify PySpark installed
python -c "import pyspark; print(pyspark.__version__)"

# 3. Install if missing
poetry add pyspark@^3.5.0

# 4. Check Delta Lake installed
python -c "import delta; print('Delta Lake OK')"
```

### Out of Memory Errors

**Symptoms**: `Container killed (OOMKilled)` or slow performance

**Solutions**:
```bash
# 1. Check current memory usage
docker stats

# 2. Increase Docker Desktop memory allocation
# Docker Desktop → Settings → Resources → Memory: 10GB

# 3. Reduce Spark memory (edit docker-compose.yml)
SPARK_DRIVER_MEMORY=512m
SPARK_EXECUTOR_MEMORY=512m

# 4. Run fewer tests in parallel
pytest -n 2  # Instead of default 4
```

---

## Performance Optimization

### Faster Test Execution

```bash
# Run tests in parallel (if RAM allows)
pytest -n 4

# Skip slow integration tests
pytest -m "not slow"

# Run only fast unit tests
pytest tests/unit/ -v
```

### Reduce Docker Resource Usage

```yaml
# Add resource limits to docker-compose.yml
services:
  spark:
    mem_limit: 2g
    cpus: "1.0"

  alertmanager:
    mem_limit: 256m
    cpus: "0.5"
```

### Clean Up Test Data

```bash
# Remove old Delta tables
rm -rf data/delta-tables/test_*

# Or add to Makefile
make clean-delta-tables
```

---

## Next Steps

1. **Read the full implementation plan**: [plan.md](./plan.md)
2. **Review service contracts**: [contracts/](./contracts/)
3. **Check test coverage**: `make test-coverage`
4. **Explore Spark UI**: http://localhost:8080
5. **Monitor alerts**: http://localhost:9093

---

## Support

**Issues?**
- Check [troubleshooting section](#troubleshooting) above
- Review service logs: `docker-compose logs <service>`
- Verify health: `docker-compose ps`
- Open GitHub issue with logs and error messages

**Performance Problems?**
- Ensure 8GB RAM minimum
- Check Docker Desktop resource allocation
- Reduce Spark memory settings if needed
- Run fewer parallel tests
