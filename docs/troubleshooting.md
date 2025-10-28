# Troubleshooting Guide

This guide covers common issues and their solutions when running the CDC demo project.

## Table of Contents

- [Quick Diagnostics](#quick-diagnostics)
- [Docker Issues](#docker-issues)
- [PostgreSQL CDC Issues](#postgresql-cdc-issues)
- [MySQL CDC Issues](#mysql-cdc-issues)
- [Kafka Issues](#kafka-issues)
- [Debezium Connector Issues](#debezium-connector-issues)
- [Spark Issues](#spark-issues)
- [DeltaLake Issues](#deltalake-issues)
- [Iceberg Issues](#iceberg-issues)
- [Monitoring Issues](#monitoring-issues)
- [Performance Issues](#performance-issues)
- [Data Quality Issues](#data-quality-issues)

---

## Quick Diagnostics

### Check All Services Status

```bash
# Check all containers are running
docker-compose ps

# Expected output: All services should be "Up"
# postgres, mysql, kafka, zookeeper, debezium-connect,
# spark-master, minio, prometheus, grafana, loki
```

### Check Logs for All Services

```bash
# View logs for all services
docker-compose logs --tail=100

# View logs for specific service
docker-compose logs postgres --tail=50
docker-compose logs debezium-connect --tail=50
```

### Verify Network Connectivity

```bash
# Test connectivity between services
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Test Postgres connection
docker exec postgres psql -U postgres -c "SELECT 1"

# Test MySQL connection
docker exec mysql mysql -uroot -pmysql -e "SELECT 1"
```

---

## Docker Issues

### Issue: Services Won't Start

**Symptoms:**
```
ERROR: for postgres  Cannot start service postgres: driver failed
```

**Causes:**
1. Insufficient Docker resources
2. Port conflicts
3. Volume permission issues

**Solutions:**

```bash
# 1. Check Docker resources
docker system df
docker system prune  # Clean up unused resources

# 2. Check for port conflicts
lsof -i :5432  # PostgreSQL
lsof -i :3306  # MySQL
lsof -i :9092  # Kafka
lsof -i :3000  # Grafana

# Kill conflicting processes
kill -9 <PID>

# 3. Fix volume permissions
sudo chown -R $USER:$USER ./data
chmod -R 755 ./data

# 4. Restart Docker
sudo systemctl restart docker  # Linux
# or restart Docker Desktop on Mac/Windows

# 5. Clean restart
docker-compose down -v
docker-compose up -d
```

### Issue: Container Keeps Restarting

**Symptoms:**
```
postgres       Up 2 seconds (health: starting)
postgres       Restarting (1) 10 seconds ago
```

**Solutions:**

```bash
# Check container logs for errors
docker-compose logs postgres

# Common errors and fixes:

# 1. "database system was shut down incorrectly"
docker-compose down
docker volume rm claude-cdc-demo_postgres_data
docker-compose up -d postgres

# 2. "port 5432 already in use"
# Stop local PostgreSQL
sudo systemctl stop postgresql  # Linux
brew services stop postgresql   # Mac

# 3. Out of memory
# Increase Docker memory limit in Docker Desktop settings
# Minimum: 8GB RAM
```

### Issue: Volumes Not Persisting Data

**Symptoms:**
- Data disappears after restart
- Configuration changes don't persist

**Solutions:**

```bash
# 1. Check volume mounts
docker-compose config | grep volumes

# 2. Verify volumes exist
docker volume ls | grep claude-cdc-demo

# 3. Recreate volumes
docker-compose down
docker volume prune  # WARNING: Deletes all unused volumes
docker-compose up -d
```

---

## PostgreSQL CDC Issues

### Issue: Logical Replication Not Working

**Symptoms:**
```
ERROR: logical decoding requires wal_level >= logical
```

**Solutions:**

```bash
# 1. Check current wal_level
docker exec postgres psql -U postgres -c "SHOW wal_level"

# 2. Update postgresql.conf (should be in docker-compose.yml)
docker exec postgres cat /var/lib/postgresql/data/postgresql.conf | grep wal_level

# Expected:
# wal_level = logical

# 3. If not set, update docker-compose.yml:
# postgres:
#   command:
#     - "postgres"
#     - "-c"
#     - "wal_level=logical"

# 4. Restart PostgreSQL
docker-compose restart postgres
```

### Issue: Replication Slot Full

**Symptoms:**
```
ERROR: replication slot "debezium_slot" is active for PID 1234
ERROR: could not create replication slot "debezium_slot": slot already exists
```

**Solutions:**

```bash
# 1. Check replication slots
docker exec postgres psql -U postgres -d demo_db -c \
  "SELECT * FROM pg_replication_slots"

# 2. Check slot lag
docker exec postgres psql -U postgres -d demo_db -c \
  "SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
   FROM pg_replication_slots"

# 3. If slot is inactive, drop and recreate
docker exec postgres psql -U postgres -d demo_db -c \
  "SELECT pg_drop_replication_slot('debezium_slot')"

# 4. Restart Debezium connector to recreate slot
curl -X DELETE http://localhost:8083/connectors/postgres-customers-connector
# Recreate connector via API or UI
```

### Issue: High WAL Growth

**Symptoms:**
- Disk filling up rapidly
- PostgreSQL performance degradation

**Solutions:**

```bash
# 1. Check WAL size
docker exec postgres psql -U postgres -c \
  "SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0'))"

# 2. Check replication lag
docker exec postgres psql -U postgres -d demo_db -c \
  "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
   FROM pg_replication_slots"

# 3. If Debezium is lagging, check connector status
curl http://localhost:8083/connectors/postgres-customers-connector/status

# 4. Force WAL archiving (emergency)
docker exec postgres psql -U postgres -c \
  "SELECT pg_switch_wal()"

# 5. Adjust max_wal_size in postgresql.conf
# max_wal_size = 2GB
```

### Issue: Initial Snapshot Stuck

**Symptoms:**
```
INFO: Starting snapshot for table public.customers
(no progress for > 10 minutes)
```

**Solutions:**

```bash
# 1. Check snapshot mode
curl http://localhost:8083/connectors/postgres-customers-connector | jq '.config."snapshot.mode"'

# 2. Use incremental snapshot mode for large tables
curl -X PUT http://localhost:8083/connectors/postgres-customers-connector/config \
  -H "Content-Type: application/json" \
  -d '{
    "snapshot.mode": "initial_only",
    "snapshot.fetch.size": 10000
  }'

# 3. Check for long-running transactions blocking snapshot
docker exec postgres psql -U postgres -d demo_db -c \
  "SELECT pid, state, query_start, query
   FROM pg_stat_activity
   WHERE state != 'idle'
   ORDER BY query_start"

# 4. Cancel blocking query (if safe)
docker exec postgres psql -U postgres -d demo_db -c \
  "SELECT pg_cancel_backend(<PID>)"
```

---

## MySQL CDC Issues

### Issue: Binlog Not Enabled

**Symptoms:**
```
ERROR: Binary logging is not enabled on the MySQL server
```

**Solutions:**

```bash
# 1. Check binlog status
docker exec mysql mysql -uroot -pmysql -e "SHOW VARIABLES LIKE 'log_bin'"

# Expected: log_bin = ON

# 2. Update docker-compose.yml with binlog settings:
# mysql:
#   command:
#     - "--log-bin=mysql-bin"
#     - "--binlog-format=ROW"
#     - "--server-id=1"

# 3. Restart MySQL
docker-compose restart mysql

# 4. Verify binlog format
docker exec mysql mysql -uroot -pmysql -e "SHOW VARIABLES LIKE 'binlog_format'"

# Expected: binlog_format = ROW
```

### Issue: Binlog Position Lost

**Symptoms:**
```
ERROR: Cannot find binlog position mysql-bin.000005:12345
```

**Solutions:**

```bash
# 1. Check current binlog position
docker exec mysql mysql -uroot -pmysql -e "SHOW MASTER STATUS"

# 2. Check available binlogs
docker exec mysql mysql -uroot -pmysql -e "SHOW BINARY LOGS"

# 3. If binlog expired, reset connector offset
curl -X DELETE http://localhost:8083/connectors/mysql-orders-connector

# 4. Recreate connector with new snapshot
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-orders-connector",
    "config": {
      "snapshot.mode": "initial"
    }
  }'
```

### Issue: GTID Inconsistencies

**Symptoms:**
```
ERROR: GTID set is inconsistent
```

**Solutions:**

```bash
# 1. Check GTID mode
docker exec mysql mysql -uroot -pmysql -e "SHOW VARIABLES LIKE 'gtid_mode'"

# 2. Enable GTID if needed (requires restart)
# Update docker-compose.yml:
# mysql:
#   command:
#     - "--gtid-mode=ON"
#     - "--enforce-gtid-consistency=ON"

# 3. Reset GTID state (WARNING: data loss possible)
docker exec mysql mysql -uroot -pmysql -e "RESET MASTER"
```

---

## Kafka Issues

### Issue: Kafka Not Starting

**Symptoms:**
```
ERROR: Cannot connect to Zookeeper
```

**Solutions:**

```bash
# 1. Ensure Zookeeper is running first
docker-compose up -d zookeeper
sleep 10
docker-compose up -d kafka

# 2. Check Zookeeper connection
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# 3. Check Kafka logs
docker-compose logs kafka | grep ERROR

# 4. Clean Kafka data (WARNING: deletes all topics)
docker-compose down
docker volume rm claude-cdc-demo_kafka_data
docker-compose up -d
```

### Issue: Topic Not Found

**Symptoms:**
```
ERROR: Topic 'debezium.public.customers' not found
```

**Solutions:**

```bash
# 1. List all topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# 2. Create topic manually
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic debezium.public.customers \
  --partitions 3 \
  --replication-factor 1

# 3. Check Debezium connector config
curl http://localhost:8083/connectors/postgres-customers-connector/config | jq '.["topic.prefix"]'

# Expected: debezium
```

### Issue: High Consumer Lag

**Symptoms:**
- Events delayed by minutes/hours
- Kafka disk filling up

**Solutions:**

```bash
# 1. Check consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group spark-streaming-consumer \
  --describe

# 2. Increase consumer parallelism (Spark config)
# spark.streaming.kafka.maxRatePerPartition=1000

# 3. Check for slow consumers
docker-compose logs spark-streaming | grep "Processing time"

# 4. Increase partition count for high-volume topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --alter \
  --topic debezium.public.customers \
  --partitions 6
```

---

## Debezium Connector Issues

### Issue: Connector Failed to Start

**Symptoms:**
```
curl http://localhost:8083/connectors/postgres-customers-connector/status
{
  "state": "FAILED",
  "worker_id": "connect:8083"
}
```

**Solutions:**

```bash
# 1. Check connector logs
docker-compose logs debezium-connect | grep ERROR

# 2. Common error: database connection
# Solution: Verify database credentials
curl http://localhost:8083/connectors/postgres-customers-connector/config | jq

# 3. Common error: missing plugin
# Solution: Verify Debezium plugin installed
docker exec debezium-connect ls /kafka/connect/debezium-connector-postgres

# 4. Restart connector
curl -X POST http://localhost:8083/connectors/postgres-customers-connector/restart

# 5. Delete and recreate connector
curl -X DELETE http://localhost:8083/connectors/postgres-customers-connector
# Recreate with correct config
```

### Issue: Connector Task Failed

**Symptoms:**
```
{
  "state": "RUNNING",
  "tasks": [
    {
      "id": 0,
      "state": "FAILED",
      "worker_id": "connect:8083"
    }
  ]
}
```

**Solutions:**

```bash
# 1. Check task error
curl http://localhost:8083/connectors/postgres-customers-connector/status | jq '.tasks[0].trace'

# 2. Restart specific task
curl -X POST http://localhost:8083/connectors/postgres-customers-connector/tasks/0/restart

# 3. Increase task timeout
curl -X PUT http://localhost:8083/connectors/postgres-customers-connector/config \
  -H "Content-Type: application/json" \
  -d '{
    "errors.tolerance": "all",
    "errors.log.enable": true,
    "errors.deadletterqueue.topic.name": "dlq"
  }'
```

### Issue: Schema Registry Errors

**Symptoms:**
```
ERROR: Failed to serialize Avro record
```

**Solutions:**

```bash
# 1. Check Schema Registry is running
curl http://localhost:8081/subjects

# 2. Check schema for topic
curl http://localhost:8081/subjects/debezium.public.customers-value/versions/latest

# 3. Delete incompatible schema (WARNING: may break consumers)
curl -X DELETE http://localhost:8081/subjects/debezium.public.customers-value/versions/latest

# 4. Configure connector for JSON instead of Avro
curl -X PUT http://localhost:8083/connectors/postgres-customers-connector/config \
  -H "Content-Type: application/json" \
  -d '{
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
  }'
```

---

## Spark Issues

### Issue: Spark Job Failing

**Symptoms:**
```
pyspark.sql.utils.AnalysisException: Table or view not found
```

**Solutions:**

```bash
# 1. Check Spark logs
docker-compose logs spark-master
docker-compose logs spark-worker

# 2. Verify Spark can connect to Kafka
docker exec spark-master /opt/spark/bin/spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0

# 3. Check Iceberg catalog configuration
docker exec spark-master cat /opt/spark/conf/spark-defaults.conf

# 4. Test connectivity to MinIO
docker exec spark-master curl http://minio:9000

# 5. Increase executor memory
# Update docker-compose.yml:
# spark-worker:
#   environment:
#     - SPARK_WORKER_MEMORY=4g
```

### Issue: Out of Memory Errors

**Symptoms:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**

```bash
# 1. Increase driver memory
# spark.driver.memory=4g

# 2. Increase executor memory
# spark.executor.memory=4g

# 3. Reduce batch size
# spark.sql.shuffle.partitions=200
# spark.streaming.kafka.maxRatePerPartition=100

# 4. Enable off-heap memory
# spark.memory.offHeap.enabled=true
# spark.memory.offHeap.size=2g
```

---

## DeltaLake Issues

### Issue: Change Data Feed Not Enabled

**Symptoms:**
```
AnalysisException: Change data feed is not enabled on table
```

**Solutions:**

```python
# 1. Enable CDF on existing table
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/tmp/delta/customers")
delta_table.alter().set("delta.enableChangeDataFeed", "true").execute()

# 2. For new tables, enable at creation
spark.sql("""
  CREATE TABLE customers (...)
  USING delta
  TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# 3. Verify CDF is enabled
spark.sql("DESCRIBE DETAIL customers").select("properties").show(truncate=False)
```

### Issue: Corrupted Delta Log

**Symptoms:**
```
ERROR: Delta log is corrupted at version 123
```

**Solutions:**

```bash
# 1. Run Delta optimize and vacuum
spark.sql("OPTIMIZE customers")
spark.sql("VACUUM customers RETAIN 168 HOURS")  # 7 days

# 2. Repair table (last resort)
spark.sql("MSCK REPAIR TABLE customers")

# 3. Restore from backup
# Use time travel to restore to last good version
spark.read.format("delta").option("versionAsOf", 122).table("customers").write...
```

---

## Iceberg Issues

### Issue: Catalog Not Found

**Symptoms:**
```
NoSuchNamespaceException: Namespace 'cdc_demo' does not exist
```

**Solutions:**

```python
from pyiceberg.catalog import load_catalog

# 1. Create catalog if it doesn't exist
catalog = load_catalog("demo_catalog", **{
    "type": "rest",
    "uri": "http://localhost:8181",
    "warehouse": "s3://warehouse"
})

# 2. Create namespace
catalog.create_namespace("cdc_demo")

# 3. Verify namespace exists
catalog.list_namespaces()
```

### Issue: S3/MinIO Connection Failed

**Symptoms:**
```
AWSClientException: Unable to execute HTTP request
```

**Solutions:**

```bash
# 1. Check MinIO is running
curl http://localhost:9000

# 2. Verify credentials
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin

# 3. Check bucket exists
docker exec minio mc ls local/warehouse

# 4. Create bucket if missing
docker exec minio mc mb local/warehouse
```

### Issue: Snapshot Expiration Removing Active Data

**Symptoms:**
- Recent data missing after `expire_snapshots()`

**Solutions:**

```python
from datetime import datetime, timedelta

# 1. Check retention policy
table.history()

# 2. Use conservative retention
table.expire_snapshots() \
    .expire_older_than(datetime.now() - timedelta(days=30)) \
    .retain_last(100) \
    .commit()

# 3. Recover from time travel (if within retention)
spark.read.format("iceberg") \
    .option("snapshot-id", snapshot_id) \
    .table("cdc_demo.customers")
```

---

## Monitoring Issues

### Issue: Grafana Not Showing Data

**Symptoms:**
- Empty dashboards
- "No data" message

**Solutions:**

```bash
# 1. Check Prometheus is scraping
curl http://localhost:9090/api/v1/targets

# 2. Verify metrics exist
curl http://localhost:9090/api/v1/query?query=cdc_lag_seconds

# 3. Check Grafana datasource
# Navigate to: http://localhost:3000/datasources
# Verify Prometheus URL: http://prometheus:9090

# 4. Reimport dashboards
cd docker/observability/grafana/dashboards
for f in *.json; do
  curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
    -H "Content-Type: application/json" \
    -d @$f
done
```

### Issue: Alerts Not Firing

**Symptoms:**
- CDC lag high but no alert

**Solutions:**

```bash
# 1. Check Alertmanager is running
curl http://localhost:9093/api/v2/status

# 2. Verify alert rules loaded
curl http://localhost:9090/api/v1/rules

# 3. Check if alert is pending/firing
curl http://localhost:9090/api/v1/alerts

# 4. Test Alertmanager routing
curl -X POST http://localhost:9093/api/v1/alerts \
  -H "Content-Type: application/json" \
  -d '[{"labels":{"alertname":"TestAlert","severity":"critical"}}]'

# 5. Check notification receivers
curl http://localhost:9093/api/v2/receivers
```

---

## Performance Issues

### Issue: High CDC Lag (> 30 seconds)

**Diagnostic Steps:**

```bash
# 1. Check Kafka consumer lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group spark-streaming-consumer --describe

# 2. Check Debezium connector lag
curl http://localhost:8083/connectors/postgres-customers-connector/status

# 3. Check database load
docker exec postgres psql -U postgres -c \
  "SELECT * FROM pg_stat_activity WHERE state = 'active'"

# 4. Check Spark processing time
docker-compose logs spark-streaming | grep "Batch processing time"
```

**Solutions:**

```bash
# 1. Increase Kafka partitions
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --alter --topic debezium.public.customers --partitions 6

# 2. Increase Spark parallelism
# spark.default.parallelism=12
# spark.sql.shuffle.partitions=24

# 3. Optimize database queries
# Add indexes on frequently queried columns

# 4. Increase connector poll interval
curl -X PUT http://localhost:8083/connectors/postgres-customers-connector/config \
  -d '{"poll.interval.ms": "100"}'
```

### Issue: High Memory Usage

**Solutions:**

```bash
# 1. Check container memory usage
docker stats

# 2. Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory → 12GB

# 3. Optimize Spark memory
# spark.executor.memory=2g
# spark.driver.memory=2g
# spark.memory.fraction=0.6

# 4. Enable Kafka log compaction
docker exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --alter --entity-type topics \
  --entity-name debezium.public.customers \
  --add-config cleanup.policy=compact,min.compaction.lag.ms=86400000
```

---

## Data Quality Issues

### Issue: Row Count Mismatch

**Diagnostic:**

```python
# Check source count
import psycopg2
conn = psycopg2.connect(host='localhost', database='demo_db', user='postgres', password='postgres')
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM customers")
source_count = cursor.fetchone()[0]

# Check destination count
from pyiceberg.catalog import load_catalog
catalog = load_catalog("demo_catalog")
table = catalog.load_table("cdc_demo.customers_analytics")
dest_count = table.scan().to_arrow().num_rows

print(f"Source: {source_count}, Destination: {dest_count}, Diff: {source_count - dest_count}")
```

**Solutions:**

```bash
# 1. Check for failed messages in DLQ (Dead Letter Queue)
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic dlq --from-beginning --max-messages 10

# 2. Check Spark processing errors
docker-compose logs spark-streaming | grep ERROR

# 3. Re-snapshot connector
curl -X DELETE http://localhost:8083/connectors/postgres-customers-connector
# Recreate with snapshot.mode=initial

# 4. Run data validation
python scripts/validate-data-integrity.py --pipeline postgres_customers_cdc
```

### Issue: Schema Mismatch

**Solutions:**

```bash
# 1. Compare schemas
docker exec postgres psql -U postgres -d demo_db -c \
  "\\d+ customers"

# vs Iceberg schema:
catalog.load_table("cdc_demo.customers_analytics").schema()

# 2. Update Iceberg schema
table.update_schema() \
    .add_column("new_column", "string") \
    .commit()

# 3. Refresh connector schema
curl -X POST http://localhost:8083/connectors/postgres-customers-connector/tasks/0/restart
```

---

## Getting Help

### Collect Diagnostic Information

```bash
#!/bin/bash
# Save diagnostic information

# 1. Container status
docker-compose ps > diagnostics.txt

# 2. Logs for all services
for service in postgres mysql kafka debezium-connect spark-master; do
  echo "=== $service ===" >> diagnostics.txt
  docker-compose logs --tail=200 $service >> diagnostics.txt
done

# 3. Connector status
curl http://localhost:8083/connectors >> diagnostics.txt
curl http://localhost:8083/connectors/postgres-customers-connector/status >> diagnostics.txt

# 4. Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list >> diagnostics.txt

# 5. Database status
docker exec postgres psql -U postgres -c "SELECT * FROM pg_replication_slots" >> diagnostics.txt
```

### Open GitHub Issue

When reporting issues, include:
1. `diagnostics.txt` file (see above)
2. Steps to reproduce
3. Expected vs actual behavior
4. OS and Docker version (`docker --version`)
5. Relevant error messages

### Community Resources

- **GitHub Issues**: https://github.com/yourorg/claude-cdc-demo/issues
- **Discussions**: https://github.com/yourorg/claude-cdc-demo/discussions
- **Debezium Community**: https://debezium.io/community/
- **Kafka Community**: https://kafka.apache.org/community
