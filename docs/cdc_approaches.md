# CDC Approaches Comparison

This document compares the different Change Data Capture (CDC) approaches implemented in this project.

## Overview

Change Data Capture (CDC) is a pattern for tracking and capturing changes to data in a database. This project demonstrates four distinct CDC approaches across different storage technologies:

1. **Logical Replication** (PostgreSQL)
2. **Binary Log Parsing** (MySQL)
3. **Change Data Feed** (DeltaLake)
4. **Snapshot-based Incremental Read** (Apache Iceberg)

## Comparison Matrix

| Approach | Technology | Latency | Resource Overhead | Setup Complexity | Schema Evolution | Data Loss Risk |
|----------|-----------|---------|-------------------|------------------|------------------|----------------|
| **Logical Replication** | PostgreSQL + Debezium | < 1s | Low | Medium | Automatic | Very Low |
| **Binlog Parsing** | MySQL + Debezium | < 1s | Low | Medium | Automatic | Very Low |
| **Change Data Feed** | DeltaLake | 1-5s | Medium | Low | Automatic | Very Low |
| **Snapshot-based** | Iceberg | 5-30s | High | Low | Manual | Low |

## Detailed Analysis

### 1. Logical Replication (PostgreSQL)

**How it works:**
- PostgreSQL's Write-Ahead Log (WAL) contains all database changes
- Logical decoding plugin (pgoutput) reads WAL and converts to logical change events
- Debezium connector consumes changes and publishes to Kafka
- Near real-time streaming of INSERT/UPDATE/DELETE operations

**Pros:**
- ✅ Very low latency (< 1 second)
- ✅ Minimal performance impact on source database
- ✅ Automatic schema evolution handling
- ✅ Captures all DML operations (INSERT/UPDATE/DELETE)
- ✅ Transactional consistency (respects COMMIT/ROLLBACK)
- ✅ Battle-tested in production environments

**Cons:**
- ❌ Requires PostgreSQL-specific configuration (wal_level = logical)
- ❌ Replication slots can fill up if consumer lags
- ❌ Complex initial snapshot process for existing data
- ❌ Requires Debezium/Kafka infrastructure
- ❌ Limited to PostgreSQL databases

**Best for:**
- Real-time analytics and dashboards
- Event-driven microservices
- Audit logging and compliance
- Data synchronization across systems
- OLTP to OLAP replication

**Configuration Example:**
```yaml
connector.class: io.debezium.connector.postgresql.PostgresConnector
database.hostname: postgres
database.dbname: demo_db
plugin.name: pgoutput
publication.autocreate.mode: filtered
slot.name: debezium_slot
```

**Performance Characteristics:**
- **Latency**: 100-500ms typical
- **Throughput**: 10,000+ events/sec on standard hardware
- **Resource Usage**: ~100MB memory per connector

---

### 2. Binary Log Parsing (MySQL)

**How it works:**
- MySQL's binary log (binlog) records all database modifications
- Debezium connector reads binlog as if it were a MySQL replica
- Changes are parsed and published to Kafka as structured events
- Supports row-based and mixed binlog formats

**Pros:**
- ✅ Very low latency (< 1 second)
- ✅ Minimal impact on MySQL performance
- ✅ Automatic schema evolution tracking
- ✅ Captures all DML operations with before/after values
- ✅ Proven at scale (used by major companies)
- ✅ No triggers or custom code in database

**Cons:**
- ❌ Requires MySQL binlog enabled (binlog_format = ROW)
- ❌ Binlog retention policy must be managed
- ❌ Initial snapshot can be slow for large tables
- ❌ Requires additional infrastructure (Debezium, Kafka)
- ❌ Limited to MySQL-compatible databases

**Best for:**
- E-commerce order tracking
- Inventory management systems
- User activity streams
- Cross-region data replication
- Legacy MySQL migration to modern data warehouse

**Configuration Example:**
```yaml
connector.class: io.debezium.connector.mysql.MySqlConnector
database.hostname: mysql
database.user: debezium
binlog.format: ROW
snapshot.mode: initial
gtid.source.includes: .*
```

**Performance Characteristics:**
- **Latency**: 200-800ms typical
- **Throughput**: 8,000+ events/sec
- **Resource Usage**: ~150MB memory per connector

---

### 3. Change Data Feed (DeltaLake)

**How it works:**
- DeltaLake maintains transaction log with all table changes
- Change Data Feed (CDF) feature tracks row-level changes (INSERT/UPDATE/DELETE)
- Changes stored alongside data files in Delta table
- Spark Structured Streaming reads CDF incrementally
- Native integration with Spark ecosystem

**Pros:**
- ✅ Native to DeltaLake - no external tools required
- ✅ Time travel capabilities (query historical changes)
- ✅ Efficient storage (only stores changed rows)
- ✅ Automatic schema evolution within Delta
- ✅ ACID transactions guarantee consistency
- ✅ Works seamlessly with Spark pipelines
- ✅ Built-in deduplication and exactly-once semantics

**Cons:**
- ❌ Higher latency than binlog/logical replication (1-5s)
- ❌ Requires Spark for processing
- ❌ CDF must be enabled at table creation
- ❌ Higher storage overhead (stores change data separately)
- ❌ Not suitable for non-Spark consumers
- ❌ Limited to DeltaLake tables

**Best for:**
- Lakehouse architectures
- Batch + streaming pipelines
- Data science workflows
- ETL/ELT with Spark
- Historical change analysis
- Compliance and audit requirements

**Configuration Example:**
```sql
CREATE TABLE customers (
  customer_id BIGINT,
  email STRING,
  name STRING
)
USING delta
TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

**Usage Example:**
```python
# Read changes between versions
changes = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .option("endingVersion", 20) \
    .table("customers")

changes.filter("_change_type = 'update_postimage'").show()
```

**Performance Characteristics:**
- **Latency**: 1-5 seconds
- **Throughput**: 5,000+ events/sec
- **Resource Usage**: Depends on Spark cluster size

---

### 4. Snapshot-based Incremental Read (Apache Iceberg)

**How it works:**
- Iceberg maintains immutable snapshots of table state
- Each write operation creates a new snapshot with metadata
- Incremental read compares two snapshots to identify changes
- Reads only data files that changed between snapshots
- Requires custom logic to determine operation type (INSERT/UPDATE/DELETE)

**Pros:**
- ✅ Simple architecture - no external CDC tools
- ✅ Works with any query engine (Spark, Trino, Flink, Presto)
- ✅ Efficient partition pruning reduces scan overhead
- ✅ Time travel to any historical snapshot
- ✅ Schema evolution well-supported
- ✅ Hidden partitioning for optimized reads
- ✅ Snapshot expiration manages storage costs

**Cons:**
- ❌ Higher latency (5-30 seconds minimum)
- ❌ Requires full table scan for non-partitioned changes
- ❌ Manual tracking of last processed snapshot
- ❌ Cannot distinguish UPDATE from DELETE + INSERT
- ❌ Higher compute cost for large tables
- ❌ No built-in change type metadata

**Best for:**
- Near-real-time analytics (5-30 second latency acceptable)
- Periodic batch processing (hourly, daily)
- Data lake consolidation
- Multi-engine data access
- Large-scale analytical workloads
- Cost-sensitive environments (no Kafka infrastructure)

**Usage Example:**
```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("demo_catalog")
table = catalog.load_table("cdc_demo.customers")

# Get snapshots
snapshots = table.snapshots()
snapshot_1 = snapshots[-2]  # Previous
snapshot_2 = snapshots[-1]  # Current

# Read changes
changes = table.scan(
    snapshot_id=snapshot_2.snapshot_id,
    ref=snapshot_1.snapshot_id
).to_arrow()

# Process changed records
for record in changes:
    # Custom logic to determine operation type
    process_change(record)
```

**Performance Characteristics:**
- **Latency**: 5-30 seconds (depending on table size)
- **Throughput**: 1,000-3,000 events/sec
- **Resource Usage**: Scales with table size and partition count

---

## Use Case Recommendations

### Real-Time Event Streaming (< 1 second latency)
**Choose:** Logical Replication (PostgreSQL) or Binlog Parsing (MySQL)

**Rationale:** Sub-second latency required for real-time dashboards, fraud detection, instant notifications.

**Example:** E-commerce fraud detection system needs to analyze transactions within 500ms.

---

### Lakehouse Analytics (1-10 second latency)
**Choose:** Change Data Feed (DeltaLake)

**Rationale:** Native integration with Spark, ACID guarantees, built-in time travel, acceptable latency for analytics.

**Example:** Customer 360 view updated every 5 seconds from operational databases.

---

### Batch-Oriented Workflows (> 10 second latency)
**Choose:** Snapshot-based (Iceberg)

**Rationale:** Simple architecture, no Kafka overhead, works with multiple engines, cost-effective.

**Example:** Nightly data warehouse refresh from production databases.

---

### Multi-Database Replication
**Choose:** Logical Replication + Binlog Parsing (via Debezium)

**Rationale:** Unified CDC platform supports multiple source databases, consistent event format, centralized monitoring.

**Example:** Consolidating data from 10 PostgreSQL + 5 MySQL databases into a central data lake.

---

### Cost-Optimized CDC
**Choose:** Snapshot-based (Iceberg) or Change Data Feed (DeltaLake)

**Rationale:** No Kafka infrastructure costs, storage-based CDC, pay only for compute when processing.

**Example:** Startup with limited budget needs CDC for 50 tables, processes changes hourly.

---

### Compliance and Audit
**Choose:** Logical Replication or Change Data Feed

**Rationale:** Immutable change log, captures before/after values, supports time travel, meets regulatory requirements.

**Example:** Financial services company needs 7-year audit trail of all customer data changes.

---

## Implementation Complexity

### Setup Time (Greenfield Project)

1. **DeltaLake CDF**: 30 minutes
   - Enable CDF on table creation
   - Configure Spark Structured Streaming

2. **Iceberg Snapshots**: 45 minutes
   - Setup Iceberg catalog
   - Implement snapshot tracking logic

3. **PostgreSQL Logical Replication**: 2 hours
   - Configure PostgreSQL (wal_level, replication slot)
   - Setup Debezium connector
   - Configure Kafka

4. **MySQL Binlog**: 2 hours
   - Configure MySQL binlog settings
   - Setup Debezium connector
   - Configure Kafka

### Operational Complexity (Day 2+)

**Low Complexity:**
- DeltaLake CDF (native to table, minimal monitoring)
- Iceberg Snapshots (stateless, simple retry logic)

**Medium Complexity:**
- PostgreSQL Logical Replication (monitor replication slots, WAL growth)
- MySQL Binlog (manage binlog retention, GTID tracking)

**High Complexity:**
- Multi-database Debezium deployment (Kafka cluster, Schema Registry, connector management)

---

## Performance Optimization Tips

### Logical Replication (PostgreSQL)
```sql
-- Increase WAL segment size to reduce overhead
ALTER SYSTEM SET wal_segment_size = '64MB';

-- Configure replication slot for better performance
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- Monitor slot lag
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;
```

### Binlog Parsing (MySQL)
```sql
-- Optimize binlog format
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';

-- Configure retention
SET GLOBAL expire_logs_days = 7;

-- Monitor binlog size
SHOW BINARY LOGS;
```

### Change Data Feed (DeltaLake)
```python
# Optimize CDF reads with partition pruning
changes = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", "2024-01-01") \
    .table("customers") \
    .where("region = 'US'")  # Partition pruning
```

### Snapshot-based (Iceberg)
```python
# Use partition evolution for better pruning
table.update_spec() \
    .add_field("event_date", "day") \
    .commit()

# Expire old snapshots to reduce metadata overhead
table.expire_snapshots() \
    .expire_older_than(datetime.now() - timedelta(days=7)) \
    .commit()
```

---

## Hybrid Approaches

### PostgreSQL → Kafka → Iceberg (This Project)
**Combines strengths of multiple approaches:**

1. **Capture**: Logical Replication (low latency, transactional consistency)
2. **Transport**: Kafka (decoupling, buffering, replay)
3. **Storage**: Iceberg (analytics-optimized, time travel, multi-engine)

**Architecture:**
```
PostgreSQL (OLTP)
    ↓ Logical Replication (Debezium)
Kafka Topics
    ↓ Spark Structured Streaming
Iceberg Tables (OLAP)
    ↓ Query Engines (Trino, Spark, Flink)
Analytics / ML
```

**Benefits:**
- Real-time capture from OLTP
- Buffering and replay via Kafka
- Analytics-optimized storage in Iceberg
- Schema evolution handled end-to-end
- Separation of concerns (capture vs storage)

**Trade-offs:**
- Increased infrastructure complexity
- Higher operational costs
- More components to monitor
- End-to-end latency: 2-10 seconds

---

## Decision Tree

```
Do you need real-time CDC (< 1 second)?
├─ YES → Use Debezium (Logical Replication or Binlog)
│   ├─ PostgreSQL source? → Logical Replication
│   └─ MySQL source? → Binlog Parsing
│
└─ NO → Can you tolerate 5-30 second latency?
    ├─ YES → Are you using a lakehouse format?
    │   ├─ DeltaLake? → Use Change Data Feed
    │   └─ Iceberg? → Use Snapshot-based CDC
    │
    └─ NO → Use Debezium + Kafka for future flexibility
```

---

## Conclusion

Each CDC approach has distinct trade-offs:

- **Logical Replication & Binlog**: Best for real-time, production-grade CDC with mature tooling
- **Change Data Feed**: Ideal for Spark-centric lakehouse architectures
- **Snapshot-based**: Simplest approach for batch-oriented, cost-sensitive use cases

This project demonstrates all four approaches to help you evaluate which fits your requirements. For most production systems, a hybrid approach (Debezium → Kafka → Lakehouse) provides the best balance of real-time capture and analytics-optimized storage.

## Further Reading

- [Debezium Documentation](https://debezium.io/documentation/)
- [DeltaLake Change Data Feed](https://docs.delta.io/latest/delta-change-data-feed.html)
- [Iceberg Table Specification](https://iceberg.apache.org/spec/)
- [PostgreSQL Logical Replication](https://www.postgresql.org/docs/current/logical-replication.html)
- [MySQL Binlog](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html)
