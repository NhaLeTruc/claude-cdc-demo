# Research: CDC Demo for Open-Source Data Storage

**Date**: 2025-10-27
**Feature**: CDC Demo for Open-Source Data Storage
**Purpose**: Research CDC approaches, technology choices, and best practices for implementing demonstrations across Postgres, MySQL, DeltaLake, and Iceberg

## Executive Summary

This research document covers the investigation of Change Data Capture (CDC) approaches for four major open-source data storage technologies. Key findings include: (1) Debezium provides unified CDC for relational databases, (2) Kafka Connect serves as ideal streaming backbone, (3) Native CDC features in lakehouse formats (DeltaLake CDF, Iceberg snapshots) offer different trade-offs than log-based CDC, (4) Python ecosystem provides comprehensive tooling for orchestration and testing.

## 1. CDC Approaches by Storage Technology

### 1.1 PostgreSQL CDC

**Decision**: Implement multiple approaches - Debezium (primary), pg_logical replication (alternative), timestamp-based (simple demo)

**Rationale**:
- PostgreSQL offers rich CDC ecosystem with multiple implementation patterns
- Debezium provides production-ready, feature-complete CDC with Kafka integration
- Demonstrating multiple approaches shows trade-offs and use cases

**Research Findings**:

**Approach 1: Debezium (Log-Based CDC)**
- Uses PostgreSQL logical replication protocol (pgoutput or wal2json)
- Captures all DML operations (INSERT, UPDATE, DELETE) from Write-Ahead Log (WAL)
- Pros: Low latency (<1s), captures all changes, schema evolution support, no application changes
- Cons: Requires WAL configuration (wal_level=logical), storage overhead for replication slots
- Best for: Production-grade CDC, real-time data pipelines, event sourcing

**Approach 2: Trigger-Based CDC**
- Creates triggers on source tables to capture changes into audit/changelog tables
- Pros: Simple to understand, works on older Postgres versions, fine-grained control
- Cons: Performance overhead on source database, requires schema modifications, misses some edge cases (TRUNCATE)
- Best for: Simple audit trails, legacy systems, selective table CDC

**Approach 3: Timestamp-Based Query CDC**
- Polls tables using updated_at/modified_at timestamp columns
- Pros: Simplest implementation, no database config changes, easy rollback
- Cons: Misses deletes (unless soft delete), poll interval creates lag, clock skew issues
- Best for: Basic batch ETL, systems with simple requirements, demo purposes

**Implementation Choice for Demo**: Debezium (primary) + timestamp-based (educational comparison)

**Alternatives Considered**:
- AWS DMS: Rejected - not open-source, requires AWS
- pglogical extension: Rejected - more complex than Debezium, less community support
- Custom WAL parsing: Rejected - reinventing the wheel, maintenance burden

### 1.2 MySQL CDC

**Decision**: Debezium for MySQL (binlog-based)

**Rationale**:
- MySQL binlog is the standard CDC mechanism
- Debezium provides mature, well-tested MySQL connector
- Consistent experience with Postgres CDC implementation

**Research Findings**:

**Approach 1: Debezium MySQL Connector (Binlog-Based)**
- Reads MySQL binary log (binlog) to capture row-level changes
- Supports all storage engines (InnoDB, MyISAM) as long as binlog is enabled
- Pros: Complete change capture, minimal source impact, global transaction ordering
- Cons: Requires binlog configuration (binlog_format=ROW), storage for binlog retention
- Best for: Production CDC, real-time replication, data lake ingestion

**Approach 2: Maxwell's Daemon**
- Alternative open-source binlog reader, outputs JSON
- Pros: Simpler than Debezium for basic use cases, JSON-first
- Cons: Less feature-complete, smaller community, less Kafka integration
- Best for: Lightweight CDC, JSON-centric pipelines

**Approach 3: Timestamp-Based Polling**
- Same concept as Postgres timestamp-based CDC
- Pros/Cons: Same as Postgres approach

**Implementation Choice for Demo**: Debezium MySQL Connector

**Alternatives Considered**:
- Oracle GoldenGate: Rejected - expensive, not open-source
- LinkedIn Databus: Rejected - project inactive, outdated
- Custom binlog parser: Rejected - complex protocol, not worth building

### 1.3 DeltaLake CDC

**Decision**: Delta Lake Change Data Feed (CDF)

**Rationale**:
- Native Delta Lake feature designed specifically for CDC use cases
- Version-based CDC aligns with lakehouse architecture
- No external tools required beyond Spark/Delta

**Research Findings**:

**Approach 1: Change Data Feed (CDF)**
- Delta Lake native feature (enabled via table property delta.enableChangeDataFeed=true)
- Captures row-level changes with operation type (_change_type: insert/update_preimage/update_postimage/delete)
- Pros: Native integration, time travel queries, efficient storage with data skipping
- Cons: Requires Delta Lake 1.2+, only captures changes made through Delta APIs
- Best for: Lakehouse CDC, incremental processing, downstream analytics

**Approach 2: Version Diff Queries**
- Query differences between table versions using Delta Lake time travel
- SELECT * FROM table VERSION AS OF 1 EXCEPT SELECT * FROM table VERSION AS OF 2
- Pros: Works on any Delta table, no configuration needed
- Cons: Full table scans for diffs, inefficient for large tables, doesn't distinguish update vs delete+insert
- Best for: Ad-hoc analysis, small tables, understanding Delta versioning

**Implementation Choice for Demo**: Change Data Feed (primary), Version Diff (educational comparison)

**Alternatives Considered**:
- External CDC on underlying Parquet: Rejected - breaks Delta transactionality, complex
- Audit table pattern: Rejected - duplicates data, not using Delta features

### 1.4 Iceberg CDC

**Decision**: Incremental Read using Snapshot Metadata

**Rationale**:
- Iceberg doesn't have native "CDC" but provides incremental read APIs
- Snapshot-based approach aligns with Iceberg's design philosophy
- Emerging patterns in Iceberg community for CDC-like workflows

**Research Findings**:

**Approach 1: Incremental Read (Snapshot-to-Snapshot)**
- Uses Iceberg's snapshot metadata to track which files changed between snapshots
- IncrementalChangelogScan or manual snapshot diff in PyIceberg
- Pros: Efficient file-level pruning, metadata-only operations for unchanged partitions, transaction isolation
- Cons: Coarser granularity than row-level CDC, requires manual snapshot tracking, limited operation type detection
- Best for: Lakehouse incremental processing, large-scale batch CDC, partition-aware workflows

**Approach 2: Manifest-Based Change Detection**
- Track added/deleted data files in manifest lists between snapshots
- More manual but gives fine-grained control
- Pros: Detailed file-level visibility, works with any Iceberg table
- Cons: Requires low-level manifest parsing, complex logic for merge-on-read tables
- Best for: Deep integration, custom change detection logic, file-level auditing

**Implementation Choice for Demo**: Incremental Read using PyIceberg (simpler for demo)

**Alternatives Considered**:
- Streaming writes with change tracking: Rejected - not mature yet (Iceberg 1.4+)
- External CDC to Iceberg: Rejected - not demonstrating Iceberg-native patterns
- Full table scans: Rejected - defeats purpose of CDC

### 1.5 Cross-Storage CDC Pipeline

**Decision**: Postgres → Kafka (Debezium) → Iceberg (via Spark Structured Streaming)

**Rationale**:
- Demonstrates real-world OLTP-to-lakehouse CDC pattern
- Leverages Debezium's maturity for Postgres capture
- Shows Kafka as CDC streaming backbone
- Iceberg as analytical destination

**Research Findings**:

**Architecture Options**:

**Option A: Debezium → Kafka → Spark → Iceberg**
- Pros: Mature stack, battle-tested, rich ecosystem, handles schema evolution
- Cons: More components (Kafka, Spark), higher resource usage
- Best for: Production-grade pipelines, high throughput, complex transformations

**Option B: Debezium → Kafka Connect → Iceberg Sink**
- Pros: Fewer components, Kafka Connect handles offsets, simpler for demos
- Cons: Limited Iceberg Kafka Connect sink maturity, less transformation capability
- Best for: Simple CDC replication, low-latency requirements

**Option C: Direct Postgres → Iceberg (Custom)**
- Pros: Fewest dependencies, lowest latency
- Cons: Reinvents CDC wheel, loses Kafka's benefits (replay, multiple consumers, buffering)
- Best for: Highly specific use cases, minimal infrastructure

**Implementation Choice for Demo**: Option A (Debezium → Kafka → Spark → Iceberg)

**Alternatives Considered**:
- Fivetran/Airbyte: Rejected - commercial/hosted, not fully open-source
- Postgres Foreign Data Wrapper (FDW): Rejected - not CDC, pull-based
- Custom polling script: Rejected - not production-pattern, educational disservice

## 2. Technology Stack Decisions

### 2.1 Orchestration & Containerization

**Decision**: Docker Compose for orchestration, Docker for containerization

**Rationale**:
- Industry standard for local development
- Single docker-compose.yml provides full stack setup
- Meets constitutional requirement for one-command setup
- Wide OS support (Linux, macOS, Windows/WSL2)

**Alternatives Considered**:
- Kubernetes/K3s: Rejected - overkill for local demo, steep learning curve
- Vagrant: Rejected - less common, heavier than Docker
- Bare metal scripts: Rejected - not portable, OS-specific

### 2.2 Programming Language

**Decision**: Python 3.11+

**Rationale**:
- Excellent data engineering ecosystem (PySpark, Pandas, Faker)
- PyIceberg for Iceberg integration
- Strong testing frameworks (pytest)
- Clear, readable syntax for educational project
- Delta-Spark Python API

**Alternatives Considered**:
- Java/Scala: Rejected - more verbose, harder for learners, Spark/Delta JVM overhead already present
- Go: Rejected - less mature data engineering libraries, overkill for orchestration scripts
- SQL-only: Rejected - insufficient for orchestration, data generation, validation logic

### 2.3 Testing Framework

**Decision**: pytest with testcontainers-python

**Rationale**:
- pytest is Python standard for testing
- testcontainers-python allows testing against real containerized services
- pytest-docker-compose for integration tests
- Faker library for realistic mock data

**Test Categories**:
1. **Unit Tests**: Pure Python logic (data generators, validators, utilities)
2. **Integration Tests**: CDC pipeline behavior with real databases/Kafka
3. **Data Quality Tests**: Integrity checks, schema evolution, lag monitoring
4. **E2E Tests**: Full workflow from source→CDC→destination

**Alternatives Considered**:
- unittest: Rejected - less feature-rich than pytest
- Robot Framework: Rejected - overkill for this project
- Custom test runner: Rejected - reinventing pytest

### 2.4 Mock Data Generation

**Decision**: Faker library with custom providers

**Rationale**:
- Industry-standard library for generating realistic fake data
- Supports wide range of data types (names, addresses, timestamps, JSON)
- Localizable (important for i18n testing)
- Extensible with custom providers

**Data Volume Strategy**:
- Small dataset: 1K records (fast smoke tests, < 10 seconds generation)
- Medium dataset: 10K records (demo workload, ~1 minute generation)
- Large dataset: 100K records (stress testing, ~5 minutes generation)

**Alternatives Considered**:
- Random standard library: Rejected - less realistic, no structured data types
- Real data dumps: Rejected - privacy concerns, not self-contained
- Manual CSV fixtures: Rejected - not scalable, tedious maintenance

### 2.5 Observability Stack

**Decision**: Prometheus (metrics), Grafana (dashboards), Loki (logs)

**Rationale**:
- All open-source, widely adopted industry standard
- Grafana provides unified interface for metrics + logs
- Prometheus pull model ideal for containerized services
- Loki integrates seamlessly with structured JSON logs

**Metrics to Track**:
- CDC lag (time between source change and destination write)
- Events processed per second (throughput)
- Error rate (failed CDC events)
- Resource utilization (container CPU/memory)

**Alternatives Considered**:
- ELK Stack (Elasticsearch, Logstash, Kibana): Rejected - heavier resource usage, slower for local dev
- Datadog/New Relic: Rejected - commercial, requires external accounts
- Simple file logging: Rejected - not meeting constitutional observability standards

### 2.6 Schema Registry

**Decision**: Confluent Schema Registry (open-source)

**Rationale**:
- De facto standard for Kafka schema management
- Supports Avro, JSON Schema, Protobuf
- Debezium integrates natively
- Schema evolution management built-in

**Alternatives Considered**:
- AWS Glue Schema Registry: Rejected - requires AWS
- Apicurio Registry: Rejected - less mature, smaller community
- No schema registry: Rejected - violates constitutional data quality standards

## 3. Best Practices Research

### 3.1 CDC Pipeline Reliability

**Best Practices Identified**:

1. **Exactly-Once Semantics**:
   - Use Kafka transactions for Debezium
   - Idempotent writes to destinations (upsert with primary keys)
   - Checkpoint/offset management

2. **Error Handling**:
   - Dead Letter Queues (DLQ) for failed events
   - Retry with exponential backoff
   - Alert on persistent failures

3. **Schema Evolution**:
   - Schema Registry for version management
   - Backward/forward compatibility rules
   - Automated schema migration tests

4. **State Management**:
   - Kafka Connect offsets in dedicated topics
   - Debezium connector state in dedicated database
   - Backup/restore procedures for state

5. **Monitoring**:
   - CDC lag alerts (> 5 seconds for demo)
   - Error rate thresholds
   - Resource saturation alerts

### 3.2 Testing Strategy for CDC Systems

**TDD Approach for CDC**:

1. **Write test for expected CDC behavior** (e.g., "INSERT captured correctly")
2. **Run test - it should FAIL** (no implementation yet)
3. **Implement CDC pipeline** (Debezium config, connectors)
4. **Run test - it should PASS**
5. **Refactor** (optimize performance, clean up config)

**Test Pyramid**:
- **Unit Tests (60%)**: Data generators, validators, utilities
- **Integration Tests (30%)**: CDC pipeline behavior with real services
- **E2E Tests (10%)**: Full workflows, most expensive but highest confidence

**Data Quality Test Patterns**:
```python
# Pattern 1: Row Count Validation
assert destination_count == source_count

# Pattern 2: Checksum Validation
assert hash(source_data) == hash(destination_data)

# Pattern 3: Schema Validation
assert destination_schema.is_compatible_with(source_schema)

# Pattern 4: Lag Validation
assert (destination_timestamp - source_timestamp) < 5_seconds
```

### 3.3 Documentation Patterns for CDC Projects

**Documentation Strategy**:

1. **README.md**: Quick start (< 5 minutes to first demo), architecture overview, troubleshooting
2. **Architecture Diagrams** (Mermaid): System overview, per-pipeline data flows, sequence diagrams
3. **Per-Pipeline READMEs**: Technology details, CDC approach explanation, pros/cons, validation steps
4. **API/Interface Docs**: Python docstrings, CLI command help, configuration options
5. **Troubleshooting Guide**: Common issues, log analysis, health check procedures

**Mermaid Diagram Types**:
- Flowcharts: CDC pipeline flows
- Sequence diagrams: Event propagation timing
- Component diagrams: Docker Compose service relationships

## 4. Performance Optimization Research

### 4.1 Resource Constraints (8GB RAM, 4 CPU cores)

**Memory Budget**:
- PostgreSQL: 512 MB
- MySQL: 512 MB
- Kafka + Zookeeper: 2 GB
- Debezium Connect: 1 GB
- Spark (for Delta/Iceberg): 2 GB
- Prometheus + Grafana + Loki: 1 GB
- MinIO (S3-compatible storage): 512 MB
- Overhead/OS: 512 MB
- **Total**: ~8 GB

**Optimization Strategies**:
1. Lazy service startup (only start what's needed for current demo)
2. Shared Kafka cluster across all CDC pipelines
3. Spark dynamic allocation (scale down when idle)
4. Limit log retention (1 hour for demo purposes)
5. Use Alpine-based Docker images where possible

### 4.2 CDC Lag Optimization

**Target**: < 5 seconds for 1000-event workload

**Optimization Techniques**:
1. Debezium snapshot mode: "schema_only" (skip initial snapshot for demos)
2. Kafka topics: 3 partitions (parallelism on 4 cores)
3. Debezium poll interval: 100ms
4. Kafka flush interval: 1s
5. Spark micro-batch: 5s interval

### 4.3 Startup Time Optimization

**Target**: < 5 minutes cold start

**Optimizations**:
1. Pre-built Docker images (avoid build-on-start)
2. Health check dependencies (services wait for dependencies)
3. Parallel service startup where possible
4. Kafka log retention: 1 hour (reduce initialization time)
5. Pre-seed configuration (no manual setup steps)

## 5. Open Questions & Decisions

### 5.1 Resolved Questions

**Q1: Should we support multiple CDC approaches per database?**
- **Decision**: Yes for Postgres (Debezium + timestamp-based), No for others
- **Rationale**: Postgres has richest CDC ecosystem; showing alternatives is educational. Other databases have clear "best" approach (MySQL binlog, Delta CDF, Iceberg snapshots)

**Q2: How to handle schema evolution in demos?**
- **Decision**: Include automated schema evolution tests with ADD COLUMN, DROP COLUMN, TYPE CHANGE scenarios
- **Rationale**: Constitutional requirement (DQ-002), critical CDC capability, common production issue

**Q3: Should we include transformation logic in CDC pipelines?**
- **Decision**: Minimal transformations (field renaming, type casting), no complex business logic
- **Rationale**: Keep focus on CDC patterns, not ETL. Complex transformations obscure CDC concepts.

**Q4: What CI/CD tooling?**
- **Decision**: GitHub Actions (if repo is on GitHub), otherwise include Makefile targets for local CI
- **Rationale**: Free for public repos, widely used, yaml-based (infrastructure as code)

### 5.2 Future Considerations (Out of Scope for V1)

- CDC from NoSQL databases (MongoDB, Cassandra)
- CDC to streaming analytics (Flink, ksqlDB)
- Multi-region CDC (conflict resolution)
- CDC with data masking/PII filtering
- CDC performance benchmarking suite

## 6. Technology Selection Summary

| Category | Technology | Version | Rationale |
|----------|-----------|---------|-----------|
| **Orchestration** | Docker Compose | 2.x | Industry standard, one-command setup |
| **CDC Tool** | Debezium | 2.x | Mature, feature-complete, Kafka-native |
| **Streaming** | Apache Kafka | 3.x | De facto CDC streaming backbone |
| **Language** | Python | 3.11+ | Rich data ecosystem, clear syntax |
| **Testing** | pytest | 7.x | Python standard, excellent plugins |
| **Mock Data** | Faker | 20+ | Realistic data generation |
| **Metrics** | Prometheus | Latest | Pull-based metrics, Grafana integration |
| **Dashboards** | Grafana | Latest | Unified metrics + logs UI |
| **Logging** | Loki | Latest | Structured logs, lightweight |
| **Schema** | Schema Registry | 7.x | Avro/JSON schema evolution |
| **Postgres** | PostgreSQL | 15+ | Latest stable with pgoutput |
| **MySQL** | MySQL | 8.0+ | Latest stable with binlog |
| **Delta** | Delta Lake | 3.0+ | PySpark integration |
| **Iceberg** | Apache Iceberg | 1.4+ | PyIceberg support |
| **Storage** | MinIO | Latest | S3-compatible local storage |
| **Linting** | Ruff | Latest | Fast Python linter |
| **Formatting** | Black | Latest | Opinionated Python formatter |
| **Type Checking** | mypy | Latest | Static type validation |

## 7. Risk Assessment & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Docker Compose resource limits on low-end machines | Medium | High | Lazy startup, memory budgets, Alpine images |
| Schema evolution breaks pipelines | Medium | High | Comprehensive schema evolution tests, Schema Registry |
| CDC lag exceeds 5s on slow machines | Low | Medium | Performance tests, optimization guide in docs |
| Container startup race conditions | Low | Medium | Health check dependencies, retry logic |
| Data loss on pipeline failure | Low | High | Offset management, idempotent writes, DLQ |
| Complex setup scares away users | Low | High | Single-command setup, clear error messages, troubleshooting guide |

## 8. Conclusion

This research establishes a comprehensive technology stack and architectural approach for the CDC demo project. Key takeaways:

1. **Debezium + Kafka** provides unified CDC infrastructure for relational databases
2. **Native CDC features** in lakehouse formats (Delta CDF, Iceberg snapshots) complement log-based CDC
3. **Python ecosystem** offers mature tooling for orchestration, testing, and data generation
4. **Docker Compose** meets constitutional requirements for local-first demonstration
5. **TDD with pytest** ensures quality while adhering to test-first principles
6. **Observability stack** (Prometheus, Grafana, Loki) provides production-grade monitoring patterns

All technology choices align with constitutional principles (open-source, local-first, clean code, observability, TDD). No constitutional violations identified.

**Next Steps**: Proceed to Phase 1 (data-model.md, contracts/, quickstart.md).
