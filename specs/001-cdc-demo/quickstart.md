# Quickstart Guide: CDC Demo for Open-Source Data Storage

**Last Updated**: 2025-10-27
**Estimated Time**: 10 minutes
**Prerequisites**: Docker Desktop installed and running

## What You'll Learn

In this quickstart, you'll:
1. Set up a complete CDC environment with multiple data storage technologies
2. Generate mock data in PostgreSQL and MySQL databases
3. Observe Change Data Capture in action across Postgres, MySQL, DeltaLake, and Iceberg
4. Validate data integrity and CDC lag
5. Monitor CDC pipelines with Grafana dashboards

## System Requirements

- **OS**: Linux, macOS, or Windows with WSL2
- **Docker**: Docker Desktop 20.10+ with Docker Compose
- **Hardware**: 8GB RAM, 4 CPU cores
- **Network**: Internet connection for initial Docker image downloads (1-2GB)
- **Disk Space**: 10GB free

## Quick Start (5 Minutes)

### Option A: One-Command Start (Recommended)

```bash
# Clone repository
git clone https://github.com/yourorg/postgres-cdc-demo.git
cd postgres-cdc-demo

# Start everything
make quickstart

# This will:
# - Validate Docker is running
# - Pull required Docker images
# - Start all services (Postgres, MySQL, Kafka, Debezium, etc.)
# - Generate sample data (10K records)
# - Start Postgres CDC pipeline
# - Run validation checks
```

**Expected Output**:
```
✓ Docker is running
✓ Pulling Docker images... (60s)
✓ Starting services... (90s)
  - PostgreSQL: ✓
  - MySQL: ✓
  - Kafka + Zookeeper: ✓
  - Debezium Connect: ✓
  - MinIO: ✓
  - Prometheus + Grafana: ✓
✓ Generating mock data... (15s)
  - customers: 10,000 rows
  - orders: 50,000 rows
  - products: 5,000 rows
✓ Starting Postgres CDC pipeline... (5s)
✓ Running validation checks... (10s)
  - Row count: PASS
  - Checksum: PASS
  - CDC lag: 2.1s (PASS)

🎉 CDC Demo is ready!

Next steps:
  1. Open Grafana: http://localhost:3000 (admin/admin)
  2. Watch CDC events: make monitor
  3. Trigger changes: make demo-changes
```

### Option B: Step-by-Step (Detailed)

If you prefer to see what's happening at each step:

```bash
# 1. Clone and setup
git clone https://github.com/yourorg/postgres-cdc-demo.git
cd postgres-cdc-demo

# 2. Start infrastructure
docker-compose up -d

# Wait for services to be healthy (60-90s)
./scripts/wait-for-services.sh

# 3. Generate mock data
./scripts/generate-data.sh --dataset medium

# 4. Start CDC pipelines
./scripts/start-cdc.sh --pipeline postgres_customers_cdc

# 5. Validate
./scripts/validate.sh --pipeline postgres_customers_cdc
```

## Exploring CDC in Action

### 1. Watch CDC Events Flow

Terminal 1 - Monitor source database changes:
```bash
# Watch Postgres customers table
watch -n 1 'docker exec postgres psql -U postgres -d demo_db -c "SELECT COUNT(*) FROM customers;"'
```

Terminal 2 - Monitor destination (DeltaLake):
```bash
# Watch destination table
watch -n 1 './scripts/count-delta-records.sh customers_cdc'
```

Terminal 3 - Generate changes:
```bash
# Insert new customer
docker exec postgres psql -U postgres -d demo_db -c \
  "INSERT INTO customers (email, first_name, last_name, registration_date, last_updated) \
   VALUES ('new.customer@example.com', 'John', 'Doe', NOW(), NOW());"

# Update existing customer
docker exec postgres psql -U postgres -d demo_db -c \
  "UPDATE customers SET customer_tier = 'Gold' WHERE customer_id = 1;"

# Delete customer
docker exec postgres psql -U postgres -d demo_db -c \
  "DELETE FROM customers WHERE customer_id = 2;"
```

**What You'll See**:
- Terminal 1: Source row count increases/decreases immediately
- Terminal 2: Destination count updates within 2-5 seconds (CDC lag)
- Grafana: CDC lag metrics spike briefly, then recover

### 2. Explore Grafana Dashboards

Open http://localhost:3000 (credentials: admin/admin)

**Available Dashboards**:

1. **CDC Overview Dashboard**
   - All pipelines status
   - Aggregate CDC lag
   - Total events processed
   - Error rates

2. **Postgres CDC Dashboard**
   - Postgres customers CDC lag
   - Postgres orders CDC lag
   - Events per second
   - Debezium connector health

3. **MySQL CDC Dashboard**
   - MySQL products CDC lag
   - Binlog position tracking
   - Events per second

4. **Data Quality Dashboard**
   - Row count validation results
   - Checksum validation results
   - Schema evolution events
   - Validation failures

### 3. Run Validation Checks

```bash
# Validate all pipelines
make validate

# Validate specific pipeline
./scripts/validate.sh --pipeline postgres_customers_cdc --type all

# Run with detailed report
./scripts/validate.sh --pipeline postgres_customers_cdc --report validation_report.json
```

**Sample Validation Output**:
```
Running validations for postgres_customers_cdc...

✓ Row Count Validation: PASS
  Source: 10,001 rows
  Destination: 10,001 rows
  Difference: 0 (0.00%)

✓ Checksum Validation: PASS
  Source checksum: a1b2c3d4e5f6
  Dest checksum: a1b2c3d4e5f6
  Sample size: 1,000 rows

✓ CDC Lag Validation: PASS
  Current lag: 2.1 seconds
  Threshold: 5.0 seconds

✓ Schema Validation: PASS
  Source schema: 15 columns
  Dest schema: 18 columns (3 CDC metadata columns)
  Compatibility: BACKWARD_COMPATIBLE

✓ Integrity Validation: PASS
  Primary key unique: ✓
  No null violations: ✓
  Referential integrity: ✓

Overall: 5/5 checks passed (100%)
```

### 4. Test Schema Evolution

CDC systems must handle schema changes. Let's test it:

```bash
# 1. Start schema evolution monitoring
./scripts/monitor-schema-evolution.sh

# 2. In another terminal, add a new column
docker exec postgres psql -U postgres -d demo_db -c \
  "ALTER TABLE customers ADD COLUMN loyalty_points INTEGER DEFAULT 0;"

# 3. Insert data with new schema
docker exec postgres psql -U postgres -d demo_db -c \
  "INSERT INTO customers (email, first_name, last_name, loyalty_points, registration_date, last_updated) \
   VALUES ('schema-test@example.com', 'Schema', 'Tester', 100, NOW(), NOW());"

# 4. Validate schema evolution was handled
./scripts/validate.sh --pipeline postgres_customers_cdc --type schema
```

**What You'll See**:
- Debezium detects schema change
- Schema Registry registers new schema version
- New events include `loyalty_points` field
- Destination schema auto-updates (Delta Lake schema evolution)
- Data integrity maintained (no data loss)

## Demo Scenarios

### Scenario 1: High-Volume Insert Test

Simulate bulk data ingestion:

```bash
# Generate 10,000 orders rapidly
./scripts/bulk-insert.sh --table orders --count 10000

# Monitor CDC lag
./scripts/monitor-lag.sh --pipeline postgres_orders_cdc

# Validate no data loss
./scripts/validate.sh --pipeline postgres_orders_cdc --type row_count
```

**Expected Behavior**:
- CDC lag spikes to 3-8 seconds during bulk insert
- Lag recovers to < 5 seconds within 30 seconds after insert completes
- Row count validation passes (no data loss)
- Kafka throughput reaches 100+ events/second

### Scenario 2: CDC Pipeline Failure & Recovery

Test CDC resilience to failures:

```bash
# 1. Start monitoring
./scripts/monitor-lag.sh --all

# 2. Stop Debezium connector (simulate failure)
docker-compose stop debezium-connect

# 3. Make changes while connector is down
./scripts/bulk-insert.sh --table customers --count 1000

# 4. Restart connector
docker-compose start debezium-connect

# Wait for connector to recover (30-60s)
sleep 60

# 5. Validate data recovered
./scripts/validate.sh --pipeline postgres_customers_cdc --type row_count
```

**Expected Behavior**:
- Changes accumulate in Postgres WAL while connector is down
- After restart, connector resumes from last committed offset
- All changes during downtime are captured and replicated
- Validation passes (no data loss)

### Scenario 3: Cross-Storage CDC Pipeline

Demonstrate OLTP to Data Lake CDC:

```bash
# 1. Start Postgres → Iceberg pipeline
./scripts/start-cdc.sh --pipeline postgres_to_iceberg

# 2. Generate customer data in Postgres
./scripts/generate-data.sh --table customers --count 5000

# 3. Watch data flow to Iceberg
./scripts/query-iceberg.sh --table customers_analytics --watch

# 4. Validate cross-storage integrity
./scripts/validate.sh --pipeline postgres_to_iceberg --type all
```

**What You'll See**:
- Data flows: Postgres → Debezium → Kafka → Spark → Iceberg
- Transformations applied (name concatenation, location derivation)
- Iceberg snapshots created for each batch
- Data queryable in Iceberg within 10 seconds

## Troubleshooting

### Issue: Docker services fail to start

**Symptoms**:
```
ERROR: Service 'postgres' failed to start
```

**Solutions**:
```bash
# Check Docker is running
docker ps

# Check port conflicts
netstat -an | grep 5432   # Postgres
netstat -an | grep 3306   # MySQL
netstat -an | grep 9092   # Kafka

# Check Docker resources
docker stats

# Increase Docker memory limit (Docker Desktop → Settings → Resources)
# Recommended: 8GB RAM, 4 CPUs
```

### Issue: CDC lag exceeds threshold

**Symptoms**:
```
✗ CDC Lag Validation: FAIL
  Current lag: 12.3 seconds
  Threshold: 5.0 seconds
```

**Solutions**:
```bash
# Check Kafka broker health
docker-compose logs kafka | tail -100

# Check Debezium connector status
curl http://localhost:8083/connectors/postgres-customers-connector/status

# Check resource utilization
docker stats

# Possible causes:
# - Insufficient resources (increase Docker limits)
# - Kafka partition count too low (increase partitions)
# - Large transactions (tune batch sizes)
```

### Issue: Row count mismatch

**Symptoms**:
```
✗ Row Count Validation: FAIL
  Source: 10,000 rows
  Destination: 9,850 rows
  Difference: 150 (1.5%)
```

**Solutions**:
```bash
# Check for in-flight events
./scripts/check-kafka-lag.sh

# Wait for CDC to catch up (5-10 seconds)
sleep 10
./scripts/validate.sh --pipeline postgres_customers_cdc --type row_count

# Check for errors in Debezium logs
docker-compose logs debezium-connect | grep ERROR

# Check Kafka Connect dead letter queue
./scripts/check-dlq.sh
```

### Issue: Services won't stop

**Symptoms**:
```
ERROR: Timeout waiting for service to stop
```

**Solutions**:
```bash
# Force stop all services
docker-compose down --timeout 10

# If still hanging, force kill
docker-compose kill

# Remove containers
docker-compose rm -f

# Clean up volumes (WARNING: deletes all data)
docker-compose down -v
```

## Next Steps

After completing the quickstart:

1. **Explore Other CDC Approaches**:
   ```bash
   # Try MySQL binlog CDC
   ./scripts/start-cdc.sh --pipeline mysql_products_cdc

   # Try DeltaLake Change Data Feed
   ./scripts/demo-deltalake-cdf.sh

   # Try Iceberg snapshot-based CDC
   ./scripts/demo-iceberg-snapshots.sh
   ```

2. **Run Test Suite**:
   ```bash
   # Run all tests (TDD verification)
   make test

   # Run specific test category
   make test-integration
   make test-data-quality
   make test-e2e
   ```

3. **Review Architecture**:
   - Read [Architecture Overview](../docs/architecture.md)
   - Review [CDC Approaches Comparison](../docs/cdc_approaches.md)
   - Study per-pipeline docs in [docs/pipelines/](../docs/pipelines/)

4. **Customize Configuration**:
   - Edit `config/pipelines/*.yaml` for pipeline settings
   - Adjust resource limits in `docker-compose.yml`
   - Configure observability in `config/observability.yaml`

5. **Develop Custom Validators**:
   - See [Validation API Contract](./contracts/validation-api.md)
   - Implement custom business rule validations
   - Integrate with your CI/CD pipeline

## Cleanup

When you're done exploring:

```bash
# Stop all services (preserve data)
docker-compose stop

# Stop and remove containers (preserve volumes)
docker-compose down

# Complete cleanup (remove containers, volumes, images)
make clean
```

## FAQ

**Q: How much data is generated by default?**
A: Medium dataset = 10K customers, 50K orders, 5K products, 25K inventory transactions (≈100MB)

**Q: Can I run just Postgres CDC without other databases?**
A: Yes! Use `make quickstart-postgres-only` for minimal setup

**Q: How do I access the Kafka UI?**
A: Kafka UI is available at http://localhost:9000 (if enabled in docker-compose.yml)

**Q: Where is the data stored?**
A: Docker volumes (use `docker volume ls | grep cdc-demo` to see them)

**Q: Can I use custom mock data?**
A: Yes! Edit `src/data_generators/schemas/*.yaml` and run `./scripts/generate-data.sh`

**Q: How do I export CDC events?**
A: Use `./scripts/export-cdc-events.sh --pipeline <pipeline-id> --format json`

**Q: Is this production-ready?**
A: No, this is a demonstration/learning project. For production, review Debezium production checklists and tune for scale.

## Resources

- **Documentation**: [docs/](../docs/)
- **Architecture Diagrams**: [docs/architecture.md](../docs/architecture.md)
- **CLI Reference**: [contracts/cli-commands.md](./contracts/cli-commands.md)
- **Validation API**: [contracts/validation-api.md](./contracts/validation-api.md)
- **Debezium Docs**: https://debezium.io/documentation/
- **Delta Lake CDC**: https://docs.delta.io/latest/delta-change-data-feed.html
- **Iceberg Docs**: https://iceberg.apache.org/docs/latest/

## Getting Help

- **Issues**: Report bugs at https://github.com/yourorg/postgres-cdc-demo/issues
- **Discussions**: Ask questions at https://github.com/yourorg/postgres-cdc-demo/discussions
- **Contributing**: See [CONTRIBUTING.md](../CONTRIBUTING.md)

## What's Next?

Now that you have CDC running:
- Experiment with different CDC patterns
- Test failure scenarios and recovery
- Integrate with your own data pipelines
- Contribute improvements back to the project!

---

**Estimated Completion Time**: 10 minutes for quickstart, 30 minutes for full exploration

Happy CDC learning! 🚀
