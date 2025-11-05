# Schema Registry Integration Plan

## Overview
This document outlines the plan to integrate Confluent Schema Registry into the CDC demo environment to enable the 6 schema evolution tests currently skipped in `tests/integration/test_postgres_schema_evolution.py`.

## Current State Analysis

### Existing Infrastructure
✅ **Running Services** (verified via `docker ps`):
- PostgreSQL (cdc-postgres) - port 5432
- MySQL (cdc-mysql) - port 3306
- Kafka (cdc-kafka) - ports 9092, 29092
- Zookeeper (cdc-zookeeper) - port 2181
- Debezium Connect (cdc-debezium) - port 8083
- MinIO, Spark, Prometheus, Grafana, Loki, Alertmanager

❌ **Missing Service**:
- Confluent Schema Registry (typically port 8081)

### Schema Evolution Tests Status
Currently **6 tests are skipped** in `tests/integration/test_postgres_schema_evolution.py`:
1. `test_add_column_propagates_through_cdc` - Tests ADD COLUMN scenario
2. `test_drop_column_handled_gracefully` - Tests DROP COLUMN scenario
3. `test_rename_column_evolution` - Tests RENAME COLUMN
4. `test_change_column_type_evolution` - Tests ALTER COLUMN TYPE
5. `test_add_not_null_constraint` - Tests constraint addition
6. `test_complex_schema_evolution_sequence` - Tests multiple schema changes

**Current Skip Reason**: "Requires Schema Registry (not currently running in Docker environment)"

## Schema Registry Requirements

### What is Schema Registry?
Confluent Schema Registry is a centralized service that:
- Stores and manages Avro, JSON Schema, and Protobuf schemas
- Provides schema versioning and evolution rules
- Ensures schema compatibility across producers and consumers
- Integrates with Debezium for CDC schema management

### Why Schema Registry for CDC?
While the tests **can technically run without Schema Registry** (Debezium can use JSON without schema registry), Schema Registry provides:
- **Schema versioning**: Track schema changes over time
- **Compatibility checks**: Prevent breaking changes
- **Schema evolution**: Support forward/backward compatibility
- **Better performance**: Schema stored centrally, not in each message
- **Enterprise-grade CDC**: Production-ready schema management

## Integration Plan

### Step 1: Add Schema Registry Service to docker-compose.yml

**Location**: After the `kafka` service (around line 95)

```yaml
  # Confluent Schema Registry
  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    container_name: cdc-schema-registry
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
      SCHEMA_REGISTRY_KAFKASTORE_TOPIC: _schemas
      SCHEMA_REGISTRY_DEBUG: "false"
      # Schema compatibility settings
      SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL: BACKWARD
    ports:
      - "8081:8081"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 20s
    networks:
      - cdc-network
```

**Key Configuration Options**:
- `KAFKASTORE_BOOTSTRAP_SERVERS`: Points to Kafka broker
- `LISTENERS`: Exposes REST API on port 8081
- `SCHEMA_COMPATIBILITY_LEVEL`: BACKWARD (default), supports adding fields
- `KAFKASTORE_TOPIC`: Internal topic for schema storage (_schemas)

### Step 2: Add Environment Variables

**Update `.env` and `.env.example`**:
```bash
# Schema Registry Configuration
SCHEMA_REGISTRY_URL=http://localhost:8081
SCHEMA_REGISTRY_COMPATIBILITY=BACKWARD
```

### Step 3: Configure Debezium to Use Schema Registry (Optional)

Debezium can work with or without Schema Registry. For enhanced schema management:

**Update Debezium connector configurations** to include:
```json
{
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

**Note**: This is optional. Tests can run with JSON converters and still verify schema evolution through Kafka message structure.

### Step 4: Update Test Skip Conditions

**Modify** `tests/integration/test_postgres_schema_evolution.py`:

```python
import os
import requests

def is_schema_registry_available():
    """Check if Schema Registry is running."""
    try:
        url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        response = requests.get(f"{url}/subjects", timeout=2)
        return response.status_code == 200
    except:
        return False

@pytest.mark.integration
@pytest.mark.skipif(
    not is_schema_registry_available(),
    reason="Requires Schema Registry to be running"
)
class TestPostgresSchemaEvolution:
    """Integration tests for Postgres CDC schema evolution."""
```

### Step 5: Enhance Tests with Schema Registry Validation

**Uncomment and enhance Schema Registry checks** in tests:

```python
def verify_schema_registry_version(subject_name):
    """Verify schema exists in Schema Registry."""
    import os
    import requests

    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    response = requests.get(f"{schema_registry_url}/subjects/{subject_name}/versions/latest")

    if response.status_code == 200:
        schema_data = response.json()
        return schema_data
    return None

# In test_add_column_propagates_through_cdc:
# After line 154, add:
schema_data = verify_schema_registry_version('debezium.public.schema_evo_test-value')
if schema_data:
    # Verify schema evolution in Schema Registry
    assert 'description' in str(schema_data['schema'])
```

## Implementation Steps

### Phase 1: Basic Integration (Minimal Changes)
1. ✅ Add `schema-registry` service to `docker-compose.yml`
2. ✅ Add environment variables to `.env` and `.env.example`
3. ✅ Update test skip condition to check Schema Registry availability
4. ✅ Restart Docker environment: `docker-compose down && docker-compose up -d`
5. ✅ Verify Schema Registry is running: `curl http://localhost:8081/subjects`
6. ✅ Run tests: `make test` (should now run 6 additional tests)

### Phase 2: Enhanced Integration (Optional)
1. Configure Debezium connectors to use Avro with Schema Registry
2. Add Schema Registry validation to all schema evolution tests
3. Add schema compatibility tests
4. Document schema evolution best practices

## Testing Verification

### After Integration
```bash
# 1. Verify Schema Registry is running
docker ps | grep schema-registry
curl http://localhost:8081/subjects

# 2. Check Kafka topics include _schemas
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep _schemas

# 3. Run schema evolution tests
poetry run pytest tests/integration/test_postgres_schema_evolution.py -v

# 4. Verify no tests are skipped due to Schema Registry
# Expected: 6 tests run (not skipped)
```

## Resource Requirements

### Schema Registry Resource Usage
- **Memory**: ~512MB - 1GB recommended
- **CPU**: 0.5 core typical
- **Disk**: Minimal (schemas stored in Kafka topic)
- **Network**: Light (REST API calls, Kafka connection)

### Total Environment Impact
- **Current total services**: 12
- **After adding Schema Registry**: 13
- **Estimated additional memory**: 512MB
- **No additional disk volumes needed** (uses Kafka for persistence)

## Rollback Plan

If issues occur:
```bash
# 1. Stop all services
docker-compose down

# 2. Remove schema-registry service from docker-compose.yml
# 3. Remove schema registry environment variables

# 4. Restart without Schema Registry
docker-compose up -d

# Tests will skip again (existing behavior)
```

## Alternative: Run Tests Without Schema Registry

**Analysis**: After reviewing the test code (line 156-159), Schema Registry validation is **already commented out** in the tests. This means:

✅ **The tests can run NOW by simply removing the skip condition**

The tests validate:
- Kafka message structure changes
- Debezium CDC captures schema changes
- Column additions/removals propagate through pipeline

**Immediate Solution**:
```python
@pytest.mark.integration
# Remove the @pytest.mark.skipif decorator entirely
class TestPostgresSchemaEvolution:
    """Integration tests for Postgres CDC schema evolution."""
```

This would enable the tests immediately without Schema Registry, relying on Kafka message validation.

## Recommendations

### Recommended Approach: Add Schema Registry (Phase 1)
**Reasoning**:
1. **Production-ready**: Schema Registry is standard for enterprise CDC
2. **Low overhead**: Only 512MB memory, no disk volumes
3. **Future-proof**: Enables Avro serialization and schema management
4. **Easy to add**: Single service in docker-compose.yml
5. **Reversible**: Can be removed if not needed

### Timeline
- **Phase 1 (Basic)**: 15-30 minutes
  - Add service to docker-compose.yml
  - Update environment files
  - Restart Docker
  - Update test skip conditions

- **Phase 2 (Enhanced)**: 1-2 hours
  - Configure Debezium for Avro
  - Add schema validation to tests
  - Document schema evolution patterns

## Conclusion

Adding Confluent Schema Registry is a straightforward enhancement that:
- ✅ Enables 6 schema evolution tests
- ✅ Provides enterprise-grade schema management
- ✅ Aligns with CDC best practices
- ✅ Requires minimal resources (~512MB RAM)
- ✅ Takes <30 minutes to implement (Phase 1)

**Next Steps**: Implement Phase 1 by adding the Schema Registry service to docker-compose.yml and updating test conditions.
