# Schema Evolution Table Naming Consistency Fix

## Issue Discovered

The user highlighted line 28 in `.env.example` which revealed a critical **naming inconsistency** across the codebase for the schema evolution test table.

## Three-Way Inconsistency Found

1. **Python Setup Script** ([setup_postgres_connector.py:99](scripts/connectors/setup_postgres_connector.py#L99))
   - Used: `schema_evolution_test` ✅ CORRECT

2. **Bash Registration Script** ([register-postgres-connector.sh:29](scripts/connectors/register-postgres-connector.sh#L29))
   - Missing: Did not include schema evolution table at all ❌

3. **Environment Files** ([.env:28](.env#L28), [.env.example:28](.env.example#L28))
   - Used: `schema_evo_test` ❌ WRONG

## Root Cause Analysis

The original naming inconsistency meant:
- Tests created table `schema_evolution_test` (correct)
- Debezium connector (Python setup) monitored `schema_evolution_test` (correct)
- Kafka topic environment variable expected `schema_evo_test` (wrong)
- Bash registration script didn't include the table at all (incomplete)

This caused the integration test failures with `UndefinedTable` errors that were **partially** caused by the wrong table name in SQL statements, but **also** because the environment configuration was inconsistent.

## Files Fixed

### 1. Environment Configuration
- **File**: `.env`
- **Line**: 28
- **Change**: `schema_evo_test` → `schema_evolution_test`

- **File**: `.env.example`
- **Line**: 28
- **Change**: `schema_evo_test` → `schema_evolution_test`

### 2. Connector Registration Scripts
- **File**: `scripts/connectors/register-postgres-connector.sh`
- **Line**: 29
- **Change**: Added `public.schema_evolution_test` to `table.include.list`
- **Before**: `"public.customers,public.orders"`
- **After**: `"public.customers,public.orders,public.products,public.inventory,public.schema_evolution_test"`

### 3. Documentation
- **File**: `docs/SCHEMA_REGISTRY_IMPLEMENTATION_COMPLETE.md`
- **Line**: 145
- **Change**: Updated curl example to use correct table name

- **File**: `docs/SCHEMA_REGISTRY_INTEGRATION_PLAN.md`
- **Line**: 159
- **Change**: Updated code example to use correct table name

- **File**: `tests/test_utils.py`
- **Line**: 14
- **Change**: Updated docstring to show correct example

### 4. Test Files (Previously Fixed)
- `tests/integration/test_postgres_schema_evolution.py` - Already fixed (15+ occurrences)

## Backward Compatibility

The `tests/test_utils.py` file maintains backward compatibility by keeping `schema_evo_test` as an alias:

```python
env_var_map = {
    'customers': 'KAFKA_TOPIC_CUSTOMERS',
    'orders': 'KAFKA_TOPIC_ORDERS',
    'products': 'KAFKA_TOPIC_PRODUCTS',
    'inventory': 'KAFKA_TOPIC_INVENTORY',
    'schema_evolution_test': 'KAFKA_TOPIC_SCHEMA_EVOLUTION',
    'schema_evo_test': 'KAFKA_TOPIC_SCHEMA_EVOLUTION',  # Alias
}
```

This ensures any code using the old name will still work.

## Standard Table Name

**CORRECT NAME**: `schema_evolution_test`

All future references should use this full name. The abbreviated `schema_evo_test` should only be used as an alias for backward compatibility.

## Action Required

To apply these changes to the running system:

```bash
# 1. Restart services to pick up new environment variables
docker-compose restart

# 2. Re-register Postgres connector with updated table list
curl -X DELETE http://localhost:8083/connectors/postgres-connector
./scripts/connectors/register-postgres-connector.sh

# 3. Verify connector is monitoring the correct tables
curl http://localhost:8083/connectors/postgres-connector/config | jq '.["table.include.list"]'
```

## Verification

After applying changes, verify:

1. **Kafka topic name**: `debezium.public.schema_evolution_test`
2. **Table name in Postgres**: `schema_evolution_test`
3. **Debezium monitoring**: `public.schema_evolution_test`
4. **Test code uses**: `schema_evolution_test`

All naming is now consistent across the entire codebase.
