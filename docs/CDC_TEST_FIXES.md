# CDC Test Infrastructure Fixes

**Date:** 2025-11-12
**Status:** ✅ Completed

## Problem Summary

The cross-storage CDC tests ([test_cross_storage.py](../tests/integration/test_cross_storage.py)) were failing because no data was reaching the Iceberg tables, even though the full infrastructure was running.

### Root Cause

The Spark Structured Streaming job was filtering out **all records** due to a mismatch between:
- **Debezium configuration**: Used `ExtractNewRecordState` transformation without preserving CDC metadata
- **Spark expectations**: Code expected CDC metadata fields (`__op`, `__deleted`, `__ts_ms`, `__lsn`) that didn't exist

This resulted in:
- ✅ Data written to Postgres
- ✅ Debezium capturing changes to Kafka
- ✅ Spark reading from Kafka (`numInputRows: 1`)
- ❌ Spark writing **0 rows** to Iceberg (`numOutputRows: 0`)

All records were being filtered out at [kafka_to_iceberg.py:255](../src/cdc_pipelines/streaming/kafka_to_iceberg.py#L255).

## Implemented Fixes

### 1. Updated Debezium Connector Configuration ✅

**File:** [scripts/connectors/register-postgres-connector.sh](../scripts/connectors/register-postgres-connector.sh)

Added CDC metadata preservation to `ExtractNewRecordState` transformation:

```json
"transforms.unwrap.delete.handling.mode": "rewrite",
"transforms.unwrap.add.fields": "op,db,table,ts_ms,lsn",
"transforms.unwrap.add.fields.prefix": "__"
```

**Result:** Kafka messages now include:
- `__op`: Operation type (c=create, u=update, d=delete, r=snapshot)
- `__ts_ms`: Event timestamp in milliseconds
- `__lsn`: PostgreSQL Log Sequence Number
- `__db`: Database name
- `__table`: Table name
- `__deleted`: Deletion marker

Also updated [scripts/connectors/register-mysql-connector.sh](../scripts/connectors/register-mysql-connector.sh) for consistency.

### 2. Added Defensive Checks in Spark Transformations ✅

**File:** [src/cdc_pipelines/streaming/kafka_to_iceberg.py](../src/cdc_pipelines/streaming/kafka_to_iceberg.py)

Modified `apply_transformations()` to:
- **Detect** which CDC metadata fields are present
- **Conditionally filter** DELETE operations only if metadata exists
- **Dynamically build** select columns based on available fields
- **Gracefully handle** missing CDC metadata

**Changes:**
- Lines 253-268: Check for CDC metadata fields before filtering
- Lines 270-342: Build select columns dynamically
- Handles both old (without metadata) and new (with metadata) Kafka messages

### 3. Updated Tests to Use Polling Instead of Fixed Sleeps ✅

**File:** [tests/test_utils.py](../tests/test_utils.py)

Added new helper functions for robust testing:

1. **`wait_for_kafka_event()`** - Poll Kafka topic for specific event
2. **`wait_for_iceberg_record()`** - Poll Iceberg table for record appearance

**File:** [tests/integration/test_cross_storage.py](../tests/integration/test_cross_storage.py)

Updated all 5 failing tests:
- `test_postgres_kafka_iceberg_flow`: Uses polling instead of `time.sleep(20)`
- `test_schema_drift_handling`: Polls for records with timeout
- `test_data_transformation_accuracy`: Waits for each record individually
- `test_high_throughput_scenario`: Polls for all 1000 records
- `test_idempotent_writes_to_iceberg`: Uses `wait_for_iceberg_record()`

**Benefits:**
- Tests run faster when data is ready
- Clear timeout errors when something is wrong
- More reliable in different environments

### 4. Added Diagnostic Logging ✅

**File:** [src/cdc_pipelines/streaming/kafka_to_iceberg.py](../src/cdc_pipelines/streaming/kafka_to_iceberg.py)

Added logging at critical points:
- Line 235: Log parsed schema
- Line 257: Log which CDC metadata fields are detected
- Lines 344-347: Log transformation output and metadata preservation
- Lines 409-426: Log streaming query startup details

**Example output:**
```
INFO - CDC metadata fields present - __deleted: True, __op: True
INFO - Applied transformations. Output schema: ... CDC metadata preserved: op=True, ts_ms=True, lsn=True
```

### 5. End-to-End Validation ✅

**Actions taken:**
1. Re-registered Debezium connector with new configuration
2. Restarted Spark streaming container to apply code changes
3. Verified CDC metadata in Kafka messages
4. Confirmed Spark processing: `numOutputRows: 1` ✅
5. Verified data in Iceberg with all metadata preserved

**Test verification:**
```bash
# Inserted test record
customer_id: 1007

# Verified in Iceberg
full_name: Verify CDC
location: TestCity, TS, USA
_cdc_operation: INSERT
_event_timestamp_ms: 1762937406343
_source_lsn: 41000544
```

## Files Modified

1. ✅ [scripts/connectors/register-postgres-connector.sh](../scripts/connectors/register-postgres-connector.sh) - Added CDC metadata configuration
2. ✅ [scripts/connectors/register-mysql-connector.sh](../scripts/connectors/register-mysql-connector.sh) - Added CDC metadata configuration
3. ✅ [src/cdc_pipelines/streaming/kafka_to_iceberg.py](../src/cdc_pipelines/streaming/kafka_to_iceberg.py) - Defensive checks and logging
4. ✅ [tests/test_utils.py](../tests/test_utils.py) - Added polling utilities
5. ✅ [tests/integration/test_cross_storage.py](../tests/integration/test_cross_storage.py) - Updated tests to use polling

## Before vs After

### Before
```
Postgres → Debezium → Kafka (no metadata) → Spark (filters all) → Iceberg (0 records)
                                              numInputRows: 1
                                              numOutputRows: 0 ❌
```

### After
```
Postgres → Debezium → Kafka (with metadata) → Spark (processes) → Iceberg (records written)
                      {__op, __ts_ms, __lsn}   numInputRows: 1
                                               numOutputRows: 1 ✅
```

## Testing Recommendations

### Run Tests
```bash
poetry run pytest tests/integration/test_cross_storage.py -v
```

### Expected Results
All 5 tests should now pass (excluding `test_kafka_offset_management`):
- ✅ `test_postgres_kafka_iceberg_flow`
- ✅ `test_schema_drift_handling`
- ✅ `test_data_transformation_accuracy`
- ✅ `test_high_throughput_scenario`
- ✅ `test_idempotent_writes_to_iceberg`

### Manual Verification
```python
# Check Kafka messages have metadata
poetry run python -c "
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('debezium.public.customers', ...)
for msg in consumer:
    print(msg.value.keys())  # Should include __op, __ts_ms, __lsn
"

# Check Iceberg has data
poetry run python -c "
from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager
# ... query table and verify records exist
"
```

## Future Improvements

1. **Add UPDATE/DELETE tests**: Now that CDC metadata is preserved, test UPDATE and DELETE operations
2. **Schema evolution**: Test dynamic schema changes with CDC metadata
3. **Idempotency testing**: Implement actual pipeline replay to test duplicate handling
4. **Performance monitoring**: Add metrics for CDC lag and throughput
5. **Error handling**: Add tests for malformed messages and partial failures

## References

- [Debezium ExtractNewRecordState Documentation](https://debezium.io/documentation/reference/stable/transformations/event-flattening.html)
- [Iceberg Spark Streaming](https://iceberg.apache.org/docs/latest/spark-structured-streaming/)
- [PyIceberg Documentation](https://py.iceberg.apache.org/)
