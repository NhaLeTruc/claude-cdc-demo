# Validation API Documentation

This document provides comprehensive API documentation for the data quality validation framework.

## Table of Contents

- [Overview](#overview)
- [IntegrityValidator](#integrityvalidator)
- [SchemaValidator](#schemavalidator)
- [LagValidator](#lagvalidator)
- [ChecksumValidator](#checksumvalidator)
- [ValidationResult](#validationresult)
- [Usage Examples](#usage-examples)
- [Integration Guide](#integration-guide)

---

## Overview

The validation framework provides a comprehensive suite of tools for ensuring data quality and integrity in CDC pipelines. Key features include:

- **Row Count Validation**: Verify source and destination record counts match
- **Schema Validation**: Ensure schema compatibility across pipeline stages
- **Checksum Validation**: Validate data integrity using cryptographic hashes
- **Lag Validation**: Monitor and alert on CDC lag metrics
- **Composite Validation**: Run multiple validation checks in sequence

---

## IntegrityValidator

The `IntegrityValidator` class provides methods for validating data integrity between source and destination.

### Class Definition

```python
from src.validation.integrity import IntegrityValidator, ValidationResult

class IntegrityValidator:
    """Validate data integrity between source and destination."""
```

### Methods

#### `validate_row_count()`

Validate that row counts match between source and destination.

```python
def validate_row_count(
    self,
    source_count: int,
    destination_count: int,
    tolerance: float = 0.0,
) -> ValidationResult:
    """Validate row count matches within tolerance.

    Args:
        source_count: Number of rows in source system
        destination_count: Number of rows in destination system
        tolerance: Acceptable mismatch percentage (0.0 - 1.0)
                   e.g., 0.01 = 1% tolerance

    Returns:
        ValidationResult with pass/fail status and details

    Example:
        >>> validator = IntegrityValidator()
        >>> result = validator.validate_row_count(
        ...     source_count=10000,
        ...     destination_count=9950,
        ...     tolerance=0.01  # 1% tolerance
        ... )
        >>> result.passed
        True
        >>> result.message
        'Row count within tolerance: 10000 vs 9950 (0.5% difference)'
    """
```

**Returns:**
```python
ValidationResult(
    check_name="row_count",
    passed=True,
    message="Row count matches: 10000 vs 10000",
    metrics={
        "source_count": 10000,
        "destination_count": 10000,
        "difference": 0,
        "difference_percentage": 0.0
    }
)
```

#### `validate_row_count_async()`

Asynchronous version of row count validation.

```python
async def validate_row_count_async(
    self,
    source_count: int,
    destination_count: int,
    tolerance: float = 0.0,
) -> ValidationResult:
    """Async row count validation."""
```

#### `validate_checksum()`

Validate data integrity using checksums.

```python
def validate_checksum(
    self,
    source_checksum: str,
    destination_checksum: str,
    algorithm: str = "md5",
) -> ValidationResult:
    """Validate data integrity using checksums.

    Args:
        source_checksum: Checksum from source system
        destination_checksum: Checksum from destination system
        algorithm: Hash algorithm used (md5, sha1, sha256)

    Returns:
        ValidationResult with checksum comparison

    Example:
        >>> validator = IntegrityValidator()
        >>> result = validator.validate_checksum(
        ...     source_checksum="5d41402abc4b2a76b9719d911017c592",
        ...     destination_checksum="5d41402abc4b2a76b9719d911017c592",
        ...     algorithm="md5"
        ... )
        >>> result.passed
        True
    """
```

**Returns:**
```python
ValidationResult(
    check_name="checksum",
    passed=True,
    message="Checksums match (md5)",
    metrics={
        "source_checksum": "5d41402abc4b2a76b9719d911017c592",
        "destination_checksum": "5d41402abc4b2a76b9719d911017c592",
        "algorithm": "md5"
    }
)
```

#### `validate_dataset_checksum()`

Calculate and validate checksum for entire datasets.

```python
def validate_dataset_checksum(
    self,
    source_data: List[Dict[str, Any]],
    destination_data: List[Dict[str, Any]],
    key_columns: List[str],
    algorithm: str = "md5",
) -> ValidationResult:
    """Validate checksum for datasets.

    Args:
        source_data: Source dataset records
        destination_data: Destination dataset records
        key_columns: Columns to include in checksum calculation
        algorithm: Hash algorithm (md5, sha1, sha256)

    Returns:
        ValidationResult with dataset checksum comparison

    Example:
        >>> source_data = [
        ...     {"id": 1, "name": "Alice", "value": 100},
        ...     {"id": 2, "name": "Bob", "value": 200}
        ... ]
        >>> dest_data = [
        ...     {"id": 1, "name": "Alice", "value": 100},
        ...     {"id": 2, "name": "Bob", "value": 200}
        ... ]
        >>> result = validator.validate_dataset_checksum(
        ...     source_data, dest_data, key_columns=["id", "name", "value"]
        ... )
        >>> result.passed
        True
    """
```

#### `validate_completeness()`

Validate data completeness (no missing required fields).

```python
def validate_completeness(
    self,
    data: List[Dict[str, Any]],
    required_fields: List[str],
) -> ValidationResult:
    """Validate all required fields are present.

    Args:
        data: Dataset to validate
        required_fields: List of required field names

    Returns:
        ValidationResult with completeness check

    Example:
        >>> data = [
        ...     {"id": 1, "name": "Alice", "email": "alice@example.com"},
        ...     {"id": 2, "name": "Bob", "email": None}
        ... ]
        >>> result = validator.validate_completeness(
        ...     data, required_fields=["id", "name", "email"]
        ... )
        >>> result.passed
        False
        >>> result.message
        'Missing values: email in 1 records'
    """
```

---

## SchemaValidator

The `SchemaValidator` class validates schema compatibility and evolution.

### Class Definition

```python
from src.validation.schema import SchemaValidator, SchemaValidationResult

class SchemaValidator:
    """Validate schema compatibility across CDC pipeline."""
```

### Methods

#### `validate_schema_compatibility()`

Validate schemas are compatible between source and destination.

```python
def validate_schema_compatibility(
    self,
    source_schema: Dict[str, str],
    destination_schema: Dict[str, str],
    strict: bool = False,
) -> SchemaValidationResult:
    """Validate schema compatibility.

    Args:
        source_schema: Source schema as {column_name: data_type}
        destination_schema: Destination schema as {column_name: data_type}
        strict: If True, schemas must match exactly
                If False, allow additional columns in destination

    Returns:
        SchemaValidationResult with compatibility details

    Example:
        >>> validator = SchemaValidator()
        >>> source_schema = {
        ...     "id": "INTEGER",
        ...     "name": "VARCHAR(100)",
        ...     "email": "VARCHAR(255)"
        ... }
        >>> dest_schema = {
        ...     "id": "BIGINT",        # Compatible type widening
        ...     "name": "STRING",      # Compatible type
        ...     "email": "STRING",
        ...     "created_at": "TIMESTAMP"  # Additional column (OK if not strict)
        ... }
        >>> result = validator.validate_schema_compatibility(
        ...     source_schema, dest_schema, strict=False
        ... )
        >>> result.passed
        True
        >>> result.compatible_columns
        ['id', 'name', 'email']
        >>> result.additional_columns
        ['created_at']
    """
```

**Returns:**
```python
SchemaValidationResult(
    passed=True,
    message="Schemas are compatible",
    compatible_columns=["id", "name", "email"],
    incompatible_columns=[],
    missing_columns=[],
    additional_columns=["created_at"],
    type_changes=[
        {"column": "id", "source": "INTEGER", "destination": "BIGINT"}
    ]
)
```

#### `detect_schema_evolution()`

Detect schema changes between two versions.

```python
def detect_schema_evolution(
    self,
    old_schema: Dict[str, str],
    new_schema: Dict[str, str],
) -> SchemaEvolutionResult:
    """Detect schema changes.

    Args:
        old_schema: Previous schema version
        new_schema: Current schema version

    Returns:
        SchemaEvolutionResult detailing all changes

    Example:
        >>> old_schema = {"id": "INTEGER", "name": "VARCHAR(100)"}
        >>> new_schema = {
        ...     "id": "INTEGER",
        ...     "name": "VARCHAR(100)",
        ...     "email": "VARCHAR(255)",  # Added
        ...     "created_at": "TIMESTAMP"  # Added
        ... }
        >>> result = validator.detect_schema_evolution(old_schema, new_schema)
        >>> result.columns_added
        ['email', 'created_at']
        >>> result.columns_removed
        []
        >>> result.columns_renamed
        []
    """
```

**Returns:**
```python
SchemaEvolutionResult(
    columns_added=["email", "created_at"],
    columns_removed=[],
    columns_renamed=[],
    type_changes=[],
    breaking_changes=False
)
```

#### `validate_type_compatibility()`

Check if a type change is compatible.

```python
def validate_type_compatibility(
    self,
    source_type: str,
    destination_type: str,
) -> bool:
    """Check if type conversion is safe.

    Compatible conversions:
    - INTEGER → BIGINT (widening)
    - VARCHAR → TEXT (widening)
    - DATE → TIMESTAMP (widening)
    - Any → STRING (general)

    Args:
        source_type: Source column type
        destination_type: Destination column type

    Returns:
        True if conversion is safe, False otherwise

    Example:
        >>> validator.validate_type_compatibility("INTEGER", "BIGINT")
        True
        >>> validator.validate_type_compatibility("BIGINT", "INTEGER")
        False  # Narrowing conversion
    """
```

---

## LagValidator

The `LagValidator` class monitors and validates CDC lag metrics.

### Class Definition

```python
from src.validation.lag import LagValidator, LagValidationResult

class LagValidator:
    """Validate CDC lag is within acceptable thresholds."""
```

### Methods

#### `validate_lag()`

Validate CDC lag against threshold.

```python
def validate_lag(
    self,
    current_lag_seconds: float,
    threshold_seconds: float,
    warning_threshold_seconds: Optional[float] = None,
) -> LagValidationResult:
    """Validate CDC lag is within threshold.

    Args:
        current_lag_seconds: Current CDC lag in seconds
        threshold_seconds: Critical threshold (fails if exceeded)
        warning_threshold_seconds: Warning threshold (warns if exceeded)

    Returns:
        LagValidationResult with lag status

    Example:
        >>> validator = LagValidator()
        >>> result = validator.validate_lag(
        ...     current_lag_seconds=8.5,
        ...     threshold_seconds=30.0,
        ...     warning_threshold_seconds=10.0
        ... )
        >>> result.passed
        True
        >>> result.severity
        'warning'
        >>> result.message
        'CDC lag is 8.5s (warning threshold: 10.0s)'
    """
```

**Returns:**
```python
LagValidationResult(
    passed=True,
    severity="warning",  # 'ok', 'warning', 'critical'
    message="CDC lag is 8.5s (warning threshold: 10.0s)",
    metrics={
        "current_lag_seconds": 8.5,
        "threshold_seconds": 30.0,
        "warning_threshold_seconds": 10.0,
        "percentage_of_threshold": 28.3
    }
)
```

#### `calculate_lag()`

Calculate CDC lag from timestamps.

```python
def calculate_lag(
    self,
    source_timestamp: datetime,
    destination_timestamp: datetime,
) -> float:
    """Calculate lag between source and destination.

    Args:
        source_timestamp: Last update timestamp in source
        destination_timestamp: Last update timestamp in destination

    Returns:
        Lag in seconds

    Example:
        >>> from datetime import datetime, timedelta
        >>> source_ts = datetime(2024, 1, 15, 10, 30, 0)
        >>> dest_ts = source_ts + timedelta(seconds=5.5)
        >>> lag = validator.calculate_lag(source_ts, dest_ts)
        >>> lag
        5.5
    """
```

#### `get_lag_trend()`

Analyze lag trend over time.

```python
def get_lag_trend(
    self,
    lag_history: List[Tuple[datetime, float]],
    window_minutes: int = 10,
) -> LagTrend:
    """Analyze lag trend.

    Args:
        lag_history: List of (timestamp, lag_seconds) tuples
        window_minutes: Time window for trend analysis

    Returns:
        LagTrend with trend direction and statistics

    Example:
        >>> lag_history = [
        ...     (datetime(2024, 1, 15, 10, 0), 2.0),
        ...     (datetime(2024, 1, 15, 10, 1), 2.5),
        ...     (datetime(2024, 1, 15, 10, 2), 3.0),
        ...     (datetime(2024, 1, 15, 10, 3), 3.5),
        ... ]
        >>> trend = validator.get_lag_trend(lag_history, window_minutes=10)
        >>> trend.direction
        'increasing'
        >>> trend.rate_of_change
        0.5  # seconds per minute
    """
```

**Returns:**
```python
LagTrend(
    direction="increasing",  # 'increasing', 'decreasing', 'stable'
    rate_of_change=0.5,      # seconds per minute
    average_lag=2.75,
    max_lag=3.5,
    min_lag=2.0
)
```

---

## ChecksumValidator

Specialized validator for cryptographic checksum validation.

### Class Definition

```python
from src.validation.checksum import ChecksumValidator

class ChecksumValidator:
    """Calculate and validate checksums for data integrity."""
```

### Methods

#### `calculate_checksum()`

Calculate checksum for dataset.

```python
def calculate_checksum(
    self,
    data: List[Dict[str, Any]],
    columns: Optional[List[str]] = None,
    algorithm: str = "md5",
) -> str:
    """Calculate checksum for dataset.

    Args:
        data: Dataset records
        columns: Columns to include (None = all columns)
        algorithm: Hash algorithm (md5, sha1, sha256, xxhash)

    Returns:
        Hexadecimal checksum string

    Example:
        >>> validator = ChecksumValidator()
        >>> data = [
        ...     {"id": 1, "name": "Alice", "value": 100},
        ...     {"id": 2, "name": "Bob", "value": 200}
        ... ]
        >>> checksum = validator.calculate_checksum(
        ...     data, columns=["id", "name"], algorithm="sha256"
        ... )
        >>> checksum
        '8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4'
    """
```

#### `calculate_row_checksum()`

Calculate checksum for a single row.

```python
def calculate_row_checksum(
    self,
    row: Dict[str, Any],
    columns: Optional[List[str]] = None,
    algorithm: str = "md5",
) -> str:
    """Calculate checksum for single row.

    Args:
        row: Single record
        columns: Columns to include
        algorithm: Hash algorithm

    Returns:
        Hexadecimal checksum string

    Example:
        >>> row = {"id": 1, "name": "Alice", "value": 100}
        >>> checksum = validator.calculate_row_checksum(row)
        >>> checksum
        '098f6bcd4621d373cade4e832627b4f6'
    """
```

#### `validate_incremental_checksum()`

Validate checksum incrementally for streaming data.

```python
def validate_incremental_checksum(
    self,
    previous_checksum: str,
    new_records: List[Dict[str, Any]],
    algorithm: str = "md5",
) -> Tuple[str, ValidationResult]:
    """Update and validate checksum incrementally.

    Args:
        previous_checksum: Checksum before new records
        new_records: New records to include
        algorithm: Hash algorithm

    Returns:
        Tuple of (new_checksum, validation_result)

    Example:
        >>> initial_data = [{"id": 1, "value": 100}]
        >>> prev_checksum = validator.calculate_checksum(initial_data)
        >>>
        >>> new_records = [{"id": 2, "value": 200}]
        >>> new_checksum, result = validator.validate_incremental_checksum(
        ...     prev_checksum, new_records
        ... )
        >>> result.passed
        True
    """
```

---

## ValidationResult

Data class for validation results.

### Class Definition

```python
from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass
class ValidationResult:
    """Result of a validation check."""

    check_name: str
    passed: bool
    message: str
    metrics: Dict[str, Any] = None
    details: Optional[str] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
        if self.metrics is None:
            self.metrics = {}
```

### Methods

#### `to_dict()`

Convert result to dictionary.

```python
def to_dict(self) -> Dict[str, Any]:
    """Convert to dictionary for serialization."""
    return {
        "check_name": self.check_name,
        "passed": self.passed,
        "message": self.message,
        "metrics": self.metrics,
        "details": self.details,
        "timestamp": self.timestamp.isoformat()
    }
```

#### `to_json()`

Convert result to JSON string.

```python
def to_json(self) -> str:
    """Convert to JSON string."""
    import json
    return json.dumps(self.to_dict(), indent=2)
```

---

## Usage Examples

### Example 1: Basic Row Count Validation

```python
from src.validation.integrity import IntegrityValidator
import psycopg2
from pyiceberg.catalog import load_catalog

# Get source count from PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost", database="demo_db",
    user="postgres", password="postgres"
)
cursor = pg_conn.cursor()
cursor.execute("SELECT COUNT(*) FROM customers")
source_count = cursor.fetchone()[0]

# Get destination count from Iceberg
catalog = load_catalog("demo_catalog")
table = catalog.load_table("cdc_demo.customers_analytics")
dest_count = table.scan().to_arrow().num_rows

# Validate
validator = IntegrityValidator()
result = validator.validate_row_count(
    source_count=source_count,
    destination_count=dest_count,
    tolerance=0.01  # 1% tolerance
)

if result.passed:
    print(f"✅ {result.message}")
else:
    print(f"❌ {result.message}")
    print(f"   Metrics: {result.metrics}")
```

### Example 2: Schema Evolution Detection

```python
from src.validation.schema import SchemaValidator

# Get schemas
old_schema = {
    "id": "INTEGER",
    "name": "VARCHAR(100)",
    "email": "VARCHAR(255)"
}

new_schema = {
    "id": "BIGINT",
    "full_name": "VARCHAR(200)",  # Renamed from 'name'
    "email": "VARCHAR(255)",
    "created_at": "TIMESTAMP"  # Added
}

# Detect evolution
validator = SchemaValidator()
evolution = validator.detect_schema_evolution(old_schema, new_schema)

print(f"Columns added: {evolution.columns_added}")
print(f"Columns removed: {evolution.columns_removed}")
print(f"Type changes: {evolution.type_changes}")
print(f"Breaking changes: {evolution.breaking_changes}")
```

### Example 3: CDC Lag Monitoring

```python
from src.validation.lag import LagValidator
from datetime import datetime

# Get timestamps from source and destination
source_max_ts = datetime(2024, 1, 15, 10, 30, 45)
dest_max_ts = datetime(2024, 1, 15, 10, 30, 38)

# Calculate lag
validator = LagValidator()
lag_seconds = validator.calculate_lag(source_max_ts, dest_max_ts)

# Validate against threshold
result = validator.validate_lag(
    current_lag_seconds=lag_seconds,
    threshold_seconds=30.0,
    warning_threshold_seconds=10.0
)

if result.severity == "critical":
    print(f"🚨 CRITICAL: {result.message}")
elif result.severity == "warning":
    print(f"⚠️  WARNING: {result.message}")
else:
    print(f"✅ OK: {result.message}")
```

### Example 4: Composite Validation

```python
from src.validation.composite import CompositeValidator
from src.validation.integrity import IntegrityValidator
from src.validation.schema import SchemaValidator
from src.validation.lag import LagValidator

# Create composite validator
composite = CompositeValidator()

# Add validators
composite.add_validator("integrity", IntegrityValidator())
composite.add_validator("schema", SchemaValidator())
composite.add_validator("lag", LagValidator())

# Run all validations
results = composite.validate_all(
    source_count=10000,
    dest_count=9995,
    source_schema={"id": "INTEGER", "name": "VARCHAR"},
    dest_schema={"id": "BIGINT", "name": "STRING"},
    lag_seconds=5.5
)

# Check overall result
if results.all_passed():
    print("✅ All validations passed")
else:
    print("❌ Some validations failed:")
    for result in results.failed_checks():
        print(f"   - {result.check_name}: {result.message}")
```

### Example 5: Continuous Validation

```python
import time
from src.validation.continuous import ContinuousValidator

# Setup continuous validation
validator = ContinuousValidator(
    pipeline_name="postgres_customers_cdc",
    interval_seconds=30,
    validators={
        "row_count": IntegrityValidator(),
        "lag": LagValidator()
    }
)

# Start validation loop
validator.start()

try:
    while True:
        time.sleep(60)

        # Get latest results
        latest = validator.get_latest_results()
        print(f"Latest validation: {latest.timestamp}")
        print(f"All passed: {latest.all_passed()}")

except KeyboardInterrupt:
    validator.stop()
    print("Validation stopped")
```

---

## Integration Guide

### Integrating with CDC Pipelines

```python
# In your CDC pipeline code
from src.validation.integrity import IntegrityValidator
from src.validation.lag import LagValidator

class PostgresCDCPipeline:
    def __init__(self):
        self.integrity_validator = IntegrityValidator()
        self.lag_validator = LagValidator()

    def process_batch(self, batch):
        # Process batch...

        # Validate after processing
        self.validate_batch(batch)

    def validate_batch(self, batch):
        # Row count validation
        source_count = self.get_source_count()
        dest_count = self.get_dest_count()

        result = self.integrity_validator.validate_row_count(
            source_count, dest_count, tolerance=0.01
        )

        if not result.passed:
            self.handle_validation_failure(result)

        # Lag validation
        lag = self.calculate_current_lag()
        lag_result = self.lag_validator.validate_lag(
            lag, threshold_seconds=30.0
        )

        if lag_result.severity == "critical":
            self.handle_high_lag(lag_result)
```

### Integration with Monitoring

```python
from prometheus_client import Gauge, Counter

# Prometheus metrics
validation_passed = Gauge(
    'cdc_validation_passed',
    'Validation check passed (1=passed, 0=failed)',
    ['pipeline', 'check_name']
)

validation_lag = Gauge(
    'cdc_validation_lag_seconds',
    'CDC lag in seconds',
    ['pipeline']
)

# Update metrics from validation results
def update_metrics(pipeline_name: str, result: ValidationResult):
    validation_passed.labels(
        pipeline=pipeline_name,
        check_name=result.check_name
    ).set(1 if result.passed else 0)

    if 'lag_seconds' in result.metrics:
        validation_lag.labels(pipeline=pipeline_name).set(
            result.metrics['lag_seconds']
        )
```

---

## Error Handling

```python
from src.validation.exceptions import (
    ValidationError,
    SchemaIncompatibilityError,
    LagThresholdExceededError
)

try:
    result = validator.validate_row_count(
        source_count, dest_count, tolerance=0.0
    )

    if not result.passed:
        raise ValidationError(
            f"Validation failed: {result.message}",
            validation_result=result
        )

except ValidationError as e:
    logger.error(f"Validation failed: {e}")
    # Handle error (retry, alert, etc.)

except Exception as e:
    logger.exception("Unexpected error during validation")
```

---

For more information, see:
- [Data Quality Tests](../../tests/data_quality/)
- [Integration Tests](../../tests/integration/)
- [Architecture Documentation](../architecture.md)
