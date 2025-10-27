# Validation API Contract

**Version**: 1.0.0
**Date**: 2025-10-27
**Purpose**: Define programmatic interface for data quality validation

## Overview

The Validation API provides Python interfaces for performing data quality checks on CDC pipelines. These APIs are used by the CLI, tests, and can be imported for custom validation logic.

## 1. Core Validation Interfaces

### 1.1 `RowCountValidator`

Validates row counts match between source and destination.

**Class Signature**:
```python
class RowCountValidator:
    def __init__(self, tolerance: float = 0.01):
        """
        Initialize row count validator.

        Args:
            tolerance: Acceptable difference as fraction (default 0.01 = 1%)
        """

    def validate(
        self,
        source_count: int,
        destination_count: int
    ) -> ValidationResult:
        """
        Validate row counts match within tolerance.

        Args:
            source_count: Number of rows in source
            destination_count: Number of rows in destination

        Returns:
            ValidationResult with status (PASS/FAIL) and details

        Raises:
            ValueError: If counts are negative
        """
```

**Usage Example**:
```python
validator = RowCountValidator(tolerance=0.01)
result = validator.validate(source_count=10000, destination_count=9998)

if result.status == ValidationStatus.PASS:
    print(f"✓ Row count validation passed: {result.message}")
else:
    print(f"✗ Row count validation failed: {result.message}")
```

**Validation Logic**:
```python
def _validate_logic(source: int, dest: int, tolerance: float) -> bool:
    if source == 0:
        return dest == 0
    diff_pct = abs(source - dest) / source
    return diff_pct <= tolerance
```

### 1.2 `ChecksumValidator`

Validates data integrity using hash-based checksums.

**Class Signature**:
```python
class ChecksumValidator:
    def __init__(
        self,
        algorithm: str = "xxhash64",
        sample_size: Optional[int] = None
    ):
        """
        Initialize checksum validator.

        Args:
            algorithm: Hash algorithm (xxhash64, md5, sha256)
            sample_size: Number of rows to sample (None = all rows)
        """

    def compute_checksum(
        self,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        key_columns: List[str]
    ) -> str:
        """
        Compute checksum for dataset.

        Args:
            data: DataFrame to hash
            key_columns: Columns to include in checksum

        Returns:
            Hex string of checksum
        """

    def validate(
        self,
        source_data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        dest_data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        key_columns: List[str]
    ) -> ValidationResult:
        """
        Validate data integrity via checksum comparison.

        Args:
            source_data: Source dataset
            dest_data: Destination dataset
            key_columns: Columns to include in checksum

        Returns:
            ValidationResult with hash match status
        """
```

**Usage Example**:
```python
validator = ChecksumValidator(algorithm="xxhash64", sample_size=1000)

source_df = pd.read_sql("SELECT * FROM customers", source_conn)
dest_df = spark.read.format("delta").load("/data/customers_cdc")

result = validator.validate(
    source_data=source_df,
    dest_data=dest_df,
    key_columns=["customer_id", "email"]
)

print(result.details["source_checksum"])  # "a1b2c3d4e5f6"
print(result.details["dest_checksum"])    # "a1b2c3d4e5f6"
```

### 1.3 `LagMonitor`

Monitors CDC lag (time between source change and destination write).

**Class Signature**:
```python
class LagMonitor:
    def __init__(self, threshold_seconds: float = 5.0):
        """
        Initialize lag monitor.

        Args:
            threshold_seconds: Maximum acceptable lag
        """

    def measure_lag(
        self,
        source_latest_ts: datetime,
        dest_latest_ts: datetime
    ) -> float:
        """
        Measure lag in seconds.

        Args:
            source_latest_ts: Latest timestamp in source
            dest_latest_ts: Latest timestamp in destination

        Returns:
            Lag in seconds
        """

    def validate(
        self,
        pipeline_id: str,
        current_lag: Optional[float] = None
    ) -> ValidationResult:
        """
        Validate CDC lag within threshold.

        Args:
            pipeline_id: CDC pipeline identifier
            current_lag: Current lag in seconds (if None, will measure)

        Returns:
            ValidationResult with lag status
        """
```

**Usage Example**:
```python
monitor = LagMonitor(threshold_seconds=5.0)

# Measure lag from timestamps
source_latest = source_conn.execute("SELECT MAX(last_updated) FROM customers").fetchone()[0]
dest_latest = dest_conn.execute("SELECT MAX(_cdc_timestamp) FROM customers_cdc").fetchone()[0]

lag_seconds = monitor.measure_lag(source_latest, dest_latest)
result = monitor.validate(pipeline_id="postgres_customers_cdc", current_lag=lag_seconds)

if result.status == ValidationStatus.WARN:
    print(f"⚠ Lag warning: {lag_seconds}s (threshold: 5s)")
```

### 1.4 `SchemaValidator`

Validates schema compatibility between source and destination.

**Class Signature**:
```python
class SchemaValidator:
    def __init__(
        self,
        compatibility_mode: str = "backward"
    ):
        """
        Initialize schema validator.

        Args:
            compatibility_mode: backward|forward|full (Avro compatibility)
        """

    def extract_schema(
        self,
        data_source: Union[str, pd.DataFrame, pyspark.sql.DataFrame]
    ) -> Dict[str, Any]:
        """
        Extract schema from data source.

        Args:
            data_source: Database table name, DataFrame, or file path

        Returns:
            Schema dictionary {field_name: {type, nullable, ...}}
        """

    def validate_compatibility(
        self,
        source_schema: Dict[str, Any],
        dest_schema: Dict[str, Any]
    ) -> ValidationResult:
        """
        Validate schema compatibility.

        Args:
            source_schema: Source schema dictionary
            dest_schema: Destination schema dictionary

        Returns:
            ValidationResult with compatibility status
        """
```

**Usage Example**:
```python
validator = SchemaValidator(compatibility_mode="backward")

source_schema = validator.extract_schema("customers")  # PostgreSQL table
dest_schema = validator.extract_schema("/data/deltalake/customers_cdc")  # Delta table

result = validator.validate_compatibility(source_schema, dest_schema)

if result.status == ValidationStatus.FAIL:
    print(f"Schema incompatibility: {result.details['mismatches']}")
    # Example: {'email': 'type mismatch: VARCHAR vs STRING (compatible)',
    #           'new_column': 'missing in destination'}
```

### 1.5 `IntegrityValidator`

Validates data integrity constraints (primary keys, nullability, referential integrity).

**Class Signature**:
```python
class IntegrityValidator:
    def __init__(self):
        """Initialize integrity validator."""

    def validate_primary_key_unique(
        self,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        pk_columns: List[str]
    ) -> ValidationResult:
        """
        Validate primary key uniqueness.

        Args:
            data: Dataset to validate
            pk_columns: Primary key column names

        Returns:
            ValidationResult with uniqueness status
        """

    def validate_nullability(
        self,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        required_columns: List[str]
    ) -> ValidationResult:
        """
        Validate required columns have no NULLs.

        Args:
            data: Dataset to validate
            required_columns: Columns that must not be NULL

        Returns:
            ValidationResult with null check status
        """

    def validate_referential_integrity(
        self,
        child_data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        parent_data: Union[pd.DataFrame, pyspark.sql.DataFrame],
        fk_column: str,
        pk_column: str
    ) -> ValidationResult:
        """
        Validate foreign key references exist in parent table.

        Args:
            child_data: Child table with foreign key
            parent_data: Parent table with primary key
            fk_column: Foreign key column in child
            pk_column: Primary key column in parent

        Returns:
            ValidationResult with referential integrity status
        """
```

**Usage Example**:
```python
validator = IntegrityValidator()

# Validate PK uniqueness
dest_df = spark.read.format("delta").load("/data/customers_cdc")
result = validator.validate_primary_key_unique(dest_df, pk_columns=["customer_id"])

if result.status == ValidationStatus.FAIL:
    print(f"PK violations: {result.details['duplicate_count']} duplicates found")

# Validate nullability
result = validator.validate_nullability(dest_df, required_columns=["email", "first_name"])

# Validate FK references
orders_df = spark.read.format("delta").load("/data/orders_cdc")
customers_df = spark.read.format("delta").load("/data/customers_cdc")
result = validator.validate_referential_integrity(
    child_data=orders_df,
    parent_data=customers_df,
    fk_column="customer_id",
    pk_column="customer_id"
)
```

## 2. Validation Result Types

### 2.1 `ValidationResult`

Standard result object returned by all validators.

**Class Definition**:
```python
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime

class ValidationStatus(Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    ERROR = "ERROR"

@dataclass
class ValidationResult:
    status: ValidationStatus
    validator_name: str
    message: str
    details: Dict[str, Any]
    timestamp: datetime
    duration_ms: int

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "status": self.status.value,
            "validator_name": self.validator_name,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "duration_ms": self.duration_ms
        }

    def __bool__(self) -> bool:
        """Allow truthiness check: if result: ..."""
        return self.status == ValidationStatus.PASS
```

**Example Usage**:
```python
result = validator.validate(...)

if result:  # Truthiness check
    print("✓ Validation passed")
else:
    print(f"✗ Validation failed: {result.message}")

# Access details
print(json.dumps(result.to_dict(), indent=2))
```

### 2.2 `ValidationReport`

Aggregates multiple validation results.

**Class Definition**:
```python
@dataclass
class ValidationReport:
    pipeline_id: str
    results: List[ValidationResult]
    overall_status: ValidationStatus
    start_time: datetime
    end_time: datetime

    @property
    def total_duration_ms(self) -> int:
        """Total validation duration in milliseconds."""
        return sum(r.duration_ms for r in self.results)

    @property
    def pass_rate(self) -> float:
        """Percentage of passed validations."""
        if not self.results:
            return 0.0
        passed = sum(1 for r in self.results if r.status == ValidationStatus.PASS)
        return (passed / len(self.results)) * 100

    def get_failures(self) -> List[ValidationResult]:
        """Get all failed validations."""
        return [r for r in self.results if r.status == ValidationStatus.FAIL]

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "pipeline_id": self.pipeline_id,
            "overall_status": self.overall_status.value,
            "pass_rate": self.pass_rate,
            "total_validations": len(self.results),
            "passed": sum(1 for r in self.results if r.status == ValidationStatus.PASS),
            "failed": sum(1 for r in self.results if r.status == ValidationStatus.FAIL),
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "total_duration_ms": self.total_duration_ms,
            "results": [r.to_dict() for r in self.results]
        }
```

## 3. Validation Orchestration

### 3.1 `ValidationOrchestrator`

Orchestrates multiple validations for a pipeline.

**Class Signature**:
```python
class ValidationOrchestrator:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize validation orchestrator.

        Args:
            config: Validation configuration dictionary
        """

    def validate_pipeline(
        self,
        pipeline_id: str,
        validation_types: Optional[List[str]] = None,
        fail_fast: bool = False
    ) -> ValidationReport:
        """
        Run all validations for a pipeline.

        Args:
            pipeline_id: Pipeline identifier
            validation_types: Specific validations to run (None = all)
            fail_fast: Stop on first failure

        Returns:
            ValidationReport with all results

        Raises:
            PipelineNotFoundError: If pipeline doesn't exist
            ValidationError: If validation cannot be performed
        """
```

**Usage Example**:
```python
config = {
    "pipelines": {
        "postgres_customers_cdc": {
            "validations": ["row_count", "checksum", "lag", "schema", "integrity"],
            "lag_threshold": 5.0,
            "checksum_sample_size": 1000
        }
    }
}

orchestrator = ValidationOrchestrator(config)
report = orchestrator.validate_pipeline(
    pipeline_id="postgres_customers_cdc",
    fail_fast=False
)

print(f"Overall Status: {report.overall_status}")
print(f"Pass Rate: {report.pass_rate}%")

for failure in report.get_failures():
    print(f"✗ {failure.validator_name}: {failure.message}")
```

## 4. Metrics Export API

### 4.1 Prometheus Metrics

Validation results are exported as Prometheus metrics.

**Exposed Metrics**:

```python
# Gauge: Current validation status (1=PASS, 0=FAIL)
validation_status{pipeline_id="...", validator="..."} 1

# Counter: Total validations performed
validation_total{pipeline_id="...", validator="...", status="PASS"} 42

# Histogram: Validation duration
validation_duration_seconds{pipeline_id="...", validator="..."} 0.123

# Gauge: Row count difference
validation_row_count_diff{pipeline_id="..."} 0

# Gauge: Checksum match (1=match, 0=mismatch)
validation_checksum_match{pipeline_id="..."} 1

# Gauge: CDC lag in seconds
validation_cdc_lag_seconds{pipeline_id="..."} 2.1
```

**Metrics Export Interface**:
```python
class PrometheusMetricsExporter:
    def __init__(self, port: int = 8000):
        """
        Initialize Prometheus metrics exporter.

        Args:
            port: HTTP port for metrics endpoint
        """

    def export_validation_result(
        self,
        result: ValidationResult,
        pipeline_id: str
    ) -> None:
        """
        Export validation result to Prometheus.

        Args:
            result: Validation result to export
            pipeline_id: Pipeline identifier
        """

    def start(self) -> None:
        """Start metrics HTTP server."""

    def stop(self) -> None:
        """Stop metrics HTTP server."""
```

## 5. Integration Examples

### 5.1 Pytest Integration

```python
import pytest
from src.validation import RowCountValidator, ValidationStatus

def test_postgres_customers_row_count():
    """Test row count validation for Postgres customers CDC."""
    validator = RowCountValidator(tolerance=0.01)

    source_count = get_postgres_row_count("customers")
    dest_count = get_delta_row_count("/data/customers_cdc")

    result = validator.validate(source_count, dest_count)

    assert result.status == ValidationStatus.PASS, f"Row count mismatch: {result.message}"
```

### 5.2 CI/CD Integration

```python
#!/usr/bin/env python3
"""CI/CD validation script."""

from src.validation import ValidationOrchestrator
import sys

def main():
    orchestrator = ValidationOrchestrator.from_config_file("config/validation.yaml")

    # Validate all pipelines
    all_passed = True
    for pipeline_id in orchestrator.get_pipeline_ids():
        report = orchestrator.validate_pipeline(pipeline_id, fail_fast=False)

        print(f"\nPipeline: {pipeline_id}")
        print(f"  Status: {report.overall_status.value}")
        print(f"  Pass Rate: {report.pass_rate}%")

        if report.overall_status != ValidationStatus.PASS:
            all_passed = False
            for failure in report.get_failures():
                print(f"  ✗ {failure.validator_name}: {failure.message}")

    # Save report
    with open("validation_report.json", "w") as f:
        json.dump([r.to_dict() for r in reports], f, indent=2)

    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    main()
```

### 5.3 Airflow DAG Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.validation import ValidationOrchestrator

def validate_cdc_pipeline(**context):
    """Airflow task to validate CDC pipeline."""
    pipeline_id = context["params"]["pipeline_id"]

    orchestrator = ValidationOrchestrator.from_config_file("config/validation.yaml")
    report = orchestrator.validate_pipeline(pipeline_id)

    if report.overall_status != ValidationStatus.PASS:
        raise AirflowException(f"Validation failed: {report.get_failures()}")

    # Push report to XCom
    context["ti"].xcom_push(key="validation_report", value=report.to_dict())

with DAG(
    "cdc_postgres_customers",
    start_date=datetime(2025, 10, 27),
    schedule_interval="@hourly"
) as dag:

    validate_task = PythonOperator(
        task_id="validate_cdc",
        python_callable=validate_cdc_pipeline,
        params={"pipeline_id": "postgres_customers_cdc"}
    )
```

## 6. Error Handling

All validation APIs use consistent error types:

```python
class ValidationError(Exception):
    """Base exception for validation errors."""
    pass

class PipelineNotFoundError(ValidationError):
    """Raised when pipeline doesn't exist."""
    pass

class DataSourceUnavailableError(ValidationError):
    """Raised when source or destination is unavailable."""
    pass

class SchemaIncompatibleError(ValidationError):
    """Raised when schemas are incompatible."""
    pass

class ValidationTimeoutError(ValidationError):
    """Raised when validation exceeds timeout."""
    pass
```

**Example**:
```python
try:
    result = validator.validate(source_count, dest_count)
except DataSourceUnavailableError as e:
    logger.error(f"Data source unavailable: {e}")
    # Handle gracefully (retry, alert, etc.)
except ValidationTimeoutError as e:
    logger.warning(f"Validation timeout: {e}")
    # Mark as WARN instead of FAIL
```

## 7. Extensibility

Custom validators can be created by inheriting from `BaseValidator`:

```python
from abc import ABC, abstractmethod

class BaseValidator(ABC):
    @abstractmethod
    def validate(self, *args, **kwargs) -> ValidationResult:
        """Perform validation."""
        pass

# Custom validator example
class CustomBusinessRuleValidator(BaseValidator):
    def validate(self, data: pd.DataFrame) -> ValidationResult:
        """Validate custom business rule."""
        # Custom validation logic
        violations = data[data["lifetime_value"] < 0]

        if len(violations) > 0:
            return ValidationResult(
                status=ValidationStatus.FAIL,
                validator_name="CustomBusinessRuleValidator",
                message=f"Found {len(violations)} negative lifetime values",
                details={"violations": violations.to_dict()},
                timestamp=datetime.now(),
                duration_ms=100
            )

        return ValidationResult(
            status=ValidationStatus.PASS,
            validator_name="CustomBusinessRuleValidator",
            message="All business rules satisfied",
            details={},
            timestamp=datetime.now(),
            duration_ms=100
        )
```

## 8. Performance Considerations

- **Sampling**: Use `sample_size` parameter for checksum validation on large datasets
- **Parallelization**: Validators support parallel execution for independent checks
- **Caching**: Schema validation results are cached for repeated validations
- **Lazy Evaluation**: Data is only loaded when needed (not eagerly)

**Example**:
```python
# Sample 10% of data for checksum validation on large dataset
validator = ChecksumValidator(sample_size=100000)  # Sample first 100K rows

# Parallel validation execution
orchestrator = ValidationOrchestrator(config, max_workers=4)
report = orchestrator.validate_pipeline("postgres_customers_cdc")
# Runs row_count, checksum, lag, schema validators in parallel
```
