"""Prometheus metrics exporters."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from typing import Optional

from src.common.config import get_settings


# CDC Pipeline Metrics
cdc_events_total = Counter(
    "cdc_events_total",
    "Total number of CDC events processed",
    ["pipeline", "operation", "table"],
)

cdc_errors_total = Counter(
    "cdc_errors_total",
    "Total number of CDC processing errors",
    ["pipeline", "error_type"],
)

cdc_lag_seconds = Gauge(
    "cdc_lag_seconds",
    "Current CDC lag in seconds",
    ["pipeline"],
)

cdc_processing_duration_seconds = Histogram(
    "cdc_processing_duration_seconds",
    "Time taken to process CDC events",
    ["pipeline"],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
)

# Data Quality Metrics
data_validation_total = Counter(
    "data_validation_total",
    "Total number of validations performed",
    ["validator", "result"],
)

data_validation_failures_total = Counter(
    "data_validation_failures_total",
    "Total number of validation failures",
    ["validator", "failure_type"],
)

row_count_difference = Gauge(
    "row_count_difference",
    "Difference between source and destination row counts",
    ["pipeline"],
)

# Postgres CDC Specific Metrics
postgres_cdc_events_processed_total = Counter(
    "postgres_cdc_events_processed_total",
    "Total Postgres CDC events processed",
    ["table", "operation"],
)

postgres_cdc_batch_size = Histogram(
    "postgres_cdc_batch_size",
    "Size of Postgres CDC event batches",
    buckets=[1, 10, 50, 100, 500, 1000, 5000],
)

postgres_cdc_lag_seconds = Gauge(
    "postgres_cdc_lag_seconds",
    "Current Postgres CDC lag in seconds",
    ["pipeline", "table"],
)

# MySQL CDC Specific Metrics
mysql_cdc_events_processed_total = Counter(
    "mysql_cdc_events_processed_total",
    "Total MySQL CDC events processed",
    ["table", "operation"],
)

mysql_cdc_batch_size = Histogram(
    "mysql_cdc_batch_size",
    "Size of MySQL CDC event batches",
    buckets=[1, 10, 50, 100, 500, 1000, 5000],
)

mysql_cdc_lag_seconds = Gauge(
    "mysql_cdc_lag_seconds",
    "Current MySQL CDC lag in seconds",
    ["pipeline", "table"],
)

mysql_binlog_position = Gauge(
    "mysql_binlog_position",
    "Current MySQL binlog position",
    ["server", "binlog_file"],
)

# DeltaLake CDC Specific Metrics
deltalake_cdf_events_processed_total = Counter(
    "deltalake_cdf_events_processed_total",
    "Total DeltaLake CDF events processed",
    ["table", "change_type"],
)

deltalake_table_version = Gauge(
    "deltalake_table_version",
    "Current DeltaLake table version",
    ["table_path"],
)

deltalake_cdf_lag_versions = Gauge(
    "deltalake_cdf_lag_versions",
    "Number of versions behind in CDF processing",
    ["table_path"],
)

deltalake_cdf_batch_size = Histogram(
    "deltalake_cdf_batch_size",
    "Size of DeltaLake CDF event batches",
    buckets=[1, 10, 50, 100, 500, 1000, 5000, 10000],
)

deltalake_version_processing_duration = Histogram(
    "deltalake_version_processing_duration_seconds",
    "Time taken to process a Delta table version",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0],
)

# Iceberg CDC Specific Metrics
iceberg_snapshot_id = Gauge(
    "iceberg_snapshot_id",
    "Current Iceberg table snapshot ID",
    ["table_identifier"],
)

iceberg_snapshots_lag = Gauge(
    "iceberg_snapshots_lag",
    "Number of snapshots behind in processing",
    ["table_identifier"],
)

iceberg_incremental_rows_processed = Counter(
    "iceberg_incremental_rows_processed_total",
    "Total rows processed incrementally from Iceberg",
    ["table_identifier"],
)

iceberg_snapshot_processing_duration = Histogram(
    "iceberg_snapshot_processing_duration_seconds",
    "Time taken to process snapshot-to-snapshot incremental read",
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
)

iceberg_incremental_batch_size = Histogram(
    "iceberg_incremental_batch_size",
    "Size of incremental data batches from Iceberg",
    buckets=[10, 100, 1000, 10000, 100000, 1000000],
)

checksum_mismatches = Counter(
    "checksum_mismatches_total",
    "Total number of checksum mismatches",
    ["pipeline", "table"],
)

# Connector Metrics
debezium_connector_status = Gauge(
    "debezium_connector_status",
    "Debezium connector status (1=running, 0=stopped, -1=failed)",
    ["connector"],
)

kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag",
    ["topic", "partition", "consumer_group"],
)

# Health Metrics
pipeline_health = Gauge(
    "pipeline_health",
    "Pipeline health status (1=healthy, 0=unhealthy)",
    ["pipeline"],
)


class MetricsExporter:
    """Prometheus metrics exporter."""

    def __init__(self, port: Optional[int] = None) -> None:
        """
        Initialize metrics exporter.

        Args:
            port: Port to expose metrics on (default from config)
        """
        self.settings = get_settings()
        self.port = port or self.settings.observability.metrics_port
        self._server_started = False

    def start(self) -> None:
        """Start metrics HTTP server."""
        if not self._server_started:
            start_http_server(self.port)
            self._server_started = True

    def record_cdc_event(
        self, pipeline: str, operation: str, table: str, duration: float
    ) -> None:
        """
        Record CDC event processing.

        Args:
            pipeline: Pipeline name
            operation: Operation type (INSERT/UPDATE/DELETE)
            table: Table name
            duration: Processing duration in seconds
        """
        cdc_events_total.labels(pipeline=pipeline, operation=operation, table=table).inc()
        cdc_processing_duration_seconds.labels(pipeline=pipeline).observe(duration)

    def record_cdc_error(self, pipeline: str, error_type: str) -> None:
        """
        Record CDC error.

        Args:
            pipeline: Pipeline name
            error_type: Error type/category
        """
        cdc_errors_total.labels(pipeline=pipeline, error_type=error_type).inc()

    def update_cdc_lag(self, pipeline: str, lag_seconds: float) -> None:
        """
        Update CDC lag metric.

        Args:
            pipeline: Pipeline name
            lag_seconds: Current lag in seconds
        """
        cdc_lag_seconds.labels(pipeline=pipeline).set(lag_seconds)

    def record_validation(
        self, validator: str, success: bool, failure_type: Optional[str] = None
    ) -> None:
        """
        Record validation result.

        Args:
            validator: Validator name
            success: Whether validation succeeded
            failure_type: Type of failure if unsuccessful
        """
        result = "success" if success else "failure"
        data_validation_total.labels(validator=validator, result=result).inc()

        if not success and failure_type:
            data_validation_failures_total.labels(
                validator=validator, failure_type=failure_type
            ).inc()

    def update_row_count_diff(self, pipeline: str, table: str, diff: int) -> None:
        """
        Update row count difference metric.

        Args:
            pipeline: Pipeline name
            table: Table name
            diff: Difference in row counts
        """
        row_count_difference.labels(pipeline=pipeline, table=table).set(diff)

    def record_checksum_mismatch(self, pipeline: str, table: str) -> None:
        """
        Record checksum mismatch.

        Args:
            pipeline: Pipeline name
            table: Table name
        """
        checksum_mismatches.labels(pipeline=pipeline, table=table).inc()

    def update_connector_status(self, connector: str, status: str) -> None:
        """
        Update Debezium connector status.

        Args:
            connector: Connector name
            status: Status (RUNNING/STOPPED/FAILED)
        """
        status_value = {"RUNNING": 1, "STOPPED": 0, "FAILED": -1}.get(status, 0)
        debezium_connector_status.labels(connector=connector).set(status_value)

    def update_pipeline_health(self, pipeline: str, healthy: bool) -> None:
        """
        Update pipeline health metric.

        Args:
            pipeline: Pipeline name
            healthy: Health status
        """
        pipeline_health.labels(pipeline=pipeline).set(1 if healthy else 0)

    def record_deltalake_cdf_event(
        self, table: str, change_type: str, batch_size: int, duration: float
    ) -> None:
        """
        Record DeltaLake CDF event processing.

        Args:
            table: Table name
            change_type: Type of change (insert/update_preimage/update_postimage/delete)
            batch_size: Number of events in batch
            duration: Processing duration in seconds
        """
        deltalake_cdf_events_processed_total.labels(
            table=table, change_type=change_type
        ).inc(batch_size)
        deltalake_cdf_batch_size.observe(batch_size)
        deltalake_version_processing_duration.observe(duration)

    def update_deltalake_version(self, table_path: str, version: int) -> None:
        """
        Update DeltaLake table version metric.

        Args:
            table_path: Path to Delta table
            version: Current version number
        """
        deltalake_table_version.labels(table_path=table_path).set(version)

    def update_deltalake_cdf_lag(self, table_path: str, versions_behind: int) -> None:
        """
        Update DeltaLake CDF processing lag.

        Args:
            table_path: Path to Delta table
            versions_behind: Number of versions behind current
        """
        deltalake_cdf_lag_versions.labels(table_path=table_path).set(versions_behind)

    def record_iceberg_incremental_read(
        self, table_identifier: str, rows_processed: int, duration: float
    ) -> None:
        """
        Record Iceberg incremental read processing.

        Args:
            table_identifier: Iceberg table identifier
            rows_processed: Number of rows processed
            duration: Processing duration in seconds
        """
        iceberg_incremental_rows_processed.labels(
            table_identifier=table_identifier
        ).inc(rows_processed)
        iceberg_incremental_batch_size.observe(rows_processed)
        iceberg_snapshot_processing_duration.observe(duration)

    def update_iceberg_snapshot_id(self, table_identifier: str, snapshot_id: int) -> None:
        """
        Update Iceberg current snapshot ID metric.

        Args:
            table_identifier: Iceberg table identifier
            snapshot_id: Current snapshot ID
        """
        iceberg_snapshot_id.labels(table_identifier=table_identifier).set(snapshot_id)

    def update_iceberg_snapshots_lag(
        self, table_identifier: str, snapshots_behind: int
    ) -> None:
        """
        Update Iceberg snapshot processing lag.

        Args:
            table_identifier: Iceberg table identifier
            snapshots_behind: Number of snapshots behind current
        """
        iceberg_snapshots_lag.labels(table_identifier=table_identifier).set(
            snapshots_behind
        )
