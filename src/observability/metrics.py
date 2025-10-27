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
