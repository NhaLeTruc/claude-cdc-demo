"""DeltaLake writer for MySQL CDC events."""

from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.observability.logging_config import get_logger
from src.observability.metrics import (
    delta_write_duration_seconds,
    delta_write_errors_total,
    delta_writes_total,
)

logger = get_logger(__name__)


class MySQLDeltaLakeWriter:
    """Writes MySQL CDC events to DeltaLake with Change Data Feed support."""

    def __init__(
        self,
        table_path: str,
        primary_key: str,
        spark_session: Optional[SparkSession] = None,
        enable_cdf: bool = True,
        partition_by: Optional[List[str]] = None,
    ) -> None:
        """
        Initialize DeltaLake writer for MySQL CDC.

        Args:
            table_path: Path to DeltaLake table
            primary_key: Primary key column name
            spark_session: Spark session (will create if not provided)
            enable_cdf: Enable Change Data Feed
            partition_by: List of columns to partition by
        """
        self.table_path = table_path
        self.primary_key = primary_key
        self._spark = spark_session
        self.enable_cdf = enable_cdf
        self.partition_by = partition_by or []
        self._table_initialized = False

    def _get_spark(self) -> SparkSession:
        """Get or create Spark session."""
        if self._spark is None:
            self._spark = (
                SparkSession.builder.appName("MySQL_CDC_Writer")
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config(
                    "spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                )
                .getOrCreate()
            )
        return self._spark

    def _initialize_table(self, sample_data: Dict[str, Any]) -> None:
        """
        Initialize DeltaLake table with schema if it doesn't exist.

        Args:
            sample_data: Sample data dict to infer schema
        """
        if self._table_initialized:
            return

        spark = self._get_spark()

        # Check if table exists
        try:
            spark.read.format("delta").load(self.table_path)
            self._table_initialized = True
            logger.info(f"DeltaLake table already exists at {self.table_path}")
            return
        except Exception:
            pass

        # Create initial table with sample data
        sample_data["_cdc_operation"] = "INSERT"
        sample_data["_cdc_timestamp"] = None

        df = spark.createDataFrame([sample_data])

        # Write with CDF enabled
        writer = df.write.format("delta").mode("overwrite")

        if self.enable_cdf:
            writer = writer.option("delta.enableChangeDataFeed", "true")

        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)

        writer.save(self.table_path)
        self._table_initialized = True
        logger.info(f"Initialized DeltaLake table at {self.table_path}")

    def write_batch(self, events: List[Dict[str, Any]]) -> None:
        """
        Write batch of CDC events to DeltaLake.

        Args:
            events: List of CDC events to write
        """
        if not events:
            logger.debug("No events to write")
            return

        import time

        start_time = time.time()

        try:
            # Separate events by operation type
            inserts = [e for e in events if e.get("operation") == "INSERT"]
            updates = [e for e in events if e.get("operation") == "UPDATE"]
            deletes = [e for e in events if e.get("operation") == "DELETE"]

            # Initialize table with first insert if needed
            if not self._table_initialized and inserts:
                self._initialize_table(inserts[0]["data"])

            # Process each operation type
            if inserts:
                self._write_inserts(inserts)

            if updates:
                self._write_updates(updates)

            if deletes:
                self._write_deletes(deletes)

            duration = time.time() - start_time
            delta_write_duration_seconds.observe(duration)
            delta_writes_total.inc()

            logger.info(
                f"Wrote batch of {len(events)} events "
                f"({len(inserts)} inserts, {len(updates)} updates, {len(deletes)} deletes) "
                f"in {duration:.2f}s"
            )

        except Exception as e:
            delta_write_errors_total.inc()
            logger.error(f"Failed to write batch: {e}")
            raise

    def _write_inserts(self, events: List[Dict[str, Any]]) -> None:
        """Write INSERT events."""
        spark = self._get_spark()

        rows = []
        for event in events:
            row = event.get("data", {}).copy()
            row["_cdc_operation"] = "INSERT"
            row["_cdc_timestamp"] = event.get("timestamp")
            rows.append(row)

        df = spark.createDataFrame(rows)

        # Append to table
        df.write.format("delta").mode("append").save(self.table_path)

        logger.debug(f"Wrote {len(events)} INSERT events")

    def _write_updates(self, events: List[Dict[str, Any]]) -> None:
        """Write UPDATE events using merge."""
        spark = self._get_spark()
        from delta.tables import DeltaTable

        rows = []
        for event in events:
            row = event.get("data", {}).copy()
            row["_cdc_operation"] = "UPDATE"
            row["_cdc_timestamp"] = event.get("timestamp")
            rows.append(row)

        update_df = spark.createDataFrame(rows)

        # Load Delta table
        delta_table = DeltaTable.forPath(spark, self.table_path)

        # Merge updates
        delta_table.alias("target").merge(
            update_df.alias("source"),
            f"target.{self.primary_key} = source.{self.primary_key}",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        logger.debug(f"Wrote {len(events)} UPDATE events")

    def _write_deletes(self, events: List[Dict[str, Any]]) -> None:
        """Write DELETE events."""
        spark = self._get_spark()
        from delta.tables import DeltaTable

        # Extract primary keys to delete
        primary_keys = [
            event.get("data", {}).get(self.primary_key) for event in events
        ]
        primary_keys = [pk for pk in primary_keys if pk is not None]

        if not primary_keys:
            logger.warning("No valid primary keys found for DELETE events")
            return

        # Load Delta table
        delta_table = DeltaTable.forPath(spark, self.table_path)

        # Delete matching records
        condition = f"{self.primary_key} IN ({','.join(map(str, primary_keys))})"
        delta_table.delete(condition)

        logger.debug(f"Wrote {len(events)} DELETE events")

    def compact(self) -> None:
        """Compact Delta table files."""
        spark = self._get_spark()

        logger.info(f"Compacting DeltaLake table at {self.table_path}")

        # Read and write to trigger compaction
        df = spark.read.format("delta").load(self.table_path)
        df.write.format("delta").mode("overwrite").option(
            "dataChange", "false"
        ).save(self.table_path)

        logger.info("Compaction complete")

    def vacuum(self, retention_hours: int = 168) -> None:
        """
        Vacuum old files from Delta table.

        Args:
            retention_hours: Retention period in hours (default 7 days)
        """
        spark = self._get_spark()
        from delta.tables import DeltaTable

        logger.info(
            f"Vacuuming DeltaLake table at {self.table_path} "
            f"(retention: {retention_hours}h)"
        )

        delta_table = DeltaTable.forPath(spark, self.table_path)
        delta_table.vacuum(retention_hours)

        logger.info("Vacuum complete")

    def get_table_version(self) -> int:
        """
        Get current Delta table version.

        Returns:
            Current version number
        """
        spark = self._get_spark()
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, self.table_path)
        history = delta_table.history(1).collect()

        if history:
            return history[0]["version"]
        return 0
