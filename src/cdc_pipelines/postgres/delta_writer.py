"""DeltaLake writer for CDC events."""

from typing import Any, Dict, List, Optional

from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class DeltaLakeWriter:
    """Writes CDC events to DeltaLake with Change Data Feed support."""

    def __init__(
        self,
        table_path: str,
        table_name: str,
        enable_cdf: bool = True,
        partition_by: Optional[List[str]] = None,
        primary_key: str = "id",
    ) -> None:
        """
        Initialize DeltaLake writer.

        Args:
            table_path: Path to DeltaLake table
            table_name: Table name
            enable_cdf: Enable Change Data Feed
            partition_by: List of columns to partition by
            primary_key: Primary key column name
        """
        self.table_path = table_path
        self.table_name = table_name
        self.enable_cdf = enable_cdf
        self.partition_by = partition_by or []
        self.primary_key = primary_key
        self._spark = None

        logger.info(
            f"Initialized DeltaLake writer for {table_name} at {table_path} "
            f"(CDF: {enable_cdf})"
        )

    def _get_spark(self) -> Any:
        """
        Get or create Spark session.

        Returns:
            Spark session
        """
        if self._spark is None:
            try:
                from pyspark.sql import SparkSession

                self._spark = (
                    SparkSession.builder.appName(f"CDC-{self.table_name}")
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                    .config(
                        "spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    )
                    .config(
                        "spark.databricks.delta.properties.defaults.enableChangeDataFeed",
                        str(self.enable_cdf).lower(),
                    )
                    .getOrCreate()
                )
                logger.info("Created Spark session for DeltaLake")
            except ImportError:
                logger.error("PySpark not available. Install with: pip install pyspark delta-spark")
                raise

        return self._spark

    def write(self, event: Dict[str, Any]) -> None:
        """
        Write single CDC event to DeltaLake.

        Args:
            event: Standardized CDC event
        """
        operation = event.get("operation")

        if operation == "INSERT":
            self._write_insert(event)
        elif operation == "UPDATE":
            self._write_update(event)
        elif operation == "DELETE":
            self._write_delete(event)
        else:
            logger.warning(f"Unknown operation: {operation}")

    def write_batch(self, events: List[Dict[str, Any]]) -> None:
        """
        Write batch of CDC events to DeltaLake.

        Args:
            events: List of standardized CDC events
        """
        if not events:
            logger.warning("No events to write")
            return

        # Group events by operation type
        inserts = [e for e in events if e.get("operation") == "INSERT"]
        updates = [e for e in events if e.get("operation") == "UPDATE"]
        deletes = [e for e in events if e.get("operation") == "DELETE"]

        logger.info(
            f"Writing batch: {len(inserts)} inserts, {len(updates)} updates, "
            f"{len(deletes)} deletes"
        )

        # Process in order: inserts, updates, deletes
        for event in inserts:
            self._write_insert(event)
        for event in updates:
            self._write_update(event)
        for event in deletes:
            self._write_delete(event)

    def _write_insert(self, event: Dict[str, Any]) -> None:
        """Write INSERT event."""
        try:
            spark = self._get_spark()
            data = event.get("data", {})

            # Add CDC metadata
            data["_cdc_operation"] = "INSERT"
            data["_cdc_timestamp"] = event.get("timestamp")

            # Create DataFrame and write
            df = spark.createDataFrame([data])

            writer = df.write.format("delta").mode("append")

            if self.partition_by:
                writer = writer.partitionBy(self.partition_by)

            if self.enable_cdf:
                writer = writer.option("delta.enableChangeDataFeed", "true")

            writer.save(self.table_path)

            logger.debug(f"Wrote INSERT for {self.table_name}")

        except Exception as e:
            logger.error(f"Failed to write INSERT: {e}", exc_info=True)
            raise

    def _write_update(self, event: Dict[str, Any]) -> None:
        """Write UPDATE event using merge."""
        try:
            spark = self._get_spark()
            from delta.tables import DeltaTable

            data = event.get("data", {})
            primary_key_value = event.get("primary_key")

            if not primary_key_value:
                logger.warning("No primary key for UPDATE, falling back to INSERT")
                self._write_insert(event)
                return

            # Add CDC metadata
            data["_cdc_operation"] = "UPDATE"
            data["_cdc_timestamp"] = event.get("timestamp")

            # Create update DataFrame
            update_df = spark.createDataFrame([data])

            # Perform merge (upsert)
            delta_table = DeltaTable.forPath(spark, self.table_path)

            delta_table.alias("target").merge(
                update_df.alias("source"),
                f"target.{self.primary_key} = source.{self.primary_key}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            logger.debug(f"Wrote UPDATE for {self.table_name}")

        except Exception as e:
            logger.error(f"Failed to write UPDATE: {e}", exc_info=True)
            raise

    def _write_delete(self, event: Dict[str, Any]) -> None:
        """Write DELETE event."""
        try:
            spark = self._get_spark()
            from delta.tables import DeltaTable

            primary_key_value = event.get("primary_key")

            if not primary_key_value:
                logger.warning("No primary key for DELETE, skipping")
                return

            # Perform delete
            delta_table = DeltaTable.forPath(spark, self.table_path)
            delta_table.delete(f"{self.primary_key} = {primary_key_value}")

            logger.debug(f"Wrote DELETE for {self.table_name}")

        except Exception as e:
            logger.error(f"Failed to write DELETE: {e}", exc_info=True)
            raise

    def get_table_version(self) -> int:
        """
        Get current table version.

        Returns:
            Current Delta table version
        """
        try:
            spark = self._get_spark()
            from delta.tables import DeltaTable

            delta_table = DeltaTable.forPath(spark, self.table_path)
            history = delta_table.history(1).collect()

            if history:
                return history[0]["version"]
            return 0

        except Exception as e:
            logger.error(f"Failed to get table version: {e}")
            return 0

    def close(self) -> None:
        """Close Spark session."""
        if self._spark:
            self._spark.stop()
            logger.info("Closed Spark session")
