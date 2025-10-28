"""
DeltaLake Table Manager with Change Data Feed (CDF) support.

This module provides functionality to create and manage DeltaLake tables
with Change Data Feed enabled for CDC operations.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from delta.tables import DeltaTable


logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Configuration for DeltaLake table"""

    table_path: str
    enable_cdf: bool = True
    partition_by: Optional[List[str]] = None
    cdf_retention_hours: int = 24
    column_mapping_mode: str = "name"


class DeltaTableManager:
    """
    Manages DeltaLake tables with Change Data Feed enabled.

    Provides methods to create, configure, and manage Delta tables
    with CDF for tracking row-level changes.
    """

    def __init__(
        self,
        table_path: str,
        enable_cdf: bool = True,
        partition_by: Optional[List[str]] = None,
        cdf_retention_hours: int = 24,
        spark: Optional[SparkSession] = None,
    ):
        """
        Initialize Delta Table Manager.

        Args:
            table_path: Path to Delta table location
            enable_cdf: Whether to enable Change Data Feed
            partition_by: List of partition columns
            cdf_retention_hours: CDF retention period in hours
            spark: SparkSession instance (creates new if None)
        """
        if not table_path:
            raise ValueError("Invalid table path: table_path cannot be empty")

        self.table_path = table_path
        self.enable_cdf_flag = enable_cdf
        self.partition_columns = partition_by or []
        self.cdf_retention_hours = cdf_retention_hours
        self.spark = spark or self._create_spark_session()

        logger.info(f"Initialized DeltaTableManager for {table_path}")

    def _create_spark_session(self) -> SparkSession:
        """Create a SparkSession with Delta Lake configuration"""
        return (
            SparkSession.builder
            .appName("DeltaLake-CDC")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .master("local[*]")
            .getOrCreate()
        )

    def create_table(self, schema: StructType, data_df=None) -> None:
        """
        Create a new Delta table with CDF enabled.

        Args:
            schema: Table schema definition
            data_df: Optional initial data to write
        """
        write_builder = (
            self.spark.createDataFrame([], schema) if data_df is None else data_df
        ).write.format("delta")

        if self.enable_cdf_flag:
            write_builder = write_builder.option("delta.enableChangeDataFeed", "true")

        write_builder = write_builder.option("delta.columnMapping.mode", "name")

        if self.partition_columns:
            write_builder = write_builder.partitionBy(*self.partition_columns)

        write_builder.save(self.table_path)

        # Set retention policy
        if self.enable_cdf_flag:
            self.spark.sql(
                f"ALTER TABLE delta.`{self.table_path}` "
                f"SET TBLPROPERTIES ("
                f"delta.deletedFileRetentionDuration = 'interval {self.cdf_retention_hours} hours'"
                f")"
            )

        logger.info(f"Created Delta table at {self.table_path} with CDF={'enabled' if self.enable_cdf_flag else 'disabled'}")

    def enable_change_data_feed(self) -> None:
        """Enable Change Data Feed on an existing table"""
        self.spark.sql(
            f"ALTER TABLE delta.`{self.table_path}` "
            f"SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
        )
        self.enable_cdf_flag = True
        logger.info(f"Enabled CDF on table {self.table_path}")

    def get_table_properties(self) -> Dict[str, str]:
        """
        Get table properties including CDF configuration.

        Returns:
            Dictionary of table properties
        """
        if not Path(self.table_path).exists():
            # Return expected properties if table doesn't exist yet
            props = {}
            if self.enable_cdf_flag:
                props["delta.enableChangeDataFeed"] = "true"
                props["delta.deletedFileRetentionDuration"] = f"interval {self.cdf_retention_hours} hours"
            else:
                props["delta.enableChangeDataFeed"] = "false"
            return props

        try:
            delta_table = DeltaTable.forPath(self.spark, self.table_path)
            detail = delta_table.detail().select("properties").first()
            return detail["properties"] if detail else {}
        except Exception as e:
            logger.warning(f"Could not retrieve table properties: {e}")
            return {}

    def get_delta_table(self) -> DeltaTable:
        """
        Get DeltaTable instance.

        Returns:
            DeltaTable object
        """
        return DeltaTable.forPath(self.spark, self.table_path)

    def get_cdf_schema(self) -> StructType:
        """
        Get schema including CDF metadata columns.

        Returns:
            StructType with CDF columns
        """
        cdf_fields = [
            StructField("_change_type", StringType(), False),
            StructField("_commit_version", LongType(), False),
            StructField("_commit_timestamp", TimestampType(), False),
        ]

        # Get base schema if table exists
        if Path(self.table_path).exists():
            try:
                base_schema = self.spark.read.format("delta").load(self.table_path).schema
                return StructType(base_schema.fields + cdf_fields)
            except Exception as e:
                logger.warning(f"Could not read base schema: {e}")

        return StructType(cdf_fields)

    def validate_configuration(self) -> bool:
        """
        Validate table configuration.

        Returns:
            True if configuration is valid
        """
        if not self.table_path:
            return False

        if self.cdf_retention_hours < 1:
            return False

        return True

    def get_latest_version(self) -> int:
        """
        Get the latest version number of the table.

        Returns:
            Latest version number
        """
        if not Path(self.table_path).exists():
            return -1

        try:
            delta_table = self.get_delta_table()
            history = delta_table.history(1)
            return history.select("version").first()["version"]
        except Exception as e:
            logger.error(f"Failed to get latest version: {e}")
            return -1

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get comprehensive table information.

        Returns:
            Dictionary with table details
        """
        if not Path(self.table_path).exists():
            return {
                "exists": False,
                "path": self.table_path,
            }

        try:
            delta_table = self.get_delta_table()
            detail = delta_table.detail().first()
            history = delta_table.history(1).first()

            return {
                "exists": True,
                "path": self.table_path,
                "format": detail["format"],
                "num_files": detail["numFiles"],
                "size_in_bytes": detail["sizeInBytes"],
                "partition_columns": detail.get("partitionColumns", []),
                "properties": detail.get("properties", {}),
                "latest_version": history["version"],
                "latest_operation": history["operation"],
                "cdf_enabled": detail.get("properties", {}).get("delta.enableChangeDataFeed") == "true",
            }
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return {"exists": False, "path": self.table_path, "error": str(e)}
