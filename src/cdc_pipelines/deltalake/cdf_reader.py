"""
DeltaLake Change Data Feed Reader.

This module provides functionality to query and read change data from
DeltaLake tables with Change Data Feed enabled.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from delta.tables import DeltaTable


logger = logging.getLogger(__name__)


class CDFReader:
    """
    Reader for DeltaLake Change Data Feed.

    Provides methods to query changes between versions or time ranges.
    """

    def __init__(self, table_path: str, spark: Optional[SparkSession] = None):
        """
        Initialize CDF Reader.

        Args:
            table_path: Path to Delta table
            spark: SparkSession instance (creates new if None)
        """
        self.table_path = table_path
        self.spark = spark or self._create_spark_session()
        logger.info(f"Initialized CDFReader for {table_path}")

    def _create_spark_session(self) -> SparkSession:
        """Create a SparkSession with Delta Lake configuration"""
        return (
            SparkSession.builder
            .appName("DeltaLake-CDF-Reader")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .master("local[*]")
            .getOrCreate()
        )

    def read_changes_between_versions(
        self,
        start_version: int,
        end_version: Optional[int] = None,
    ) -> DataFrame:
        """
        Read changes between two table versions.

        Args:
            start_version: Starting version (inclusive)
            end_version: Ending version (inclusive), latest if None

        Returns:
            DataFrame with change data including _change_type column
        """
        logger.info(
            f"Reading changes from version {start_version} to {end_version or 'latest'}"
        )

        read_builder = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", start_version)
        )

        if end_version is not None:
            read_builder = read_builder.option("endingVersion", end_version)

        try:
            changes_df = read_builder.load(self.table_path)
            logger.info(f"Read {changes_df.count()} change records")
            return changes_df
        except Exception as e:
            logger.error(f"Failed to read changes: {e}")
            raise

    def read_changes_since_timestamp(
        self,
        start_timestamp: datetime,
        end_timestamp: Optional[datetime] = None,
    ) -> DataFrame:
        """
        Read changes since a specific timestamp.

        Args:
            start_timestamp: Starting timestamp
            end_timestamp: Ending timestamp (now if None)

        Returns:
            DataFrame with change data
        """
        logger.info(
            f"Reading changes from {start_timestamp} to {end_timestamp or 'now'}"
        )

        read_builder = (
            self.spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingTimestamp", start_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
        )

        if end_timestamp is not None:
            read_builder = read_builder.option(
                "endingTimestamp", end_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            )

        try:
            changes_df = read_builder.load(self.table_path)
            logger.info(f"Read {changes_df.count()} change records")
            return changes_df
        except Exception as e:
            logger.error(f"Failed to read changes: {e}")
            raise

    def get_changes_by_type(
        self,
        change_type: str,
        start_version: int,
        end_version: Optional[int] = None,
    ) -> DataFrame:
        """
        Get changes of a specific type (insert, update_preimage, update_postimage, delete).

        Args:
            change_type: Type of change to filter
            start_version: Starting version
            end_version: Ending version (latest if None)

        Returns:
            DataFrame with filtered changes
        """
        changes_df = self.read_changes_between_versions(start_version, end_version)
        return changes_df.filter(col("_change_type") == change_type)

    def get_inserts(
        self, start_version: int, end_version: Optional[int] = None
    ) -> DataFrame:
        """Get INSERT operations"""
        return self.get_changes_by_type("insert", start_version, end_version)

    def get_updates(
        self, start_version: int, end_version: Optional[int] = None
    ) -> DataFrame:
        """Get UPDATE operations (both preimage and postimage)"""
        changes_df = self.read_changes_between_versions(start_version, end_version)
        return changes_df.filter(
            col("_change_type").isin(["update_preimage", "update_postimage"])
        )

    def get_deletes(
        self, start_version: int, end_version: Optional[int] = None
    ) -> DataFrame:
        """Get DELETE operations"""
        return self.get_changes_by_type("delete", start_version, end_version)

    def get_change_summary(
        self, start_version: int, end_version: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Get summary of changes by type.

        Args:
            start_version: Starting version
            end_version: Ending version (latest if None)

        Returns:
            Dictionary with counts by change type
        """
        changes_df = self.read_changes_between_versions(start_version, end_version)

        summary = (
            changes_df.groupBy("_change_type")
            .count()
            .collect()
        )

        return {row["_change_type"]: row["count"] for row in summary}

    def get_latest_changes(self, num_versions: int = 1) -> DataFrame:
        """
        Get changes from the most recent versions.

        Args:
            num_versions: Number of recent versions to include

        Returns:
            DataFrame with recent changes
        """
        delta_table = DeltaTable.forPath(self.spark, self.table_path)
        latest_version = delta_table.history(1).select("version").first()["version"]

        start_version = max(0, latest_version - num_versions + 1)
        return self.read_changes_between_versions(start_version, latest_version)

    def get_change_stats(
        self, start_version: int, end_version: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get detailed statistics about changes.

        Args:
            start_version: Starting version
            end_version: Ending version (latest if None)

        Returns:
            Dictionary with change statistics
        """
        changes_df = self.read_changes_between_versions(start_version, end_version)

        total_changes = changes_df.count()
        summary = self.get_change_summary(start_version, end_version)

        return {
            "total_changes": total_changes,
            "by_type": summary,
            "start_version": start_version,
            "end_version": end_version or "latest",
        }

    def read_cdf_with_metadata(
        self, start_version: int, end_version: Optional[int] = None
    ) -> DataFrame:
        """
        Read CDF with all metadata columns.

        Args:
            start_version: Starting version
            end_version: Ending version (latest if None)

        Returns:
            DataFrame with all CDF metadata
        """
        return self.read_changes_between_versions(start_version, end_version)

    def is_cdf_enabled(self) -> bool:
        """
        Check if Change Data Feed is enabled on the table.

        Returns:
            True if CDF is enabled
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, self.table_path)
            properties = delta_table.detail().select("properties").first()["properties"]
            return properties.get("delta.enableChangeDataFeed") == "true"
        except Exception as e:
            logger.error(f"Failed to check CDF status: {e}")
            return False
