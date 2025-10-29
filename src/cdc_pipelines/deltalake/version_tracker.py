"""
DeltaLake Version Tracker.

This module tracks Delta table versions and provides version management
functionality for CDC operations.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass

from pyspark.sql import SparkSession
from delta.tables import DeltaTable


logger = logging.getLogger(__name__)


@dataclass
class VersionInfo:
    """Information about a Delta table version"""

    version: int
    timestamp: datetime
    operation: str
    operation_metrics: Dict[str, Any]
    user_metadata: Optional[str] = None


class VersionTracker:
    """
    Tracks Delta table versions for CDC processing.

    Maintains version history and provides version comparison capabilities.
    """

    def __init__(self, table_path: str, spark: Optional[SparkSession] = None):
        """
        Initialize Version Tracker.

        Args:
            table_path: Path to Delta table
            spark: SparkSession instance (creates new if None)
        """
        self.table_path = table_path
        self.spark = spark or self._create_spark_session()
        self.delta_table = None
        self._write_version_counter = 0
        self._rollback_history = []
        logger.info(f"Initialized VersionTracker for {table_path}")

    def _create_spark_session(self) -> SparkSession:
        """Create a SparkSession with Delta Lake configuration"""
        return (
            SparkSession.builder
            .appName("DeltaLake-VersionTracker")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .master("local[*]")
            .getOrCreate()
        )

    def _get_delta_table(self) -> DeltaTable:
        """Get or create DeltaTable instance"""
        if self.delta_table is None:
            self.delta_table = DeltaTable.forPath(self.spark, self.table_path)
        return self.delta_table

    def get_latest_version(self) -> int:
        """
        Get the latest version number.

        Returns:
            Latest version number
        """
        try:
            delta_table = self._get_delta_table()
            history = delta_table.history(1)
            return history.select("version").first()["version"]
        except Exception as e:
            logger.error(f"Failed to get latest version: {e}")
            return -1

    def get_version_info(self, version: int) -> Optional[VersionInfo]:
        """
        Get detailed information about a specific version.

        Args:
            version: Version number to query

        Returns:
            VersionInfo object or None if version not found
        """
        try:
            delta_table = self._get_delta_table()
            history = delta_table.history()
            version_row = history.filter(f"version = {version}").first()

            if not version_row:
                return None

            return VersionInfo(
                version=version_row["version"],
                timestamp=version_row["timestamp"],
                operation=version_row["operation"],
                operation_metrics=version_row.get("operationMetrics", {}),
                user_metadata=version_row.get("userMetadata"),
            )
        except Exception as e:
            logger.error(f"Failed to get version info for version {version}: {e}")
            return None

    def get_version_history(self, limit: Optional[int] = None) -> List[VersionInfo]:
        """
        Get version history.

        Args:
            limit: Maximum number of versions to return (all if None)

        Returns:
            List of VersionInfo objects
        """
        try:
            delta_table = self._get_delta_table()
            history_df = delta_table.history(limit) if limit else delta_table.history()

            versions = []
            for row in history_df.collect():
                versions.append(
                    VersionInfo(
                        version=row["version"],
                        timestamp=row["timestamp"],
                        operation=row["operation"],
                        operation_metrics=row.get("operationMetrics", {}),
                        user_metadata=row.get("userMetadata"),
                    )
                )

            return versions
        except Exception as e:
            logger.error(f"Failed to get version history: {e}")
            return []

    def get_versions_in_range(
        self, start_version: int, end_version: int
    ) -> List[VersionInfo]:
        """
        Get versions within a specific range.

        Args:
            start_version: Start version (inclusive)
            end_version: End version (inclusive)

        Returns:
            List of VersionInfo objects
        """
        try:
            delta_table = self._get_delta_table()
            history_df = delta_table.history()
            filtered_df = history_df.filter(
                f"version >= {start_version} AND version <= {end_version}"
            )

            versions = []
            for row in filtered_df.collect():
                versions.append(
                    VersionInfo(
                        version=row["version"],
                        timestamp=row["timestamp"],
                        operation=row["operation"],
                        operation_metrics=row.get("operationMetrics", {}),
                        user_metadata=row.get("userMetadata"),
                    )
                )

            return sorted(versions, key=lambda v: v.version)
        except Exception as e:
            logger.error(f"Failed to get versions in range: {e}")
            return []

    def get_versions_since_timestamp(self, since: datetime) -> List[VersionInfo]:
        """
        Get versions created since a specific timestamp.

        Args:
            since: Timestamp to query from

        Returns:
            List of VersionInfo objects
        """
        try:
            delta_table = self._get_delta_table()
            history_df = delta_table.history()

            # Filter by timestamp
            filtered_df = history_df.filter(
                history_df["timestamp"] >= since
            )

            versions = []
            for row in filtered_df.collect():
                versions.append(
                    VersionInfo(
                        version=row["version"],
                        timestamp=row["timestamp"],
                        operation=row["operation"],
                        operation_metrics=row.get("operationMetrics", {}),
                        user_metadata=row.get("userMetadata"),
                    )
                )

            return sorted(versions, key=lambda v: v.version)
        except Exception as e:
            logger.error(f"Failed to get versions since timestamp: {e}")
            return []

    def find_version_by_timestamp(self, timestamp: datetime) -> Optional[int]:
        """
        Find the version number closest to a given timestamp.

        Args:
            timestamp: Target timestamp

        Returns:
            Version number or None if not found
        """
        try:
            # Use Delta's time travel feature
            df = (
                self.spark.read
                .format("delta")
                .option("timestampAsOf", timestamp.strftime("%Y-%m-%d %H:%M:%S"))
                .load(self.table_path)
            )

            # Get version from history
            delta_table = self._get_delta_table()
            history = delta_table.history()

            # Find version at or before the timestamp
            versions = (
                history
                .filter(history["timestamp"] <= timestamp)
                .orderBy(history["timestamp"].desc())
                .select("version")
                .first()
            )

            return versions["version"] if versions else None
        except Exception as e:
            logger.error(f"Failed to find version by timestamp: {e}")
            return None

    def get_operation_counts(self) -> Dict[str, int]:
        """
        Get count of operations by type across all versions.

        Returns:
            Dictionary with operation counts
        """
        try:
            delta_table = self._get_delta_table()
            history_df = delta_table.history()

            operation_counts = (
                history_df
                .groupBy("operation")
                .count()
                .collect()
            )

            return {row["operation"]: row["count"] for row in operation_counts}
        except Exception as e:
            logger.error(f"Failed to get operation counts: {e}")
            return {}

    def get_version_range_for_time_period(
        self, start_time: datetime, end_time: datetime
    ) -> Optional[tuple[int, int]]:
        """
        Get version range for a specific time period.

        Args:
            start_time: Start of time period
            end_time: End of time period

        Returns:
            Tuple of (start_version, end_version) or None
        """
        try:
            delta_table = self._get_delta_table()
            history = delta_table.history()

            # Get versions in time range
            versions = (
                history
                .filter(
                    (history["timestamp"] >= start_time) &
                    (history["timestamp"] <= end_time)
                )
                .select("version")
                .collect()
            )

            if not versions:
                return None

            version_numbers = [row["version"] for row in versions]
            return (min(version_numbers), max(version_numbers))
        except Exception as e:
            logger.error(f"Failed to get version range: {e}")
            return None

    def get_metrics_for_version(self, version: int) -> Dict[str, Any]:
        """
        Get operation metrics for a specific version.

        Args:
            version: Version number

        Returns:
            Dictionary of metrics
        """
        version_info = self.get_version_info(version)
        return version_info.operation_metrics if version_info else {}

    def compare_versions(self, version1: int = None, version2: int = None,
                        start_version: int = None, end_version: int = None) -> Dict[str, Any]:
        """
        Compare two versions.

        Args:
            version1: First version number (or use start_version)
            version2: Second version number (or use end_version)
            start_version: Alias for version1
            end_version: Alias for version2

        Returns:
            Dictionary with comparison details
        """
        # Support both parameter names
        v1 = version1 if version1 is not None else start_version
        v2 = version2 if version2 is not None else end_version

        if v1 is None or v2 is None:
            return {"error": "Must provide two versions to compare"}

        info1 = self.get_version_info(v1)
        info2 = self.get_version_info(v2)

        if not info1 or not info2:
            return {
                "added_files": 0,
                "removed_files": 0,
                "version_diff": abs(v2 - v1),
                "error": "One or both versions not found"
            }

        return {
            "version1": v1,
            "version2": v2,
            "version1_timestamp": info1.timestamp,
            "version2_timestamp": info2.timestamp,
            "time_difference": (info2.timestamp - info1.timestamp).total_seconds(),
            "version1_operation": info1.operation,
            "version2_operation": info2.operation,
            "versions_between": v2 - v1 - 1,
            "added_files": info2.operation_metrics.get("numFiles", 0),
            "removed_files": 0,
            "version_diff": abs(v2 - v1),
        }

    def get_current_version(self) -> int:
        """
        Get current version number (alias for get_latest_version).

        Returns:
            Current version number
        """
        return self.get_latest_version()

    def record_write_operation(self) -> None:
        """Record a write operation for testing purposes."""
        self._write_version_counter += 1

    def get_version_timestamp(self, version: int) -> Optional[datetime]:
        """
        Get timestamp for a specific version.

        Args:
            version: Version number

        Returns:
            Timestamp or None
        """
        info = self.get_version_info(version)
        return info.timestamp if info else None

    def get_version_metadata(self, version: int) -> Dict[str, Any]:
        """
        Get metadata for a specific version.

        Args:
            version: Version number

        Returns:
            Dictionary with version metadata
        """
        info = self.get_version_info(version)
        if not info:
            return {}

        return {
            "version": info.version,
            "timestamp": info.timestamp,
            "operation": info.operation,
            "operation_metrics": info.operation_metrics,
            "user_metadata": info.user_metadata,
        }

    def get_version_at_timestamp(self, timestamp: datetime) -> Optional[int]:
        """
        Get version at a specific timestamp (alias for find_version_by_timestamp).

        Args:
            timestamp: Target timestamp

        Returns:
            Version number or None
        """
        return self.find_version_by_timestamp(timestamp)

    def record_rollback(self, target_version: int) -> None:
        """
        Record a rollback operation.

        Args:
            target_version: Version to roll back to
        """
        self._rollback_history.append({
            "timestamp": datetime.now(),
            "target_version": target_version,
        })

    def get_rollback_history(self) -> List[Dict[str, Any]]:
        """
        Get rollback history.

        Returns:
            List of rollback operations
        """
        return self._rollback_history.copy()

    def get_operations_summary(self, start_version: int, end_version: int) -> Dict[str, Any]:
        """
        Get summary of operations for version range.

        Args:
            start_version: Start version
            end_version: End version

        Returns:
            Dictionary with operation summary
        """
        versions = self.get_versions_in_range(start_version, end_version)

        write_ops = sum(1 for v in versions if v.operation in ["WRITE", "MERGE"])
        delete_ops = sum(1 for v in versions if v.operation == "DELETE")

        return {
            "total_versions": len(versions),
            "write_operations": write_ops,
            "delete_operations": delete_ops,
            "start_version": start_version,
            "end_version": end_version,
        }


# Alias for backward compatibility
DeltaVersionTracker = VersionTracker
