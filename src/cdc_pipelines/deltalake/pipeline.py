"""
DeltaLake CDC Pipeline Orchestrator.

This module orchestrates the DeltaLake Change Data Feed pipeline,
coordinating table management, CDF reading, and version tracking.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame

from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager
from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
from src.cdc_pipelines.deltalake.version_tracker import VersionTracker


logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for DeltaLake CDC Pipeline"""

    table_path: str
    enable_cdf: bool = True
    partition_by: Optional[List[str]] = None
    cdf_retention_hours: int = 24
    checkpoint_location: Optional[str] = None


class DeltaCDCPipeline:
    """
    DeltaLake Change Data Capture Pipeline.

    Orchestrates CDC operations using DeltaLake's Change Data Feed feature.
    Coordinates table management, change reading, and version tracking.
    """

    def __init__(
        self,
        config: PipelineConfig,
        spark: Optional[SparkSession] = None,
    ):
        """
        Initialize DeltaLake CDC Pipeline.

        Args:
            config: Pipeline configuration
            spark: SparkSession instance (creates new if None)
        """
        self.config = config
        self.spark = spark or self._create_spark_session()

        # Initialize components
        self.table_manager = DeltaTableManager(
            table_path=config.table_path,
            enable_cdf=config.enable_cdf,
            partition_by=config.partition_by,
            cdf_retention_hours=config.cdf_retention_hours,
            spark=self.spark,
        )

        self.cdf_reader = CDFReader(
            table_path=config.table_path,
            spark=self.spark,
        )

        self.version_tracker = VersionTracker(
            table_path=config.table_path,
            spark=self.spark,
        )

        self.last_processed_version: Optional[int] = None
        self.pipeline_start_time = datetime.now()

        logger.info(f"Initialized DeltaCDCPipeline for {config.table_path}")

    def _create_spark_session(self) -> SparkSession:
        """Create a SparkSession with Delta Lake configuration"""
        return (
            SparkSession.builder
            .appName("DeltaLake-CDC-Pipeline")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .master("local[*]")
            .getOrCreate()
        )

    def start(self) -> Dict[str, Any]:
        """
        Start the CDC pipeline.

        Returns:
            Dictionary with startup status
        """
        logger.info("Starting DeltaLake CDC pipeline")

        # Get current version as starting point
        current_version = self.table_manager.get_latest_version()
        self.last_processed_version = current_version

        status = {
            "status": "started",
            "table_path": self.config.table_path,
            "cdf_enabled": self.cdf_reader.is_cdf_enabled(),
            "current_version": current_version,
            "start_time": self.pipeline_start_time,
        }

        logger.info(f"Pipeline started at version {current_version}")
        return status

    def get_incremental_changes(
        self,
        start_version: Optional[int] = None,
        end_version: Optional[int] = None,
    ) -> DataFrame:
        """
        Get incremental changes since last processed version.

        Args:
            start_version: Starting version (uses last_processed_version if None)
            end_version: Ending version (latest if None)

        Returns:
            DataFrame with change data
        """
        if start_version is None:
            if self.last_processed_version is None:
                raise ValueError("No starting version specified and no last processed version")
            start_version = self.last_processed_version

        logger.info(f"Getting incremental changes from version {start_version}")

        changes_df = self.cdf_reader.read_changes_between_versions(
            start_version=start_version,
            end_version=end_version,
        )

        # Update last processed version
        if end_version is not None:
            self.last_processed_version = end_version
        else:
            self.last_processed_version = self.table_manager.get_latest_version()

        return changes_df

    def get_changes_since_timestamp(
        self,
        since: datetime,
        until: Optional[datetime] = None,
    ) -> DataFrame:
        """
        Get changes since a specific timestamp.

        Args:
            since: Start timestamp
            until: End timestamp (now if None)

        Returns:
            DataFrame with change data
        """
        logger.info(f"Getting changes since {since}")
        return self.cdf_reader.read_changes_since_timestamp(since, until)

    def get_change_statistics(
        self,
        start_version: Optional[int] = None,
        end_version: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get statistics about changes.

        Args:
            start_version: Starting version (uses last_processed_version if None)
            end_version: Ending version (latest if None)

        Returns:
            Dictionary with change statistics
        """
        if start_version is None:
            start_version = self.last_processed_version or 0

        stats = self.cdf_reader.get_change_stats(start_version, end_version)

        # Add version info
        if end_version is None:
            end_version = self.table_manager.get_latest_version()

        stats["version_range"] = {
            "start": start_version,
            "end": end_version,
            "count": end_version - start_version + 1,
        }

        return stats

    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status.

        Returns:
            Dictionary with pipeline status
        """
        table_info = self.table_manager.get_table_info()
        latest_version = self.version_tracker.get_latest_version()

        return {
            "pipeline_id": f"delta_cdc_{self.config.table_path.split('/')[-1]}",
            "table_path": self.config.table_path,
            "table_exists": table_info.get("exists", False),
            "cdf_enabled": table_info.get("cdf_enabled", False),
            "latest_version": latest_version,
            "last_processed_version": self.last_processed_version,
            "versions_pending": (
                latest_version - self.last_processed_version
                if self.last_processed_version is not None and latest_version >= 0
                else 0
            ),
            "pipeline_uptime_seconds": (
                (datetime.now() - self.pipeline_start_time).total_seconds()
            ),
            "table_info": table_info,
        }

    def process_changes(
        self,
        processor_func,
        start_version: Optional[int] = None,
        end_version: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Process changes with a custom function.

        Args:
            processor_func: Function that takes DataFrame and returns result
            start_version: Starting version (uses last_processed_version if None)
            end_version: Ending version (latest if None)

        Returns:
            Dictionary with processing results
        """
        changes_df = self.get_incremental_changes(start_version, end_version)

        logger.info(f"Processing {changes_df.count()} changes")

        try:
            result = processor_func(changes_df)
            return {
                "status": "success",
                "records_processed": changes_df.count(),
                "result": result,
            }
        except Exception as e:
            logger.error(f"Error processing changes: {e}")
            return {
                "status": "error",
                "error": str(e),
                "records_processed": 0,
            }

    def validate_pipeline(self) -> Dict[str, Any]:
        """
        Validate pipeline configuration and status.

        Returns:
            Dictionary with validation results
        """
        validations = {
            "config_valid": self.table_manager.validate_configuration(),
            "cdf_enabled": self.cdf_reader.is_cdf_enabled(),
            "table_exists": self.table_manager.get_table_info().get("exists", False),
        }

        validations["all_valid"] = all(validations.values())

        return validations

    def get_version_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent version history.

        Args:
            limit: Maximum number of versions to return

        Returns:
            List of version information dictionaries
        """
        versions = self.version_tracker.get_version_history(limit)

        return [
            {
                "version": v.version,
                "timestamp": v.timestamp,
                "operation": v.operation,
                "metrics": v.operation_metrics,
            }
            for v in versions
        ]

    def stop(self) -> Dict[str, Any]:
        """
        Stop the CDC pipeline.

        Returns:
            Dictionary with shutdown status
        """
        logger.info("Stopping DeltaLake CDC pipeline")

        uptime = (datetime.now() - self.pipeline_start_time).total_seconds()

        status = {
            "status": "stopped",
            "uptime_seconds": uptime,
            "last_processed_version": self.last_processed_version,
            "final_version": self.table_manager.get_latest_version(),
        }

        logger.info(f"Pipeline stopped after {uptime:.2f} seconds")
        return status
