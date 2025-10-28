"""
Apache Iceberg CDC Pipeline Orchestrator.

This module orchestrates the Iceberg incremental read pipeline,
coordinating table management, snapshot tracking, and incremental data processing.
"""

import logging
from typing import Optional, Dict, Any, List, Callable
from datetime import datetime
from dataclasses import dataclass

try:
    from pyiceberg.table import Table
    import pyarrow as pa
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False

from src.cdc_pipelines.iceberg.table_manager import IcebergTableManager, IcebergTableConfig
from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader


logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuration for Iceberg CDC Pipeline"""

    table_config: IcebergTableConfig
    checkpoint_location: Optional[str] = None
    processing_mode: str = "batch"  # batch or streaming


class IcebergCDCPipeline:
    """
    Apache Iceberg CDC Pipeline.

    Orchestrates CDC operations using Iceberg's snapshot-based
    incremental read capabilities. Coordinates table management,
    snapshot tracking, and data processing.
    """

    def __init__(self, config: PipelineConfig):
        """
        Initialize Iceberg CDC Pipeline.

        Args:
            config: Pipeline configuration

        Raises:
            ImportError: If PyIceberg is not installed
        """
        if not PYICEBERG_AVAILABLE:
            raise ImportError(
                "PyIceberg is not installed. "
                "Install it with: pip install pyiceberg"
            )

        self.config = config
        self.table_manager = IcebergTableManager(config.table_config)

        # These will be initialized when table is loaded
        self.table: Optional[Table] = None
        self.snapshot_tracker: Optional[SnapshotTracker] = None
        self.incremental_reader: Optional[IncrementalReader] = None

        self.last_processed_snapshot_id: Optional[int] = None
        self.pipeline_start_time = datetime.now()

        logger.info(
            f"Initialized IcebergCDCPipeline for "
            f"{config.table_config.namespace}.{config.table_config.table_name}"
        )

    def start(self) -> Dict[str, Any]:
        """
        Start the CDC pipeline.

        Returns:
            Dictionary with startup status
        """
        logger.info("Starting Iceberg CDC pipeline")

        # Load table
        try:
            self.table = self.table_manager.load_table()
        except Exception as e:
            logger.error(f"Failed to load table: {e}")
            return {
                "status": "error",
                "error": str(e),
                "message": "Failed to load Iceberg table",
            }

        # Initialize components
        self.snapshot_tracker = SnapshotTracker(self.table)
        self.incremental_reader = IncrementalReader(self.table)

        # Get current snapshot as starting point
        current_snapshot_id = self.snapshot_tracker.get_current_snapshot_id()
        self.last_processed_snapshot_id = current_snapshot_id

        status = {
            "status": "started",
            "table": f"{self.config.table_config.namespace}.{self.config.table_config.table_name}",
            "current_snapshot_id": current_snapshot_id,
            "start_time": self.pipeline_start_time,
            "processing_mode": self.config.processing_mode,
        }

        logger.info(f"Pipeline started at snapshot {current_snapshot_id}")
        return status

    def get_incremental_data(
        self,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Optional[int] = None,
    ) -> pa.Table:
        """
        Get incremental data since last processed snapshot.

        Args:
            start_snapshot_id: Starting snapshot (uses last_processed_snapshot_id if None)
            end_snapshot_id: Ending snapshot (latest if None)

        Returns:
            PyArrow Table with incremental data
        """
        if not self.incremental_reader:
            raise RuntimeError("Pipeline not started. Call start() first.")

        if start_snapshot_id is None:
            if self.last_processed_snapshot_id is None:
                raise ValueError("No starting snapshot specified and no last processed snapshot")
            start_snapshot_id = self.last_processed_snapshot_id

        logger.info(f"Getting incremental data from snapshot {start_snapshot_id}")

        data = self.incremental_reader.read_incremental(
            start_snapshot_id=start_snapshot_id,
            end_snapshot_id=end_snapshot_id,
        )

        # Update last processed snapshot
        if end_snapshot_id is not None:
            self.last_processed_snapshot_id = end_snapshot_id
        else:
            self.last_processed_snapshot_id = self.snapshot_tracker.get_current_snapshot_id()

        return data

    def get_data_since_timestamp(
        self,
        since: datetime,
        until: Optional[datetime] = None,
    ) -> pa.Table:
        """
        Get data added/modified since a specific timestamp.

        Args:
            since: Start timestamp
            until: End timestamp (now if None)

        Returns:
            PyArrow Table with data
        """
        if not self.incremental_reader:
            raise RuntimeError("Pipeline not started. Call start() first.")

        logger.info(f"Getting data since {since}")
        return self.incremental_reader.read_since_timestamp(since, until)

    def get_incremental_statistics(
        self,
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get statistics about incremental data.

        Args:
            start_snapshot_id: Starting snapshot (uses last_processed_snapshot_id if None)
            end_snapshot_id: Ending snapshot (latest if None)

        Returns:
            Dictionary with statistics
        """
        if not self.incremental_reader:
            raise RuntimeError("Pipeline not started. Call start() first.")

        if start_snapshot_id is None:
            start_snapshot_id = self.last_processed_snapshot_id or 0

        stats = self.incremental_reader.get_incremental_statistics(
            start_snapshot_id, end_snapshot_id
        )

        # Add pipeline context
        if end_snapshot_id is None:
            end_snapshot_id = self.snapshot_tracker.get_current_snapshot_id()

        stats["pipeline_context"] = {
            "last_processed_snapshot_id": self.last_processed_snapshot_id,
            "snapshots_pending": (
                end_snapshot_id - self.last_processed_snapshot_id
                if self.last_processed_snapshot_id and end_snapshot_id
                else 0
            ),
        }

        return stats

    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status.

        Returns:
            Dictionary with pipeline status
        """
        table_metadata = self.table_manager.get_table_metadata()

        status = {
            "pipeline_id": f"iceberg_cdc_{self.config.table_config.table_name}",
            "table": f"{self.config.table_config.namespace}.{self.config.table_config.table_name}",
            "table_exists": table_metadata.get("exists", False),
            "processing_mode": self.config.processing_mode,
            "pipeline_uptime_seconds": (
                (datetime.now() - self.pipeline_start_time).total_seconds()
            ),
        }

        if self.snapshot_tracker:
            current_snapshot_id = self.snapshot_tracker.get_current_snapshot_id()
            status.update({
                "current_snapshot_id": current_snapshot_id,
                "last_processed_snapshot_id": self.last_processed_snapshot_id,
                "snapshots_pending": (
                    current_snapshot_id - self.last_processed_snapshot_id
                    if self.last_processed_snapshot_id and current_snapshot_id
                    else 0
                ),
            })

        status["table_metadata"] = table_metadata

        return status

    def process_incremental_data(
        self,
        processor_func: Callable[[pa.Table], Any],
        start_snapshot_id: Optional[int] = None,
        end_snapshot_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Process incremental data with a custom function.

        Args:
            processor_func: Function that takes PyArrow Table and returns result
            start_snapshot_id: Starting snapshot (uses last_processed_snapshot_id if None)
            end_snapshot_id: Ending snapshot (latest if None)

        Returns:
            Dictionary with processing results
        """
        data = self.get_incremental_data(start_snapshot_id, end_snapshot_id)

        logger.info(f"Processing {data.num_rows} rows")

        try:
            result = processor_func(data)
            return {
                "status": "success",
                "records_processed": data.num_rows,
                "result": result,
            }
        except Exception as e:
            logger.error(f"Error processing data: {e}")
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
            "table_exists": self.table_manager.table_exists(),
        }

        if self.table:
            validations["table_loaded"] = True
            validations["has_snapshots"] = (
                self.snapshot_tracker.get_current_snapshot_id() is not None
                if self.snapshot_tracker
                else False
            )
        else:
            validations["table_loaded"] = False
            validations["has_snapshots"] = False

        validations["all_valid"] = all(validations.values())

        return validations

    def get_snapshot_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent snapshot history.

        Args:
            limit: Maximum number of snapshots to return

        Returns:
            List of snapshot information dictionaries
        """
        if not self.snapshot_tracker:
            raise RuntimeError("Pipeline not started. Call start() first.")

        snapshots = self.snapshot_tracker.get_latest_snapshots(limit)

        return [
            {
                "snapshot_id": s.snapshot_id,
                "parent_snapshot_id": s.parent_snapshot_id,
                "timestamp": s.timestamp,
                "operation": s.operation,
                "summary": s.summary,
            }
            for s in snapshots
        ]

    def get_table_statistics(self) -> Dict[str, Any]:
        """
        Get comprehensive table statistics.

        Returns:
            Dictionary with table statistics
        """
        if not self.snapshot_tracker:
            return {"error": "Pipeline not started"}

        table_stats = self.table_manager.get_table_statistics()
        snapshot_stats = self.snapshot_tracker.get_snapshot_statistics()

        return {
            "table": table_stats,
            "snapshots": snapshot_stats,
            "pipeline": {
                "last_processed_snapshot_id": self.last_processed_snapshot_id,
                "uptime_seconds": (
                    datetime.now() - self.pipeline_start_time
                ).total_seconds(),
            },
        }

    def compare_snapshots(
        self, snapshot_id1: int, snapshot_id2: int
    ) -> Dict[str, Any]:
        """
        Compare two snapshots.

        Args:
            snapshot_id1: First snapshot ID
            snapshot_id2: Second snapshot ID

        Returns:
            Dictionary with comparison results
        """
        if not self.incremental_reader:
            raise RuntimeError("Pipeline not started. Call start() first.")

        return self.incremental_reader.compare_snapshots(snapshot_id1, snapshot_id2)

    def stop(self) -> Dict[str, Any]:
        """
        Stop the CDC pipeline.

        Returns:
            Dictionary with shutdown status
        """
        logger.info("Stopping Iceberg CDC pipeline")

        uptime = (datetime.now() - self.pipeline_start_time).total_seconds()

        status = {
            "status": "stopped",
            "uptime_seconds": uptime,
            "last_processed_snapshot_id": self.last_processed_snapshot_id,
            "final_snapshot_id": (
                self.snapshot_tracker.get_current_snapshot_id()
                if self.snapshot_tracker
                else None
            ),
        }

        logger.info(f"Pipeline stopped after {uptime:.2f} seconds")
        return status
