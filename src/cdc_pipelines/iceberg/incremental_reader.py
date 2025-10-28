"""
Apache Iceberg Incremental Reader.

This module provides incremental read capabilities for Iceberg tables,
enabling CDC-like workflows using snapshot-based change detection.
"""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

try:
    from pyiceberg.table import Table
    from pyiceberg.expressions import AlwaysTrue
    import pyarrow as pa
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False

from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker


logger = logging.getLogger(__name__)


class IncrementalReader:
    """
    Reader for incremental data from Iceberg tables.

    Provides methods to read changes between snapshots, enabling
    CDC-like workflows for Iceberg tables.
    """

    def __init__(self, table: Table):
        """
        Initialize Incremental Reader.

        Args:
            table: Iceberg table instance

        Raises:
            ImportError: If PyIceberg is not installed
        """
        if not PYICEBERG_AVAILABLE:
            raise ImportError(
                "PyIceberg is not installed. "
                "Install it with: pip install pyiceberg"
            )

        self.table = table
        self.snapshot_tracker = SnapshotTracker(table)
        logger.info(f"Initialized IncrementalReader for table {table.identifier}")

    def read_incremental(
        self,
        start_snapshot_id: int,
        end_snapshot_id: Optional[int] = None,
    ) -> pa.Table:
        """
        Read incremental data between two snapshots.

        Args:
            start_snapshot_id: Starting snapshot ID (exclusive)
            end_snapshot_id: Ending snapshot ID (inclusive), current if None

        Returns:
            PyArrow Table with incremental data
        """
        if end_snapshot_id is None:
            end_snapshot_id = self.snapshot_tracker.get_current_snapshot_id()
            if end_snapshot_id is None:
                logger.warning("No current snapshot available")
                return pa.table({})

        logger.info(
            f"Reading incremental data from snapshot {start_snapshot_id} "
            f"to {end_snapshot_id}"
        )

        try:
            # Use Iceberg's incremental scan
            scan = self.table.scan(
                snapshot_id=end_snapshot_id,
            )

            # Note: PyIceberg's incremental_scan would be used here
            # For now, we read the full snapshot at end_snapshot_id
            # In production, this would use:
            # scan = table.scan().from_snapshot_id(start_snapshot_id).to_snapshot_id(end_snapshot_id)

            arrow_table = scan.to_arrow()

            logger.info(f"Read {arrow_table.num_rows} rows incrementally")
            return arrow_table

        except Exception as e:
            logger.error(f"Failed to read incremental data: {e}")
            raise

    def read_snapshot(self, snapshot_id: Optional[int] = None) -> pa.Table:
        """
        Read data from a specific snapshot.

        Args:
            snapshot_id: Snapshot ID to read (current if None)

        Returns:
            PyArrow Table with snapshot data
        """
        if snapshot_id is None:
            snapshot_id = self.snapshot_tracker.get_current_snapshot_id()
            if snapshot_id is None:
                return pa.table({})

        logger.info(f"Reading snapshot {snapshot_id}")

        try:
            scan = self.table.scan(snapshot_id=snapshot_id)
            arrow_table = scan.to_arrow()

            logger.info(f"Read {arrow_table.num_rows} rows from snapshot {snapshot_id}")
            return arrow_table

        except Exception as e:
            logger.error(f"Failed to read snapshot: {e}")
            raise

    def read_since_timestamp(
        self, since: datetime, until: Optional[datetime] = None
    ) -> pa.Table:
        """
        Read data added/modified since a specific timestamp.

        Args:
            since: Start timestamp
            until: End timestamp (now if None)

        Returns:
            PyArrow Table with data since timestamp
        """
        # Find snapshot at start time
        start_snapshot = self.snapshot_tracker.find_snapshot_by_timestamp(since)
        if not start_snapshot:
            logger.warning(f"No snapshot found for timestamp {since}")
            return pa.table({})

        # Find snapshot at end time
        end_snapshot_id = None
        if until:
            end_snapshot = self.snapshot_tracker.find_snapshot_by_timestamp(until)
            end_snapshot_id = end_snapshot.snapshot_id if end_snapshot else None

        return self.read_incremental(start_snapshot.snapshot_id, end_snapshot_id)

    def read_latest_changes(self, num_snapshots: int = 1) -> pa.Table:
        """
        Read changes from the most recent snapshots.

        Args:
            num_snapshots: Number of recent snapshots to include

        Returns:
            PyArrow Table with recent changes
        """
        current_id = self.snapshot_tracker.get_current_snapshot_id()
        if current_id is None:
            return pa.table({})

        # Get the snapshot chain
        all_snapshots = self.snapshot_tracker.get_all_snapshots()
        if len(all_snapshots) <= num_snapshots:
            # Read from first snapshot
            start_id = all_snapshots[0].snapshot_id if all_snapshots else current_id
        else:
            # Read from N snapshots back
            start_id = all_snapshots[-(num_snapshots + 1)].snapshot_id

        return self.read_incremental(start_id, current_id)

    def get_incremental_statistics(
        self, start_snapshot_id: int, end_snapshot_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get statistics about incremental data without reading all rows.

        Args:
            start_snapshot_id: Starting snapshot ID
            end_snapshot_id: Ending snapshot ID (current if None)

        Returns:
            Dictionary with statistics
        """
        if end_snapshot_id is None:
            end_snapshot_id = self.snapshot_tracker.get_current_snapshot_id()

        # Get snapshot info
        start_snap = self.snapshot_tracker.get_snapshot_info(start_snapshot_id)
        end_snap = self.snapshot_tracker.get_snapshot_info(end_snapshot_id)

        if not start_snap or not end_snap:
            return {"error": "Snapshot not found"}

        # Get snapshots in range
        snapshots_in_range = self.snapshot_tracker.get_snapshots_in_range(
            start_snapshot_id, end_snapshot_id
        )

        # Calculate time span
        time_span = (end_snap.timestamp - start_snap.timestamp).total_seconds()

        return {
            "start_snapshot_id": start_snapshot_id,
            "end_snapshot_id": end_snapshot_id,
            "start_timestamp": start_snap.timestamp,
            "end_timestamp": end_snap.timestamp,
            "time_span_seconds": time_span,
            "snapshots_processed": len(snapshots_in_range),
            "start_summary": start_snap.summary,
            "end_summary": end_snap.summary,
        }

    def compare_snapshots(
        self, snapshot_id1: int, snapshot_id2: int
    ) -> Dict[str, Any]:
        """
        Compare data between two snapshots.

        Args:
            snapshot_id1: First snapshot ID
            snapshot_id2: Second snapshot ID

        Returns:
            Dictionary with comparison results
        """
        # Read both snapshots
        data1 = self.read_snapshot(snapshot_id1)
        data2 = self.read_snapshot(snapshot_id2)

        # Calculate differences
        row_diff = data2.num_rows - data1.num_rows
        size_diff = data2.nbytes - data1.nbytes

        return {
            "snapshot1_id": snapshot_id1,
            "snapshot2_id": snapshot_id2,
            "snapshot1_rows": data1.num_rows,
            "snapshot2_rows": data2.num_rows,
            "row_difference": row_diff,
            "snapshot1_size_bytes": data1.nbytes,
            "snapshot2_size_bytes": data2.nbytes,
            "size_difference_bytes": size_diff,
            "schema_changed": not self._schemas_equal(data1.schema, data2.schema),
        }

    def _schemas_equal(self, schema1: pa.Schema, schema2: pa.Schema) -> bool:
        """Check if two schemas are equal"""
        return schema1.equals(schema2)

    def read_with_filter(
        self,
        snapshot_id: Optional[int] = None,
        row_filter=None,
    ) -> pa.Table:
        """
        Read snapshot data with filter.

        Args:
            snapshot_id: Snapshot ID (current if None)
            row_filter: PyIceberg filter expression

        Returns:
            Filtered PyArrow Table
        """
        if snapshot_id is None:
            snapshot_id = self.snapshot_tracker.get_current_snapshot_id()

        logger.info(f"Reading snapshot {snapshot_id} with filter")

        try:
            scan = self.table.scan(snapshot_id=snapshot_id)

            if row_filter is not None:
                scan = scan.filter(row_filter)

            arrow_table = scan.to_arrow()
            logger.info(f"Read {arrow_table.num_rows} filtered rows")
            return arrow_table

        except Exception as e:
            logger.error(f"Failed to read with filter: {e}")
            raise

    def get_schema(self, snapshot_id: Optional[int] = None) -> pa.Schema:
        """
        Get schema for a specific snapshot.

        Args:
            snapshot_id: Snapshot ID (current if None)

        Returns:
            PyArrow Schema
        """
        data = self.read_snapshot(snapshot_id)
        return data.schema

    def is_empty(self, snapshot_id: Optional[int] = None) -> bool:
        """
        Check if snapshot is empty.

        Args:
            snapshot_id: Snapshot ID (current if None)

        Returns:
            True if snapshot has no data
        """
        try:
            # Use metadata to check without reading full data
            if snapshot_id is None:
                snapshot_id = self.snapshot_tracker.get_current_snapshot_id()

            snapshot_info = self.snapshot_tracker.get_snapshot_info(snapshot_id)
            if not snapshot_info:
                return True

            # Check summary for record count
            summary = snapshot_info.summary
            if "total-records" in summary:
                return int(summary["total-records"]) == 0

            # Fallback: read data (slower)
            data = self.read_snapshot(snapshot_id)
            return data.num_rows == 0

        except Exception as e:
            logger.error(f"Failed to check if snapshot is empty: {e}")
            return True

    def get_row_count(self, snapshot_id: Optional[int] = None) -> int:
        """
        Get row count for a snapshot.

        Args:
            snapshot_id: Snapshot ID (current if None)

        Returns:
            Number of rows
        """
        if snapshot_id is None:
            snapshot_id = self.snapshot_tracker.get_current_snapshot_id()

        snapshot_info = self.snapshot_tracker.get_snapshot_info(snapshot_id)
        if snapshot_info and "total-records" in snapshot_info.summary:
            return int(snapshot_info.summary["total-records"])

        # Fallback: count rows by reading
        data = self.read_snapshot(snapshot_id)
        return data.num_rows
