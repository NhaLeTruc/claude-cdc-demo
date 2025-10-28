"""
Apache Iceberg Snapshot Tracker.

This module tracks Iceberg table snapshots and provides snapshot management
functionality for CDC operations.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass

try:
    from pyiceberg.table import Table, Snapshot
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


logger = logging.getLogger(__name__)


@dataclass
class SnapshotInfo:
    """Information about an Iceberg snapshot"""

    snapshot_id: int
    parent_snapshot_id: Optional[int]
    timestamp_ms: int
    timestamp: datetime
    operation: str
    summary: Dict[str, Any]
    manifest_list: str


class SnapshotTracker:
    """
    Tracks Iceberg table snapshots for CDC processing.

    Maintains snapshot history and provides snapshot comparison capabilities
    for incremental data processing.
    """

    def __init__(self, table: Table):
        """
        Initialize Snapshot Tracker.

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
        self._snapshot_cache: Dict[int, SnapshotInfo] = {}
        logger.info(f"Initialized SnapshotTracker for table {table.identifier}")

    def _parse_snapshot(self, snapshot: Snapshot) -> SnapshotInfo:
        """
        Parse Iceberg snapshot into SnapshotInfo.

        Args:
            snapshot: Iceberg snapshot object

        Returns:
            SnapshotInfo object
        """
        summary = snapshot.summary.additional_properties if snapshot.summary else {}

        return SnapshotInfo(
            snapshot_id=snapshot.snapshot_id,
            parent_snapshot_id=snapshot.parent_snapshot_id,
            timestamp_ms=snapshot.timestamp_ms,
            timestamp=datetime.fromtimestamp(snapshot.timestamp_ms / 1000),
            operation=summary.get("operation", "unknown"),
            summary=summary,
            manifest_list=snapshot.manifest_list,
        )

    def get_current_snapshot_id(self) -> Optional[int]:
        """
        Get the current snapshot ID.

        Returns:
            Current snapshot ID or None
        """
        return self.table.metadata.current_snapshot_id

    def get_current_snapshot(self) -> Optional[SnapshotInfo]:
        """
        Get current snapshot information.

        Returns:
            SnapshotInfo for current snapshot or None
        """
        current_id = self.get_current_snapshot_id()
        if current_id is None:
            return None

        return self.get_snapshot_info(current_id)

    def get_snapshot_info(self, snapshot_id: int) -> Optional[SnapshotInfo]:
        """
        Get detailed information about a specific snapshot.

        Args:
            snapshot_id: Snapshot ID to query

        Returns:
            SnapshotInfo object or None if not found
        """
        # Check cache first
        if snapshot_id in self._snapshot_cache:
            return self._snapshot_cache[snapshot_id]

        # Find in table snapshots
        for snapshot in self.table.metadata.snapshots:
            if snapshot.snapshot_id == snapshot_id:
                info = self._parse_snapshot(snapshot)
                self._snapshot_cache[snapshot_id] = info
                return info

        return None

    def get_all_snapshots(self) -> List[SnapshotInfo]:
        """
        Get all table snapshots.

        Returns:
            List of SnapshotInfo objects, ordered by timestamp
        """
        snapshots = []

        for snapshot in self.table.metadata.snapshots:
            info = self._parse_snapshot(snapshot)
            self._snapshot_cache[info.snapshot_id] = info
            snapshots.append(info)

        # Sort by timestamp
        return sorted(snapshots, key=lambda s: s.timestamp_ms)

    def get_snapshots_since(self, since_timestamp: datetime) -> List[SnapshotInfo]:
        """
        Get snapshots created since a specific timestamp.

        Args:
            since_timestamp: Timestamp to query from

        Returns:
            List of SnapshotInfo objects
        """
        since_ms = int(since_timestamp.timestamp() * 1000)
        snapshots = self.get_all_snapshots()

        return [s for s in snapshots if s.timestamp_ms >= since_ms]

    def get_snapshots_in_range(
        self, start_snapshot_id: int, end_snapshot_id: Optional[int] = None
    ) -> List[SnapshotInfo]:
        """
        Get snapshots within a specific ID range.

        Args:
            start_snapshot_id: Starting snapshot ID (inclusive)
            end_snapshot_id: Ending snapshot ID (inclusive), current if None

        Returns:
            List of SnapshotInfo objects
        """
        if end_snapshot_id is None:
            end_snapshot_id = self.get_current_snapshot_id()
            if end_snapshot_id is None:
                return []

        all_snapshots = self.get_all_snapshots()

        # Build snapshot chain
        snapshot_chain = []
        current = end_snapshot_id

        # Walk backwards from end to start
        while current is not None:
            snapshot_info = next(
                (s for s in all_snapshots if s.snapshot_id == current), None
            )

            if snapshot_info:
                snapshot_chain.append(snapshot_info)

                if current == start_snapshot_id:
                    break

                current = snapshot_info.parent_snapshot_id
            else:
                break

        # Reverse to get chronological order
        return list(reversed(snapshot_chain))

    def find_snapshot_by_timestamp(self, timestamp: datetime) -> Optional[SnapshotInfo]:
        """
        Find the snapshot closest to a given timestamp.

        Args:
            timestamp: Target timestamp

        Returns:
            SnapshotInfo for closest snapshot or None
        """
        target_ms = int(timestamp.timestamp() * 1000)
        snapshots = self.get_all_snapshots()

        if not snapshots:
            return None

        # Find snapshot at or before the timestamp
        candidates = [s for s in snapshots if s.timestamp_ms <= target_ms]

        if candidates:
            return max(candidates, key=lambda s: s.timestamp_ms)

        # If no snapshot before timestamp, return earliest
        return min(snapshots, key=lambda s: s.timestamp_ms)

    def get_snapshot_chain(
        self, start_snapshot_id: Optional[int] = None
    ) -> List[SnapshotInfo]:
        """
        Get the full snapshot chain from start to current.

        Args:
            start_snapshot_id: Starting snapshot (earliest if None)

        Returns:
            List of SnapshotInfo objects in chronological order
        """
        all_snapshots = self.get_all_snapshots()

        if not all_snapshots:
            return []

        if start_snapshot_id is None:
            # Return all snapshots
            return all_snapshots

        # Find starting point
        start_idx = next(
            (i for i, s in enumerate(all_snapshots) if s.snapshot_id == start_snapshot_id),
            0
        )

        return all_snapshots[start_idx:]

    def compare_snapshots(
        self, snapshot_id1: int, snapshot_id2: int
    ) -> Dict[str, Any]:
        """
        Compare two snapshots.

        Args:
            snapshot_id1: First snapshot ID
            snapshot_id2: Second snapshot ID

        Returns:
            Dictionary with comparison details
        """
        snap1 = self.get_snapshot_info(snapshot_id1)
        snap2 = self.get_snapshot_info(snapshot_id2)

        if not snap1 or not snap2:
            return {"error": "One or both snapshots not found"}

        time_diff = (snap2.timestamp - snap1.timestamp).total_seconds()

        # Get snapshots between
        snapshots_between = self.get_snapshots_in_range(snapshot_id1, snapshot_id2)
        num_between = len(snapshots_between) - 2  # Exclude start and end

        return {
            "snapshot1_id": snapshot_id1,
            "snapshot2_id": snapshot_id2,
            "snapshot1_timestamp": snap1.timestamp,
            "snapshot2_timestamp": snap2.timestamp,
            "time_difference_seconds": time_diff,
            "snapshot1_operation": snap1.operation,
            "snapshot2_operation": snap2.operation,
            "snapshots_between": max(0, num_between),
            "snapshot1_summary": snap1.summary,
            "snapshot2_summary": snap2.summary,
        }

    def get_snapshot_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about all snapshots.

        Returns:
            Dictionary with snapshot statistics
        """
        snapshots = self.get_all_snapshots()

        if not snapshots:
            return {
                "total_snapshots": 0,
                "current_snapshot_id": None,
            }

        # Count operations
        operation_counts: Dict[str, int] = {}
        for snap in snapshots:
            operation_counts[snap.operation] = operation_counts.get(snap.operation, 0) + 1

        earliest = snapshots[0]
        latest = snapshots[-1]

        return {
            "total_snapshots": len(snapshots),
            "current_snapshot_id": self.get_current_snapshot_id(),
            "earliest_snapshot": {
                "snapshot_id": earliest.snapshot_id,
                "timestamp": earliest.timestamp,
                "operation": earliest.operation,
            },
            "latest_snapshot": {
                "snapshot_id": latest.snapshot_id,
                "timestamp": latest.timestamp,
                "operation": latest.operation,
            },
            "operations": operation_counts,
            "time_span_seconds": (latest.timestamp - earliest.timestamp).total_seconds(),
        }

    def get_parent_snapshot_id(self, snapshot_id: int) -> Optional[int]:
        """
        Get the parent snapshot ID.

        Args:
            snapshot_id: Snapshot ID

        Returns:
            Parent snapshot ID or None
        """
        snapshot = self.get_snapshot_info(snapshot_id)
        return snapshot.parent_snapshot_id if snapshot else None

    def is_ancestor(self, ancestor_id: int, descendant_id: int) -> bool:
        """
        Check if one snapshot is an ancestor of another.

        Args:
            ancestor_id: Potential ancestor snapshot ID
            descendant_id: Potential descendant snapshot ID

        Returns:
            True if ancestor_id is an ancestor of descendant_id
        """
        current = descendant_id

        while current is not None:
            if current == ancestor_id:
                return True

            snapshot = self.get_snapshot_info(current)
            if not snapshot:
                break

            current = snapshot.parent_snapshot_id

        return False

    def get_latest_snapshots(self, count: int = 10) -> List[SnapshotInfo]:
        """
        Get the most recent snapshots.

        Args:
            count: Number of snapshots to return

        Returns:
            List of recent SnapshotInfo objects
        """
        snapshots = self.get_all_snapshots()
        return snapshots[-count:] if len(snapshots) > count else snapshots

    def clear_cache(self) -> None:
        """Clear the snapshot cache"""
        self._snapshot_cache.clear()
        logger.info("Cleared snapshot cache")
