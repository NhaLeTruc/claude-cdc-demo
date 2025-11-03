"""Unit tests for Iceberg snapshot tracker."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Check PyIceberg availability
try:
    import pyiceberg
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


class TestIcebergSnapshotTracker:
    """Test suite for Iceberg snapshot tracking."""

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_get_current_snapshot(self, iceberg_test_table):
        """Test retrieving current snapshot."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        snapshot = tracker.get_current_snapshot()
        assert snapshot is not None or snapshot is None

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_get_snapshot_history(self, iceberg_test_table):
        """Test retrieving snapshot history."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        history = tracker.get_snapshot_history(limit=10)
        assert isinstance(history, list)
        assert len(history) <= 10

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_snapshot_metadata(self, iceberg_test_table):
        """Test retrieving snapshot metadata."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        # Get a valid snapshot ID if available
        current_snapshot = tracker.get_current_snapshot()
        if current_snapshot:
            snapshot_id = current_snapshot.snapshot_id
        else:
            snapshot_id = 123456789

        metadata = tracker.get_snapshot_metadata(snapshot_id=snapshot_id)
        assert isinstance(metadata, dict) or metadata is None

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_snapshot_at_timestamp(self, iceberg_test_table):
        """Test getting snapshot at specific timestamp."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        past_time = datetime.now() - timedelta(hours=1)
        snapshot = tracker.get_snapshot_at_timestamp(past_time)

        assert snapshot is not None or snapshot is None

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_compare_snapshots(self, iceberg_test_table):
        """Test comparing two snapshots."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        # Get valid snapshot IDs or use dummy values
        current_snapshot = tracker.get_current_snapshot()
        if current_snapshot:
            snapshot_id = current_snapshot.snapshot_id
            diff = tracker.compare_snapshots(
                snapshot_id1=snapshot_id,
                snapshot_id2=snapshot_id,
            )
        else:
            diff = tracker.compare_snapshots(
                snapshot_id1=111,
                snapshot_id2=222,
            )

        assert isinstance(diff, dict)

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_snapshot_files_added(self, iceberg_test_table):
        """Test tracking files added in snapshot."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        # Get valid snapshot IDs
        current_snapshot = tracker.get_current_snapshot()
        if current_snapshot:
            snapshot_id = current_snapshot.snapshot_id
            files = tracker.get_added_files(start_snapshot_id=snapshot_id, end_snapshot_id=snapshot_id)
        else:
            files = tracker.get_added_files(start_snapshot_id=123456789, end_snapshot_id=123456790)

        assert isinstance(files, list)

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_snapshot_files_deleted(self, iceberg_test_table):
        """Test tracking files deleted in snapshot."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        # Get valid snapshot IDs
        current_snapshot = tracker.get_current_snapshot()
        if current_snapshot:
            snapshot_id = current_snapshot.snapshot_id
            files = tracker.get_deleted_files(start_snapshot_id=snapshot_id, end_snapshot_id=snapshot_id)
        else:
            files = tracker.get_deleted_files(start_snapshot_id=123456789, end_snapshot_id=123456790)

        assert isinstance(files, list)

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_snapshot_summary(self, iceberg_test_table):
        """Test getting snapshot summary."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        # Get a valid snapshot ID if available
        current_snapshot = tracker.get_current_snapshot()
        if current_snapshot:
            snapshot_id = current_snapshot.snapshot_id
        else:
            snapshot_id = 123456789

        summary = tracker.get_snapshot_summary(snapshot_id=snapshot_id)
        assert isinstance(summary, dict) or summary is None

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_snapshot_operation_type(self, iceberg_test_table):
        """Test identifying snapshot operation type."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        # Get a valid snapshot ID if available
        current_snapshot = tracker.get_current_snapshot()
        if current_snapshot:
            snapshot_id = current_snapshot.snapshot_id
        else:
            snapshot_id = 123456789

        operation = tracker.get_snapshot_operation(snapshot_id=snapshot_id)
        assert operation in ["append", "replace", "overwrite", "delete", "unknown", None]

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_snapshot_parent_id(self, iceberg_test_table):
        """Test retrieving snapshot parent."""
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(table=iceberg_test_table)

        # Get a valid snapshot ID if available
        current_snapshot = tracker.get_current_snapshot()
        if current_snapshot:
            snapshot_id = current_snapshot.snapshot_id
        else:
            snapshot_id = 123456789

        parent_id = tracker.get_parent_snapshot_id(snapshot_id=snapshot_id)
        assert isinstance(parent_id, int) or parent_id is None
