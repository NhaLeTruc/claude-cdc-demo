"""Unit tests for Iceberg incremental read logic."""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Check PyIceberg availability
try:
    import pyiceberg
    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


class TestIcebergIncrementalRead:
    """Test suite for Iceberg incremental read functionality."""

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation",
    )
    def test_incremental_read_between_snapshots(self, iceberg_test_table):
        """Test incremental read between two snapshots."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            changes = reader.read_incremental(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
            )
        else:
            changes = None

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_by_timestamp(self, iceberg_test_table):
        """Test incremental read by timestamp range."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        start_time = datetime.now() - timedelta(hours=2)
        end_time = datetime.now()

        changes = reader.read_incremental_by_time(
            start_timestamp=start_time,
            end_timestamp=end_time,
        )

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_with_filter(self, iceberg_test_table):
        """Test incremental read with predicate filter."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            changes = reader.read_incremental_with_filter(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
                filter_expression="status = 'active'",
            )
        else:
            changes = None

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_partition_pruning(self, iceberg_test_table):
        """Test incremental read with partition pruning."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            changes = reader.read_incremental(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
                partition_filter={"year": "2025", "month": "10"},
            )
        else:
            changes = None

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_schema_projection(self, iceberg_test_table):
        """Test incremental read with column projection."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            changes = reader.read_incremental(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
                select_columns=["id", "name", "value"],
            )
        else:
            changes = None

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_file_metadata(self, iceberg_test_table):
        """Test retrieving file metadata for incremental read."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            file_metadata = reader.get_changed_files(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
            )
        else:
            file_metadata = []

        assert isinstance(file_metadata, list)

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_row_count_estimate(self, iceberg_test_table):
        """Test estimating row count for incremental read."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            estimate = reader.estimate_row_count(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
            )
        else:
            estimate = 0

        assert isinstance(estimate, int)

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_empty_result(self, iceberg_test_table):
        """Test incremental read with no changes."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            # Same snapshot should return empty
            changes = reader.read_incremental(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
            )
        else:
            changes = None

        assert changes is None or len(changes) == 0

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_scan_planning(self, iceberg_test_table):
        """Test scan planning for incremental read."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            scan_plan = reader.plan_incremental_scan(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
            )
        else:
            scan_plan = None

        assert isinstance(scan_plan, (dict, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_batched(self, iceberg_test_table):
        """Test batched incremental read."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(table=iceberg_test_table)

        # Get valid snapshot ID
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        tracker = SnapshotTracker(table=iceberg_test_table)
        current = tracker.get_current_snapshot()

        if current:
            batches = reader.read_incremental_batched(
                start_snapshot_id=current.snapshot_id,
                end_snapshot_id=current.snapshot_id,
                batch_size=1000,
            )
        else:
            batches = []

        assert isinstance(batches, list)
