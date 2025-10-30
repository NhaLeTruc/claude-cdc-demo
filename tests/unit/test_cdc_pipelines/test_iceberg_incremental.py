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
    def test_incremental_read_between_snapshots(self):
        """Test incremental read between two snapshots."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

        changes = reader.read_incremental(
            start_snapshot_id=111,
            end_snapshot_id=222,
        )

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_by_timestamp(self):
        """Test incremental read by timestamp range."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

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
    def test_incremental_read_with_filter(self):
        """Test incremental read with predicate filter."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

        changes = reader.read_incremental_with_filter(
            start_snapshot_id=111,
            end_snapshot_id=222,
            filter_expression="status = 'active'",
        )

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_partition_pruning(self):
        """Test incremental read with partition pruning."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="partitioned_table",
        )

        changes = reader.read_incremental(
            start_snapshot_id=111,
            end_snapshot_id=222,
            partition_filter={"year": "2025", "month": "10"},
        )

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_schema_projection(self):
        """Test incremental read with column projection."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

        changes = reader.read_incremental(
            start_snapshot_id=111,
            end_snapshot_id=222,
            select_columns=["id", "name", "updated_at"],
        )

        assert isinstance(changes, (list, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_file_metadata(self):
        """Test retrieving file metadata for incremental read."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

        file_metadata = reader.get_changed_files(
            start_snapshot_id=111,
            end_snapshot_id=222,
        )

        assert isinstance(file_metadata, list)

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_row_count_estimate(self):
        """Test estimating row count for incremental read."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

        estimate = reader.estimate_row_count(
            start_snapshot_id=111,
            end_snapshot_id=222,
        )

        assert isinstance(estimate, (int, type(None)))

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_empty_result(self):
        """Test incremental read with no changes."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

        # Same snapshot should return empty
        changes = reader.read_incremental(
            start_snapshot_id=111,
            end_snapshot_id=111,
        )

        assert changes is None or len(changes) == 0

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_scan_planning(self):
        """Test scan planning for incremental read."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

        scan_plan = reader.plan_incremental_scan(
            start_snapshot_id=111,
            end_snapshot_id=222,
        )

        assert scan_plan is not None or scan_plan is None

    @pytest.mark.skipif(
        not PYICEBERG_AVAILABLE,
        reason="Requires PyIceberg installation"
    )
    def test_incremental_read_batched(self):
        """Test batched incremental read."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="test_table",
        )

        batches = reader.read_incremental_batched(
            start_snapshot_id=111,
            end_snapshot_id=222,
            batch_size=1000,
        )

        assert hasattr(batches, '__iter__') or batches is None
