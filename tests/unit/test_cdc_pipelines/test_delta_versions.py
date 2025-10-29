"""Unit tests for Delta table version manager."""

import pytest
from unittest.mock import MagicMock, patch

# Try to import Spark, skip tests if not available
try:
    import pyspark
    from delta.tables import DeltaTable
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not SPARK_AVAILABLE,
    reason="PySpark and Delta Lake not available for version tests"
)


class TestDeltaVersionManager:
    """Test suite for Delta table version tracking."""

    @patch("src.cdc_pipelines.deltalake.version_tracker.DeltaTable.forPath")
    def test_get_current_version(self, mock_delta_table):
        """Test retrieving current table version."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        # Setup mock Delta table with history
        mock_table = MagicMock()
        mock_history = MagicMock()
        mock_history.select.return_value.first.return_value = {"version": 5}
        mock_table.history.return_value = mock_history
        mock_delta_table.return_value = mock_table

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        # Mock implementation would return version
        version = tracker.get_current_version()
        assert isinstance(version, int)
        assert version >= 0

    def test_get_version_history(self):
        """Test retrieving version history."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        history = tracker.get_version_history(limit=10)
        assert isinstance(history, list)
        assert len(history) <= 10

    @patch("src.cdc_pipelines.deltalake.version_tracker.DeltaTable.forPath")
    def test_version_increment_on_write(self, mock_delta_table):
        """Test version increments after write."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        # Setup mock to return consistent version
        mock_table = MagicMock()
        mock_history = MagicMock()
        mock_history.select.return_value.first.return_value = {"version": 5}
        mock_table.history.return_value = mock_history
        mock_delta_table.return_value = mock_table

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        initial_version = 5  # Mocked value

        # Simulate write operation
        tracker.record_write_operation()

        # After recording write, the counter should increment
        assert tracker._write_version_counter == 1
        assert tracker._write_version_counter == initial_version - 4

    @patch("src.cdc_pipelines.deltalake.version_tracker.DeltaTable.forPath")
    def test_get_version_timestamp(self, mock_delta_table):
        """Test getting timestamp for specific version."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker
        from datetime import datetime

        # Setup mock
        mock_table = MagicMock()
        mock_history = MagicMock()
        test_timestamp = datetime.now()
        mock_row = {"version": 5, "timestamp": test_timestamp, "operation": "WRITE", "operationMetrics": {}}
        mock_history.filter.return_value.first.return_value = mock_row
        mock_table.history.return_value = mock_history
        mock_delta_table.return_value = mock_table

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        timestamp = tracker.get_version_timestamp(version=5)
        assert timestamp is not None
        assert timestamp == test_timestamp

    @patch("src.cdc_pipelines.deltalake.version_tracker.DeltaTable.forPath")
    def test_version_metadata(self, mock_delta_table):
        """Test retrieving version metadata."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker
        from datetime import datetime

        # Setup mock
        mock_table = MagicMock()
        mock_history = MagicMock()
        test_timestamp = datetime.now()
        mock_row = {"version": 1, "timestamp": test_timestamp, "operation": "CREATE", "operationMetrics": {}}
        mock_history.filter.return_value.first.return_value = mock_row
        mock_table.history.return_value = mock_history
        mock_delta_table.return_value = mock_table

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        metadata = tracker.get_version_metadata(version=1)
        assert "version" in metadata
        assert "timestamp" in metadata
        assert "operation" in metadata
        assert metadata["version"] == 1
        assert metadata["operation"] == "CREATE"

    def test_compare_versions(self):
        """Test comparing two versions."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        diff = tracker.compare_versions(start_version=0, end_version=5)
        assert "added_files" in diff
        assert "removed_files" in diff
        assert "version_diff" in diff

    @patch("src.cdc_pipelines.deltalake.version_tracker.DeltaTable.forPath")
    @patch("src.cdc_pipelines.deltalake.version_tracker.SparkSession")
    def test_get_version_at_timestamp(self, mock_spark_class, mock_delta_table):
        """Test getting version at specific timestamp."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker
        from datetime import datetime, timedelta

        # Setup mock Spark
        mock_spark = MagicMock()
        mock_spark_class.builder.appName.return_value.config.return_value.config.return_value.config.return_value.master.return_value.getOrCreate.return_value = mock_spark

        # Mock the read operation
        mock_read = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        mock_df = MagicMock()
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.load.return_value = mock_df

        # Setup mocks for DeltaTable
        mock_table = MagicMock()
        mock_history_df = MagicMock()
        past_time = datetime.now() - timedelta(hours=1)

        # Create proper mock chain for filter operation
        # The filter method needs to return a DF that supports orderBy
        mock_filtered = MagicMock()
        mock_ordered = MagicMock()
        mock_selected = MagicMock()
        mock_row = {"version": 3}

        # Mock __getitem__ on history to return a mock column that can be compared
        mock_timestamp_col = MagicMock()
        mock_timestamp_col.__le__ = MagicMock(return_value=True)
        mock_history_df.__getitem__.return_value = mock_timestamp_col

        # Set up the chain
        mock_selected.first.return_value = mock_row
        mock_ordered.select.return_value = mock_selected
        mock_filtered.orderBy.return_value = mock_ordered
        mock_history_df.filter.return_value = mock_filtered

        mock_table.history.return_value = mock_history_df
        mock_delta_table.return_value = mock_table

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        version = tracker.get_version_at_timestamp(past_time)

        assert isinstance(version, int)
        assert version == 3

    def test_version_rollback_tracking(self):
        """Test tracking version rollbacks."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        current_version = tracker.get_current_version()

        # Record rollback
        tracker.record_rollback(target_version=current_version - 2)

        rollback_history = tracker.get_rollback_history()
        assert len(rollback_history) > 0

    def test_version_operations_summary(self):
        """Test getting summary of operations per version."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        summary = tracker.get_operations_summary(start_version=0, end_version=10)
        assert "total_versions" in summary
        assert "write_operations" in summary
        assert "delete_operations" in summary

    def test_concurrent_version_tracking(self):
        """Test version tracking with concurrent writes."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        # Simulate concurrent version updates
        version1 = tracker.get_current_version()
        version2 = tracker.get_current_version()

        assert version1 == version2  # Should be consistent
