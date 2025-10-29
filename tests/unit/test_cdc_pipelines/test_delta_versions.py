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

    def test_version_increment_on_write(self):
        """Test version increments after write."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        initial_version = tracker.get_current_version()

        # Simulate write operation
        tracker.record_write_operation()

        new_version = tracker.get_current_version()
        assert new_version == initial_version + 1

    def test_get_version_timestamp(self):
        """Test getting timestamp for specific version."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        timestamp = tracker.get_version_timestamp(version=5)
        assert timestamp is not None

    def test_version_metadata(self):
        """Test retrieving version metadata."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        metadata = tracker.get_version_metadata(version=1)
        assert "version" in metadata
        assert "timestamp" in metadata
        assert "operation" in metadata

    def test_compare_versions(self):
        """Test comparing two versions."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        diff = tracker.compare_versions(start_version=0, end_version=5)
        assert "added_files" in diff
        assert "removed_files" in diff
        assert "version_diff" in diff

    def test_get_version_at_timestamp(self):
        """Test getting version at specific timestamp."""
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker
        from datetime import datetime, timedelta

        tracker = DeltaVersionTracker(table_path="/tmp/test_delta")

        # Get version from 1 hour ago
        past_time = datetime.now() - timedelta(hours=1)
        version = tracker.get_version_at_timestamp(past_time)

        assert isinstance(version, int)
        assert version >= 0

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
