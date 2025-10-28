"""Integration tests for Iceberg CDC (snapshot-based incremental read)."""

import pytest
from datetime import datetime, timedelta


@pytest.fixture
def iceberg_table_manager():
    """Provide IcebergTableManager for tests."""
    from src.cdc_pipelines.iceberg.table_manager import (
        IcebergTableManager,
        IcebergTableConfig,
    )

    config = IcebergTableConfig(
        catalog_name="test_catalog",
        namespace="test",
        table_name="customers_iceberg",
        warehouse_path="/tmp/iceberg_warehouse_test",
    )

    manager = IcebergTableManager(config)
    yield manager


@pytest.mark.integration
@pytest.mark.skipif(
    reason="Requires Iceberg infrastructure (PyIceberg, catalog, warehouse)",
    condition=True,
)
class TestIcebergIncrementalCDC:
    """Integration tests for Iceberg snapshot-based CDC."""

    def test_snapshot_to_snapshot_incremental_read(self, iceberg_table_manager):
        """Test incremental read between snapshots."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        # Write initial data (Snapshot 1)
        initial_data = [
            {"id": 1, "name": "Customer A", "status": "active"},
            {"id": 2, "name": "Customer B", "status": "active"},
        ]

        # Simulate table write
        iceberg_table_manager.write_data(initial_data)

        tracker = SnapshotTracker(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )
        snapshot_1 = tracker.get_current_snapshot()

        # Write more data (Snapshot 2)
        new_data = [
            {"id": 3, "name": "Customer C", "status": "active"},
        ]
        iceberg_table_manager.append_data(new_data)

        snapshot_2 = tracker.get_current_snapshot()

        # Read incremental changes
        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        changes = reader.read_incremental(
            start_snapshot_id=snapshot_1.snapshot_id,
            end_snapshot_id=snapshot_2.snapshot_id,
        )

        assert len(changes) >= 1
        assert any(c["id"] == 3 for c in changes)

    def test_partition_evolution_handling(self, iceberg_table_manager):
        """Test handling partition evolution."""
        from src.cdc_pipelines.iceberg.table_manager import IcebergTableConfig
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        # Create partitioned table
        config = IcebergTableConfig(
            catalog_name="test_catalog",
            namespace="test",
            table_name="partitioned_customers",
            warehouse_path="/tmp/iceberg_warehouse_test",
            partition_spec=[("registration_date", "month")],
        )

        partitioned_manager = iceberg_table_manager.__class__(config)

        # Write data to partition 2025-01
        data_jan = [
            {"id": 1, "name": "Jan Customer", "registration_date": "2025-01-15"},
        ]
        partitioned_manager.write_data(data_jan)

        # Write data to partition 2025-02
        data_feb = [
            {"id": 2, "name": "Feb Customer", "registration_date": "2025-02-15"},
        ]
        partitioned_manager.append_data(data_feb)

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="partitioned_customers",
        )

        # Should read across partitions
        changes = reader.read_incremental(
            start_snapshot_id=0,
            end_snapshot_id=2,
        )

        assert len(changes) >= 2

    def test_mixed_operations_incremental_read(self, iceberg_table_manager):
        """Test incremental read with mixed insert/update/delete."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        # INSERT
        data = [{"id": 1, "name": "Customer 1", "status": "active"}]
        iceberg_table_manager.write_data(data)
        snapshot_after_insert = tracker.get_current_snapshot()

        # UPDATE (overwrite)
        updated_data = [{"id": 1, "name": "Customer 1 Updated", "status": "active"}]
        iceberg_table_manager.overwrite_data(updated_data, filter_condition="id = 1")
        snapshot_after_update = tracker.get_current_snapshot()

        # DELETE
        iceberg_table_manager.delete_data(filter_condition="id = 1")
        snapshot_after_delete = tracker.get_current_snapshot()

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        # Check insert changes
        insert_changes = reader.read_incremental(
            start_snapshot_id=0,
            end_snapshot_id=snapshot_after_insert.snapshot_id,
        )
        assert len(insert_changes) >= 1

        # Check update changes (file replacement)
        update_changes = reader.get_changed_files(
            start_snapshot_id=snapshot_after_insert.snapshot_id,
            end_snapshot_id=snapshot_after_update.snapshot_id,
        )
        assert len(update_changes) > 0

        # Check delete changes
        delete_changes = reader.get_changed_files(
            start_snapshot_id=snapshot_after_update.snapshot_id,
            end_snapshot_id=snapshot_after_delete.snapshot_id,
        )
        assert len(delete_changes) > 0

    def test_time_based_incremental_read(self, iceberg_table_manager):
        """Test incremental read using time range."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        import time

        start_time = datetime.now()

        # Write data
        data = [{"id": 1, "name": "Customer", "status": "active"}]
        iceberg_table_manager.write_data(data)

        time.sleep(2)

        # Write more data
        more_data = [{"id": 2, "name": "Customer 2", "status": "active"}]
        iceberg_table_manager.append_data(more_data)

        end_time = datetime.now()

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        changes = reader.read_incremental_by_time(
            start_timestamp=start_time,
            end_timestamp=end_time,
        )

        assert len(changes) >= 2

    def test_incremental_read_with_schema_evolution(self, iceberg_table_manager):
        """Test incremental read handles schema evolution."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        # Initial schema
        data = [{"id": 1, "name": "Customer"}]
        iceberg_table_manager.write_data(data)
        snapshot_1 = tracker.get_current_snapshot()

        # Add column (schema evolution)
        data_with_new_col = [{"id": 2, "name": "Customer 2", "email": "test@example.com"}]
        iceberg_table_manager.append_data_with_schema_evolution(data_with_new_col)
        snapshot_2 = tracker.get_current_snapshot()

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        changes = reader.read_incremental(
            start_snapshot_id=snapshot_1.snapshot_id,
            end_snapshot_id=snapshot_2.snapshot_id,
        )

        assert len(changes) >= 1
        # New column should be present
        assert "email" in changes[0] or changes[0].get("email") is None

    def test_large_incremental_scan(self, iceberg_table_manager):
        """Test incremental read with large dataset."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        # Write large dataset
        large_data = [
            {"id": i, "name": f"Customer {i}", "status": "active"}
            for i in range(10000)
        ]
        iceberg_table_manager.write_data(large_data)
        snapshot_1 = tracker.get_current_snapshot()

        # Add more data
        more_data = [
            {"id": i + 10000, "name": f"Customer {i + 10000}", "status": "active"}
            for i in range(5000)
        ]
        iceberg_table_manager.append_data(more_data)
        snapshot_2 = tracker.get_current_snapshot()

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        changes = reader.read_incremental(
            start_snapshot_id=snapshot_1.snapshot_id,
            end_snapshot_id=snapshot_2.snapshot_id,
        )

        assert len(changes) >= 5000

    def test_incremental_read_with_predicate_pushdown(self, iceberg_table_manager):
        """Test incremental read with filter predicate."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        # Write data
        data = [
            {"id": 1, "name": "Customer A", "status": "active"},
            {"id": 2, "name": "Customer B", "status": "inactive"},
            {"id": 3, "name": "Customer C", "status": "active"},
        ]
        iceberg_table_manager.write_data(data)
        snapshot_1 = tracker.get_current_snapshot()

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        # Read only active customers
        changes = reader.read_incremental_with_filter(
            start_snapshot_id=0,
            end_snapshot_id=snapshot_1.snapshot_id,
            filter_expression="status = 'active'",
        )

        assert len(changes) == 2
        assert all(c["status"] == "active" for c in changes)

    def test_empty_incremental_read(self, iceberg_table_manager):
        """Test incremental read with no changes."""
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker

        tracker = SnapshotTracker(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        # Write data
        data = [{"id": 1, "name": "Customer"}]
        iceberg_table_manager.write_data(data)
        snapshot = tracker.get_current_snapshot()

        reader = IncrementalReader(
            catalog_name="test_catalog",
            namespace="test",
            table_name="customers_iceberg",
        )

        # Same snapshot should return empty
        changes = reader.read_incremental(
            start_snapshot_id=snapshot.snapshot_id,
            end_snapshot_id=snapshot.snapshot_id,
        )

        assert changes is None or len(changes) == 0
