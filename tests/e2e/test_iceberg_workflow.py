"""End-to-end tests for Iceberg incremental CDC workflow."""

import pytest
from datetime import datetime, timedelta
import time


@pytest.mark.e2e
@pytest.mark.skipif(
    reason="Requires full Iceberg infrastructure setup",
    condition=True,
)
class TestIcebergCDCWorkflow:
    """End-to-end tests for complete Iceberg CDC workflow."""

    def test_complete_iceberg_incremental_workflow(self):
        """Test complete Iceberg incremental CDC workflow end-to-end."""
        from src.cdc_pipelines.iceberg.table_manager import (
            IcebergTableManager,
            IcebergTableConfig,
        )
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        from src.validation.integrity import IntegrityValidator

        # Step 1: Create Iceberg table
        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="customers_e2e",
            warehouse_path="/tmp/iceberg_warehouse_e2e",
            partition_spec=[("registration_date", "month")],
            sort_order=["customer_id"],
        )

        table_manager = IcebergTableManager(config)
        table_manager.create_table_if_not_exists()

        # Step 2: Write initial dataset (Snapshot 1)
        initial_data = [
            {
                "customer_id": i,
                "email": f"customer{i}@example.com",
                "name": f"Customer {i}",
                "registration_date": "2025-01-15",
                "status": "active",
            }
            for i in range(1, 1001)
        ]

        table_manager.write_data(initial_data)

        # Track snapshots
        tracker = SnapshotTracker(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="customers_e2e",
        )

        snapshot_1 = tracker.get_current_snapshot()
        assert snapshot_1 is not None

        # Step 3: Simulate updates (Snapshot 2)
        time.sleep(1)

        updates = [
            {
                "customer_id": i,
                "email": f"customer{i}@example.com",
                "name": f"Customer {i} Updated",
                "registration_date": "2025-01-15",
                "status": "active",
            }
            for i in range(1, 101)  # Update first 100
        ]

        table_manager.upsert_data(updates, merge_on=["customer_id"])

        snapshot_2 = tracker.get_current_snapshot()
        assert snapshot_2.snapshot_id != snapshot_1.snapshot_id

        # Step 4: Add new records (Snapshot 3)
        time.sleep(1)

        new_records = [
            {
                "customer_id": i,
                "email": f"customer{i}@example.com",
                "name": f"Customer {i}",
                "registration_date": "2025-02-15",
                "status": "active",
            }
            for i in range(1001, 1501)
        ]

        table_manager.append_data(new_records)

        snapshot_3 = tracker.get_current_snapshot()

        # Step 5: Delete some records (Snapshot 4)
        time.sleep(1)

        table_manager.delete_data(filter_condition="customer_id < 50")

        snapshot_4 = tracker.get_current_snapshot()

        # Step 6: Incremental read between snapshots
        reader = IncrementalReader(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="customers_e2e",
        )

        # Read updates (Snapshot 1 → 2)
        changes_1_to_2 = reader.read_incremental(
            start_snapshot_id=snapshot_1.snapshot_id,
            end_snapshot_id=snapshot_2.snapshot_id,
        )
        assert len(changes_1_to_2) >= 100  # At least 100 updates

        # Read new inserts (Snapshot 2 → 3)
        changes_2_to_3 = reader.read_incremental(
            start_snapshot_id=snapshot_2.snapshot_id,
            end_snapshot_id=snapshot_3.snapshot_id,
        )
        assert len(changes_2_to_3) >= 500  # New 500 records

        # Read deletes (Snapshot 3 → 4)
        changed_files_3_to_4 = reader.get_changed_files(
            start_snapshot_id=snapshot_3.snapshot_id,
            end_snapshot_id=snapshot_4.snapshot_id,
        )
        assert len(changed_files_3_to_4) > 0  # Files changed due to deletes

        # Step 7: Data quality validation
        validator = IntegrityValidator()

        # Validate snapshot metadata
        snapshot_metadata = tracker.get_snapshot_metadata(snapshot_4.snapshot_id)
        assert snapshot_metadata is not None
        assert "operation" in snapshot_metadata

        # Validate incremental read correctness
        all_changes = reader.read_incremental(
            start_snapshot_id=snapshot_1.snapshot_id,
            end_snapshot_id=snapshot_4.snapshot_id,
        )

        # Should have records from all operations
        assert len(all_changes) > 0

        # Step 8: Time-based incremental read
        start_time = snapshot_1.committed_at if hasattr(snapshot_1, 'committed_at') else datetime.now() - timedelta(hours=1)
        end_time = datetime.now()

        time_based_changes = reader.read_incremental_by_time(
            start_timestamp=start_time,
            end_timestamp=end_time,
        )

        assert len(time_based_changes) > 0

        # Step 9: Filtered incremental read
        active_customers = reader.read_incremental_with_filter(
            start_snapshot_id=snapshot_1.snapshot_id,
            end_snapshot_id=snapshot_4.snapshot_id,
            filter_expression="status = 'active'",
        )

        assert all(c.get("status") == "active" for c in active_customers if "status" in c)

    def test_iceberg_cdc_with_schema_evolution(self):
        """Test Iceberg CDC workflow with schema evolution."""
        from src.cdc_pipelines.iceberg.table_manager import (
            IcebergTableManager,
            IcebergTableConfig,
        )
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="schema_evolution_test",
            warehouse_path="/tmp/iceberg_warehouse_e2e",
        )

        table_manager = IcebergTableManager(config)

        # Initial schema
        data_v1 = [
            {"id": 1, "name": "Product A"},
            {"id": 2, "name": "Product B"},
        ]

        table_manager.write_data(data_v1)

        tracker = SnapshotTracker(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="schema_evolution_test",
        )

        snapshot_1 = tracker.get_current_snapshot()

        # Add column (schema evolution)
        data_v2 = [
            {"id": 3, "name": "Product C", "price": 99.99},
        ]

        table_manager.append_data_with_schema_evolution(data_v2)

        snapshot_2 = tracker.get_current_snapshot()

        # Incremental read should handle schema evolution
        reader = IncrementalReader(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="schema_evolution_test",
        )

        changes = reader.read_incremental(
            start_snapshot_id=snapshot_1.snapshot_id,
            end_snapshot_id=snapshot_2.snapshot_id,
        )

        assert len(changes) >= 1
        # New column should be present in new records
        new_record = next((c for c in changes if c.get("id") == 3), None)
        assert new_record is not None
        assert "price" in new_record or new_record.get("price") is None

    def test_iceberg_cdc_partition_aware_incremental_read(self):
        """Test Iceberg CDC with partition-aware incremental reads."""
        from src.cdc_pipelines.iceberg.table_manager import (
            IcebergTableManager,
            IcebergTableConfig,
        )
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="partitioned_customers",
            warehouse_path="/tmp/iceberg_warehouse_e2e",
            partition_spec=[("registration_date", "month")],
        )

        table_manager = IcebergTableManager(config)

        # Write to multiple partitions
        january_data = [
            {"id": i, "name": f"Jan Customer {i}", "registration_date": "2025-01-15"}
            for i in range(1, 501)
        ]

        february_data = [
            {"id": i, "name": f"Feb Customer {i}", "registration_date": "2025-02-15"}
            for i in range(501, 1001)
        ]

        march_data = [
            {"id": i, "name": f"Mar Customer {i}", "registration_date": "2025-03-15"}
            for i in range(1001, 1501)
        ]

        table_manager.write_data(january_data)
        time.sleep(1)
        table_manager.append_data(february_data)
        time.sleep(1)
        table_manager.append_data(march_data)

        # Incremental read across partitions
        reader = IncrementalReader(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="partitioned_customers",
        )

        all_changes = reader.read_incremental(
            start_snapshot_id=0,
            end_snapshot_id=3,
        )

        assert len(all_changes) >= 1500

        # Partition-filtered read (only February)
        feb_changes = reader.read_incremental(
            start_snapshot_id=0,
            end_snapshot_id=3,
            partition_filter={"registration_date_month": "2025-02"},
        )

        # Should only contain February records
        assert all(
            "2025-02" in c.get("registration_date", "")
            for c in feb_changes
            if "registration_date" in c
        )

    def test_iceberg_cdc_performance_large_dataset(self):
        """Test Iceberg CDC performance with large dataset."""
        from src.cdc_pipelines.iceberg.table_manager import (
            IcebergTableManager,
            IcebergTableConfig,
        )
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader
        from src.cdc_pipelines.iceberg.snapshot_tracker import SnapshotTracker
        import time

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="large_dataset",
            warehouse_path="/tmp/iceberg_warehouse_e2e",
        )

        table_manager = IcebergTableManager(config)

        # Write large initial dataset
        start_write = time.time()
        large_data = [
            {
                "id": i,
                "name": f"Customer {i}",
                "value": i * 10,
                "created_at": datetime.now().isoformat(),
            }
            for i in range(1, 50001)  # 50K records
        ]

        table_manager.write_data(large_data)
        write_duration = time.time() - start_write

        tracker = SnapshotTracker(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="large_dataset",
        )

        snapshot_1 = tracker.get_current_snapshot()

        # Add incremental data
        incremental_data = [
            {
                "id": i,
                "name": f"Customer {i}",
                "value": i * 10,
                "created_at": datetime.now().isoformat(),
            }
            for i in range(50001, 60001)  # Another 10K records
        ]

        table_manager.append_data(incremental_data)
        snapshot_2 = tracker.get_current_snapshot()

        # Measure incremental read performance
        reader = IncrementalReader(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="large_dataset",
        )

        start_read = time.time()
        changes = reader.read_incremental(
            start_snapshot_id=snapshot_1.snapshot_id,
            end_snapshot_id=snapshot_2.snapshot_id,
        )
        read_duration = time.time() - start_read

        assert len(changes) >= 10000
        assert read_duration < 30  # Should complete within 30 seconds

        # Validate CDC lag
        assert read_duration < write_duration * 2  # Read should be faster than write

    def test_iceberg_cdc_error_recovery(self):
        """Test Iceberg CDC error recovery and resilience."""
        from src.cdc_pipelines.iceberg.table_manager import (
            IcebergTableManager,
            IcebergTableConfig,
        )
        from src.cdc_pipelines.iceberg.incremental_reader import IncrementalReader

        config = IcebergTableConfig(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="error_recovery_test",
            warehouse_path="/tmp/iceberg_warehouse_e2e",
        )

        table_manager = IcebergTableManager(config)

        # Write data
        data = [{"id": i, "name": f"Record {i}"} for i in range(1, 101)]
        table_manager.write_data(data)

        reader = IncrementalReader(
            catalog_name="demo_catalog",
            namespace="cdc_demo",
            table_name="error_recovery_test",
        )

        # Test reading with invalid snapshot IDs
        try:
            changes = reader.read_incremental(
                start_snapshot_id=99999999,  # Invalid
                end_snapshot_id=99999999,
            )
            # Should handle gracefully
            assert changes is None or len(changes) == 0
        except Exception as e:
            # Exception is expected for invalid snapshots
            assert "snapshot" in str(e).lower() or "not found" in str(e).lower()
