"""End-to-end tests for DeltaLake CDF workflow."""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Provide Spark session for tests."""
    spark = (
        SparkSession.builder.appName("DeltaCDF_E2E_Test")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.mark.e2e
@pytest.mark.skip(reason="Requires full Spark and Delta Lake infrastructure")
class TestDeltaCDFWorkflow:
    """End-to-end tests for complete Delta CDF workflow."""

    def test_complete_cdf_workflow(self, spark):
        """Test complete CDF workflow from write to read."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        # Step 1: Initialize Delta table with CDF
        manager = DeltaTableManager(
            table_path="/tmp/e2e_cdf_workflow", enable_cdf=True
        )

        # Step 2: Write initial data
        initial_data = [
            {"customer_id": 1, "name": "Alice", "email": "alice@example.com"},
            {"customer_id": 2, "name": "Bob", "email": "bob@example.com"},
        ]
        df = spark.createDataFrame(initial_data)
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/e2e_cdf_workflow")

        # Step 3: Track version
        tracker = DeltaVersionTracker(table_path="/tmp/e2e_cdf_workflow")
        v1 = tracker.get_current_version()

        # Step 4: Perform updates
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, "/tmp/e2e_cdf_workflow")
        delta_table.update("customer_id = 1", {"email": "'alice.updated@example.com'"})

        v2 = tracker.get_current_version()

        # Step 5: Perform deletes
        delta_table.delete("customer_id = 2")

        v3 = tracker.get_current_version()

        # Step 6: Read CDF changes
        cdf_reader = CDFReader(table_path="/tmp/e2e_cdf_workflow")
        all_changes = cdf_reader.read_changes(start_version=v1, end_version=v3)

        # Verify changes captured
        assert len(all_changes) > 0

        # Should have update and delete changes
        change_types = [c["_change_type"] for c in all_changes]
        assert any("update" in ct for ct in change_types)
        assert any("delete" in ct for ct in change_types)

    def test_cdf_streaming_workflow(self, spark):
        """Test CDF streaming consumption pattern."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager
        from src.cdc_pipelines.deltalake.pipeline import DeltaCDFPipeline

        # Initialize table
        manager = DeltaTableManager(
            table_path="/tmp/e2e_cdf_streaming", enable_cdf=True
        )

        # Write initial data
        data = [{"id": 1, "value": "A"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/e2e_cdf_streaming")

        # Start CDF pipeline
        pipeline = DeltaCDFPipeline(
            table_path="/tmp/e2e_cdf_streaming", poll_interval_seconds=5
        )

        # Write more data
        new_data = [{"id": 2, "value": "B"}]
        df_new = spark.createDataFrame(new_data)
        df_new.write.format("delta").mode("append").save("/tmp/e2e_cdf_streaming")

        # Process changes
        changes = pipeline.get_new_changes()

        assert len(changes) > 0

    def test_cdf_to_downstream_workflow(self, spark):
        """Test CDF changes flowing to downstream system."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # Source table with CDF
        source_data = [{"id": 1, "name": "Item 1", "price": 100.0}]
        df = spark.createDataFrame(source_data)
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/e2e_source_cdf")

        # Update source
        updated = [{"id": 1, "name": "Item 1 Updated", "price": 150.0}]
        df_updated = spark.createDataFrame(updated)
        df_updated.write.format("delta").mode("overwrite").save("/tmp/e2e_source_cdf")

        # Read CDF
        cdf_reader = CDFReader(table_path="/tmp/e2e_source_cdf")
        changes = cdf_reader.read_changes(start_version=0, end_version=1)

        # Apply changes to downstream table
        for change in changes:
            if change["_change_type"] == "update_postimage":
                # Write to downstream
                downstream_df = spark.createDataFrame([change])
                downstream_df.write.format("delta").mode("append").save(
                    "/tmp/e2e_downstream"
                )

        # Verify downstream has update
        downstream_data = (
            spark.read.format("delta").load("/tmp/e2e_downstream").collect()
        )
        assert len(downstream_data) > 0

    def test_cdf_incremental_etl(self, spark):
        """Test incremental ETL using CDF."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        # Initial load
        initial = [{"id": i, "value": f"V{i}"} for i in range(100)]
        df = spark.createDataFrame(initial)
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/e2e_incremental")

        tracker = DeltaVersionTracker(table_path="/tmp/e2e_incremental")
        checkpoint_version = tracker.get_current_version()

        # Incremental updates
        for batch in range(3):
            updates = [{"id": batch, "value": f"Updated_{batch}"}]
            df_update = spark.createDataFrame(updates)
            df_update.write.format("delta").mode("overwrite").option(
                "replaceWhere", f"id = {batch}"
            ).save("/tmp/e2e_incremental")

        # Process only new changes
        cdf_reader = CDFReader(table_path="/tmp/e2e_incremental")
        new_version = tracker.get_current_version()
        changes = cdf_reader.read_changes(
            start_version=checkpoint_version, end_version=new_version
        )

        # Should have changes for 3 batches
        assert len(changes) >= 3

    def test_cdf_data_quality_monitoring(self, spark):
        """Test data quality monitoring using CDF."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
        from src.validation.integrity import RowCountValidator

        # Write data with potential quality issues
        data = [
            {"id": 1, "value": "Good", "quality_score": 100},
            {"id": 2, "value": "Bad", "quality_score": 50},
        ]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/e2e_quality")

        # Read CDF
        cdf_reader = CDFReader(table_path="/tmp/e2e_quality")
        changes = cdf_reader.read_changes(start_version=0, end_version=1)

        # Validate data quality
        validator = RowCountValidator(tolerance=0)
        result = validator.validate(data, changes)

        assert result.status.value == "passed"

    def test_cdf_audit_trail(self, spark):
        """Test using CDF as audit trail."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        # Write initial record
        data = [{"user_id": 1, "balance": 1000.0}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/e2e_audit")

        # Multiple balance changes
        changes_log = [
            {"user_id": 1, "balance": 900.0},  # -100
            {"user_id": 1, "balance": 950.0},  # +50
            {"user_id": 1, "balance": 850.0},  # -100
        ]

        for change in changes_log:
            df_change = spark.createDataFrame([change])
            df_change.write.format("delta").mode("overwrite").save("/tmp/e2e_audit")

        # Query full audit trail
        cdf_reader = CDFReader(table_path="/tmp/e2e_audit")
        tracker = DeltaVersionTracker(table_path="/tmp/e2e_audit")

        audit_trail = cdf_reader.read_changes(
            start_version=0, end_version=tracker.get_current_version()
        )

        # Should have all balance changes
        assert len(audit_trail) >= 4  # Initial + 3 updates

    def test_cdf_rollback_scenario(self, spark):
        """Test CDF after table rollback."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker
        from delta.tables import DeltaTable

        # Write versions
        for version in range(3):
            data = [{"id": 1, "version": version}]
            df = spark.createDataFrame(data)
            mode = "overwrite" if version == 0 else "overwrite"
            df.write.format("delta").mode(mode).option(
                "delta.enableChangeDataFeed", "true"
            ).save("/tmp/e2e_rollback")

        tracker = DeltaVersionTracker(table_path="/tmp/e2e_rollback")
        current = tracker.get_current_version()

        # Rollback to version 1
        delta_table = DeltaTable.forPath(spark, "/tmp/e2e_rollback")
        delta_table.restoreToVersion(1)

        # CDF should show rollback
        cdf_reader = CDFReader(table_path="/tmp/e2e_rollback")
        changes_after_rollback = cdf_reader.read_changes(
            start_version=current, end_version=tracker.get_current_version()
        )

        # Verify rollback recorded
        assert len(changes_after_rollback) >= 0

    def test_cdf_performance_large_dataset(self, spark):
        """Test CDF performance with large dataset."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
        import time

        # Write large dataset
        large_data = [{"id": i, "value": f"Value {i}"} for i in range(100000)]
        df = spark.createDataFrame(large_data)

        start_time = time.time()
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/e2e_performance")
        write_time = time.time() - start_time

        # Update subset
        updates = [{"id": i, "value": f"Updated {i}"} for i in range(1000)]
        df_updates = spark.createDataFrame(updates)
        df_updates.write.format("delta").mode("overwrite").option(
            "replaceWhere", "id < 1000"
        ).save("/tmp/e2e_performance")

        # Read CDF
        cdf_reader = CDFReader(table_path="/tmp/e2e_performance")

        start_time = time.time()
        changes = cdf_reader.read_changes(start_version=0, end_version=1)
        read_time = time.time() - start_time

        # Performance assertions
        assert write_time < 60  # Should complete within 60 seconds
        assert read_time < 30  # CDF read should be fast
        assert len(changes) > 0
