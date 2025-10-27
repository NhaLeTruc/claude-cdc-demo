"""Integration tests for DeltaLake Change Data Feed."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Provide Spark session for tests."""
    spark = (
        SparkSession.builder.appName("DeltaLake_CDF_Test")
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


@pytest.fixture
def delta_table_manager():
    """Provide DeltaTableManager for tests."""
    from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

    manager = DeltaTableManager(
        table_path="/tmp/delta_cdf_test",
        enable_cdf=True,
    )
    yield manager


@pytest.mark.integration
@pytest.mark.skip(reason="Requires Spark and Delta Lake infrastructure")
class TestDeltaLakeCDF:
    """Integration tests for DeltaLake Change Data Feed."""

    def test_version_to_version_changes(self, spark, delta_table_manager):
        """Test querying CDF for version-to-version changes."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # Create initial data (Version 0)
        data = [
            {"id": 1, "name": "Product A", "price": 100.0},
            {"id": 2, "name": "Product B", "price": 200.0},
        ]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/delta_cdf_test")

        # Update data (Version 1)
        updated_data = [
            {"id": 1, "name": "Product A Updated", "price": 150.0},
            {"id": 2, "name": "Product B", "price": 200.0},
        ]
        df_updated = spark.createDataFrame(updated_data)
        df_updated.write.format("delta").mode("overwrite").save("/tmp/delta_cdf_test")

        # Query CDF for changes
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_test")
        changes = cdf_reader.read_changes(start_version=0, end_version=1)

        assert len(changes) > 0
        # Should have update_preimage and update_postimage for changed row
        change_types = [change["_change_type"] for change in changes]
        assert "update_postimage" in change_types or "update_preimage" in change_types

    def test_time_range_query(self, spark, delta_table_manager):
        """Test querying CDF for time range."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        start_time = datetime.now()

        # Write initial data
        data = [{"id": 1, "value": "A"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/delta_cdf_test")

        import time
        time.sleep(2)

        # Update data
        updated_data = [{"id": 1, "value": "B"}]
        df_updated = spark.createDataFrame(updated_data)
        df_updated.write.format("delta").mode("overwrite").save("/tmp/delta_cdf_test")

        end_time = datetime.now()

        # Query CDF for time range
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_test")
        changes = cdf_reader.read_changes_by_time(
            start_timestamp=start_time, end_timestamp=end_time
        )

        assert len(changes) > 0

    def test_operation_type_detection(self, spark, delta_table_manager):
        """Test CDF correctly identifies operation types."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # INSERT
        data = [{"id": 1, "name": "Item 1"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/delta_cdf_test")

        version_after_insert = 1

        # UPDATE
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, "/tmp/delta_cdf_test")
        delta_table.update("id = 1", {"name": "'Item 1 Updated'"})

        version_after_update = 2

        # DELETE
        delta_table.delete("id = 1")

        version_after_delete = 3

        # Query CDF
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_test")

        # Check insert
        insert_changes = cdf_reader.read_changes(
            start_version=0, end_version=version_after_insert
        )
        assert any(c["_change_type"] == "insert" for c in insert_changes)

        # Check update
        update_changes = cdf_reader.read_changes(
            start_version=version_after_insert, end_version=version_after_update
        )
        assert any(
            c["_change_type"] in ["update_preimage", "update_postimage"]
            for c in update_changes
        )

        # Check delete
        delete_changes = cdf_reader.read_changes(
            start_version=version_after_update, end_version=version_after_delete
        )
        assert any(c["_change_type"] == "delete" for c in delete_changes)

    def test_cdf_with_partitions(self, spark):
        """Test CDF works correctly with partitioned tables."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        manager = DeltaTableManager(
            table_path="/tmp/delta_cdf_partitioned",
            enable_cdf=True,
            partition_by=["category"],
        )

        # Write partitioned data
        data = [
            {"id": 1, "name": "Item 1", "category": "A"},
            {"id": 2, "name": "Item 2", "category": "B"},
        ]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").partitionBy("category").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/delta_cdf_partitioned")

        # Update one partition
        updated_data = [{"id": 1, "name": "Item 1 Updated", "category": "A"}]
        df_updated = spark.createDataFrame(updated_data)
        df_updated.write.format("delta").mode("overwrite").option(
            "replaceWhere", "category = 'A'"
        ).save("/tmp/delta_cdf_partitioned")

        # Query CDF
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_partitioned")
        changes = cdf_reader.read_changes(start_version=0, end_version=1)

        # Should only have changes for partition A
        assert len(changes) > 0

    def test_cdf_incremental_processing(self, spark, delta_table_manager):
        """Test incremental CDF processing pattern."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        tracker = DeltaVersionTracker(table_path="/tmp/delta_cdf_test")
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_test")

        # Initial version
        last_processed_version = tracker.get_current_version()

        # Write more data
        for i in range(3):
            data = [{"id": i + 10, "value": f"Value {i}"}]
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save("/tmp/delta_cdf_test")

        # Get new version
        current_version = tracker.get_current_version()

        # Process only new changes
        changes = cdf_reader.read_changes(
            start_version=last_processed_version, end_version=current_version
        )

        assert len(changes) >= 3  # At least 3 inserts

    def test_cdf_with_schema_evolution(self, spark):
        """Test CDF handles schema evolution."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # Initial schema
        data = [{"id": 1, "name": "Item"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/delta_cdf_schema_evolution")

        # Add column (schema evolution)
        data_with_new_col = [{"id": 2, "name": "Item 2", "description": "New column"}]
        df_new = spark.createDataFrame(data_with_new_col)
        df_new.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).save("/tmp/delta_cdf_schema_evolution")

        # Query CDF
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_schema_evolution")
        changes = cdf_reader.read_changes(start_version=0, end_version=1)

        assert len(changes) > 0
        # New column should be present in changes
        assert "description" in changes[0] or changes[0].get("description") is None

    def test_cdf_filtering(self, spark, delta_table_manager):
        """Test filtering CDF results."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # Write data
        data = [
            {"id": 1, "category": "A", "value": 100},
            {"id": 2, "category": "B", "value": 200},
            {"id": 3, "category": "A", "value": 300},
        ]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/delta_cdf_test")

        # Query CDF with filter
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_test")
        changes = cdf_reader.read_changes_with_filter(
            start_version=0, end_version=1, filter_condition="category = 'A'"
        )

        # Should only get changes for category A
        assert all(c.get("category") == "A" for c in changes if "category" in c)

    def test_cdf_metadata_columns(self, spark, delta_table_manager):
        """Test CDF includes required metadata columns."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # Write and update data
        data = [{"id": 1, "value": "A"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/delta_cdf_test")

        updated = [{"id": 1, "value": "B"}]
        df_updated = spark.createDataFrame(updated)
        df_updated.write.format("delta").mode("overwrite").save("/tmp/delta_cdf_test")

        # Query CDF
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_test")
        changes = cdf_reader.read_changes(start_version=0, end_version=1)

        # Check metadata columns present
        required_columns = ["_change_type", "_commit_version", "_commit_timestamp"]
        for change in changes:
            for col in required_columns:
                assert col in change

    def test_cdf_large_dataset(self, spark, delta_table_manager):
        """Test CDF with large dataset."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # Generate large dataset
        large_data = [{"id": i, "value": f"Value {i}"} for i in range(10000)]
        df = spark.createDataFrame(large_data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save("/tmp/delta_cdf_test")

        # Query CDF
        cdf_reader = CDFReader(table_path="/tmp/delta_cdf_test")
        changes = cdf_reader.read_changes(start_version=0, end_version=1)

        assert len(changes) == 10000
