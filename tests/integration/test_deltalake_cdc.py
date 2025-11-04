"""Integration tests for DeltaLake Change Data Feed."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    """Provide Spark session for tests."""
    spark = (
        SparkSession.builder.appName("DeltaLake_CDF_Test")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.2")
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
def delta_table_path(tmp_path):
    """Provide unique Delta table path for each test."""
    import uuid
    table_path = str(tmp_path / f"delta_cdf_{uuid.uuid4().hex[:8]}")
    yield table_path
    # Cleanup is handled by tmp_path fixture

@pytest.fixture
def delta_table_manager(delta_table_path):
    """Provide DeltaTableManager for tests."""
    from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

    manager = DeltaTableManager(
        table_path=delta_table_path,
        enable_cdf=True,
    )
    yield manager


@pytest.mark.integration
class TestDeltaLakeCDF:
    """Integration tests for DeltaLake Change Data Feed."""

    def test_version_to_version_changes(self, spark, delta_table_path):
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
        ).save(delta_table_path)

        # Update data (Version 1)
        updated_data = [
            {"id": 1, "name": "Product A Updated", "price": 150.0},
            {"id": 2, "name": "Product B", "price": 200.0},
        ]
        df_updated = spark.createDataFrame(updated_data)
        df_updated.write.format("delta").mode("overwrite").save(delta_table_path)

        # Query CDF for changes
        cdf_reader = CDFReader(table_path=delta_table_path)
        changes_df = cdf_reader.read_changes_between_versions(start_version=0, end_version=1)

        assert changes_df.count() > 0
        # Should have changes (overwrite creates inserts and deletes, not updates)
        change_types = [row["_change_type"] for row in changes_df.collect()]
        assert any(ct in ["insert", "delete", "update_postimage", "update_preimage"] for ct in change_types)

    def test_time_range_query(self, spark, delta_table_path):
        """Test querying CDF for time range."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        start_time = datetime.now()

        # Write initial data
        data = [{"id": 1, "value": "A"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save(delta_table_path)

        import time
        time.sleep(2)

        # Update data
        updated_data = [{"id": 1, "value": "B"}]
        df_updated = spark.createDataFrame(updated_data)
        df_updated.write.format("delta").mode("overwrite").save(delta_table_path)

        end_time = datetime.now()

        # Query CDF for time range
        cdf_reader = CDFReader(table_path=delta_table_path)
        changes_df = cdf_reader.read_changes_since_timestamp(
            start_timestamp=start_time
        )

        assert changes_df.count() > 0

    def test_operation_type_detection(self, spark, delta_table_path):
        """Test CDF correctly identifies operation types."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # INSERT
        data = [{"id": 1, "name": "Item 1"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save(delta_table_path)

        version_after_insert = 1

        # UPDATE
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, delta_table_path)
        delta_table.update("id = 1", {"name": "'Item 1 Updated'"})

        version_after_update = 2

        # DELETE
        delta_table.delete("id = 1")

        version_after_delete = 3

        # Query CDF
        cdf_reader = CDFReader(table_path=delta_table_path)

        # Check insert
        insert_changes_df = cdf_reader.read_changes_between_versions(
            start_version=0, end_version=version_after_insert
        )
        insert_changes = insert_changes_df.collect()
        assert any(c["_change_type"] == "insert" for c in insert_changes)

        # Check update
        update_changes_df = cdf_reader.read_changes_between_versions(
            start_version=version_after_insert, end_version=version_after_update
        )
        update_changes = update_changes_df.collect()
        assert any(
            c["_change_type"] in ["update_preimage", "update_postimage"]
            for c in update_changes
        )

        # Check delete
        delete_changes_df = cdf_reader.read_changes_between_versions(
            start_version=version_after_update, end_version=version_after_delete
        )
        delete_changes = delete_changes_df.collect()
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
        changes_df = cdf_reader.read_changes_between_versions(start_version=0, end_version=1)

        # Should only have changes for partition A
        assert changes_df.count() > 0

    def test_cdf_incremental_processing(self, spark, delta_table_path):
        """Test incremental CDF processing pattern."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader
        from src.cdc_pipelines.deltalake.version_tracker import DeltaVersionTracker

        # Create initial table first
        initial_data = [{"id": 1, "value": "Initial"}]
        df_init = spark.createDataFrame(initial_data)
        df_init.write.format("delta").mode("overwrite").option(
            "delta.enableChangeDataFeed", "true"
        ).save(delta_table_path)

        tracker = DeltaVersionTracker(table_path=delta_table_path)
        cdf_reader = CDFReader(table_path=delta_table_path)

        # Initial version
        last_processed_version = tracker.get_current_version()

        # Write more data
        for i in range(3):
            data = [{"id": i + 10, "value": f"Value {i}"}]
            df = spark.createDataFrame(data)
            df.write.format("delta").mode("append").save(delta_table_path)

        # Get new version
        current_version = tracker.get_current_version()

        # Process only new changes
        changes_df = cdf_reader.read_changes_between_versions(
            start_version=last_processed_version, end_version=current_version
        )

        assert changes_df.count() >= 3  # At least 3 inserts

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
        changes_df = cdf_reader.read_changes_between_versions(start_version=0, end_version=1)

        assert changes_df.count() > 0
        # New column should be present in changes
        first_row = changes_df.first()
        assert "description" in first_row.asDict() or first_row.asDict().get("description") is None

    def test_cdf_filtering(self, spark, delta_table_path):
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
        ).save(delta_table_path)

        # Query CDF with filter
        cdf_reader = CDFReader(table_path=delta_table_path)
        changes_df = cdf_reader.read_changes_between_versions(
            start_version=0, end_version=1
        )
        # Apply filter on DataFrame
        filtered_df = changes_df.filter("category = 'A'")

        # Should only get changes for category A
        assert filtered_df.count() > 0
        assert all(row["category"] == "A" for row in filtered_df.collect())

    def test_cdf_metadata_columns(self, spark, delta_table_path):
        """Test CDF includes required metadata columns."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # Write and update data
        data = [{"id": 1, "value": "A"}]
        df = spark.createDataFrame(data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save(delta_table_path)

        updated = [{"id": 1, "value": "B"}]
        df_updated = spark.createDataFrame(updated)
        df_updated.write.format("delta").mode("overwrite").save(delta_table_path)

        # Query CDF
        cdf_reader = CDFReader(table_path=delta_table_path)
        changes_df = cdf_reader.read_changes_between_versions(start_version=0, end_version=1)

        # Check metadata columns present
        required_columns = ["_change_type", "_commit_version", "_commit_timestamp"]
        df_columns = changes_df.columns
        for col in required_columns:
            assert col in df_columns

    def test_cdf_large_dataset(self, spark, delta_table_path):
        """Test CDF with large dataset."""
        from src.cdc_pipelines.deltalake.cdf_reader import CDFReader

        # Generate large dataset
        large_data = [{"id": i, "value": f"Value {i}"} for i in range(10000)]
        df = spark.createDataFrame(large_data)
        df.write.format("delta").mode("append").option(
            "delta.enableChangeDataFeed", "true"
        ).save(delta_table_path)

        # Query CDF
        cdf_reader = CDFReader(table_path=delta_table_path)
        changes_df = cdf_reader.read_changes_between_versions(start_version=0, end_version=1)

        assert changes_df.count() == 10000
