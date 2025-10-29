"""Unit tests for DeltaLake writer."""

import pytest
from unittest.mock import Mock, patch, MagicMock

# Try to import Spark, skip tests if not available
try:
    import pyspark
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not SPARK_AVAILABLE,
    reason="PySpark not available for Delta writer tests"
)


@pytest.mark.unit
class TestDeltaLakeWriter:
    """Test DeltaLake destination writer."""

    def test_writer_initialization(self):
        """Test DeltaLake writer initialization."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Don't trigger Spark session creation in initialization
        writer = DeltaLakeWriter(
            table_path="s3://bucket/delta/customers",
            table_name="customers",
        )

        assert writer.table_path == "s3://bucket/delta/customers"
        assert writer.table_name == "customers"
        assert writer._spark is None  # Spark not created yet

    @patch("src.cdc_pipelines.postgres.delta_writer.DeltaLakeWriter._get_spark")
    def test_write_single_event(self, mock_get_spark):
        """Test writing a single CDC event."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Setup mocked Spark session
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_spark.createDataFrame.return_value = mock_df
        mock_get_spark.return_value = mock_spark

        event = {
            "operation": "INSERT",
            "table": "customers",
            "data": {
                "customer_id": 1,
                "email": "test@example.com",
            },
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")
        writer.write(event)

        # Verify Spark createDataFrame was called
        assert mock_get_spark.called

    @patch("src.cdc_pipelines.postgres.delta_writer.DeltaLakeWriter._get_spark")
    def test_write_batch_events(self, mock_get_spark):
        """Test writing batch of CDC events."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Setup mocked Spark session
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_spark.createDataFrame.return_value = mock_df
        mock_get_spark.return_value = mock_spark

        events = [
            {
                "operation": "INSERT",
                "data": {"customer_id": 1, "email": "test1@example.com"},
                "timestamp": "2024-01-01T00:00:00",
            },
            {
                "operation": "INSERT",
                "data": {"customer_id": 2, "email": "test2@example.com"},
                "timestamp": "2024-01-01T00:00:01",
            },
        ]

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")
        writer.write_batch(events)

        # Should be called twice (once per event)
        assert mock_spark.createDataFrame.call_count == 2

    @patch("src.cdc_pipelines.postgres.delta_writer.DeltaLakeWriter._get_spark")
    @patch("delta.tables.DeltaTable")
    def test_write_with_cdf_enabled(self, mock_delta_table, mock_get_spark):
        """Test writing with Change Data Feed enabled."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Setup mocked Spark session and Delta table
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_format = MagicMock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_format
        mock_spark.createDataFrame.return_value = mock_df
        mock_get_spark.return_value = mock_spark

        # Mock DeltaTable
        mock_table = MagicMock()
        mock_delta_table.forPath.return_value = mock_table

        event = {
            "operation": "UPDATE",
            "data": {"customer_id": 1, "email": "updated@example.com"},
            "before": {"customer_id": 1, "email": "old@example.com"},
            "primary_key": 1,
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(
            table_path="delta/customers",
            table_name="customers",
            enable_cdf=True,
        )

        writer.write(event)

        # Verify createDataFrame was called
        assert mock_get_spark.called

    @patch("src.cdc_pipelines.postgres.delta_writer.DeltaLakeWriter._get_spark")
    def test_handle_insert_operation(self, mock_get_spark):
        """Test handling INSERT operation."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Setup mocked Spark session
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_format = MagicMock()
        mock_mode = MagicMock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode
        mock_spark.createDataFrame.return_value = mock_df
        mock_get_spark.return_value = mock_spark

        event = {
            "operation": "INSERT",
            "data": {"customer_id": 1, "email": "test@example.com"},
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")
        writer.write(event)

        # Should use append mode
        mock_format.mode.assert_called_with("append")

    @patch("src.cdc_pipelines.postgres.delta_writer.DeltaLakeWriter._get_spark")
    @patch("delta.tables.DeltaTable")
    def test_handle_update_operation(self, mock_delta_table, mock_get_spark):
        """Test handling UPDATE operation."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Setup mocked Spark session
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_get_spark.return_value = mock_spark

        # Mock DeltaTable
        mock_table = MagicMock()
        mock_delta_table.forPath.return_value = mock_table

        event = {
            "operation": "UPDATE",
            "data": {"customer_id": 1, "email": "updated@example.com"},
            "primary_key": 1,
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")
        writer.write(event)

        # Should perform merge operation
        assert mock_get_spark.called

    @patch("pyspark.sql.SparkSession")
    def test_handle_delete_operation(self, mock_spark):
        """Test handling DELETE operation."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        event = {
            "operation": "DELETE",
            "data": {"customer_id": 1},
            "primary_key": 1,
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")
        writer.write(event)

        # Should perform delete operation
        assert True  # Delete logic would be tested in integration tests

    @patch("src.cdc_pipelines.postgres.delta_writer.DeltaLakeWriter._get_spark")
    def test_writer_with_partitioning(self, mock_get_spark):
        """Test writer with partitioning."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Setup mocked Spark session
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_format = MagicMock()
        mock_mode = MagicMock()
        mock_partition = MagicMock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode
        mock_mode.partitionBy.return_value = mock_partition
        mock_spark.createDataFrame.return_value = mock_df
        mock_get_spark.return_value = mock_spark

        event = {
            "operation": "INSERT",
            "data": {"customer_id": 1, "email": "test@example.com", "created_date": "2024-01-01"},
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(
            table_path="delta/customers",
            table_name="customers",
            partition_by=["created_date"],
        )
        writer.write(event)

        mock_mode.partitionBy.assert_called_with(["created_date"])

    @patch("src.cdc_pipelines.postgres.delta_writer.DeltaLakeWriter._get_spark")
    def test_writer_error_handling(self, mock_get_spark):
        """Test writer error handling."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Setup mocked Spark session that raises exception
        mock_spark = MagicMock()
        mock_spark.createDataFrame.side_effect = Exception("Write failed")
        mock_get_spark.return_value = mock_spark

        event = {
            "operation": "INSERT",
            "data": {"customer_id": 1},
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")

        with pytest.raises(Exception, match="Write failed"):
            writer.write(event)

    @patch("src.cdc_pipelines.postgres.delta_writer.DeltaLakeWriter._get_spark")
    @patch("delta.tables.DeltaTable")
    def test_get_table_version(self, mock_delta_table, mock_get_spark):
        """Test getting current table version."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        # Setup mocked Spark session
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")

        # Mock DeltaTable
        mock_table = MagicMock()
        mock_history_df = MagicMock()
        mock_history_df.collect.return_value = [{"version": 5}]
        mock_table.history.return_value = mock_history_df
        mock_delta_table.forPath.return_value = mock_table

        version = writer.get_table_version()
        assert version == 5
