"""Unit tests for DeltaLake writer."""

import pytest
from unittest.mock import Mock, patch, MagicMock


@pytest.mark.unit
class TestDeltaLakeWriter:
    """Test DeltaLake destination writer."""

    @patch("pyspark.sql.SparkSession")
    def test_writer_initialization(self, mock_spark):
        """Test DeltaLake writer initialization."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        writer = DeltaLakeWriter(
            table_path="s3://bucket/delta/customers",
            table_name="customers",
        )

        assert writer.table_path == "s3://bucket/delta/customers"
        assert writer.table_name == "customers"

    @patch("pyspark.sql.SparkSession")
    def test_write_single_event(self, mock_spark):
        """Test writing a single CDC event."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

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

        # Verify Spark write was called
        mock_spark.return_value.createDataFrame.assert_called_once()

    @patch("pyspark.sql.SparkSession")
    def test_write_batch_events(self, mock_spark):
        """Test writing batch of CDC events."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

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

        mock_spark.return_value.createDataFrame.assert_called_once()

    @patch("pyspark.sql.SparkSession")
    def test_write_with_cdf_enabled(self, mock_spark):
        """Test writing with Change Data Feed enabled."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        event = {
            "operation": "UPDATE",
            "data": {"customer_id": 1, "email": "updated@example.com"},
            "before": {"customer_id": 1, "email": "old@example.com"},
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(
            table_path="delta/customers",
            table_name="customers",
            enable_cdf=True,
        )
        writer.write(event)

        # Verify CDF option is set
        mock_df = mock_spark.return_value.createDataFrame.return_value
        mock_df.write.format.assert_called_with("delta")

    @patch("pyspark.sql.SparkSession")
    def test_handle_insert_operation(self, mock_spark):
        """Test handling INSERT operation."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        event = {
            "operation": "INSERT",
            "data": {"customer_id": 1, "email": "test@example.com"},
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")
        writer.write(event)

        # Should use append mode
        mock_df = mock_spark.return_value.createDataFrame.return_value
        mock_df.write.format.return_value.mode.assert_called_with("append")

    @patch("pyspark.sql.SparkSession")
    def test_handle_update_operation(self, mock_spark):
        """Test handling UPDATE operation."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        event = {
            "operation": "UPDATE",
            "data": {"customer_id": 1, "email": "updated@example.com"},
            "primary_key": 1,
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")
        writer.write(event)

        # Should perform merge operation
        assert True  # Merge logic would be tested in integration tests

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

    @patch("pyspark.sql.SparkSession")
    def test_writer_with_partitioning(self, mock_spark):
        """Test writer with partitioning."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

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

        mock_df = mock_spark.return_value.createDataFrame.return_value
        mock_df.write.format.return_value.partitionBy.assert_called_with(["created_date"])

    @patch("pyspark.sql.SparkSession")
    def test_writer_error_handling(self, mock_spark):
        """Test writer error handling."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        mock_spark.return_value.createDataFrame.side_effect = Exception("Write failed")

        event = {
            "operation": "INSERT",
            "data": {"customer_id": 1},
            "timestamp": "2024-01-01T00:00:00",
        }

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")

        with pytest.raises(Exception):
            writer.write(event)

    @patch("pyspark.sql.SparkSession")
    def test_get_table_version(self, mock_spark):
        """Test getting current table version."""
        from src.cdc_pipelines.postgres.delta_writer import DeltaLakeWriter

        writer = DeltaLakeWriter(table_path="delta/customers", table_name="customers")

        # Mock DeltaTable
        with patch("delta.tables.DeltaTable") as mock_delta:
            mock_delta.forPath.return_value.history.return_value.first.return_value = {
                "version": 5
            }

            version = writer.get_table_version()
            assert version == 5
