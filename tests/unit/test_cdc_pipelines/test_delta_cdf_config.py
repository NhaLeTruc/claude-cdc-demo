"""Unit tests for DeltaLake CDF configuration."""

import pytest

# Try to import Spark, skip tests if not available
try:
    import pyspark
    from delta.tables import DeltaTable
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not SPARK_AVAILABLE,
    reason="PySpark and Delta Lake not available for CDF tests"
)


class TestDeltaCDFConfig:
    """Test suite for DeltaLake CDF configuration."""

    def test_enable_cdf_on_new_table(self):
        """Test enabling CDF on a new table."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

        manager = DeltaTableManager(
            table_path="/tmp/test_delta_cdf",
            enable_cdf=True,
        )

        config = manager.get_table_properties()
        assert config["delta.enableChangeDataFeed"] == "true"

    def test_cdf_disabled_by_default(self):
        """Test CDF is disabled by default."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

        manager = DeltaTableManager(
            table_path="/tmp/test_delta_no_cdf",
            enable_cdf=False,
        )

        config = manager.get_table_properties()
        assert config.get("delta.enableChangeDataFeed", "false") == "false"

    def test_alter_table_to_enable_cdf(self):
        """Test enabling CDF on existing table."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager
        from pyspark.sql.types import StructType, StructField, IntegerType

        # Create table without CDF first
        manager = DeltaTableManager(
            table_path="/tmp/test_delta_alter_cdf",
            enable_cdf=False,
        )

        # Create the table with a simple schema
        schema = StructType([StructField("id", IntegerType(), False)])
        manager.create_table(schema)

        # Enable CDF
        manager.enable_change_data_feed()

        config = manager.get_table_properties()
        assert config["delta.enableChangeDataFeed"] == "true"

    def test_partition_configuration(self):
        """Test partition configuration with CDF."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

        manager = DeltaTableManager(
            table_path="/tmp/test_delta_partitioned",
            enable_cdf=True,
            partition_by=["year", "month"],
        )

        assert manager.partition_columns == ["year", "month"]

    def test_invalid_table_path(self):
        """Test handling of invalid table path."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

        with pytest.raises(ValueError, match="Invalid table path"):
            DeltaTableManager(table_path="", enable_cdf=True)

    def test_cdf_retention_period(self):
        """Test CDF retention period configuration."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

        manager = DeltaTableManager(
            table_path="/tmp/test_delta_retention",
            enable_cdf=True,
            cdf_retention_hours=48,
        )

        config = manager.get_table_properties()
        assert config.get("delta.deletedFileRetentionDuration") == "interval 48 hours"

    def test_table_properties_validation(self):
        """Test table properties validation."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

        manager = DeltaTableManager(
            table_path="/tmp/test_delta_props",
            enable_cdf=True,
        )

        assert manager.validate_configuration() is True

    def test_cdf_column_mapping(self):
        """Test CDF adds required columns."""
        from src.cdc_pipelines.deltalake.table_manager import DeltaTableManager

        manager = DeltaTableManager(
            table_path="/tmp/test_delta_columns",
            enable_cdf=True,
        )

        expected_cdf_columns = [
            "_change_type",
            "_commit_version",
            "_commit_timestamp",
        ]

        cdf_schema = manager.get_cdf_schema()
        for col in expected_cdf_columns:
            assert col in [field.name for field in cdf_schema]
