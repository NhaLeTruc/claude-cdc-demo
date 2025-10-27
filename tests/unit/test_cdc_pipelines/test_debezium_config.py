"""Unit tests for Debezium connector configuration builder."""

import pytest


@pytest.mark.unit
class TestDebeziumConfigBuilder:
    """Test Debezium connector configuration builder."""

    def test_build_postgres_connector_config(self):
        """Test building Postgres connector configuration."""
        from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder

        builder = DebeziumConfigBuilder(
            connector_name="postgres-connector",
            database_hostname="localhost",
            database_port=5432,
            database_user="cdcuser",
            database_password="cdcpass",
            database_name="cdcdb",
        )

        config = builder.build()

        assert config["name"] == "postgres-connector"
        assert config["config"]["connector.class"] == "io.debezium.connector.postgresql.PostgresConnector"
        assert config["config"]["database.hostname"] == "localhost"
        assert config["config"]["database.port"] == "5432"
        assert config["config"]["database.user"] == "cdcuser"
        assert config["config"]["database.dbname"] == "cdcdb"

    def test_config_with_table_whitelist(self):
        """Test configuration with table whitelist."""
        from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder

        builder = DebeziumConfigBuilder(
            connector_name="postgres-connector",
            database_hostname="localhost",
            database_port=5432,
            database_user="cdcuser",
            database_password="cdcpass",
            database_name="cdcdb",
            table_include_list=["public.customers", "public.orders"],
        )

        config = builder.build()

        assert "table.include.list" in config["config"]
        assert "public.customers" in config["config"]["table.include.list"]
        assert "public.orders" in config["config"]["table.include.list"]

    def test_config_with_topic_prefix(self):
        """Test configuration with custom topic prefix."""
        from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder

        builder = DebeziumConfigBuilder(
            connector_name="postgres-connector",
            database_hostname="localhost",
            database_port=5432,
            database_user="cdcuser",
            database_password="cdcpass",
            database_name="cdcdb",
            topic_prefix="cdc.postgres",
        )

        config = builder.build()

        assert config["config"]["topic.prefix"] == "cdc.postgres"

    def test_config_with_slot_name(self):
        """Test configuration with custom replication slot."""
        from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder

        builder = DebeziumConfigBuilder(
            connector_name="postgres-connector",
            database_hostname="localhost",
            database_port=5432,
            database_user="cdcuser",
            database_password="cdcpass",
            database_name="cdcdb",
            slot_name="custom_slot",
        )

        config = builder.build()

        assert config["config"]["slot.name"] == "custom_slot"

    def test_config_with_plugin_name(self):
        """Test configuration with custom plugin."""
        from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder

        builder = DebeziumConfigBuilder(
            connector_name="postgres-connector",
            database_hostname="localhost",
            database_port=5432,
            database_user="cdcuser",
            database_password="cdcpass",
            database_name="cdcdb",
            plugin_name="pgoutput",
        )

        config = builder.build()

        assert config["config"]["plugin.name"] == "pgoutput"

    def test_config_validation(self):
        """Test configuration validation."""
        from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder

        builder = DebeziumConfigBuilder(
            connector_name="postgres-connector",
            database_hostname="localhost",
            database_port=5432,
            database_user="cdcuser",
            database_password="cdcpass",
            database_name="cdcdb",
        )

        is_valid = builder.validate()

        assert is_valid is True

    def test_config_to_json(self):
        """Test converting configuration to JSON."""
        from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder
        import json

        builder = DebeziumConfigBuilder(
            connector_name="postgres-connector",
            database_hostname="localhost",
            database_port=5432,
            database_user="cdcuser",
            database_password="cdcpass",
            database_name="cdcdb",
        )

        json_config = builder.to_json()
        parsed_config = json.loads(json_config)

        assert "name" in parsed_config
        assert "config" in parsed_config

    def test_config_with_transformations(self):
        """Test configuration with transformations."""
        from src.cdc_pipelines.postgres.debezium_config import DebeziumConfigBuilder

        builder = DebeziumConfigBuilder(
            connector_name="postgres-connector",
            database_hostname="localhost",
            database_port=5432,
            database_user="cdcuser",
            database_password="cdcpass",
            database_name="cdcdb",
            enable_unwrap_transform=True,
        )

        config = builder.build()

        assert "transforms" in config["config"]
        assert "unwrap" in config["config"]["transforms"]
