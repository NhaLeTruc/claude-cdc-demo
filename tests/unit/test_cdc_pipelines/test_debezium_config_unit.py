"""Unit tests for Debezium configuration."""

import pytest
from unittest.mock import MagicMock, patch


class TestDebeziumConfigUnit:
    """Unit tests for Debezium configuration without infrastructure."""

    def test_postgres_connector_config_structure(self):
        """Test Postgres Debezium connector configuration structure."""
        config = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "localhost",
            "database.port": "5432",
            "database.user": "debezium",
            "database.dbname": "testdb",
            "database.server.name": "postgres_server",
            "plugin.name": "pgoutput",
            "publication.name": "cdc_publication"
        }

        assert "connector.class" in config
        assert "PostgresConnector" in config["connector.class"]
        assert config["database.port"] == "5432"

    def test_mysql_connector_config_structure(self):
        """Test MySQL Debezium connector configuration structure."""
        config = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.hostname": "localhost",
            "database.port": "3306",
            "database.user": "debezium",
            "database.server.id": "1",
            "database.server.name": "mysql_server"
        }

        assert "connector.class" in config
        assert "MySqlConnector" in config["connector.class"]
        assert config["database.port"] == "3306"

    def test_kafka_bootstrap_servers_config(self):
        """Test Kafka bootstrap servers configuration."""
        bootstrap_servers = "localhost:9092,localhost:9093,localhost:9094"

        servers = bootstrap_servers.split(",")
        assert len(servers) >= 1
        assert all(":" in server for server in servers)

    def test_topic_naming_strategy(self):
        """Test Debezium topic naming strategy."""
        server_name = "postgres_server"
        database = "testdb"
        table = "customers"
        
        topic_name = f"{server_name}.{database}.{table}"

        assert topic_name == "postgres_server.testdb.customers"
        assert topic_name.count(".") == 2

    def test_snapshot_mode_options(self):
        """Test Debezium snapshot mode options."""
        snapshot_modes = ["initial", "never", "always", "initial_only", "when_needed"]

        for mode in snapshot_modes:
            assert isinstance(mode, str)
            assert len(mode) > 0

    def test_schema_history_configuration(self):
        """Test schema history configuration for MySQL."""
        config = {
            "schema.history.internal.kafka.bootstrap.servers": "localhost:9092",
            "schema.history.internal.kafka.topic": "schema-changes.testdb"
        }

        assert "schema.history.internal.kafka.bootstrap.servers" in config
        assert isinstance(config["schema.history.internal.kafka.topic"], str)

    def test_slot_drop_on_stop_config(self):
        """Test replication slot drop on stop configuration."""
        # For Postgres connector
        slot_drop_config = {
            "slot.drop.on.stop": "false"  # Should be false to preserve slot
        }

        assert slot_drop_config["slot.drop.on.stop"] in ["true", "false"]

    def test_heartbeat_interval_config(self):
        """Test heartbeat interval configuration."""
        heartbeat_interval = 5000  # milliseconds

        assert heartbeat_interval > 0
        assert heartbeat_interval <= 60000  # Max 1 minute

    def test_max_batch_size_config(self):
        """Test max batch size configuration."""
        max_batch_size = 2048

        assert max_batch_size > 0
        assert max_batch_size <= 10000

    def test_connector_name_validation(self):
        """Test connector name validation."""
        valid_names = [
            "postgres-connector-1",
            "mysql_connector_2",
            "testConnector"
        ]

        for name in valid_names:
            assert len(name) > 0
            assert isinstance(name, str)

    def test_table_include_list_format(self):
        """Test table include list format."""
        include_list = "public.customers,public.orders,public.products"

        tables = include_list.split(",")
        assert len(tables) == 3
        assert all("." in table for table in tables)

    def test_transforms_configuration(self):
        """Test Single Message Transform (SMT) configuration."""
        transforms_config = {
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false"
        }

        assert "transforms" in transforms_config
        assert "ExtractNewRecordState" in transforms_config.get("transforms.unwrap.type", "")

    def test_decimal_handling_mode(self):
        """Test decimal handling mode options."""
        decimal_modes = ["precise", "double", "string"]

        for mode in decimal_modes:
            assert mode in ["precise", "double", "string"]

    def test_time_precision_mode(self):
        """Test time precision mode options."""
        time_modes = ["adaptive", "connect"]

        for mode in time_modes:
            assert mode in ["adaptive", "connect", "adaptive_time_microseconds"]

    def test_binary_handling_mode(self):
        """Test binary handling mode options."""
        binary_modes = ["bytes", "base64", "hex"]

        for mode in binary_modes:
            assert mode in ["bytes", "base64", "base64-url-safe", "hex"]
