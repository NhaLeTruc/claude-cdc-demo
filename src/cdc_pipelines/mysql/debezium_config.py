"""Debezium connector configuration builder for MySQL."""

import json
from typing import Any, Dict, List, Optional

from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class DebeziumMySQLConfigBuilder:
    """Builds Debezium connector configuration for MySQL CDC."""

    def __init__(
        self,
        connector_name: str,
        database_hostname: str,
        database_port: int,
        database_user: str,
        database_password: str,
        database_include_list: Optional[List[str]] = None,
        table_include_list: Optional[List[str]] = None,
        topic_prefix: str = "cdc.mysql",
        server_id: Optional[int] = None,
        enable_unwrap_transform: bool = True,
        snapshot_mode: str = "initial",
    ) -> None:
        """
        Initialize Debezium MySQL config builder.

        Args:
            connector_name: Name of the connector
            database_hostname: Database host
            database_port: Database port
            database_user: Database user
            database_password: Database password
            database_include_list: List of databases to include
            table_include_list: List of tables to include (format: db.table)
            topic_prefix: Kafka topic prefix
            server_id: MySQL server ID for replication
            enable_unwrap_transform: Enable unwrap transformation
            snapshot_mode: Snapshot mode (initial, when_needed, never, schema_only)
        """
        self.connector_name = connector_name
        self.database_hostname = database_hostname
        self.database_port = database_port
        self.database_user = database_user
        self.database_password = database_password
        self.database_include_list = database_include_list or []
        self.table_include_list = table_include_list or []
        self.topic_prefix = topic_prefix
        self.server_id = server_id or 1
        self.enable_unwrap_transform = enable_unwrap_transform
        self.snapshot_mode = snapshot_mode

    def build(self) -> Dict[str, Any]:
        """
        Build Debezium connector configuration.

        Returns:
            Connector configuration dictionary
        """
        config: Dict[str, Any] = {
            "name": self.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": self.database_hostname,
                "database.port": str(self.database_port),
                "database.user": self.database_user,
                "database.password": self.database_password,
                "database.server.id": str(self.server_id),
                "database.server.name": self.topic_prefix.replace(".", "_"),
                "topic.prefix": self.topic_prefix,
                "snapshot.mode": self.snapshot_mode,
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                # Include schema changes
                "include.schema.changes": "true",
                # Binlog configuration
                "binlog.buffer.size": "8192",
                # Time precision
                "time.precision.mode": "adaptive_time_microseconds",
                # Decimal handling
                "decimal.handling.mode": "precise",
            },
        }

        # Add database include list if specified
        if self.database_include_list:
            config["config"]["database.include.list"] = ",".join(
                self.database_include_list
            )

        # Add table include list if specified
        if self.table_include_list:
            config["config"]["table.include.list"] = ",".join(self.table_include_list)

        # Add unwrap transformation if enabled
        if self.enable_unwrap_transform:
            config["config"]["transforms"] = "unwrap"
            config["config"]["transforms.unwrap.type"] = (
                "io.debezium.transforms.ExtractNewRecordState"
            )
            config["config"]["transforms.unwrap.drop.tombstones"] = "false"
            config["config"]["transforms.unwrap.delete.handling.mode"] = "rewrite"

        logger.info(f"Built Debezium MySQL configuration for connector: {self.connector_name}")
        return config

    def validate(self) -> bool:
        """
        Validate configuration.

        Returns:
            True if configuration is valid, False otherwise
        """
        required_fields = [
            self.connector_name,
            self.database_hostname,
            self.database_user,
            self.database_password,
        ]

        if not all(required_fields):
            logger.error("Missing required configuration fields")
            return False

        if self.database_port <= 0 or self.database_port > 65535:
            logger.error(f"Invalid database port: {self.database_port}")
            return False

        if self.server_id <= 0:
            logger.error(f"Invalid server_id: {self.server_id}")
            return False

        valid_snapshot_modes = ["initial", "when_needed", "never", "schema_only"]
        if self.snapshot_mode not in valid_snapshot_modes:
            logger.error(
                f"Invalid snapshot_mode: {self.snapshot_mode}. "
                f"Must be one of {valid_snapshot_modes}"
            )
            return False

        return True

    def to_json(self) -> str:
        """
        Convert configuration to JSON string.

        Returns:
            JSON string representation
        """
        config = self.build()
        return json.dumps(config, indent=2)
