"""Debezium connector configuration builder for Postgres."""

import json
from typing import Any, Dict, List, Optional

from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class DebeziumConfigBuilder:
    """Builds Debezium connector configuration for Postgres CDC."""

    def __init__(
        self,
        connector_name: str,
        database_hostname: str,
        database_port: int,
        database_user: str,
        database_password: str,
        database_name: str,
        table_include_list: Optional[List[str]] = None,
        topic_prefix: str = "cdc.postgres",
        slot_name: str = "debezium",
        plugin_name: str = "pgoutput",
        publication_name: str = "dbz_publication",
        enable_unwrap_transform: bool = True,
    ) -> None:
        """
        Initialize Debezium config builder.

        Args:
            connector_name: Name of the connector
            database_hostname: Database host
            database_port: Database port
            database_user: Database user
            database_password: Database password
            database_name: Database name
            table_include_list: List of tables to include
            topic_prefix: Kafka topic prefix
            slot_name: Replication slot name
            plugin_name: Logical decoding plugin name
            publication_name: Publication name
            enable_unwrap_transform: Enable unwrap transformation
        """
        self.connector_name = connector_name
        self.database_hostname = database_hostname
        self.database_port = database_port
        self.database_user = database_user
        self.database_password = database_password
        self.database_name = database_name
        self.table_include_list = table_include_list or []
        self.topic_prefix = topic_prefix
        self.slot_name = slot_name
        self.plugin_name = plugin_name
        self.publication_name = publication_name
        self.enable_unwrap_transform = enable_unwrap_transform

    def build(self) -> Dict[str, Any]:
        """
        Build Debezium connector configuration.

        Returns:
            Connector configuration dictionary
        """
        config: Dict[str, Any] = {
            "name": self.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": self.database_hostname,
                "database.port": str(self.database_port),
                "database.user": self.database_user,
                "database.password": self.database_password,
                "database.dbname": self.database_name,
                "database.server.name": self.topic_prefix.replace(".", "_"),
                "topic.prefix": self.topic_prefix,
                "plugin.name": self.plugin_name,
                "slot.name": self.slot_name,
                "publication.name": self.publication_name,
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
            },
        }

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

        logger.info(f"Built Debezium configuration for connector: {self.connector_name}")
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
            self.database_name,
        ]

        if not all(required_fields):
            logger.error("Missing required configuration fields")
            return False

        if self.database_port <= 0 or self.database_port > 65535:
            logger.error(f"Invalid database port: {self.database_port}")
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
