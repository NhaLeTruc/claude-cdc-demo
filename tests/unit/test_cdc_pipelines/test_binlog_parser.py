"""Unit tests for MySQL binlog event parser."""

import pytest
from datetime import datetime


class TestBinlogParser:
    """Test suite for MySQL binlog event parser."""

    def test_parse_insert_event(self):
        """Test parsing INSERT event from binlog."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",  # create/insert
            "after": {
                "product_id": 1,
                "name": "Widget",
                "category": "Electronics",
                "price": 99.99,
                "stock_quantity": 100,
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1609459200000,
                "file": "mysql-bin.000001",
                "pos": 12345,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert event["operation"] == "INSERT"
        assert event["table"] == "products"
        assert event["data"]["product_id"] == 1
        assert event["data"]["name"] == "Widget"
        assert event["data"]["price"] == 99.99
        assert event["metadata"]["database"] == "cdcdb"
        assert event["metadata"]["binlog_file"] == "mysql-bin.000001"
        assert event["metadata"]["binlog_position"] == 12345

    def test_parse_update_event(self):
        """Test parsing UPDATE event from binlog."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "u",  # update
            "before": {
                "product_id": 1,
                "name": "Widget",
                "price": 99.99,
                "stock_quantity": 100,
            },
            "after": {
                "product_id": 1,
                "name": "Widget Pro",
                "price": 129.99,
                "stock_quantity": 95,
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1609459260000,
                "file": "mysql-bin.000001",
                "pos": 12456,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert event["operation"] == "UPDATE"
        assert event["table"] == "products"
        assert event["data"]["name"] == "Widget Pro"
        assert event["data"]["price"] == 129.99
        assert "before" in event
        assert event["before"]["price"] == 99.99

    def test_parse_delete_event(self):
        """Test parsing DELETE event from binlog."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "d",  # delete
            "before": {
                "product_id": 1,
                "name": "Widget Pro",
                "price": 129.99,
                "stock_quantity": 95,
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1609459320000,
                "file": "mysql-bin.000001",
                "pos": 12567,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert event["operation"] == "DELETE"
        assert event["table"] == "products"
        assert event["data"]["product_id"] == 1
        assert event["metadata"]["binlog_file"] == "mysql-bin.000001"

    def test_parse_batch_events(self):
        """Test parsing batch of events."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        events = [
            {
                "op": "c",
                "after": {"product_id": 1, "name": "Product 1"},
                "source": {"db": "cdcdb", "table": "products", "ts_ms": 1000, "file": "bin.001", "pos": 100},
            },
            {
                "op": "c",
                "after": {"product_id": 2, "name": "Product 2"},
                "source": {"db": "cdcdb", "table": "products", "ts_ms": 2000, "file": "bin.001", "pos": 200},
            },
            {
                "op": "u",
                "before": {"product_id": 1, "name": "Product 1"},
                "after": {"product_id": 1, "name": "Product 1 Updated"},
                "source": {"db": "cdcdb", "table": "products", "ts_ms": 3000, "file": "bin.001", "pos": 300},
            },
        ]

        parser = BinlogEventParser()
        parsed_events = parser.parse_batch(events)

        assert len(parsed_events) == 3
        assert parsed_events[0]["operation"] == "INSERT"
        assert parsed_events[1]["operation"] == "INSERT"
        assert parsed_events[2]["operation"] == "UPDATE"

    def test_extract_metadata(self):
        """Test metadata extraction from binlog event."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",
            "after": {"product_id": 1},
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1609459200000,
                "file": "mysql-bin.000001",
                "pos": 12345,
                "server_id": 1,
                "gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        metadata = event["metadata"]
        assert metadata["database"] == "cdcdb"
        assert metadata["binlog_file"] == "mysql-bin.000001"
        assert metadata["binlog_position"] == 12345
        assert metadata["server_id"] == 1
        assert metadata["gtid"] == "3E11FA47-71CA-11E1-9E33-C80AA9429562:23"

    def test_parse_invalid_event(self):
        """Test parsing invalid event raises error."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        parser = BinlogEventParser()

        with pytest.raises(ValueError, match="Invalid.*event"):
            parser.parse({})

        with pytest.raises(ValueError, match="Invalid.*event"):
            parser.parse({"op": "c"})  # Missing source

    def test_operation_type_mapping(self):
        """Test operation type mapping."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        parser = BinlogEventParser()

        test_cases = [
            ("c", "INSERT"),
            ("u", "UPDATE"),
            ("d", "DELETE"),
            ("r", "READ"),  # Initial snapshot
        ]

        for debezium_op, expected_op in test_cases:
            event = {
                "op": debezium_op,
                "after": {"id": 1} if debezium_op in ["c", "u", "r"] else None,
                "before": {"id": 1} if debezium_op in ["u", "d"] else None,
                "source": {"db": "test", "table": "test", "ts_ms": 1000, "file": "bin.001", "pos": 100},
            }
            parsed = parser.parse(event)
            assert parsed["operation"] == expected_op

    def test_timestamp_parsing(self):
        """Test timestamp parsing from milliseconds."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",
            "after": {"product_id": 1},
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1609459200000,  # 2021-01-01 00:00:00 UTC
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        # Timestamp should be converted to ISO format or datetime
        assert "timestamp" in event
        assert event["timestamp"] is not None
