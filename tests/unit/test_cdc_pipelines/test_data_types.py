"""Unit tests for MySQL data type preservation."""

import json
import pytest
from datetime import datetime, date
from decimal import Decimal


class TestDataTypePreservation:
    """Test suite for MySQL data type handling."""

    def test_varchar_preservation(self):
        """Test VARCHAR data type is preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "name": "Test Product with Special Chars: @#$%",
                "description": "Long description " * 100,  # Test long VARCHAR
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert isinstance(event["data"]["name"], str)
        assert event["data"]["name"] == "Test Product with Special Chars: @#$%"
        assert isinstance(event["data"]["description"], str)
        assert len(event["data"]["description"]) > 1000

    def test_int_preservation(self):
        """Test INT data type is preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 2147483647,  # Max INT value
                "stock_quantity": 0,
                "reorder_level": -100,  # Negative int
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert isinstance(event["data"]["product_id"], int)
        assert event["data"]["product_id"] == 2147483647
        assert isinstance(event["data"]["stock_quantity"], int)
        assert event["data"]["stock_quantity"] == 0
        assert isinstance(event["data"]["reorder_level"], int)
        assert event["data"]["reorder_level"] == -100

    def test_decimal_preservation(self):
        """Test DECIMAL data type is preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "price": 99.99,
                "discount_rate": 0.15,
                "tax_amount": 12.5,
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        # Should preserve decimal precision
        assert isinstance(event["data"]["price"], (float, Decimal))
        assert event["data"]["price"] == 99.99
        assert isinstance(event["data"]["discount_rate"], (float, Decimal))
        assert event["data"]["discount_rate"] == 0.15

    def test_datetime_preservation(self):
        """Test DATETIME data type is preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        # Debezium typically sends datetime as milliseconds or ISO string
        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "created_at": 1609459200000,  # 2021-01-01 00:00:00 UTC
                "updated_at": "2021-01-01T12:30:00Z",
                "last_restocked": None,  # NULL datetime
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        # Datetime should be parseable
        assert event["data"]["created_at"] is not None
        assert event["data"]["updated_at"] is not None
        assert event["data"]["last_restocked"] is None

    def test_json_preservation(self):
        """Test JSON data type is preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        metadata_json = {
            "tags": ["electronics", "gadget"],
            "specs": {
                "weight": 1.5,
                "dimensions": {"width": 10, "height": 20, "depth": 5},
            },
            "reviews_count": 42,
        }

        # Debezium sends JSON as string
        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "metadata": json.dumps(metadata_json),
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        # JSON should be preserved as string or dict
        metadata = event["data"]["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        assert metadata["tags"] == ["electronics", "gadget"]
        assert metadata["specs"]["weight"] == 1.5
        assert metadata["reviews_count"] == 42

    def test_boolean_preservation(self):
        """Test BOOLEAN/TINYINT(1) data type is preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "is_active": True,
                "is_featured": False,
                "in_stock": 1,  # MySQL BOOLEAN stored as TINYINT
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert isinstance(event["data"]["is_active"], (bool, int))
        assert event["data"]["is_active"] is True or event["data"]["is_active"] == 1
        assert isinstance(event["data"]["is_featured"], (bool, int))

    def test_null_value_preservation(self):
        """Test NULL values are preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "name": "Product",
                "description": None,
                "category": None,
                "price": None,
                "stock_quantity": 0,  # 0 is different from NULL
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert event["data"]["description"] is None
        assert event["data"]["category"] is None
        assert event["data"]["price"] is None
        assert event["data"]["stock_quantity"] == 0  # Not None

    def test_blob_text_preservation(self):
        """Test BLOB and TEXT data types are preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        large_text = "Lorem ipsum " * 10000  # Large TEXT content

        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "long_description": large_text,
                "binary_data": "base64encodeddata==",  # BLOB as base64
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert isinstance(event["data"]["long_description"], str)
        assert len(event["data"]["long_description"]) > 100000
        assert isinstance(event["data"]["binary_data"], str)

    def test_enum_set_preservation(self):
        """Test ENUM and SET data types are preserved correctly."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "status": "active",  # ENUM
                "features": "wifi,bluetooth,gps",  # SET
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        assert isinstance(event["data"]["status"], str)
        assert event["data"]["status"] == "active"
        assert isinstance(event["data"]["features"], str)
        assert "wifi" in event["data"]["features"]

    def test_special_characters_in_strings(self):
        """Test special characters in VARCHAR/TEXT are preserved."""
        from src.cdc_pipelines.mysql.event_parser import BinlogEventParser

        special_string = "Test 'quotes' \"double\" \\backslash \n newline \t tab émojis: 🎉🔥"

        debezium_event = {
            "op": "c",
            "after": {
                "product_id": 1,
                "name": special_string,
            },
            "source": {
                "db": "cdcdb",
                "table": "products",
                "ts_ms": 1000,
                "file": "bin.001",
                "pos": 100,
            },
        }

        parser = BinlogEventParser()
        event = parser.parse(debezium_event)

        # All special characters should be preserved
        assert event["data"]["name"] == special_string
        assert "🎉" in event["data"]["name"]
        assert "\\backslash" in event["data"]["name"]
