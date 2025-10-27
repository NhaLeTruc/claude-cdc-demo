"""Unit tests for CDC event parser."""

import pytest
from datetime import datetime


@pytest.mark.unit
class TestCDCEventParser:
    """Test CDC event parser (Debezium format → standard format)."""

    def test_parse_insert_event(self):
        """Test parsing INSERT event."""
        from src.cdc_pipelines.postgres.event_parser import CDCEventParser

        debezium_event = {
            "op": "c",  # create/insert
            "after": {
                "customer_id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john@example.com",
            },
            "source": {
                "db": "cdcdb",
                "table": "customers",
                "ts_ms": 1609459200000,
            },
        }

        parser = CDCEventParser()
        event = parser.parse(debezium_event)

        assert event["operation"] == "INSERT"
        assert event["table"] == "customers"
        assert event["data"]["customer_id"] == 1
        assert event["data"]["email"] == "john@example.com"
        assert "timestamp" in event

    def test_parse_update_event(self):
        """Test parsing UPDATE event."""
        from src.cdc_pipelines.postgres.event_parser import CDCEventParser

        debezium_event = {
            "op": "u",  # update
            "before": {
                "customer_id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john@example.com",
            },
            "after": {
                "customer_id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john.doe@example.com",
            },
            "source": {
                "db": "cdcdb",
                "table": "customers",
                "ts_ms": 1609459200000,
            },
        }

        parser = CDCEventParser()
        event = parser.parse(debezium_event)

        assert event["operation"] == "UPDATE"
        assert event["data"]["email"] == "john.doe@example.com"
        assert "before" in event
        assert event["before"]["email"] == "john@example.com"

    def test_parse_delete_event(self):
        """Test parsing DELETE event."""
        from src.cdc_pipelines.postgres.event_parser import CDCEventParser

        debezium_event = {
            "op": "d",  # delete
            "before": {
                "customer_id": 1,
                "first_name": "John",
                "last_name": "Doe",
                "email": "john@example.com",
            },
            "source": {
                "db": "cdcdb",
                "table": "customers",
                "ts_ms": 1609459200000,
            },
        }

        parser = CDCEventParser()
        event = parser.parse(debezium_event)

        assert event["operation"] == "DELETE"
        assert event["data"]["customer_id"] == 1
        assert "before" in event

    def test_parse_event_with_metadata(self):
        """Test parsing event includes metadata."""
        from src.cdc_pipelines.postgres.event_parser import CDCEventParser

        debezium_event = {
            "op": "c",
            "after": {"customer_id": 1, "email": "test@example.com"},
            "source": {
                "db": "cdcdb",
                "table": "customers",
                "ts_ms": 1609459200000,
                "lsn": 123456789,
            },
        }

        parser = CDCEventParser()
        event = parser.parse(debezium_event)

        assert "metadata" in event
        assert event["metadata"]["database"] == "cdcdb"
        assert event["metadata"]["lsn"] == 123456789

    def test_parse_batch_events(self):
        """Test parsing multiple events."""
        from src.cdc_pipelines.postgres.event_parser import CDCEventParser

        debezium_events = [
            {"op": "c", "after": {"id": 1}, "source": {"table": "customers", "ts_ms": 1000}},
            {"op": "u", "after": {"id": 2}, "source": {"table": "customers", "ts_ms": 2000}},
            {"op": "d", "before": {"id": 3}, "source": {"table": "customers", "ts_ms": 3000}},
        ]

        parser = CDCEventParser()
        events = parser.parse_batch(debezium_events)

        assert len(events) == 3
        assert events[0]["operation"] == "INSERT"
        assert events[1]["operation"] == "UPDATE"
        assert events[2]["operation"] == "DELETE"

    def test_parse_event_with_null_values(self):
        """Test parsing event with null values."""
        from src.cdc_pipelines.postgres.event_parser import CDCEventParser

        debezium_event = {
            "op": "c",
            "after": {
                "customer_id": 1,
                "email": None,
                "phone": None,
            },
            "source": {"table": "customers", "ts_ms": 1000},
        }

        parser = CDCEventParser()
        event = parser.parse(debezium_event)

        assert event["data"]["email"] is None
        assert event["data"]["phone"] is None

    def test_parse_invalid_event(self):
        """Test parsing invalid event raises error."""
        from src.cdc_pipelines.postgres.event_parser import CDCEventParser

        invalid_event = {"invalid": "event"}

        parser = CDCEventParser()

        with pytest.raises(ValueError):
            parser.parse(invalid_event)

    def test_extract_primary_key(self):
        """Test extracting primary key from event."""
        from src.cdc_pipelines.postgres.event_parser import CDCEventParser

        debezium_event = {
            "op": "c",
            "after": {"customer_id": 1, "email": "test@example.com"},
            "source": {"table": "customers", "ts_ms": 1000},
        }

        parser = CDCEventParser(primary_key_field="customer_id")
        event = parser.parse(debezium_event)

        assert event["primary_key"] == 1
