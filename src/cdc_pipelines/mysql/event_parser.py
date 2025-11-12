"""MySQL binlog event parser for CDC operations."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class BinlogEventParser:
    """Parses Debezium MySQL binlog events into standardized format."""

    OPERATION_MAP = {
        "c": "INSERT",  # create
        "u": "UPDATE",  # update
        "d": "DELETE",  # delete
        "r": "READ",  # read (snapshot)
    }

    def __init__(self) -> None:
        """Initialize binlog event parser."""
        pass

    def parse(self, debezium_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Debezium CDC event into standard format.

        Supports both:
        - Full envelope format (with 'op', 'before', 'after', 'source')
        - Flattened format (with '__op', '__ts_ms', etc. from ExtractNewRecordState)

        Args:
            debezium_event: Raw Debezium event from Kafka

        Returns:
            Standardized CDC event dictionary

        Raises:
            ValueError: If event is invalid or missing required fields
        """
        if not debezium_event:
            raise ValueError("Invalid Debezium event: empty event")

        # Check if this is a flattened event (with __ prefix) or full envelope
        is_flattened = "__op" in debezium_event

        if is_flattened:
            # Flattened format from ExtractNewRecordState transformation
            if "__op" not in debezium_event:
                raise ValueError("Invalid Debezium event: missing '__op' field in flattened format")

            operation = self._parse_operation(debezium_event["__op"])

            # In flattened format, all business data fields are at the top level
            # Extract business data by filtering out CDC metadata fields
            data = {k: v for k, v in debezium_event.items() if not k.startswith("__")}
            before = None  # Flattened format doesn't include before state

            # Build standardized event
            event: Dict[str, Any] = {
                "operation": operation,
                "table": debezium_event.get("__table", "unknown"),
                "data": data,
                "timestamp": self._parse_timestamp(debezium_event.get("__ts_ms")),
                "metadata": {
                    "database": debezium_event.get("__db"),
                    "binlog_file": None,  # Not available in flattened format
                    "binlog_position": None,
                    "server_id": None,
                    "gtid": None,
                    "thread": None,
                },
            }
        else:
            # Full envelope format
            if "op" not in debezium_event:
                raise ValueError("Invalid Debezium event: missing 'op' field")

            if "source" not in debezium_event:
                raise ValueError("Invalid Debezium event: missing 'source' field")

            operation = self._parse_operation(debezium_event["op"])
            source = debezium_event.get("source", {})

            # Extract data based on operation type
            if operation == "INSERT" or operation == "READ":
                data = debezium_event.get("after", {})
                before = None
            elif operation == "UPDATE":
                data = debezium_event.get("after", {})
                before = debezium_event.get("before", {})
            elif operation == "DELETE":
                data = debezium_event.get("before", {})
                before = debezium_event.get("before", {})
            else:
                data = {}
                before = None

            # Build standardized event
            event: Dict[str, Any] = {
                "operation": operation,
                "table": source.get("table", "unknown"),
                "data": data,
                "timestamp": self._parse_timestamp(source.get("ts_ms")),
                "metadata": {
                    "database": source.get("db"),
                    "binlog_file": source.get("file"),
                    "binlog_position": source.get("pos"),
                    "server_id": source.get("server_id"),
                    "gtid": source.get("gtid"),
                    "thread": source.get("thread"),
                },
            }

            # Add before state for updates
            if before is not None:
                event["before"] = before

        return event

    def parse_batch(self, debezium_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse batch of Debezium events.

        Args:
            debezium_events: List of raw Debezium events

        Returns:
            List of standardized CDC events
        """
        parsed_events = []
        for event in debezium_events:
            try:
                parsed_event = self.parse(event)
                parsed_events.append(parsed_event)
            except Exception as e:
                logger.error(f"Failed to parse event: {e}")
                logger.debug(f"Problem event: {event}")
                continue

        logger.info(
            f"Parsed {len(parsed_events)}/{len(debezium_events)} events successfully"
        )
        return parsed_events

    def _parse_operation(self, op_code: str) -> str:
        """
        Convert Debezium operation code to standard operation name.

        Args:
            op_code: Debezium operation code (c, u, d, r)

        Returns:
            Standard operation name (INSERT, UPDATE, DELETE, READ)
        """
        return self.OPERATION_MAP.get(op_code, "UNKNOWN")

    def _parse_timestamp(self, ts_ms: Optional[int]) -> Optional[str]:
        """
        Convert timestamp from milliseconds to ISO format.

        Args:
            ts_ms: Timestamp in milliseconds

        Returns:
            ISO format timestamp string or None
        """
        if ts_ms is None:
            return None

        try:
            dt = datetime.fromtimestamp(ts_ms / 1000.0)
            return dt.isoformat()
        except Exception as e:
            logger.warning(f"Failed to parse timestamp {ts_ms}: {e}")
            return None

    def extract_primary_key(
        self, event: Dict[str, Any], primary_key_field: str
    ) -> Optional[Any]:
        """
        Extract primary key value from event.

        Args:
            event: Parsed CDC event
            primary_key_field: Name of the primary key field

        Returns:
            Primary key value or None
        """
        data = event.get("data", {})
        return data.get(primary_key_field)

    def get_changed_fields(self, event: Dict[str, Any]) -> List[str]:
        """
        Get list of changed fields in UPDATE event.

        Args:
            event: Parsed CDC event

        Returns:
            List of field names that changed
        """
        if event.get("operation") != "UPDATE":
            return []

        before = event.get("before", {})
        after = event.get("data", {})

        changed_fields = []
        for field, new_value in after.items():
            old_value = before.get(field)
            if old_value != new_value:
                changed_fields.append(field)

        return changed_fields
