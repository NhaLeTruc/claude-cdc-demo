"""CDC event parser for Debezium format."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.common.utils import parse_cdc_timestamp
from src.observability.logging_config import get_logger

logger = get_logger(__name__)


class CDCEventParser:
    """Parses Debezium CDC events into standardized format."""

    def __init__(self, primary_key_field: Optional[str] = None) -> None:
        """
        Initialize CDC event parser.

        Args:
            primary_key_field: Name of the primary key field
        """
        self.primary_key_field = primary_key_field

    def parse(self, debezium_event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Debezium CDC event into standard format.

        Args:
            debezium_event: Debezium event dictionary

        Returns:
            Standardized CDC event

        Raises:
            ValueError: If event is invalid or missing required fields
        """
        if not debezium_event or "op" not in debezium_event:
            raise ValueError("Invalid Debezium event: missing 'op' field")

        operation = self._parse_operation(debezium_event["op"])
        source = debezium_event.get("source", {})

        # Extract data based on operation type
        if operation == "INSERT":
            data = debezium_event.get("after", {})
            before = None
        elif operation == "UPDATE":
            data = debezium_event.get("after", {})
            before = debezium_event.get("before", {})
        elif operation == "DELETE":
            data = debezium_event.get("before", {})
            before = debezium_event.get("before", {})
        else:
            raise ValueError(f"Unknown operation: {debezium_event['op']}")

        # Build standardized event
        event: Dict[str, Any] = {
            "operation": operation,
            "table": source.get("table", "unknown"),
            "data": data,
            "timestamp": self._parse_timestamp(source.get("ts_ms")),
            "metadata": {
                "database": source.get("db"),
                "schema": source.get("schema", "public"),
                "connector": source.get("connector"),
                "lsn": source.get("lsn"),
                "txId": source.get("txId"),
            },
        }

        # Add before state for UPDATE/DELETE
        if before:
            event["before"] = before

        # Extract primary key if field specified
        if self.primary_key_field and data:
            event["primary_key"] = data.get(self.primary_key_field)

        logger.debug(
            f"Parsed {operation} event for table {event['table']}: "
            f"pk={event.get('primary_key', 'N/A')}"
        )

        return event

    def parse_batch(self, debezium_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse batch of Debezium CDC events.

        Args:
            debezium_events: List of Debezium events

        Returns:
            List of standardized CDC events
        """
        parsed_events = []
        errors = 0

        for i, event in enumerate(debezium_events):
            try:
                parsed = self.parse(event)
                parsed_events.append(parsed)
            except Exception as e:
                logger.error(f"Failed to parse event {i}: {e}")
                errors += 1

        if errors > 0:
            logger.warning(f"Failed to parse {errors}/{len(debezium_events)} events")

        return parsed_events

    def _parse_operation(self, op_code: str) -> str:
        """
        Parse Debezium operation code to standard operation name.

        Args:
            op_code: Debezium operation code (c/u/d/r)

        Returns:
            Standard operation name (INSERT/UPDATE/DELETE/READ)
        """
        operation_map = {
            "c": "INSERT",  # create
            "u": "UPDATE",  # update
            "d": "DELETE",  # delete
            "r": "READ",    # read (initial snapshot)
        }
        return operation_map.get(op_code, "UNKNOWN")

    def _parse_timestamp(self, ts_ms: Optional[int]) -> str:
        """
        Parse timestamp from milliseconds since epoch.

        Args:
            ts_ms: Timestamp in milliseconds

        Returns:
            ISO format timestamp string
        """
        if ts_ms is None:
            return datetime.now().isoformat()

        dt = parse_cdc_timestamp(ts_ms)
        return dt.isoformat() if dt else datetime.now().isoformat()
