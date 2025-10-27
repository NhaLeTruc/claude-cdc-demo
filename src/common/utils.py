"""Common utility functions."""

import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4


def generate_event_id() -> str:
    """Generate a unique event ID."""
    return str(uuid4())


def calculate_checksum(data: Dict[str, Any]) -> str:
    """Calculate MD5 checksum of data dictionary."""
    # Sort keys for consistent hashing
    sorted_data = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(sorted_data.encode()).hexdigest()


def calculate_row_checksum(row: Dict[str, Any], exclude_fields: Optional[List[str]] = None) -> str:
    """
    Calculate checksum for a row, excluding specified fields.

    Args:
        row: Row data as dictionary
        exclude_fields: Fields to exclude from checksum (e.g., timestamps)

    Returns:
        MD5 checksum hex string
    """
    exclude = exclude_fields or ["created_at", "updated_at"]
    filtered_row = {k: v for k, v in row.items() if k not in exclude}
    return calculate_checksum(filtered_row)


def timestamp_to_iso(ts: Optional[datetime]) -> Optional[str]:
    """Convert timestamp to ISO format string."""
    if ts is None:
        return None
    return ts.isoformat()


def iso_to_timestamp(iso_str: Optional[str]) -> Optional[datetime]:
    """Convert ISO format string to timestamp."""
    if iso_str is None:
        return None
    return datetime.fromisoformat(iso_str)


def safe_get(data: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Safely get value from dictionary with default."""
    return data.get(key, default)


def flatten_dict(data: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    """
    Flatten nested dictionary.

    Args:
        data: Dictionary to flatten
        parent_key: Parent key for recursion
        sep: Separator for nested keys

    Returns:
        Flattened dictionary
    """
    items: List[tuple] = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def parse_cdc_timestamp(ts_value: Any) -> Optional[datetime]:
    """
    Parse CDC timestamp from various formats.

    Args:
        ts_value: Timestamp value (can be datetime, int (epoch), or ISO string)

    Returns:
        Parsed datetime object
    """
    if ts_value is None:
        return None

    if isinstance(ts_value, datetime):
        return ts_value

    if isinstance(ts_value, int):
        # Assume epoch milliseconds
        return datetime.fromtimestamp(ts_value / 1000.0)

    if isinstance(ts_value, str):
        return iso_to_timestamp(ts_value)

    return None


def format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def retry_with_backoff(
    func: callable,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
) -> Any:
    """
    Retry function with exponential backoff.

    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        initial_delay: Initial delay in seconds
        backoff_factor: Backoff multiplier

    Returns:
        Function result

    Raises:
        Last exception if all retries fail
    """
    import time

    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                time.sleep(delay)
                delay *= backoff_factor
            else:
                raise last_exception

    raise last_exception  # type: ignore
