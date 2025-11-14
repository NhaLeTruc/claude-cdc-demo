"""Test utility functions for CDC integration and E2E tests."""

import json
import os
import time
from typing import Optional, Callable, Any


def get_kafka_topic(table_name: str) -> str:
    """
    Get Kafka topic name for a given table from environment variables.

    Args:
        table_name: The table name (e.g., 'customers', 'orders', 'schema_evolution_test')

    Returns:
        The Kafka topic name from environment variables

    Raises:
        ValueError: If the environment variable is not found
    """
    # Map table names to environment variable names
    env_var_map = {
        'customers': 'KAFKA_TOPIC_CUSTOMERS',
        'orders': 'KAFKA_TOPIC_ORDERS',
        'products': 'KAFKA_TOPIC_PRODUCTS',
        'inventory': 'KAFKA_TOPIC_INVENTORY',
        'schema_evolution_test': 'KAFKA_TOPIC_SCHEMA_EVOLUTION',
        'schema_evo_test': 'KAFKA_TOPIC_SCHEMA_EVOLUTION',  # Alias for the actual table name
    }

    try:
        env_var = env_var_map[table_name]
        topic = os.getenv(env_var)

        if topic is None:
            raise ValueError(
                f"Environment variable '{env_var}' for table '{table_name}' is not set. "
                f"Please ensure it's defined in your .env file."
            )

        return topic
    except KeyError:
        raise ValueError(
            f"Unknown table name '{table_name}'. "
            f"Supported tables: {', '.join(env_var_map.keys())}"
        )


def safe_json_deserializer(m: Optional[bytes]) -> Optional[dict]:
    """
    Safely deserialize Kafka message bytes to JSON.

    Handles tombstone records (None values) which are sent for DELETE operations.

    Args:
        m: Message bytes from Kafka (can be None for tombstone records)

    Returns:
        Deserialized JSON dict, or None for tombstone records

    Raises:
        json.JSONDecodeError: If message cannot be decoded as JSON
        UnicodeDecodeError: If message bytes cannot be decoded as UTF-8
    """
    try:
        if m is None:
            # Tombstone record (DELETE operation)
            return None

        return json.loads(m.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise type(e)(
            f"Failed to deserialize Kafka message: {e}. "
            f"Message bytes (first 100): {m[:100] if m else None}"
        )


def wait_for_condition(
    condition_func: Callable[[], bool],
    timeout_seconds: int = 60,
    poll_interval: float = 2.0,
    error_message: str = "Condition not met within timeout",
    adaptive: bool = True,
    max_poll_interval: float = 10.0
) -> bool:
    """
    Wait for a condition to become true with timeout and adaptive polling.

    Args:
        condition_func: Function that returns True when condition is met
        timeout_seconds: Maximum time to wait in seconds (default: 60)
        poll_interval: Initial time between checks in seconds (default: 2.0)
        error_message: Error message if timeout occurs
        adaptive: Use exponential backoff for polling (default: True)
        max_poll_interval: Maximum poll interval when using adaptive mode

    Returns:
        True if condition was met

    Raises:
        TimeoutError: If condition not met within timeout
    """
    start_time = time.time()
    last_exception = None
    current_poll_interval = poll_interval
    attempts = 0

    while time.time() - start_time < timeout_seconds:
        try:
            if condition_func():
                elapsed = time.time() - start_time
                if attempts > 0:
                    print(f"  Condition met after {elapsed:.1f}s ({attempts} attempts)")
                return True
        except Exception as e:
            # Store exception but continue polling
            last_exception = e

        attempts += 1
        time.sleep(current_poll_interval)

        # Adaptive backoff: start fast, slow down as we wait longer
        if adaptive and attempts > 3:
            current_poll_interval = min(
                current_poll_interval * 1.5,  # Increase by 50%
                max_poll_interval
            )

    # Timeout occurred
    elapsed = time.time() - start_time
    if last_exception:
        raise TimeoutError(
            f"{error_message} (timeout: {timeout_seconds}s, elapsed: {elapsed:.1f}s). "
            f"Last exception: {type(last_exception).__name__}: {last_exception}"
        )
    else:
        raise TimeoutError(
            f"{error_message} (timeout: {timeout_seconds}s, elapsed: {elapsed:.1f}s, {attempts} attempts)"
        )


def ensure_delta_table_exists(spark, table_path: str, timeout_seconds: int = 60) -> bool:
    """
    Wait for Delta Lake table to exist at the specified path.

    This is useful in E2E tests where the CDC pipeline creates tables lazily
    on first write. Uses retry logic with timeout.

    Args:
        spark: SparkSession instance
        table_path: Path to the Delta Lake table
        timeout_seconds: Maximum time to wait for table creation (default: 60)

    Returns:
        True if table exists

    Raises:
        TimeoutError: If table not created within timeout
    """
    from delta import DeltaTable

    def check_table_exists() -> bool:
        try:
            return DeltaTable.isDeltaTable(spark, table_path)
        except Exception:
            return False

    return wait_for_condition(
        condition_func=check_table_exists,
        timeout_seconds=timeout_seconds,
        poll_interval=2.0,
        error_message=f"Delta Lake table not created at {table_path}"
    )


def ensure_iceberg_table_exists(
    table_manager,
    timeout_seconds: int = 60
) -> bool:
    """
    Wait for Iceberg table to exist.

    This is useful in E2E tests where tables are created lazily by streaming
    pipelines. Uses retry logic with timeout.

    Args:
        table_manager: IcebergTableManager instance
        timeout_seconds: Maximum time to wait for table creation (default: 60)

    Returns:
        True if table exists

    Raises:
        TimeoutError: If table not created within timeout
    """
    def check_table_exists() -> bool:
        try:
            return table_manager.table_exists()
        except Exception:
            return False

    table_name = f"{table_manager.config.namespace}.{table_manager.config.table_name}"
    return wait_for_condition(
        condition_func=check_table_exists,
        timeout_seconds=timeout_seconds,
        poll_interval=2.0,
        error_message=f"Iceberg table {table_name} not created"
    )


def wait_for_kafka_event(
    topic: str,
    filter_func: Callable[[dict], bool],
    timeout_seconds: int = 30,
    bootstrap_servers: str = "localhost:29092"
) -> Optional[dict]:
    """
    Wait for a specific event to appear in Kafka topic.

    Args:
        topic: Kafka topic name
        filter_func: Function that returns True when the desired event is found
        timeout_seconds: Maximum time to wait (default: 30)
        bootstrap_servers: Kafka bootstrap servers (default: localhost:29092)

    Returns:
        The matching event, or None if not found

    Raises:
        TimeoutError: If event not found within timeout
    """
    from kafka import KafkaConsumer
    import json

    start_time = time.time()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        consumer_timeout_ms=int((timeout_seconds * 1000) * 0.8),  # 80% of total timeout
        value_deserializer=safe_json_deserializer
    )

    try:
        for message in consumer:
            payload = message.value
            # Skip tombstone records
            if payload is None:
                continue

            if filter_func(payload):
                consumer.close()
                return payload

            # Check timeout
            if time.time() - start_time > timeout_seconds:
                break

        consumer.close()
        raise TimeoutError(f"Event not found in topic {topic} within {timeout_seconds}s")

    except Exception as e:
        consumer.close()
        if isinstance(e, TimeoutError):
            raise
        raise TimeoutError(f"Error while waiting for Kafka event: {e}")


def wait_for_iceberg_record(
    table_manager,
    filter_condition: str,
    timeout_seconds: int = 60,
    poll_interval: float = 3.0,
    min_count: int = 1
) -> list:
    """
    Wait for record(s) to appear in Iceberg table.

    Args:
        table_manager: IcebergTableManager instance
        filter_condition: SQL-like filter condition (e.g., "customer_id = 123")
        timeout_seconds: Maximum time to wait (default: 60)
        poll_interval: Time between checks in seconds (default: 3.0)
        min_count: Minimum number of records expected (default: 1)

    Returns:
        List of matching records

    Raises:
        TimeoutError: If records not found within timeout
    """
    start_time = time.time()
    last_count = 0

    while time.time() - start_time < timeout_seconds:
        try:
            if not table_manager.table_exists():
                time.sleep(poll_interval)
                continue

            result = table_manager.query_table(filter_condition=filter_condition)
            last_count = len(result)

            if last_count >= min_count:
                return result

        except Exception as e:
            # Log but continue polling
            pass

        time.sleep(poll_interval)

    # Timeout occurred
    table_name = f"{table_manager.config.namespace}.{table_manager.config.table_name}"
    raise TimeoutError(
        f"Expected at least {min_count} record(s) in Iceberg table {table_name} "
        f"with filter '{filter_condition}', but found {last_count} after {timeout_seconds}s"
    )


def read_delta_table_with_retry(
    spark,
    table_path: str,
    max_retries: int = 5,
    retry_delay: float = 3.0
):
    """
    Read Delta Lake table with retry logic.

    Handles cases where table might not exist yet or might be in the process
    of being created by a CDC pipeline.

    Args:
        spark: SparkSession instance
        table_path: Path to the Delta Lake table
        max_retries: Maximum number of retry attempts (default: 5)
        retry_delay: Delay between retries in seconds (default: 3.0)

    Returns:
        Spark DataFrame

    Raises:
        Exception: If table cannot be read after all retries
    """
    from delta import DeltaTable

    for attempt in range(max_retries):
        try:
            # First check if it's a Delta table
            if DeltaTable.isDeltaTable(spark, table_path):
                return spark.read.format("delta").load(table_path)
            else:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise FileNotFoundError(f"Path {table_path} is not a Delta table")
        except Exception as e:
            if attempt < max_retries - 1:
                # Wait before retry
                time.sleep(retry_delay)
            else:
                # Last attempt failed, raise the error
                raise

    raise Exception(f"Failed to read Delta table at {table_path} after {max_retries} attempts")
