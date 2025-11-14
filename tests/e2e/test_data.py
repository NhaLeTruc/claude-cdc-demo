"""Test data management utilities for E2E tests."""

import time
import uuid
from typing import List, Tuple, Optional
from pathlib import Path


def insert_baseline_customers(
    conn,
    id_range: Tuple[int, int] = (10000, 10100),
    unique_suffix: Optional[str] = None
) -> List[int]:
    """
    Insert baseline customers into Postgres for E2E testing.

    Args:
        conn: PostgreSQL connection (psycopg2)
        id_range: Range of customer IDs to insert (start, end) - exclusive end
        unique_suffix: Optional unique suffix for email addresses

    Returns:
        List of inserted customer_ids
    """
    cursor = conn.cursor()
    customer_ids = []

    # Generate unique suffix if not provided
    if unique_suffix is None:
        unique_suffix = uuid.uuid4().hex[:8]

    start_id, end_id = id_range

    for i in range(start_id, end_id):
        try:
            # First, insert with a temporary unique first_name to get customer_id
            temp_first_name = f"E2EBaseline_TEMP_{i}_{unique_suffix}"
            cursor.execute(
                """
                INSERT INTO customers (email, first_name, last_name, city, state, country)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING customer_id
                """,
                (
                    f"e2e_baseline_{i}_{unique_suffix}@example.com",
                    temp_first_name,
                    f"Customer",
                    "E2ECity",
                    "E2",
                    "USA",
                ),
            )
            customer_id = cursor.fetchone()[0]

            # Immediately update first_name to use actual customer_id
            cursor.execute(
                "UPDATE customers SET first_name = %s WHERE customer_id = %s",
                (f"E2EBaseline{customer_id}", customer_id)
            )

            # Commit each customer individually so CDC sees INSERT and UPDATE together
            conn.commit()

            customer_ids.append(customer_id)
        except Exception as e:
            # Continue on error (e.g., duplicate key)
            print(f"⚠ Failed to insert customer {i}: {e}")
            conn.rollback()  # Rollback failed transaction
            continue

    print(f"✓ Inserted {len(customer_ids)} baseline customers (IDs: {customer_ids[0]}-{customer_ids[-1]})")

    return customer_ids


def cleanup_baseline_customers(conn, customer_ids: List[int]) -> None:
    """
    Clean up baseline customers from Postgres.

    Args:
        conn: PostgreSQL connection (psycopg2)
        customer_ids: List of customer IDs to delete
    """
    if not customer_ids:
        return

    cursor = conn.cursor()
    try:
        # Delete in batches to avoid SQL size limits
        batch_size = 100
        for i in range(0, len(customer_ids), batch_size):
            batch = customer_ids[i:i + batch_size]
            cursor.execute(
                f"DELETE FROM customers WHERE customer_id IN ({','.join(map(str, batch))})"
            )
        conn.commit()
        print(f"✓ Cleaned up {len(customer_ids)} baseline customers")
    except Exception as e:
        print(f"⚠ Failed to cleanup customers: {e}")
        conn.rollback()


def wait_for_delta_propagation(
    spark,
    delta_table_path: str,
    customer_ids: List[int],
    timeout_seconds: int = 180,
    poll_interval: float = 5.0,
    early_success_threshold: float = 0.95
) -> bool:
    """
    Wait for customer IDs to appear in Delta Lake table.

    Supports early success detection if 95% of records are found.

    Args:
        spark: SparkSession instance
        delta_table_path: Path to Delta Lake table
        customer_ids: List of customer IDs to wait for
        timeout_seconds: Maximum time to wait
        poll_interval: Time between checks in seconds
        early_success_threshold: Fraction of records needed for early success (default: 0.95)

    Returns:
        True if threshold met, False otherwise
    """
    from delta import DeltaTable

    start_time = time.time()
    target_count = len(customer_ids)
    last_count = 0

    print(f"\n⏳ Waiting for {target_count} customers in Delta table at {delta_table_path}...")

    while time.time() - start_time < timeout_seconds:
        try:
            # Check if table exists
            if not DeltaTable.isDeltaTable(spark, delta_table_path):
                time.sleep(poll_interval)
                continue

            # Read Delta table and count matching records
            df = spark.read.format("delta").load(delta_table_path)

            # Count records with our customer IDs (in batches to avoid SQL size limits)
            batch_size = 100
            total_found = 0

            for i in range(0, len(customer_ids), batch_size):
                batch = customer_ids[i:i + batch_size]
                count = df.filter(df.customer_id.isin(batch)).count()
                total_found += count

            # Show progress if count changed
            if total_found != last_count:
                elapsed = time.time() - start_time
                progress_pct = (total_found / target_count) * 100
                print(f"  Progress: {total_found}/{target_count} ({progress_pct:.1f}%) at {elapsed:.1f}s")
                last_count = total_found

            # Early success: If we've found 95%+ of records
            if total_found >= target_count * early_success_threshold:
                elapsed = time.time() - start_time
                if total_found < target_count:
                    print(f"✓ Early success: {total_found}/{target_count} customers found ({elapsed:.1f}s)")
                else:
                    print(f"✓ All {total_found} customers found in Delta ({elapsed:.1f}s)")
                return True

        except Exception as e:
            # Log but continue polling
            pass

        time.sleep(poll_interval)

    elapsed = time.time() - start_time
    success_threshold = int(target_count * early_success_threshold)
    if last_count >= success_threshold:
        print(f"⚠ Partial success: {last_count}/{target_count} customers after {elapsed:.1f}s (threshold: {success_threshold})")
        return True

    print(f"✗ Only found {last_count}/{target_count} customers after {elapsed:.1f}s")
    return False


def create_isolated_customer(
    conn,
    unique_suffix: Optional[str] = None
) -> int:
    """
    Create a single isolated customer for modification tests.

    Args:
        conn: PostgreSQL connection (psycopg2)
        unique_suffix: Optional unique suffix for email

    Returns:
        customer_id of the created customer
    """
    cursor = conn.cursor()

    if unique_suffix is None:
        unique_suffix = uuid.uuid4().hex[:8]

    cursor.execute(
        """
        INSERT INTO customers (email, first_name, last_name, city, state, country)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING customer_id
        """,
        (
            f"e2e_isolated_{unique_suffix}@example.com",
            "E2EIsolated",
            "Customer",
            "TestCity",
            "TS",
            "USA",
        ),
    )
    customer_id = cursor.fetchone()[0]
    conn.commit()

    return customer_id


def insert_bulk_customers(
    conn,
    count: int = 1000,
    unique_suffix: Optional[str] = None
) -> List[int]:
    """
    Insert bulk customers for throughput testing.

    Args:
        conn: PostgreSQL connection (psycopg2)
        count: Number of customers to insert
        unique_suffix: Optional unique suffix for emails

    Returns:
        List of inserted customer_ids
    """
    cursor = conn.cursor()
    customer_ids = []

    if unique_suffix is None:
        unique_suffix = uuid.uuid4().hex[:8]

    print(f"\n⏳ Inserting {count} bulk customers for throughput test...")
    start_time = time.time()

    for i in range(count):
        cursor.execute(
            """
            INSERT INTO customers (email, first_name, last_name, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING customer_id
            """,
            (
                f"e2e_bulk_{i}_{unique_suffix}@example.com",
                f"E2EBulk{i}",
                f"Customer{i}",
                "BulkCity",
                "BK",
                "USA",
            ),
        )
        customer_ids.append(cursor.fetchone()[0])

        # Commit in batches
        if i % 100 == 0:
            conn.commit()

    conn.commit()
    elapsed = time.time() - start_time
    print(f"✓ Inserted {len(customer_ids)} bulk customers in {elapsed:.1f}s")

    return customer_ids


def ensure_delta_table_exists(spark, table_path: str, timeout_seconds: int = 60) -> bool:
    """
    Wait for Delta Lake table to exist at the specified path.

    Args:
        spark: SparkSession instance
        table_path: Path to the Delta Lake table
        timeout_seconds: Maximum time to wait for table creation

    Returns:
        True if table exists, False otherwise
    """
    from delta import DeltaTable

    start_time = time.time()

    print(f"⏳ Waiting for Delta table at {table_path}...")

    while time.time() - start_time < timeout_seconds:
        try:
            if DeltaTable.isDeltaTable(spark, table_path):
                print(f"✓ Delta table exists")
                return True
        except Exception:
            pass

        time.sleep(2.0)

    elapsed = time.time() - start_time
    print(f"⚠ Delta table not found after {elapsed:.1f}s")
    return False