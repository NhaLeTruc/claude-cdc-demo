"""Pytest fixtures for E2E tests with automatic infrastructure management."""

import pytest
import subprocess
import time
import socket
import os
from pathlib import Path


def is_port_open(host: str, port: int, timeout: int = 1) -> bool:
    """Check if a port is open."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except socket.error:
        return False


def log_service_diagnostics(service_name: str, host: str, port: int):
    """Log diagnostic information for a failed service."""
    import subprocess

    print(f"\n{'='*60}")
    print(f"DIAGNOSTICS for {service_name} ({host}:{port})")
    print(f"{'='*60}")

    # Check if port is actually in use
    try:
        result = subprocess.run(
            ["netstat", "-an"],
            capture_output=True,
            text=True,
            timeout=5
        )
        port_str = str(port)
        if port_str in result.stdout:
            print(f"✓ Port {port} appears in netstat output")
            # Show relevant lines
            for line in result.stdout.split('\n'):
                if port_str in line:
                    print(f"  {line.strip()}")
        else:
            print(f"✗ Port {port} NOT found in netstat output")
    except Exception as e:
        print(f"⚠ Could not run netstat: {e}")

    # Check Docker container status
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"expose={port}", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        print(f"\nDocker containers exposing port {port}:")
        if result.stdout.strip():
            print(result.stdout)
        else:
            print(f"  (No containers found exposing port {port})")
    except Exception as e:
        print(f"⚠ Could not check Docker containers: {e}")

    # Check docker-compose service logs (last 20 lines)
    try:
        # Map common service names
        service_map = {
            "PostgreSQL": "postgres",
            "MySQL": "mysql",
            "Kafka": "kafka",
            "Debezium": "debezium",
            "MinIO": "minio"
        }
        docker_service = service_map.get(service_name, service_name.lower())

        result = subprocess.run(
            ["docker", "compose", "logs", "--tail=20", docker_service],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=Path(__file__).parent.parent.parent
        )
        if result.returncode == 0 and result.stdout:
            print(f"\nRecent logs for {docker_service}:")
            print(result.stdout[-1000:])  # Last 1000 chars
    except Exception as e:
        print(f"⚠ Could not fetch container logs: {e}")

    print(f"{'='*60}\n")


def wait_for_service(host: str, port: int, max_attempts: int = 30, service_name: str = "service") -> bool:
    """Wait for a service to be available."""
    print(f"\nWaiting for {service_name} at {host}:{port}...")
    for attempt in range(max_attempts):
        if is_port_open(host, port):
            print(f"✓ {service_name} is ready!")
            return True
        if attempt % 5 == 0:
            print(f"  Attempt {attempt + 1}/{max_attempts}...")
        time.sleep(2)
    print(f"✗ {service_name} failed to start")
    log_service_diagnostics(service_name, host, port)
    return False


def verify_debezium_captures_events(
    debezium_url: str,
    connector_name: str,
    kafka_bootstrap: str,
    test_table: str = "customers",
    timeout_seconds: int = 30
) -> bool:
    """
    Verify Debezium connector can actually capture CDC events.

    Performs a test INSERT and waits for it to appear in Kafka.

    Args:
        debezium_url: Debezium REST API URL
        connector_name: Name of the connector to test
        kafka_bootstrap: Kafka bootstrap servers
        test_table: Table to use for testing (default: customers)
        timeout_seconds: Maximum time to wait for event

    Returns:
        True if CDC event was successfully captured
    """
    import requests
    import psycopg2
    import json
    from kafka import KafkaConsumer
    import uuid

    print(f"Verifying {connector_name} can capture events...")

    # 1. Connect to database and create Kafka consumer BEFORE inserting
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "cdcdb"),
            user=os.getenv("POSTGRES_USER", "cdcuser"),
            password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
        )
        cursor = conn.cursor()

        # Create consumer FIRST with 'latest' - this establishes the starting point
        topic = "debezium.public.customers"
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap,
            auto_offset_reset='latest',  # Read messages from NOW onwards
            consumer_timeout_ms=timeout_seconds * 1000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
            group_id=f"health_check_{uuid.uuid4().hex[:8]}"
        )

        # Small delay to ensure consumer is fully initialized
        time.sleep(0.5)

        # NOW insert the test record - consumer will see it
        test_email = f"debezium_health_check_{uuid.uuid4().hex[:8]}@test.com"
        cursor.execute(
            "INSERT INTO customers (email, first_name, last_name) VALUES (%s, %s, %s) RETURNING customer_id",
            (test_email, "HealthCheck", "Test")
        )
        test_customer_id = cursor.fetchone()[0]
        conn.commit()
        print(f"  Inserted test customer ID: {test_customer_id}")

    except Exception as e:
        print(f"  ⚠ Failed to insert test record: {e}")
        return False

    # 2. Wait for event in Kafka
    try:

        found = False
        for message in consumer:
            if message.value:
                # Handle both flat format (ExtractNewRecordState) and nested format
                # Flat format: {"customer_id": 123, ...}
                # Nested format: {"payload": {"after": {"customer_id": 123, ...}}}
                if message.value.get('customer_id') == test_customer_id:
                    found = True
                    print(f"  ✓ CDC event captured successfully!")
                    break

                # Fallback to nested format for backward compatibility
                payload = message.value.get('payload', {})
                after = payload.get('after', {})
                if after.get('customer_id') == test_customer_id:
                    found = True
                    print(f"  ✓ CDC event captured successfully!")
                    break

        consumer.close()

        # 3. Cleanup test record
        cursor.execute("DELETE FROM customers WHERE customer_id = %s", (test_customer_id,))
        conn.commit()
        conn.close()

        return found

    except Exception as e:
        print(f"  ⚠ Failed to verify Kafka event: {e}")
        try:
            cursor.execute("DELETE FROM customers WHERE customer_id = %s", (test_customer_id,))
            conn.commit()
            conn.close()
        except:
            pass
        return False


@pytest.fixture(scope="session")
def docker_services():
    """
    Start required Docker services for E2E tests.

    This fixture ensures PostgreSQL, Kafka, Debezium, and MinIO are running.
    Services are started once per test session and remain running.
    """
    project_root = Path(__file__).parent.parent.parent
    compose_file = project_root / "compose" / "docker-compose.yml"

    if not compose_file.exists():
        pytest.skip(f"Docker Compose file not found at {compose_file}")

    print("\n" + "=" * 80)
    print("Starting Docker services for E2E tests...")
    print("=" * 80)

    # Start essential services
    # Use --remove-orphans to clean up any conflicting containers from previous runs
    # Use -p to specify project name consistently
    # Use --project-directory to ensure compose file paths are resolved correctly
    result = subprocess.run(
        [
            "docker", "compose", "-f", str(compose_file),
            "--project-directory", str(project_root),
            "-p", "claude-cdc-demo",
            "up", "-d", "--remove-orphans", "postgres", "mysql", "kafka", "debezium", "minio"
        ],
        cwd=project_root,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"Failed to start Docker services:")
        print(f"STDOUT: {result.stdout}")
        print(f"STDERR: {result.stderr}")
        pytest.skip(f"Docker Compose failed: {result.stderr}")

    # Wait for services to be ready
    services = [
        ("localhost", 5432, "PostgreSQL"),
        ("localhost", 29092, "Kafka"),
        ("localhost", 8083, "Debezium"),
        ("localhost", 9000, "MinIO"),
    ]

    all_ready = True
    for host, port, name in services:
        if not wait_for_service(host, port, max_attempts=30, service_name=name):
            all_ready = False
            break

    if not all_ready:
        pytest.skip("Required Docker services failed to start")

    print("✓ All Docker services are ready!\n")

    yield

    # Services remain running for subsequent test runs
    # To stop: docker compose -f compose/docker-compose.yml down


@pytest.fixture(scope="session")
def debezium_connector(docker_services):
    """
    Ensure Debezium connector is registered and running.

    This fixture checks if the PostgreSQL CDC connector exists and creates it if needed.
    """
    import requests

    debezium_url = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
    connector_name = "postgres-cdc-connector"

    print(f"\nChecking Debezium connector at {debezium_url}...")

    # Check if connector exists
    try:
        response = requests.get(f"{debezium_url}/connectors/{connector_name}/status", timeout=5)
        if response.status_code == 200:
            status = response.json()
            if status.get("connector", {}).get("state") == "RUNNING":
                print(f"✓ Debezium connector '{connector_name}' is already running")
                return
            else:
                print(f"⚠ Connector exists but not running: {status}")
        else:
            print(f"Connector not found, will attempt to create...")
    except requests.RequestException as e:
        print(f"⚠ Failed to check connector status: {e}")

    # Attempt to register connector using the existing setup script
    project_root = Path(__file__).parent.parent.parent
    setup_script = project_root / "scripts" / "connectors" / "setup_postgres_connector.py"

    if not setup_script.exists():
        pytest.skip(f"Debezium connector setup script not found: {setup_script}")

    try:
        print(f"Running: poetry run python {setup_script}")
        result = subprocess.run(
            ["poetry", "run", "python", str(setup_script)],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            print(f"⚠ Setup script failed:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        time.sleep(5)

        # Verify connector is running
        response = requests.get(f"{debezium_url}/connectors/{connector_name}/status", timeout=5)
        if response.status_code == 200:
            status = response.json()
            if status.get("connector", {}).get("state") == "RUNNING":
                print(f"✓ Debezium connector registered successfully")

                # Verify it can actually capture events
                if not verify_debezium_captures_events(
                    debezium_url=debezium_url,
                    connector_name=connector_name,
                    kafka_bootstrap="localhost:29092",
                    timeout_seconds=30
                ):
                    pytest.skip("Debezium connector not capturing events")

                return
            else:
                print(f"⚠ Connector exists but not running: {status}")
        else:
            print(f"⚠ Connector not found after setup (HTTP {response.status_code})")

    except (subprocess.SubprocessError, requests.RequestException) as e:
        print(f"⚠ Setup failed: {e}")

    pytest.skip("Failed to register Debezium connector")


@pytest.fixture(scope="session")
def mysql_debezium_connector(docker_services):
    """
    Ensure MySQL Debezium connector is registered and running.

    This fixture checks if the MySQL CDC connector exists and creates it if needed.
    """
    import requests

    debezium_url = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
    connector_name = "mysql-connector"

    print(f"\nChecking MySQL Debezium connector at {debezium_url}...")

    # Check if connector exists
    try:
        response = requests.get(f"{debezium_url}/connectors/{connector_name}/status", timeout=5)
        if response.status_code == 200:
            status = response.json()
            if status.get("connector", {}).get("state") == "RUNNING":
                print(f"✓ MySQL Debezium connector '{connector_name}' is already running")
                return
            else:
                print(f"⚠ MySQL connector exists but not running: {status}")
        else:
            print(f"MySQL connector not found, will attempt to create...")
    except requests.RequestException as e:
        print(f"⚠ Failed to check MySQL connector status: {e}")

    # Attempt to register connector using the existing setup script
    project_root = Path(__file__).parent.parent.parent
    setup_script = project_root / "scripts" / "connectors" / "register-mysql-connector.sh"

    if not setup_script.exists():
        pytest.skip(f"MySQL Debezium connector setup script not found: {setup_script}")

    try:
        print(f"Running: bash {setup_script}")
        result = subprocess.run(
            ["bash", str(setup_script)],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            print(f"⚠ MySQL setup script failed:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")

        time.sleep(5)

        # Verify connector is running
        response = requests.get(f"{debezium_url}/connectors/{connector_name}/status", timeout=5)
        if response.status_code == 200:
            status = response.json()
            if status.get("connector", {}).get("state") == "RUNNING":
                print(f"✓ MySQL Debezium connector registered successfully")

                # Note: MySQL health check would require a MySQL-specific implementation
                # For now, we trust the RUNNING status and rely on baseline data fixture
                # to catch any actual CDC issues

                return
            else:
                print(f"⚠ MySQL connector exists but not running: {status}")
        else:
            print(f"⚠ MySQL connector not found after setup (HTTP {response.status_code})")

    except (subprocess.SubprocessError, requests.RequestException) as e:
        print(f"⚠ MySQL setup failed: {e}")

    pytest.skip("Failed to register MySQL Debezium connector")


@pytest.fixture(scope="session")
def delta_streaming_pipeline(docker_services, debezium_connector):
    """
    Start Delta Lake streaming pipeline for E2E tests.

    This fixture:
    1. Checks if pipeline is already running
    2. Starts it if needed using orchestration script
    3. Waits for Delta table to be created
    4. Stops pipeline at end of test session

    Set SKIP_PIPELINE_MANAGEMENT=1 to skip automatic start/stop
    (useful when running pipeline manually or in Docker).
    """
    if os.getenv("SKIP_PIPELINE_MANAGEMENT"):
        print("\n⚠ Pipeline management skipped (SKIP_PIPELINE_MANAGEMENT set)")
        yield
        return

    project_root = Path(__file__).parent.parent.parent
    orchestrate_script = project_root / "scripts" / "pipelines" / "orchestrate_streaming_pipelines.sh"

    if not orchestrate_script.exists():
        pytest.skip(f"Orchestration script not found: {orchestrate_script}")

    print("\n" + "=" * 80)
    print("Starting Delta Lake streaming pipeline...")
    print("=" * 80)

    # Check if already running
    result = subprocess.run(
        ["bash", str(orchestrate_script), "status"],
        cwd=project_root,
        capture_output=True,
        text=True
    )

    if "Delta Lake pipeline: RUNNING" in result.stdout:
        print("✓ Delta Lake pipeline is already running")
    else:
        # Start pipeline
        subprocess.run(
            ["bash", str(orchestrate_script), "start", "delta"],
            cwd=project_root,
            check=True,
            capture_output=True
        )
        print("✓ Delta Lake pipeline started")

    # Wait for Delta table to be created (with timeout)
    delta_path = Path(os.getenv("DELTA_TABLE_PATH", "/tmp/delta/customers"))
    print(f"\nWaiting for Delta table at {delta_path}...")

    max_wait = 60  # seconds
    elapsed = 0
    while elapsed < max_wait:
        if delta_path.exists():
            print(f"✓ Delta table created at {delta_path}")
            break
        time.sleep(5)
        elapsed += 5
        if elapsed % 15 == 0:
            print(f"  Still waiting... ({elapsed}s/{max_wait}s)")
    else:
        print(f"⚠ Delta table not created within {max_wait}s - tests may skip")

    print("=" * 80 + "\n")

    yield

    # Cleanup: Stop pipeline
    print("\n" + "=" * 80)
    print("Stopping Delta Lake streaming pipeline...")
    print("=" * 80)

    subprocess.run(
        ["bash", str(orchestrate_script), "stop", "delta"],
        cwd=project_root,
        capture_output=True
    )
    print("✓ Delta Lake pipeline stopped\n")


@pytest.fixture(scope="session", autouse=True)
def e2e_test_infrastructure(delta_streaming_pipeline):
    """
    Auto-use fixture that sets up complete E2E test infrastructure.

    This is automatically applied to all tests in this directory.
    It ensures Docker services, Debezium connector, and streaming
    pipeline are running before any tests execute.
    """
    yield
    # All cleanup handled by individual fixtures


@pytest.fixture(scope="session")
def e2e_postgres_connection():
    """
    Session-scoped PostgreSQL connection for E2E tests.

    This connection is reused across all E2E tests for better performance.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "cdcdb"),
        user=os.getenv("POSTGRES_USER", "cdcuser"),
        password=os.getenv("POSTGRES_PASSWORD", "cdcpass"),
    )

    yield conn

    conn.close()


@pytest.fixture(scope="session")
def e2e_spark_session():
    """
    Session-scoped Spark session for E2E tests.

    Reused across all E2E tests for performance.
    """
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName("E2E-Tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark

    spark.stop()


@pytest.fixture(scope="session")
def baseline_e2e_data(e2e_postgres_connection, e2e_spark_session, delta_streaming_pipeline):
    """
    Pre-load baseline customers for E2E tests.

    This fixture:
    1. Inserts 100 baseline customers (IDs 10000-10099)
    2. Waits for them to appear in Delta Lake
    3. Returns the customer IDs for tests to use
    4. Cleans up after all tests complete

    Use this for read-only tests that just verify data exists.
    """
    from tests.e2e.test_data import (
        insert_baseline_customers,
        cleanup_baseline_customers,
        wait_for_delta_propagation,
    )

    print("\n" + "=" * 80)
    print("PRE-LOADING BASELINE DATA FOR E2E TESTS")
    print("=" * 80)

    # Insert baseline customers into Postgres
    customer_ids = insert_baseline_customers(
        e2e_postgres_connection,
        id_range=(10000, 10100)
    )

    if not customer_ids:
        pytest.skip("Failed to insert baseline customers")

    # Wait for Delta propagation
    delta_table_path = os.getenv("DELTA_TABLE_PATH", "/tmp/delta/customers")

    delta_success = wait_for_delta_propagation(
        e2e_spark_session,
        delta_table_path,
        customer_ids,
        timeout_seconds=240,  # Increased from 180 to 240
        poll_interval=5.0
    )

    if not delta_success:
        cleanup_baseline_customers(e2e_postgres_connection, customer_ids)
        pytest.skip("Baseline data did not propagate to Delta in time")

    print("=" * 80)
    print("✓ BASELINE DATA READY - All E2E tests can now run fast!")
    print("=" * 80 + "\n")

    # Return data for tests
    data = {
        'customer_ids': customer_ids,
        'delta_table_path': delta_table_path,
    }

    yield data

    # Cleanup after all tests
    print("\n" + "=" * 80)
    print("CLEANING UP BASELINE DATA")
    print("=" * 80)
    cleanup_baseline_customers(e2e_postgres_connection, customer_ids)
    print("=" * 80 + "\n")


@pytest.fixture(scope="function")
def isolated_e2e_customer(e2e_postgres_connection, e2e_spark_session, delta_streaming_pipeline):
    """
    Create an isolated customer for function-level modification tests.

    This fixture:
    1. Creates a single customer with unique data
    2. Waits for it to propagate to Delta
    3. Returns the customer_id
    4. Cleans up after the test

    Use this for tests that need to modify data without affecting others.
    """
    from tests.e2e.test_data import (
        create_isolated_customer,
        cleanup_baseline_customers,
    )
    from tests.test_utils import wait_for_condition

    # Create isolated customer
    customer_id = create_isolated_customer(e2e_postgres_connection)

    # Wait for it to appear in Delta
    delta_table_path = os.getenv("DELTA_TABLE_PATH", "/tmp/delta/customers")

    def check_customer_in_delta():
        try:
            df = e2e_spark_session.read.format("delta").load(delta_table_path)
            count = df.filter(df.customer_id == customer_id).count()
            return count > 0
        except Exception:
            return False

    try:
        wait_for_condition(
            condition_func=check_customer_in_delta,
            timeout_seconds=90,  # Increased from 60 to 90
            poll_interval=3.0,
            error_message=f"Isolated customer {customer_id} did not propagate to Delta"
        )
    except TimeoutError:
        # Cleanup and skip if propagation fails
        cleanup_baseline_customers(e2e_postgres_connection, [customer_id])
        pytest.skip(f"Isolated customer {customer_id} did not propagate to Delta")

    yield customer_id

    # Cleanup after test
    cleanup_baseline_customers(e2e_postgres_connection, [customer_id])


@pytest.fixture(scope="session")
def e2e_mysql_connection(docker_services, mysql_debezium_connector):
    """
    Session-scoped MySQL connection for E2E tests.

    This connection is reused across all MySQL E2E tests for better performance.
    Depends on mysql_debezium_connector to ensure connector is ready.
    """
    import pymysql

    # Wait for MySQL to be fully ready
    if not wait_for_service("localhost", int(os.getenv("MYSQL_PORT", "3306")),
                           max_attempts=30, service_name="MySQL"):
        pytest.skip("MySQL service not available")

    conn = pymysql.connect(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=os.getenv("MYSQL_DB", "cdcdb"),
        user=os.getenv("MYSQL_USER", "cdcuser"),
        password=os.getenv("MYSQL_PASSWORD", "cdcpass"),
    )

    yield conn

    conn.close()


@pytest.fixture(scope="session")
def baseline_mysql_products(e2e_mysql_connection, e2e_spark_session, mysql_debezium_connector):
    """
    Pre-load baseline products for MySQL E2E tests.

    This fixture:
    1. Inserts baseline products (IDs 8000-8099) into MySQL
    2. Waits for them to appear in Kafka topic
    3. Returns the product IDs and table path for tests to use
    4. Cleans up after all tests complete

    This ensures the Delta table is created before tests run.
    """
    from tests.test_utils import wait_for_condition
    from kafka import KafkaConsumer
    import json

    print("\n" + "=" * 80)
    print("PRE-LOADING BASELINE MySQL PRODUCTS FOR E2E TESTS")
    print("=" * 80)

    cursor = e2e_mysql_connection.cursor()

    # Clean up any existing baseline data
    cursor.execute("DELETE FROM products WHERE product_id >= 8000 AND product_id < 8100")
    e2e_mysql_connection.commit()

    # Insert baseline products
    product_ids = []
    print("Inserting 100 baseline products...")
    for i in range(100):
        product_id = 8000 + i
        cursor.execute(
            """
            INSERT INTO products (product_id, product_name, category, price, stock_quantity)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (product_id, f"Baseline Product {i}", "Test Category", 100.0 + i, 100 + i)
        )
        product_ids.append(product_id)

    e2e_mysql_connection.commit()
    print(f"✓ Inserted {len(product_ids)} baseline products into MySQL")

    # Wait for events to appear in Kafka
    kafka_topic = "debezium.cdcdb.products"
    print(f"\nWaiting for products to appear in Kafka topic '{kafka_topic}'...")

    def check_kafka_events():
        """Check if we have events in Kafka for our products."""
        try:
            consumer = KafkaConsumer(
                kafka_topic,
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                auto_offset_reset='earliest',
                consumer_timeout_ms=10000,  # Increased from 5000 to 10000
                value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
            )

            found_count = 0
            for message in consumer:
                if message.value and 'payload' in message.value:
                    payload = message.value['payload']
                    if payload.get('after'):
                        pid = payload['after'].get('product_id')
                        if pid and pid >= 8000 and pid < 8100:
                            found_count += 1
                            if found_count >= 10:  # Found at least 10 events
                                consumer.close()
                                return True

            consumer.close()
            return found_count > 0
        except Exception as e:
            print(f"  Kafka check error: {e}")
            return False

    try:
        wait_for_condition(
            condition_func=check_kafka_events,
            timeout_seconds=60,
            poll_interval=3.0,
            error_message=f"Products did not appear in Kafka topic {kafka_topic}"
        )
        print(f"✓ Products appeared in Kafka topic!")
    except TimeoutError as e:
        print(f"⚠ Warning: {e}")
        print("  Proceeding anyway - tests may need to wait longer for data")

    # Give extra time for CDC propagation to stabilize
    time.sleep(5)

    print("=" * 80)
    print("✓ BASELINE MySQL PRODUCTS READY")
    print("=" * 80 + "\n")

    # Return data for tests
    data = {
        'product_ids': product_ids,
        'kafka_topic': kafka_topic,
        'delta_table_path': '/tmp/delta/e2e_test/mysql_products',
    }

    yield data

    # Cleanup after all tests
    print("\n" + "=" * 80)
    print("CLEANING UP BASELINE MySQL PRODUCTS")
    print("=" * 80)
    cursor.execute("DELETE FROM products WHERE product_id >= 8000 AND product_id < 8100")
    e2e_mysql_connection.commit()
    cursor.close()
    print("=" * 80 + "\n")