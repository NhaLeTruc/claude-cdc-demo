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
            "up", "-d", "--remove-orphans", "postgres", "kafka", "debezium", "minio"
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
                return
            else:
                print(f"⚠ Connector exists but not running: {status}")
        else:
            print(f"⚠ Connector not found after setup (HTTP {response.status_code})")

    except (subprocess.SubprocessError, requests.RequestException) as e:
        print(f"⚠ Setup failed: {e}")

    pytest.skip("Failed to register Debezium connector")


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