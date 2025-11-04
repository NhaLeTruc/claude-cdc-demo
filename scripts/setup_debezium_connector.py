#!/usr/bin/env python3
"""
Setup Debezium PostgreSQL CDC connector.

This script registers a Debezium connector with Kafka Connect to capture
change data from PostgreSQL.
"""

import json
import os
import sys
import time
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def wait_for_debezium(url: str, max_retries: int = 30, delay: int = 2) -> bool:
    """Wait for Debezium to be ready."""
    for i in range(max_retries):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"✓ Debezium is ready at {url}")
                return True
        except requests.exceptions.RequestException:
            pass

        if i < max_retries - 1:
            print(f"Waiting for Debezium... ({i+1}/{max_retries})")
            time.sleep(delay)

    return False


def check_connector_exists(base_url: str, connector_name: str) -> bool:
    """Check if connector already exists."""
    try:
        response = requests.get(f"{base_url}/connectors/{connector_name}", timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def delete_connector(base_url: str, connector_name: str) -> bool:
    """Delete existing connector."""
    try:
        response = requests.delete(f"{base_url}/connectors/{connector_name}", timeout=5)
        if response.status_code in [200, 204]:
            print(f"✓ Deleted existing connector: {connector_name}")
            time.sleep(2)  # Wait for deletion to complete
            return True
        return False
    except requests.exceptions.RequestException as e:
        print(f"✗ Error deleting connector: {e}")
        return False


def create_postgres_connector(
    base_url: str,
    connector_name: str = "postgres-cdc-connector",
    database_hostname: str = None,
    database_port: int = None,
    database_user: str = None,
    database_password: str = None,
    database_dbname: str = None,
    topic_prefix: str = "postgres",
) -> bool:
    """Create Debezium PostgreSQL connector."""

    # Get configuration from environment or use defaults
    hostname = database_hostname or os.getenv("POSTGRES_HOST", "cdc-postgres")
    port = database_port or int(os.getenv("POSTGRES_PORT", "5432"))
    user = database_user or os.getenv("POSTGRES_USER", "cdcuser")
    password = database_password or os.getenv("POSTGRES_PASSWORD", "cdcpass")
    dbname = database_dbname or os.getenv("POSTGRES_DB", "cdcdb")

    connector_config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": hostname,
            "database.port": str(port),
            "database.user": user,
            "database.password": password,
            "database.dbname": dbname,
            "database.server.name": topic_prefix,
            "topic.prefix": topic_prefix,
            "plugin.name": "pgoutput",
            "publication.autocreate.mode": "filtered",
            "table.include.list": "public.customers,public.orders,public.products,public.inventory",
            "slot.name": f"{topic_prefix}_debezium_slot",
            "slot.drop.on.stop": "false",
            "heartbeat.interval.ms": "10000",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.add.fields": "op,db,table,ts_ms,lsn",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "decimal.handling.mode": "double",
            "time.precision.mode": "adaptive_time_microseconds",
            "include.schema.changes": "false",
        }
    }

    try:
        response = requests.post(
            f"{base_url}/connectors",
            json=connector_config,
            headers={"Content-Type": "application/json"},
            timeout=10
        )

        if response.status_code in [200, 201]:
            print(f"✓ Created Debezium connector: {connector_name}")
            print(f"  - Database: {hostname}:{port}/{dbname}")
            print(f"  - Topic prefix: {topic_prefix}")
            print(f"  - Tables: public.customers, public.orders, public.products, public.inventory")
            return True
        else:
            print(f"✗ Failed to create connector: {response.status_code}")
            print(f"  Response: {response.text}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"✗ Error creating connector: {e}")
        return False


def verify_connector_status(base_url: str, connector_name: str, max_retries: int = 10) -> bool:
    """Verify connector is running."""
    for i in range(max_retries):
        try:
            response = requests.get(f"{base_url}/connectors/{connector_name}/status", timeout=5)
            if response.status_code == 200:
                status = response.json()
                connector_state = status.get("connector", {}).get("state")

                if connector_state == "RUNNING":
                    # Check task status
                    tasks = status.get("tasks", [])
                    if tasks and all(t.get("state") == "RUNNING" for t in tasks):
                        print(f"✓ Connector is RUNNING with {len(tasks)} task(s)")
                        return True
                    else:
                        print(f"  Connector state: {connector_state}, waiting for tasks...")
                else:
                    print(f"  Connector state: {connector_state}, waiting...")
        except requests.exceptions.RequestException as e:
            print(f"  Error checking status: {e}")

        if i < max_retries - 1:
            time.sleep(2)

    print(f"✗ Connector failed to reach RUNNING state")
    return False


def main():
    """Main entry point."""
    debezium_url = os.getenv("DEBEZIUM_URL", "http://localhost:8083")
    connector_name = "postgres-cdc-connector"

    print("=" * 60)
    print("Debezium PostgreSQL CDC Connector Setup")
    print("=" * 60)

    # Wait for Debezium to be ready
    print("\n1. Checking Debezium availability...")
    if not wait_for_debezium(debezium_url):
        print("✗ Debezium is not available")
        sys.exit(1)

    # Check if connector already exists
    print(f"\n2. Checking for existing connector '{connector_name}'...")
    if check_connector_exists(debezium_url, connector_name):
        print(f"  Connector '{connector_name}' already exists")
        print("  Deleting existing connector...")
        if not delete_connector(debezium_url, connector_name):
            print("✗ Failed to delete existing connector")
            sys.exit(1)

    # Create connector
    print(f"\n3. Creating connector '{connector_name}'...")
    if not create_postgres_connector(debezium_url, connector_name):
        print("✗ Failed to create connector")
        sys.exit(1)

    # Verify connector status
    print(f"\n4. Verifying connector status...")
    if not verify_connector_status(debezium_url, connector_name):
        print("✗ Connector is not running properly")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✓ Debezium connector setup completed successfully!")
    print("=" * 60)
    print(f"\nConnector Name: {connector_name}")
    print(f"Topics will be created with prefix: postgres")
    print(f"Example topics:")
    print(f"  - postgres.public.customers")
    print(f"  - postgres.public.orders")
    print(f"  - postgres.public.products")
    print(f"  - postgres.public.inventory")
    print("\nYou can check connector status with:")
    print(f"  curl {debezium_url}/connectors/{connector_name}/status")


if __name__ == "__main__":
    main()
