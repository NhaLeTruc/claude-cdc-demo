#!/bin/bash
# Demo script for Apache Iceberg snapshot-based CDC
#
# This script demonstrates Iceberg's snapshot-based incremental read
# capabilities for CDC-like workflows.

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-/tmp/iceberg-cdc-demo}"
CATALOG_NAME="${CATALOG_NAME:-demo_catalog}"
NAMESPACE="${NAMESPACE:-cdc_demo}"
TABLE_NAME="${TABLE_NAME:-customers}"

echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}Apache Iceberg Snapshot-Based CDC Demo${NC}"
echo -e "${BLUE}==========================================${NC}"
echo ""

# Function to print step headers
print_step() {
    echo -e "${GREEN}>>> $1${NC}"
}

# Function to print info
print_info() {
    echo -e "${YELLOW}[INFO] $1${NC}"
}

# Function to print errors
print_error() {
    echo -e "${RED}[ERROR] $1${NC}"
}

# Check if PyIceberg is installed
print_info "Checking for PyIceberg installation..."
python3 -c "import pyiceberg" 2>/dev/null || {
    print_error "PyIceberg is not installed"
    echo "Install it with: pip install pyiceberg"
    exit 1
}

print_info "PyIceberg is installed ✓"
echo ""

# Step 1: Initialize Iceberg table
print_step "Step 1: Creating Iceberg table"
python3 << 'EOF'
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    DoubleType,
    TimestampType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import MonthTransform
import os

# Configuration
warehouse = os.getenv("ICEBERG_WAREHOUSE", "/tmp/iceberg-cdc-demo")
catalog_name = os.getenv("CATALOG_NAME", "demo_catalog")
namespace = os.getenv("NAMESPACE", "cdc_demo")
table_name = os.getenv("TABLE_NAME", "customers")

# Create catalog (filesystem-based for demo)
catalog = load_catalog(
    catalog_name,
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    }
)

# Create namespace if it doesn't exist
try:
    catalog.create_namespace(namespace)
    print(f"✓ Created namespace: {namespace}")
except Exception:
    print(f"✓ Namespace {namespace} already exists")

# Define schema
schema = Schema(
    NestedField(1, "customer_id", LongType(), required=True),
    NestedField(2, "email", StringType(), required=True),
    NestedField(3, "first_name", StringType(), required=True),
    NestedField(4, "last_name", StringType(), required=True),
    NestedField(5, "tier", StringType(), required=False),
    NestedField(6, "lifetime_value", DoubleType(), required=False),
    NestedField(7, "registration_date", TimestampType(), required=True),
)

# Create table
table_identifier = f"{namespace}.{table_name}"
try:
    table = catalog.create_table(
        identifier=table_identifier,
        schema=schema,
        properties={"format-version": "2"},
    )
    print(f"✓ Created Iceberg table: {table_identifier}")
    print(f"✓ Location: {table.location()}")
    print(f"✓ Current snapshot: {table.metadata.current_snapshot_id}")
except Exception as e:
    print(f"✓ Table {table_identifier} already exists or error: {e}")
EOF

sleep 1

# Step 2: Write initial data (Snapshot 1)
print_step "Step 2: Writing initial data (creates Snapshot 1)"
python3 << 'EOF'
from pyiceberg.catalog import load_catalog
import pyarrow as pa
from datetime import datetime
import os

warehouse = os.getenv("ICEBERG_WAREHOUSE", "/tmp/iceberg-cdc-demo")
catalog_name = os.getenv("CATALOG_NAME", "demo_catalog")
namespace = os.getenv("NAMESPACE", "cdc_demo")
table_name = os.getenv("TABLE_NAME", "customers")

catalog = load_catalog(
    catalog_name,
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    }
)

table = catalog.load_table(f"{namespace}.{table_name}")

# Create initial data
data = pa.table({
    "customer_id": [1, 2, 3],
    "email": ["john@example.com", "jane@example.com", "bob@example.com"],
    "first_name": ["John", "Jane", "Bob"],
    "last_name": ["Doe", "Smith", "Johnson"],
    "tier": ["Gold", "Silver", "Bronze"],
    "lifetime_value": [1500.0, 800.0, 250.0],
    "registration_date": [datetime.now(), datetime.now(), datetime.now()],
})

# Append data
table.append(data)
print(f"✓ Inserted 3 customers (Snapshot 1)")

# Show current snapshot
table.refresh()
print(f"✓ Current snapshot ID: {table.metadata.current_snapshot_id}")
EOF

sleep 1

# Step 3: Append more data (Snapshot 2)
print_step "Step 3: Appending new customers (creates Snapshot 2)"
python3 << 'EOF'
from pyiceberg.catalog import load_catalog
import pyarrow as pa
from datetime import datetime
import os

warehouse = os.getenv("ICEBERG_WAREHOUSE", "/tmp/iceberg-cdc-demo")
catalog_name = os.getenv("CATALOG_NAME", "demo_catalog")
namespace = os.getenv("NAMESPACE", "cdc_demo")
table_name = os.getenv("TABLE_NAME", "customers")

catalog = load_catalog(
    catalog_name,
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    }
)

table = catalog.load_table(f"{namespace}.{table_name}")

# Store previous snapshot for comparison
prev_snapshot = table.metadata.current_snapshot_id

# Append new data
new_data = pa.table({
    "customer_id": [4, 5],
    "email": ["alice@example.com", "charlie@example.com"],
    "first_name": ["Alice", "Charlie"],
    "last_name": ["Williams", "Brown"],
    "tier": ["Platinum", "Gold"],
    "lifetime_value": [5000.0, 2000.0],
    "registration_date": [datetime.now(), datetime.now()],
})

table.append(new_data)
print(f"✓ Appended 2 new customers (Snapshot 2)")

# Show snapshot progression
table.refresh()
print(f"✓ Previous snapshot: {prev_snapshot}")
print(f"✓ Current snapshot: {table.metadata.current_snapshot_id}")
EOF

sleep 1

# Step 4: Query snapshot history
print_step "Step 4: Viewing snapshot history"
python3 << 'EOF'
from pyiceberg.catalog import load_catalog
import os

warehouse = os.getenv("ICEBERG_WAREHOUSE", "/tmp/iceberg-cdc-demo")
catalog_name = os.getenv("CATALOG_NAME", "demo_catalog")
namespace = os.getenv("NAMESPACE", "cdc_demo")
table_name = os.getenv("TABLE_NAME", "customers")

catalog = load_catalog(
    catalog_name,
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    }
)

table = catalog.load_table(f"{namespace}.{table_name}")

print("\n" + "="*80)
print("SNAPSHOT HISTORY:")
print("="*80)

for i, snapshot in enumerate(table.metadata.snapshots, 1):
    timestamp = datetime.fromtimestamp(snapshot.timestamp_ms / 1000)
    summary = snapshot.summary.additional_properties if snapshot.summary else {}
    operation = summary.get("operation", "unknown")

    print(f"\nSnapshot {i}:")
    print(f"  ID: {snapshot.snapshot_id}")
    print(f"  Parent: {snapshot.parent_snapshot_id}")
    print(f"  Timestamp: {timestamp}")
    print(f"  Operation: {operation}")
    print(f"  Summary: {summary}")

from datetime import datetime
EOF

sleep 1

# Step 5: Read specific snapshot
print_step "Step 5: Reading data from specific snapshots"
python3 << 'EOF'
from pyiceberg.catalog import load_catalog
import os

warehouse = os.getenv("ICEBERG_WAREHOUSE", "/tmp/iceberg-cdc-demo")
catalog_name = os.getenv("CATALOG_NAME", "demo_catalog")
namespace = os.getenv("NAMESPACE", "cdc_demo")
table_name = os.getenv("TABLE_NAME", "customers")

catalog = load_catalog(
    catalog_name,
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    }
)

table = catalog.load_table(f"{namespace}.{table_name}")

snapshots = list(table.metadata.snapshots)

print("\n" + "="*80)
print("SNAPSHOT COMPARISON:")
print("="*80)

# Read first snapshot
if len(snapshots) >= 1:
    snapshot1 = snapshots[0]
    data1 = table.scan(snapshot_id=snapshot1.snapshot_id).to_arrow()
    print(f"\nSnapshot 1 (ID: {snapshot1.snapshot_id}):")
    print(f"  Rows: {data1.num_rows}")
    print(f"  Columns: {data1.column_names}")
    print("\nData:")
    print(data1.to_pandas())

# Read latest snapshot
if len(snapshots) >= 2:
    snapshot2 = snapshots[-1]
    data2 = table.scan(snapshot_id=snapshot2.snapshot_id).to_arrow()
    print(f"\nSnapshot 2 (ID: {snapshot2.snapshot_id}):")
    print(f"  Rows: {data2.num_rows}")
    print(f"  Rows added: {data2.num_rows - data1.num_rows}")
EOF

# Step 6: Demonstrate incremental read
print_step "Step 6: Incremental read (simulated)"
python3 << 'EOF'
from pyiceberg.catalog import load_catalog
import os

warehouse = os.getenv("ICEBERG_WAREHOUSE", "/tmp/iceberg-cdc-demo")
catalog_name = os.getenv("CATALOG_NAME", "demo_catalog")
namespace = os.getenv("NAMESPACE", "cdc_demo")
table_name = os.getenv("TABLE_NAME", "customers")

catalog = load_catalog(
    catalog_name,
    **{
        "type": "sql",
        "uri": f"sqlite:///{warehouse}/catalog.db",
        "warehouse": f"file://{warehouse}",
    }
)

table = catalog.load_table(f"{namespace}.{table_name}")

print("\n" + "="*80)
print("INCREMENTAL READ (Snapshot-to-Snapshot):")
print("="*80)

snapshots = list(table.metadata.snapshots)

if len(snapshots) >= 2:
    # Read all snapshots and compare
    for i in range(len(snapshots) - 1):
        snap_from = snapshots[i]
        snap_to = snapshots[i + 1]

        data_from = table.scan(snapshot_id=snap_from.snapshot_id).to_arrow()
        data_to = table.scan(snapshot_id=snap_to.snapshot_id).to_arrow()

        added_rows = data_to.num_rows - data_from.num_rows

        print(f"\nIncremental: Snapshot {snap_from.snapshot_id} → {snap_to.snapshot_id}")
        print(f"  Rows added: {added_rows}")
        print(f"  Total rows: {data_from.num_rows} → {data_to.num_rows}")

        # Note: Full incremental read implementation would:
        # - Use manifest files to identify added/deleted data files
        # - Read only the changed data files
        # - This demo shows the concept with full snapshot reads
else:
    print("Not enough snapshots for incremental read demo")
EOF

# Summary
echo ""
echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}Demo Complete!${NC}"
echo -e "${BLUE}==========================================${NC}"
echo ""
print_info "What we demonstrated:"
echo "  1. Created Iceberg table with schema"
echo "  2. Wrote initial data (created Snapshot 1)"
echo "  3. Appended more data (created Snapshot 2)"
echo "  4. Queried snapshot history"
echo "  5. Read data from specific snapshots"
echo "  6. Demonstrated incremental read concept"
echo ""
print_info "Key Takeaways:"
echo "  • Iceberg tracks changes through snapshots"
echo "  • Each write creates a new snapshot"
echo "  • Snapshots are immutable and versioned"
echo "  • Incremental reads use snapshot metadata"
echo "  • Time travel: query any historical snapshot"
echo "  • CDC-like workflows without external tools"
echo ""
print_info "Warehouse location: ${ICEBERG_WAREHOUSE}"
echo ""

# Optional: Cleanup
read -p "Do you want to cleanup the demo warehouse? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Cleaning up demo warehouse..."
    rm -rf "${ICEBERG_WAREHOUSE}"
    echo -e "${GREEN}✓ Cleanup complete${NC}"
fi
