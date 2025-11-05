#!/bin/bash
# Demo script for DeltaLake Change Data Feed (CDF)
#
# This script demonstrates DeltaLake's native CDC capabilities using
# Change Data Feed feature to track row-level changes.

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Configuration
DELTA_TABLE_PATH="${DELTA_TABLE_PATH:-/tmp/delta-cdc-demo/customers}"
DEMO_RECORDS="${DEMO_RECORDS:-100}"

echo -e "${BLUE}=======================================${NC}"
echo -e "${BLUE}DeltaLake Change Data Feed (CDF) Demo${NC}"
echo -e "${BLUE}=======================================${NC}"
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

# Step 1: Initialize Delta table with CDF enabled
print_step "Step 1: Creating Delta table with CDF enabled"
python3 << EOF
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from datetime import datetime

# Create Spark session
spark = (
    SparkSession.builder
    .appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)

# Create initial data
data = [
    (1, "john.doe@example.com", "John", "Doe", "Gold", 1500.00),
    (2, "jane.smith@example.com", "Jane", "Smith", "Silver", 800.00),
    (3, "bob.johnson@example.com", "Bob", "Johnson", "Bronze", 250.00),
]

df = spark.createDataFrame(
    data,
    ["customer_id", "email", "first_name", "last_name", "tier", "lifetime_value"]
)

# Write with CDF enabled
print("Creating Delta table with CDF enabled...")
(
    df.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .option("delta.columnMapping.mode", "name")
    .mode("overwrite")
    .save("${DELTA_TABLE_PATH}")
)

print(f"✓ Created Delta table at ${DELTA_TABLE_PATH} (version 0)")
print(f"✓ CDF enabled: {DeltaTable.forPath(spark, '${DELTA_TABLE_PATH}').detail().select('properties').first()['properties'].get('delta.enableChangeDataFeed')}")
spark.stop()
EOF

sleep 1

# Step 2: Insert new records
print_step "Step 2: Inserting new customers (version 1)"
python3 << EOF
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)

# Insert new customers
new_data = [
    (4, "alice.williams@example.com", "Alice", "Williams", "Platinum", 5000.00),
    (5, "charlie.brown@example.com", "Charlie", "Brown", "Gold", 2000.00),
]

df = spark.createDataFrame(
    new_data,
    ["customer_id", "email", "first_name", "last_name", "tier", "lifetime_value"]
)

df.write.format("delta").mode("append").save("${DELTA_TABLE_PATH}")
print("✓ Inserted 2 new customers (INSERT operations)")

# Show current data
print("\nCurrent table contents:")
spark.read.format("delta").load("${DELTA_TABLE_PATH}").show()

spark.stop()
EOF

sleep 1

# Step 3: Update existing records
print_step "Step 3: Updating customer tiers (version 2)"
python3 << EOF
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)

# Update customer tier
delta_table = DeltaTable.forPath(spark, "${DELTA_TABLE_PATH}")
delta_table.update(
    condition="customer_id = 2",
    set={"tier": "'Gold'", "lifetime_value": "1200.00"}
)

print("✓ Updated customer 2: tier=Gold, lifetime_value=1200.00 (UPDATE operation)")

spark.stop()
EOF

sleep 1

# Step 4: Delete a record
print_step "Step 4: Deleting a customer (version 3)"
python3 << EOF
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)

# Delete customer
delta_table = DeltaTable.forPath(spark, "${DELTA_TABLE_PATH}")
delta_table.delete("customer_id = 3")

print("✓ Deleted customer 3 (DELETE operation)")

spark.stop()
EOF

sleep 1

# Step 5: Read Change Data Feed
print_step "Step 5: Reading Change Data Feed (versions 0 → 3)"
python3 << EOF
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)

# Read CDF from version 1 to latest
print("\n" + "="*80)
print("CHANGE DATA FEED (versions 1 → 3):")
print("="*80)

changes = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .load("${DELTA_TABLE_PATH}")
)

# Show all changes
changes.select(
    "_change_type",
    "_commit_version",
    "customer_id",
    "email",
    "tier",
    "lifetime_value"
).orderBy("_commit_version", "_change_type").show(truncate=False)

# Summary by change type
print("\nCHANGE SUMMARY:")
print("-" * 40)
summary = changes.groupBy("_change_type").count().collect()
for row in summary:
    print(f"  {row['_change_type']:20s}: {row['count']:3d} records")

spark.stop()
EOF

# Step 6: Query specific change types
print_step "Step 6: Querying specific change types"
python3 << EOF
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)

changes = (
    spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .load("${DELTA_TABLE_PATH}")
)

# Get only INSERT operations
print("\nINSERT operations:")
print("-" * 40)
inserts = changes.filter(col("_change_type") == "insert")
inserts.select("customer_id", "email", "tier").show()

# Get only UPDATE operations
print("\nUPDATE operations (preimage + postimage):")
print("-" * 40)
updates = changes.filter(col("_change_type").isin(["update_preimage", "update_postimage"]))
updates.select("_change_type", "customer_id", "tier", "lifetime_value").show()

# Get only DELETE operations
print("\nDELETE operations:")
print("-" * 40)
deletes = changes.filter(col("_change_type") == "delete")
deletes.select("customer_id", "email").show()

spark.stop()
EOF

# Step 7: Show table version history
print_step "Step 7: Viewing table version history"
python3 << EOF
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

spark = (
    SparkSession.builder
    .appName("DeltaCDF-Demo")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)

delta_table = DeltaTable.forPath(spark, "${DELTA_TABLE_PATH}")

print("\nTABLE VERSION HISTORY:")
print("="*80)
delta_table.history().select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

spark.stop()
EOF

# Summary
echo ""
echo -e "${BLUE}=======================================${NC}"
echo -e "${BLUE}Demo Complete!${NC}"
echo -e "${BLUE}=======================================${NC}"
echo ""
print_info "What we demonstrated:"
echo "  1. Created Delta table with CDF enabled"
echo "  2. Performed INSERT operations (new customers)"
echo "  3. Performed UPDATE operations (tier/value changes)"
echo "  4. Performed DELETE operations (customer removal)"
echo "  5. Queried CDF to retrieve all changes"
echo "  6. Filtered changes by operation type"
echo "  7. Reviewed version history"
echo ""
print_info "Key Takeaways:"
echo "  • CDF captures all row-level changes automatically"
echo "  • Changes include operation type (_change_type)"
echo "  • Updates show both before (preimage) and after (postimage)"
echo "  • Changes are queryable like regular Delta tables"
echo "  • Version history provides audit trail"
echo ""
print_info "Delta table location: ${DELTA_TABLE_PATH}"
echo ""

# Optional: Cleanup
read -p "Do you want to cleanup the demo table? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Cleaning up demo table..."
    rm -rf "${DELTA_TABLE_PATH}"
    echo -e "${GREEN}✓ Cleanup complete${NC}"
fi
