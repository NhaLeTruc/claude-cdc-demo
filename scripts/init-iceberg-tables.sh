#!/bin/bash
# Initialize Apache Iceberg tables

set -e

echo "Initializing Apache Iceberg tables..."

python3 <<EOF
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
)

# Load catalog (using local filesystem for demo)
catalog = load_catalog("demo", **{
    "type": "rest",
    "uri": "http://localhost:8181",
    "warehouse": "./iceberg-warehouse"
})

# Create customers Iceberg table
customers_schema = Schema(
    NestedField(1, "customer_id", IntegerType(), required=True),
    NestedField(2, "first_name", StringType(), required=False),
    NestedField(3, "last_name", StringType(), required=False),
    NestedField(4, "email", StringType(), required=False),
    NestedField(5, "created_at", TimestampType(), required=False)
)

try:
    catalog.create_table(
        "demo.customers",
        schema=customers_schema
    )
    print("✓ Created Iceberg table: customers")
except Exception as e:
    print(f"Note: customers table may already exist ({e})")

# Create products Iceberg table
products_schema = Schema(
    NestedField(1, "product_id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "description", StringType(), required=False),
    NestedField(4, "price", DecimalType(10, 2), required=False),
    NestedField(5, "created_at", TimestampType(), required=False)
)

try:
    catalog.create_table(
        "demo.products",
        schema=products_schema
    )
    print("✓ Created Iceberg table: products")
except Exception as e:
    print(f"Note: products table may already exist ({e})")

print("\nIceberg tables initialized successfully!")
EOF

echo "Iceberg initialization complete!"
