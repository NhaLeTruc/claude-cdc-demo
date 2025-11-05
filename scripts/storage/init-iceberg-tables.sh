#!/bin/bash
# Initialize Apache Iceberg tables

set -e

echo "Initializing Apache Iceberg tables..."

python3 <<EOF
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
)

# Load REST catalog
catalog = RestCatalog(
    "demo_catalog",
    **{
        "uri": "http://iceberg-rest:8181",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
        "warehouse": "s3://warehouse/iceberg"
    }
)

# Create cdc namespace for test tables
try:
    catalog.create_namespace("cdc")
    print("✓ Created namespace: cdc")
except Exception as e:
    print(f"Note: cdc namespace may already exist ({e})")

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
    print("✓ Created Iceberg table: demo.customers")
except Exception as e:
    print(f"Note: demo.customers table may already exist ({e})")

# Create customers table in cdc namespace (for tests)
try:
    catalog.create_table(
        "cdc.customers",
        schema=customers_schema
    )
    print("✓ Created Iceberg table: cdc.customers")
except Exception as e:
    print(f"Note: cdc.customers table may already exist ({e})")

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
