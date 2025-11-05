#!/bin/bash
# Initialize DeltaLake tables with Change Data Feed enabled

set -e

echo "Initializing DeltaLake tables with CDF enabled..."

# This script would typically use PySpark to create Delta tables
# For now, we'll create a placeholder that can be executed via Python

python3 <<EOF
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

# Initialize Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("DeltaLake CDC Init") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true") \
    .getOrCreate()

# Create customers Delta table
customers_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

customers_df = spark.createDataFrame([], customers_schema)
customers_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .save("./delta-lake/customers")

print("✓ Created Delta table: customers (with CDF enabled)")

# Create orders Delta table
orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("total_amount", DecimalType(10, 2), True),
    StructField("status", StringType(), True)
])

orders_df = spark.createDataFrame([], orders_schema)
orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .save("./delta-lake/orders")

print("✓ Created Delta table: orders (with CDF enabled)")

# Create products Delta table for MySQL CDC (used by e2e tests)
products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DecimalType(10, 2), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("metadata", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("updated_at", TimestampType(), True),
])

products_df = spark.createDataFrame([], products_schema)
products_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .save("/tmp/delta/e2e_test/mysql_products")

print("✓ Created Delta table: mysql_products (with CDF enabled)")

spark.stop()
print("\nDeltaLake tables initialized successfully!")
EOF

echo "DeltaLake initialization complete!"
