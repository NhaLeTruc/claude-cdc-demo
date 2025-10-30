#!/bin/sh
# MinIO bucket initialization script
# This script creates the required buckets for Iceberg and Delta Lake testing

set -e

echo "Waiting for MinIO to be ready..."
until mc alias set myminio http://minio:9000 minioadmin minioadmin; do
  echo "MinIO not ready yet, waiting..."
  sleep 2
done

echo "MinIO is ready. Creating buckets..."

# Create buckets if they don't exist
mc mb --ignore-existing myminio/warehouse
mc mb --ignore-existing myminio/iceberg
mc mb --ignore-existing myminio/delta

echo "Buckets created successfully:"
mc ls myminio/

echo "Setting bucket policies to allow public read (for testing)..."
mc anonymous set download myminio/warehouse
mc anonymous set download myminio/iceberg
mc anonymous set download myminio/delta

echo "MinIO initialization complete!"
