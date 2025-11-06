#!/bin/bash

###############################################################################
# Spark Job Submission Script for Cross-Storage CDC Pipeline
#
# Submits the Kafka → Iceberg CDC Spark Structured Streaming job
#
# Usage:
#   ./submit-spark-job.sh [kafka_servers] [kafka_topic] [warehouse_path]
#
# Examples:
#   # Use defaults
#   ./submit-spark-job.sh
#
#   # Custom configuration
#   ./submit-spark-job.sh localhost:9092 debezium.public.customers /data/iceberg
#
###############################################################################

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Default configuration
KAFKA_BOOTSTRAP_SERVERS="${1:-localhost:9092}"
KAFKA_TOPIC="${2:-debezium.public.customers}"
ICEBERG_CATALOG="${3:-iceberg}"
ICEBERG_WAREHOUSE="${4:-/data/iceberg/warehouse}"
ICEBERG_NAMESPACE="${5:-analytics}"
ICEBERG_TABLE="${6:-customers_analytics}"
CHECKPOINT_LOCATION="${7:-/tmp/spark-checkpoints/postgres-to-iceberg}"

# Spark configuration
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-2g}"

# Spark job file
SPARK_JOB_FILE="$PROJECT_ROOT/src/cdc_pipelines/cross_storage/spark_job.py"

# Maven dependencies
ICEBERG_VERSION="1.4.0"
SPARK_VERSION="3.3.0"
SCALA_VERSION="2.12"

PACKAGES=(
    "org.apache.iceberg:iceberg-spark-runtime-3.3_${SCALA_VERSION}:${ICEBERG_VERSION}"
    "org.apache.spark:spark-sql-kafka-0-10_${SCALA_VERSION}:${SPARK_VERSION}"
)

PACKAGES_STR=$(IFS=, ; echo "${PACKAGES[*]}")

###############################################################################
# Helper Functions
###############################################################################

print_header() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

###############################################################################
# Pre-flight Checks
###############################################################################

check_dependencies() {
    print_header "Pre-flight Checks"

    # Check if spark-submit is available
    if ! command -v spark-submit &> /dev/null; then
        print_error "spark-submit not found in PATH"
        echo ""
        echo "Please install Apache Spark:"
        echo "  1. Download from https://spark.apache.org/downloads.html"
        echo "  2. Extract and add bin/ to PATH"
        echo "  3. Or install via package manager:"
        echo "     brew install apache-spark (macOS)"
        echo "     apt install spark (Ubuntu/Debian)"
        exit 1
    fi
    print_success "spark-submit found: $(which spark-submit)"

    # Check if Python job file exists
    if [ ! -f "$SPARK_JOB_FILE" ]; then
        print_error "Spark job file not found: $SPARK_JOB_FILE"
        exit 1
    fi
    print_success "Spark job file found: $SPARK_JOB_FILE"

    # Check if Kafka is accessible (optional warning)
    if command -v nc &> /dev/null; then
        KAFKA_HOST=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d: -f1)
        KAFKA_PORT=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d: -f2)

        if ! nc -z -w5 "$KAFKA_HOST" "$KAFKA_PORT" 2>/dev/null; then
            print_warning "Cannot connect to Kafka at $KAFKA_BOOTSTRAP_SERVERS"
            print_info "Make sure Kafka is running before starting the job"
        else
            print_success "Kafka is accessible at $KAFKA_BOOTSTRAP_SERVERS"
        fi
    fi

    echo ""
}

###############################################################################
# Configuration Display
###############################################################################

display_config() {
    print_header "Spark Job Configuration"

    echo -e "${BLUE}Source (Kafka):${NC}"
    echo "  Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
    echo "  Topic:             $KAFKA_TOPIC"
    echo ""

    echo -e "${BLUE}Destination (Iceberg):${NC}"
    echo "  Catalog:           $ICEBERG_CATALOG"
    echo "  Warehouse:         $ICEBERG_WAREHOUSE"
    echo "  Namespace:         $ICEBERG_NAMESPACE"
    echo "  Table:             $ICEBERG_TABLE"
    echo "  Full Table Name:   ${ICEBERG_CATALOG}.${ICEBERG_NAMESPACE}.${ICEBERG_TABLE}"
    echo ""

    echo -e "${BLUE}Spark Configuration:${NC}"
    echo "  Master:            $SPARK_MASTER"
    echo "  Driver Memory:     $SPARK_DRIVER_MEMORY"
    echo "  Executor Memory:   $SPARK_EXECUTOR_MEMORY"
    echo "  Checkpoint:        $CHECKPOINT_LOCATION"
    echo ""

    echo -e "${BLUE}Dependencies:${NC}"
    for package in "${PACKAGES[@]}"; do
        echo "  - $package"
    done
    echo ""
}

###############################################################################
# Spark Job Submission
###############################################################################

submit_job() {
    print_header "Submitting Spark Job"

    print_info "Starting Spark Structured Streaming job..."
    print_info "Press Ctrl+C to stop the job"
    echo ""

    # Export environment variables for the Spark job
    export KAFKA_BOOTSTRAP_SERVERS
    export KAFKA_TOPIC
    export ICEBERG_CATALOG
    export ICEBERG_WAREHOUSE
    export ICEBERG_NAMESPACE
    export ICEBERG_TABLE
    export CHECKPOINT_LOCATION

    # Submit Spark job
    spark-submit \
        --master "$SPARK_MASTER" \
        --driver-memory "$SPARK_DRIVER_MEMORY" \
        --executor-memory "$SPARK_EXECUTOR_MEMORY" \
        --packages "$PACKAGES_STR" \
        --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
        --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
        --conf "spark.sql.catalog.iceberg.type=hadoop" \
        --conf "spark.sql.catalog.iceberg.warehouse=$ICEBERG_WAREHOUSE" \
        --conf "spark.sql.streaming.checkpointLocation=$CHECKPOINT_LOCATION" \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.sql.shuffle.partitions=10" \
        --conf "spark.default.parallelism=10" \
        "$SPARK_JOB_FILE" \
        "$KAFKA_BOOTSTRAP_SERVERS" \
        "$KAFKA_TOPIC" \
        "$ICEBERG_WAREHOUSE"

    # Check exit code
    EXIT_CODE=$?

    echo ""
    if [ $EXIT_CODE -eq 0 ]; then
        print_success "Spark job completed successfully"
    else
        print_error "Spark job failed with exit code $EXIT_CODE"
        exit $EXIT_CODE
    fi
}

###############################################################################
# Cleanup Handler
###############################################################################

cleanup() {
    echo ""
    print_header "Cleanup"
    print_info "Spark job stopped"

    # Optional: Show some stats
    if [ -d "$CHECKPOINT_LOCATION" ]; then
        CHECKPOINT_SIZE=$(du -sh "$CHECKPOINT_LOCATION" 2>/dev/null | cut -f1)
        print_info "Checkpoint directory size: $CHECKPOINT_SIZE"
    fi

    exit 0
}

trap cleanup SIGINT SIGTERM

###############################################################################
# Main Execution
###############################################################################

main() {
    clear

    print_header "Spark CDC Streaming Job Submission"
    echo ""

    check_dependencies
    display_config

    # Confirmation prompt
    read -p "Submit Spark job? (y/N): " -n 1 -r
    echo ""

    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        print_info "Job submission cancelled"
        exit 0
    fi

    echo ""
    submit_job
}

###############################################################################
# Help Message
###############################################################################

if [[ "${1}" == "--help" ]] || [[ "${1}" == "-h" ]]; then
    cat << EOF
Spark Job Submission Script for Cross-Storage CDC Pipeline

USAGE:
    $0 [kafka_servers] [kafka_topic] [warehouse_path]

ARGUMENTS:
    kafka_servers    Kafka bootstrap servers (default: localhost:9092)
    kafka_topic      Kafka topic to consume (default: debezium.public.customers)
    warehouse_path   Iceberg warehouse path (default: /data/iceberg/warehouse)

ENVIRONMENT VARIABLES:
    SPARK_MASTER          Spark master URL (default: local[*])
    SPARK_DRIVER_MEMORY   Driver memory (default: 2g)
    SPARK_EXECUTOR_MEMORY Executor memory (default: 2g)

EXAMPLES:
    # Use all defaults
    $0

    # Custom Kafka server
    $0 kafka:9092

    # Full custom configuration
    $0 kafka:9092 mysql.inventory.customers /mnt/iceberg

    # Use Spark cluster
    SPARK_MASTER=spark://master:7077 $0

    # Increase memory
    SPARK_DRIVER_MEMORY=4g SPARK_EXECUTOR_MEMORY=4g $0

NOTES:
    - This script submits a long-running Spark Structured Streaming job
    - The job will run until manually stopped with Ctrl+C
    - Kafka and Iceberg dependencies are downloaded automatically
    - Checkpoints are stored at: $CHECKPOINT_LOCATION

For more information, see docs/pipelines/cross_storage.md

EOF
    exit 0
fi

# Run main
main
