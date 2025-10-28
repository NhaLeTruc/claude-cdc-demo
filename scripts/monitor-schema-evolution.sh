#!/usr/bin/env bash
#
# Schema Evolution Monitoring Script
#
# Monitors schema changes in CDC pipelines and validates that changes
# are properly propagated through the system.
#
# Usage:
#   ./scripts/monitor-schema-evolution.sh [options]
#
# Options:
#   --table TABLE      Monitor specific table (default: all tables)
#   --database DB      Target database (default: demo_db)
#   --interval SECS    Polling interval in seconds (default: 5)
#   --duration SECS    Total monitoring duration (default: 60)
#   --output FILE      Output file for schema change log
#   --verbose          Enable verbose output
#

set -euo pipefail

# Default configuration
TABLE="${1:-all}"
DATABASE="${2:-demo_db}"
INTERVAL="${3:-5}"
DURATION="${4:-60}"
OUTPUT_FILE=""
VERBOSE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --table)
            TABLE="$2"
            shift 2
            ;;
        --database)
            DATABASE="$2"
            shift 2
            ;;
        --interval)
            INTERVAL="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    local level=$1
    shift
    local message="$@"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    case $level in
        INFO)
            echo -e "${BLUE}[INFO]${NC} ${timestamp} - ${message}"
            ;;
        WARN)
            echo -e "${YELLOW}[WARN]${NC} ${timestamp} - ${message}"
            ;;
        ERROR)
            echo -e "${RED}[ERROR]${NC} ${timestamp} - ${message}"
            ;;
        SUCCESS)
            echo -e "${GREEN}[SUCCESS]${NC} ${timestamp} - ${message}"
            ;;
    esac

    # Log to file if specified
    if [[ -n "$OUTPUT_FILE" ]]; then
        echo "[${level}] ${timestamp} - ${message}" >> "$OUTPUT_FILE"
    fi
}

# Get current schema for a table
get_table_schema() {
    local table_name=$1

    docker exec postgres psql -U postgres -d "$DATABASE" -t -A -F"|" <<EOF
SELECT
    column_name,
    data_type,
    character_maximum_length,
    numeric_precision,
    numeric_scale,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = '$table_name'
ORDER BY ordinal_position;
EOF
}

# Get list of all tables
get_all_tables() {
    docker exec postgres psql -U postgres -d "$DATABASE" -t -A <<EOF
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
AND table_type = 'BASE TABLE'
ORDER BY table_name;
EOF
}

# Compare schemas and detect changes
detect_schema_changes() {
    local table_name=$1
    local old_schema=$2
    local new_schema=$3

    # Create temporary files
    local old_file=$(mktemp)
    local new_file=$(mktemp)

    echo "$old_schema" > "$old_file"
    echo "$new_schema" > "$new_file"

    # Compare schemas
    local diff_output=$(diff -u "$old_file" "$new_file" || true)

    # Cleanup
    rm -f "$old_file" "$new_file"

    if [[ -n "$diff_output" ]]; then
        echo "$diff_output"
        return 0
    else
        return 1
    fi
}

# Analyze schema changes
analyze_schema_change() {
    local table_name=$1
    local diff_output=$2

    local changes_detected=false

    # Detect added columns
    if echo "$diff_output" | grep -q "^+"; then
        local added_columns=$(echo "$diff_output" | grep "^+" | grep -v "^+++" | cut -d'|' -f1)
        if [[ -n "$added_columns" ]]; then
            log SUCCESS "Table '$table_name': Column(s) ADDED - $added_columns"
            changes_detected=true
        fi
    fi

    # Detect removed columns
    if echo "$diff_output" | grep -q "^-"; then
        local removed_columns=$(echo "$diff_output" | grep "^-" | grep -v "^---" | cut -d'|' -f1)
        if [[ -n "$removed_columns" ]]; then
            log WARN "Table '$table_name': Column(s) REMOVED - $removed_columns"
            changes_detected=true
        fi
    fi

    # Detect type changes (requires more sophisticated parsing)
    if echo "$diff_output" | grep -qE "^\+.*\|.*\|" && echo "$diff_output" | grep -qE "^-.*\|.*\|"; then
        log WARN "Table '$table_name': Potential TYPE CHANGE detected"
        changes_detected=true
    fi

    if $changes_detected; then
        if $VERBOSE; then
            log INFO "Detailed changes:\n$diff_output"
        fi
    fi
}

# Monitor Debezium schema registry
check_schema_registry() {
    local table_name=$1

    # Check if schema registry has the latest schema
    local schema_subject="${DATABASE}.public.${table_name}-value"

    local schema_versions=$(curl -s "http://localhost:8081/subjects/${schema_subject}/versions" 2>/dev/null || echo "[]")

    if [[ "$schema_versions" != "[]" ]]; then
        local latest_version=$(echo "$schema_versions" | jq -r '.[-1]' 2>/dev/null || echo "unknown")
        log INFO "Table '$table_name': Schema Registry version = $latest_version"
    else
        log WARN "Table '$table_name': No schema found in Schema Registry"
    fi
}

# Check CDC lag after schema change
check_cdc_lag() {
    local table_name=$1

    # Get row count from source
    local source_count=$(docker exec postgres psql -U postgres -d "$DATABASE" -t -A -c \
        "SELECT COUNT(*) FROM $table_name")

    # Wait a moment for CDC to propagate
    sleep 2

    # Check Kafka topic for events
    local topic="debezium.public.${table_name}"
    local kafka_offset=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 --topic "$topic" --time -1 2>/dev/null | tail -1 | cut -d':' -f3 || echo "0")

    log INFO "Table '$table_name': Source rows=$source_count, Kafka offset=$kafka_offset"

    # Check if CDC is lagging
    if [[ $source_count -gt $((kafka_offset + 100)) ]]; then
        log WARN "Table '$table_name': Potential CDC lag detected"
    fi
}

# Main monitoring loop
main() {
    log INFO "Starting schema evolution monitoring"
    log INFO "Database: $DATABASE"
    log INFO "Table: $TABLE"
    log INFO "Interval: ${INTERVAL}s, Duration: ${DURATION}s"

    # Get initial table list
    local tables
    if [[ "$TABLE" == "all" ]]; then
        tables=$(get_all_tables)
    else
        tables="$TABLE"
    fi

    # Store initial schemas
    declare -A initial_schemas
    declare -A current_schemas

    for table in $tables; do
        initial_schemas[$table]=$(get_table_schema "$table")
        current_schemas[$table]="${initial_schemas[$table]}"
        log INFO "Monitoring table: $table (${#initial_schemas[$table]} bytes)"
    done

    # Monitoring loop
    local elapsed=0
    local iterations=0

    while [[ $elapsed -lt $DURATION ]]; do
        iterations=$((iterations + 1))
        log INFO "Iteration $iterations (elapsed: ${elapsed}s)"

        for table in $tables; do
            # Get current schema
            local new_schema=$(get_table_schema "$table")

            # Compare with previous schema
            if detect_schema_changes "$table" "${current_schemas[$table]}" "$new_schema"; then
                local diff=$(detect_schema_changes "$table" "${current_schemas[$table]}" "$new_schema")
                log WARN "Schema change detected for table: $table"

                # Analyze the change
                analyze_schema_change "$table" "$diff"

                # Check schema registry
                check_schema_registry "$table"

                # Check CDC lag
                check_cdc_lag "$table"

                # Update current schema
                current_schemas[$table]="$new_schema"
            fi
        done

        # Sleep for interval
        sleep "$INTERVAL"
        elapsed=$((elapsed + INTERVAL))
    done

    # Final summary
    log INFO "Monitoring complete after $iterations iterations"

    # Report any tables with schema changes
    local changes_detected=false
    for table in $tables; do
        if [[ "${current_schemas[$table]}" != "${initial_schemas[$table]}" ]]; then
            log SUCCESS "Table '$table' had schema changes during monitoring period"
            changes_detected=true
        fi
    done

    if ! $changes_detected; then
        log INFO "No schema changes detected during monitoring period"
    fi

    if [[ -n "$OUTPUT_FILE" ]]; then
        log INFO "Full log written to: $OUTPUT_FILE"
    fi
}

# Run main function
main
