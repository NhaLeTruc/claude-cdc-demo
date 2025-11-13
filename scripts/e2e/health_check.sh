#!/bin/bash
# Health check script for E2E test infrastructure
# Returns 0 if all services are healthy, 1 otherwise

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Load environment
if [ -f "${PROJECT_ROOT}/.env" ]; then
    source "${PROJECT_ROOT}/.env"
fi

POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
KAFKA_HOST=$(echo "${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}" | cut -d':' -f1)
KAFKA_PORT=$(echo "${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}" | cut -d':' -f2)
DEBEZIUM_URL="${DEBEZIUM_URL:-http://localhost:8083}"
DELTA_TABLE_PATH="${DELTA_TABLE_PATH:-/tmp/delta/customers}"

all_healthy=true

check_service() {
    local name=$1
    local host=$2
    local port=$3

    if nc -z "$host" "$port" 2>/dev/null; then
        echo -e "${GREEN}✓${NC} $name ($host:$port)"
        return 0
    else
        echo -e "${RED}✗${NC} $name ($host:$port) - NOT REACHABLE"
        all_healthy=false
        return 1
    fi
}

check_debezium_connector() {
    local connector_name="postgres-cdc-connector"

    if curl -s "${DEBEZIUM_URL}/connectors/${connector_name}/status" 2>/dev/null | grep -q '"state":"RUNNING"'; then
        echo -e "${GREEN}✓${NC} Debezium connector '${connector_name}' (RUNNING)"
        return 0
    else
        echo -e "${RED}✗${NC} Debezium connector '${connector_name}' - NOT RUNNING"
        all_healthy=false
        return 1
    fi
}

check_delta_table() {
    if [ -d "$DELTA_TABLE_PATH" ]; then
        echo -e "${GREEN}✓${NC} Delta table exists ($DELTA_TABLE_PATH)"
        return 0
    else
        echo -e "${YELLOW}⚠${NC} Delta table not found ($DELTA_TABLE_PATH)"
        echo "   This is normal if streaming pipeline hasn't processed any events yet"
        # Don't mark as unhealthy - this is expected on first run
        return 0
    fi
}

check_streaming_pipeline() {
    local pid_file="/tmp/kafka-to-delta.pid"

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Delta streaming pipeline (PID: $pid)"
            return 0
        else
            echo -e "${RED}✗${NC} Delta streaming pipeline - STOPPED (stale PID)"
            all_healthy=false
            return 1
        fi
    else
        # Check if running in Docker
        if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "cdc-spark-delta-streaming"; then
            echo -e "${GREEN}✓${NC} Delta streaming pipeline (Docker container)"
            return 0
        else
            echo -e "${YELLOW}⚠${NC} Delta streaming pipeline - NOT RUNNING"
            echo "   Start with: ./scripts/pipelines/orchestrate_streaming_pipelines.sh start delta"
            echo "   Or: docker compose -f compose/docker-compose.yml up -d spark-delta-streaming"
            all_healthy=false
            return 1
        fi
    fi
}

main() {
    echo "E2E Infrastructure Health Check"
    echo "================================"
    echo ""

    echo "Docker Services:"
    check_service "PostgreSQL" "$POSTGRES_HOST" "$POSTGRES_PORT"
    check_service "Kafka" "$KAFKA_HOST" "$KAFKA_PORT"
    check_service "Debezium" "localhost" "8083"
    check_service "MinIO" "localhost" "9000"
    echo ""

    echo "CDC Components:"
    check_debezium_connector
    echo ""

    echo "Streaming Pipeline:"
    check_streaming_pipeline
    echo ""

    echo "Data Storage:"
    check_delta_table
    echo ""

    echo "================================"
    if [ "$all_healthy" = true ]; then
        echo -e "${GREEN}✓ All services are healthy${NC}"
        echo ""
        echo "Ready to run E2E tests:"
        echo "  poetry run pytest tests/e2e/test_postgres_to_delta.py -v"
        exit 0
    else
        echo -e "${RED}✗ Some services are unhealthy${NC}"
        echo ""
        echo "To fix:"
        echo "  1. Start infrastructure: ./scripts/e2e/start_test_infrastructure.sh"
        echo "  2. Check logs: docker compose -f compose/docker-compose.yml logs"
        echo "  3. Check pipeline: tail -f /tmp/kafka-to-delta.log"
        exit 1
    fi
}

main "$@"