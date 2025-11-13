#!/bin/bash
# Helper script to start all infrastructure required for E2E tests
# This script starts Docker services and the Delta Lake streaming pipeline

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Load environment variables
if [ -f "${PROJECT_ROOT}/.env" ]; then
    source "${PROJECT_ROOT}/.env"
fi

check_command() {
    if ! command -v "$1" &> /dev/null; then
        log_error "$1 is not installed. Please install it first."
        return 1
    fi
}

wait_for_service() {
    local service_name=$1
    local host=$2
    local port=$3
    local max_attempts=${4:-30}
    local attempt=1

    log_info "Waiting for $service_name at $host:$port..."

    while [ $attempt -le $max_attempts ]; do
        if nc -z "$host" "$port" 2>/dev/null; then
            log_info "$service_name is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo ""
    log_error "$service_name failed to start within $((max_attempts * 2)) seconds"
    return 1
}

wait_for_debezium_connector() {
    local max_attempts=${1:-30}
    local attempt=1
    local debezium_url="${DEBEZIUM_URL:-http://localhost:8083}"

    log_info "Waiting for Debezium connector to be registered..."

    while [ $attempt -le $max_attempts ]; do
        if curl -s "${debezium_url}/connectors/postgres-cdc-connector/status" 2>/dev/null | grep -q '"state":"RUNNING"'; then
            log_info "Debezium connector is running!"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done

    echo ""
    log_warn "Debezium connector is not running yet. You may need to register it manually."
    return 1
}

main() {
    log_step "Starting E2E Test Infrastructure"
    echo ""

    # Check prerequisites
    log_step "Checking prerequisites..."
    check_command "docker" || exit 1
    check_command "nc" || exit 1
    check_command "curl" || exit 1

    cd "${PROJECT_ROOT}"

    # Step 1: Start Docker services
    log_step "Step 1/5: Starting Docker Compose services..."
    docker compose -f compose/docker-compose.yml up -d postgres kafka debezium minio
    echo ""

    # Step 2: Wait for services to be ready
    log_step "Step 2/5: Waiting for services to be ready..."
    wait_for_service "PostgreSQL" "localhost" "5432" 30
    wait_for_service "Kafka" "localhost" "29092" 30
    wait_for_service "Debezium" "localhost" "8083" 60
    wait_for_service "MinIO" "localhost" "9000" 30
    echo ""

    # Step 3: Check/Setup Debezium connector
    log_step "Step 3/5: Checking Debezium connector..."

    if ! wait_for_debezium_connector 5; then
        log_info "Attempting to register Debezium connector..."

        # Check if setup script exists
        if [ -f "${PROJECT_ROOT}/src/cli/commands/setup.py" ]; then
            poetry run python -m src.cli.main setup debezium 2>&1 | grep -v "^$" || true
        elif [ -f "${PROJECT_ROOT}/scripts/setup_debezium_connector.py" ]; then
            poetry run python "${PROJECT_ROOT}/scripts/setup_debezium_connector.py" 2>&1 | grep -v "^$" || true
        else
            log_warn "No Debezium setup script found. You may need to register the connector manually."
        fi

        # Wait again after registration attempt
        wait_for_debezium_connector 15 || log_warn "Connector setup may need manual intervention"
    fi
    echo ""

    # Step 4: Generate test data (if needed)
    log_step "Step 4/5: Ensuring test data exists..."

    # Check if customers table has data
    if docker exec cdc-postgres psql -U cdcuser -d cdcdb -tAc "SELECT COUNT(*) FROM customers" 2>/dev/null | grep -q "^0$"; then
        log_info "No data found. Generating sample data..."
        if [ -f "${PROJECT_ROOT}/scripts/generate_sample_data.py" ]; then
            poetry run python "${PROJECT_ROOT}/scripts/generate_sample_data.py" 2>&1 | tail -5 || true
        elif [ -f "${PROJECT_ROOT}/src/data_generators/generators.py" ]; then
            poetry run python -m src.data_generators.generators 2>&1 | tail -5 || true
        else
            log_warn "No data generation script found. Tests may fail without data."
        fi
    else
        log_info "Test data already exists in database"
    fi
    echo ""

    # Step 5: Start Delta Lake streaming pipeline
    log_step "Step 5/5: Starting Delta Lake streaming pipeline..."

    if [ -f "${PROJECT_ROOT}/scripts/pipelines/orchestrate_streaming_pipelines.sh" ]; then
        bash "${PROJECT_ROOT}/scripts/pipelines/orchestrate_streaming_pipelines.sh" start delta
    else
        log_error "Streaming pipeline script not found!"
        exit 1
    fi
    echo ""

    # Final status
    log_step "Infrastructure Status Check"
    echo ""
    bash "${PROJECT_ROOT}/scripts/pipelines/orchestrate_streaming_pipelines.sh" status
    echo ""

    log_info "✓ E2E test infrastructure is ready!"
    log_info ""
    log_info "Next steps:"
    log_info "  1. Run E2E tests: poetry run pytest tests/e2e/test_postgres_to_delta.py -v"
    log_info "  2. View streaming logs: bash scripts/pipelines/orchestrate_streaming_pipelines.sh logs delta"
    log_info "  3. Stop infrastructure: bash scripts/e2e/stop_test_infrastructure.sh"
    echo ""
}

main "$@"