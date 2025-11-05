#!/bin/bash
# Single initialization script for CDC infrastructure
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
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

echo "============================================================"
echo "  CDC Infrastructure Initialization"
echo "============================================================"
echo "Project Root: ${PROJECT_ROOT}"
echo ""

# Wait for services to be ready
log_info "[1/5] Waiting for services to stabilize..."
sleep 15

# Register Debezium connectors
log_info "[2/5] Registering Debezium connectors..."
cd "${PROJECT_ROOT}"

# Postgres connector
log_info "  - Registering Postgres CDC connector..."
if poetry run python scripts/connectors/setup_postgres_connector.py; then
    log_info "    ✓ Postgres connector registered successfully"
else
    log_warn "    ✗ Postgres connector setup failed"
fi
sleep 5

# MySQL connector
log_info "  - Registering MySQL CDC connector..."
if bash scripts/connectors/register-mysql-connector.sh; then
    log_info "    ✓ MySQL connector registered successfully"
else
    log_warn "    ✗ MySQL connector setup failed"
fi
sleep 5

# Initialize storage tables
log_info "[3/5] Initializing Delta and Iceberg tables..."

log_info "  - Initializing Delta Lake tables..."
if bash scripts/storage/init-delta-tables.sh; then
    log_info "    ✓ Delta tables initialized"
else
    log_warn "    ✗ Delta tables init failed"
fi

log_info "  - Initializing Iceberg tables..."
if bash scripts/storage/init-iceberg-tables.sh; then
    log_info "    ✓ Iceberg tables initialized"
else
    log_warn "    ✗ Iceberg tables init failed"
fi

# Start streaming pipelines
log_info "[4/5] Starting CDC streaming pipelines..."
if bash scripts/pipelines/orchestrate_streaming_pipelines.sh start all; then
    log_info "    ✓ Streaming pipelines started"
else
    log_warn "    ✗ Streaming pipelines failed to start"
fi

log_info "[5/5] Initialization complete!"
echo ""
echo "============================================================"
echo "  CDC Infrastructure Ready"
echo "============================================================"
echo ""
echo "Monitor streaming logs:"
echo "  Delta:   tail -f /tmp/kafka-to-delta.log"
echo "  Iceberg: tail -f /tmp/kafka-to-iceberg.log"
echo ""
echo "Check connector status:"
echo "  curl http://debezium:8083/connectors/postgres-cdc-connector/status"
echo "  curl http://debezium:8083/connectors/mysql-connector/status"
echo ""
log_info "Keeping container alive for streaming pipelines..."
tail -f /dev/null
