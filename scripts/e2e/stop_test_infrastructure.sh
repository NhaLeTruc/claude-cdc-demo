#!/bin/bash
# Helper script to stop all E2E test infrastructure

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

main() {
    log_step "Stopping E2E Test Infrastructure"
    echo ""

    cd "${PROJECT_ROOT}"

    # Step 1: Stop streaming pipeline
    log_step "Step 1/2: Stopping Delta Lake streaming pipeline..."
    if [ -f "${PROJECT_ROOT}/scripts/pipelines/orchestrate_streaming_pipelines.sh" ]; then
        bash "${PROJECT_ROOT}/scripts/pipelines/orchestrate_streaming_pipelines.sh" stop delta
    fi
    echo ""

    # Step 2: Stop Docker services (optional - keep them running for next test run)
    log_step "Step 2/2: Docker services status..."
    log_info "Docker services are still running. To stop them, run:"
    log_info "  docker compose -f compose/docker-compose.yml down"
    echo ""

    log_info "✓ E2E test infrastructure stopped!"
    echo ""
}

main "$@"