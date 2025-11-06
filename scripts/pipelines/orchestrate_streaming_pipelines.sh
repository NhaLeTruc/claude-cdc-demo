#!/bin/bash
# Orchestrate CDC streaming pipelines
# This script manages Spark Structured Streaming jobs for Kafka → Delta Lake and Kafka → Iceberg

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Load environment variables (skip if running in Docker container)
# Docker containers should use environment variables from docker-compose
if [ -f "${PROJECT_ROOT}/.env" ] && [ ! -f "/.dockerenv" ]; then
    source "${PROJECT_ROOT}/.env"
fi

# Default configuration
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-debezium.public.customers}"
DELTA_TABLE_PATH="${DELTA_TABLE_PATH:-/tmp/delta-cdc/customers}"
DELTA_CHECKPOINT="${DELTA_CHECKPOINT:-/tmp/spark-checkpoints/kafka-to-delta}"
ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE:-/tmp/iceberg-warehouse}"
ICEBERG_CHECKPOINT="${ICEBERG_CHECKPOINT:-/tmp/spark-checkpoints/kafka-to-iceberg}"

# PID files
DELTA_PID_FILE="/tmp/kafka-to-delta.pid"
ICEBERG_PID_FILE="/tmp/kafka-to-iceberg.pid"

# Log files
DELTA_LOG_FILE="/tmp/kafka-to-delta.log"
ICEBERG_LOG_FILE="/tmp/kafka-to-iceberg.log"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
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

check_service() {
    local service_name=$1
    local host=$2
    local port=$3

    if nc -z "$host" "$port" 2>/dev/null; then
        log_info "$service_name is available at $host:$port"
        return 0
    else
        log_error "$service_name is NOT available at $host:$port"
        return 1
    fi
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Parse Kafka bootstrap servers to get host and port
    # Format: host:port or host1:port1,host2:port2
    KAFKA_HOST=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d',' -f1 | cut -d':' -f1)
    KAFKA_PORT=$(echo "$KAFKA_BOOTSTRAP_SERVERS" | cut -d',' -f1 | cut -d':' -f2)

    # Default to standard port if parsing fails
    KAFKA_PORT=${KAFKA_PORT:-9092}

    # Check Kafka (non-fatal, just a warning)
    if check_service "Kafka" "$KAFKA_HOST" "$KAFKA_PORT"; then
        log_info "Kafka is available and ready"
    else
        log_warn "Kafka check failed at ${KAFKA_HOST}:${KAFKA_PORT}. Streaming jobs will retry on connection."
    fi

    # Determine Debezium URL based on environment
    DEBEZIUM_URL="${DEBEZIUM_URL:-http://localhost:8083}"

    # Check Debezium connector (non-fatal warning)
    if curl -s "${DEBEZIUM_URL}/connectors/postgres-cdc-connector/status" 2>/dev/null | grep -q '"state":"RUNNING"'; then
        log_info "Debezium connector is running"
    else
        log_warn "Debezium connector is not running or not reachable at ${DEBEZIUM_URL}"
        log_warn "Streaming jobs may fail until connector is registered"
    fi

    # Create checkpoint directories
    mkdir -p "${DELTA_CHECKPOINT}"
    mkdir -p "${ICEBERG_CHECKPOINT}"

    log_info "Prerequisites check completed"
}

start_delta_pipeline() {
    log_info "Starting Kafka → Delta Lake streaming pipeline..."

    if [ -f "${DELTA_PID_FILE}" ]; then
        local pid=$(cat "${DELTA_PID_FILE}")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_warn "Delta pipeline is already running (PID: $pid)"
            return 0
        else
            rm -f "${DELTA_PID_FILE}"
        fi
    fi

    cd "${PROJECT_ROOT}"

    # Determine Python command (prefer poetry if available)
    if command -v poetry &> /dev/null; then
        PYTHON_CMD="poetry run python"
    else
        PYTHON_CMD="python3"
    fi

    # Export environment variables for the streaming job
    export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}"
    export KAFKA_TOPIC="${KAFKA_TOPIC}"
    export DELTA_TABLE_PATH="${DELTA_TABLE_PATH}"
    export CHECKPOINT_LOCATION="${DELTA_CHECKPOINT}"

    # Don't pass command-line arguments - let the script use environment variables
    # This prevents issues with .env file values overriding docker-compose env vars
    nohup $PYTHON_CMD -m src.cdc_pipelines.streaming.kafka_to_delta \
        > "${DELTA_LOG_FILE}" 2>&1 &

    local pid=$!
    echo "$pid" > "${DELTA_PID_FILE}"

    log_info "Delta pipeline started (PID: $pid)"
    log_info "  Log file: ${DELTA_LOG_FILE}"
    log_info "  Kafka topic: ${KAFKA_TOPIC}"
    log_info "  Delta table: ${DELTA_TABLE_PATH}"
}

start_iceberg_pipeline() {
    log_info "Starting Kafka → Iceberg streaming pipeline..."

    if [ -f "${ICEBERG_PID_FILE}" ]; then
        local pid=$(cat "${ICEBERG_PID_FILE}")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_warn "Iceberg pipeline is already running (PID: $pid)"
            return 0
        else
            rm -f "${ICEBERG_PID_FILE}"
        fi
    fi

    cd "${PROJECT_ROOT}"

    # Determine Python command (prefer poetry if available)
    if command -v poetry &> /dev/null; then
        PYTHON_CMD="poetry run python"
    else
        PYTHON_CMD="python3"
    fi

    # Export environment variables for the streaming job
    export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS}"
    export KAFKA_TOPIC="${KAFKA_TOPIC}"
    export ICEBERG_WAREHOUSE="${ICEBERG_WAREHOUSE}"
    export ICEBERG_CATALOG="${ICEBERG_CATALOG:-iceberg}"
    export ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-analytics}"
    export ICEBERG_TABLE="${ICEBERG_TABLE:-customers_analytics}"
    export CHECKPOINT_LOCATION="${ICEBERG_CHECKPOINT}"

    # Don't pass command-line arguments - let the script use environment variables
    # This prevents issues with .env file values overriding docker-compose env vars
    nohup $PYTHON_CMD -m src.cdc_pipelines.streaming.kafka_to_iceberg \
        > "${ICEBERG_LOG_FILE}" 2>&1 &

    local pid=$!
    echo "$pid" > "${ICEBERG_PID_FILE}"

    log_info "Iceberg pipeline started (PID: $pid)"
    log_info "  Log file: ${ICEBERG_LOG_FILE}"
    log_info "  Kafka topic: ${KAFKA_TOPIC}"
    log_info "  Iceberg warehouse: ${ICEBERG_WAREHOUSE}"
}

stop_delta_pipeline() {
    if [ -f "${DELTA_PID_FILE}" ]; then
        local pid=$(cat "${DELTA_PID_FILE}")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping Delta pipeline (PID: $pid)..."
            kill "$pid"
            sleep 2
            if ps -p "$pid" > /dev/null 2>&1; then
                log_warn "Forcefully stopping Delta pipeline..."
                kill -9 "$pid"
            fi
            rm -f "${DELTA_PID_FILE}"
            log_info "Delta pipeline stopped"
        else
            log_warn "Delta pipeline is not running"
            rm -f "${DELTA_PID_FILE}"
        fi
    else
        log_warn "No Delta pipeline PID file found"
    fi
}

stop_iceberg_pipeline() {
    if [ -f "${ICEBERG_PID_FILE}" ]; then
        local pid=$(cat "${ICEBERG_PID_FILE}")
        if ps -p "$pid" > /dev/null 2>&1; then
            log_info "Stopping Iceberg pipeline (PID: $pid)..."
            kill "$pid"
            sleep 2
            if ps -p "$pid" > /dev/null 2>&1; then
                log_warn "Forcefully stopping Iceberg pipeline..."
                kill -9 "$pid"
            fi
            rm -f "${ICEBERG_PID_FILE}"
            log_info "Iceberg pipeline stopped"
        else
            log_warn "Iceberg pipeline is not running"
            rm -f "${ICEBERG_PID_FILE}"
        fi
    else
        log_warn "No Iceberg pipeline PID file found"
    fi
}

status_pipelines() {
    log_info "Pipeline Status:"

    # Delta pipeline
    if [ -f "${DELTA_PID_FILE}" ]; then
        local pid=$(cat "${DELTA_PID_FILE}")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "  ${GREEN}✓${NC} Delta Lake pipeline: RUNNING (PID: $pid)"
            echo "    Log: ${DELTA_LOG_FILE}"
        else
            echo -e "  ${RED}✗${NC} Delta Lake pipeline: NOT RUNNING (stale PID file)"
        fi
    else
        echo -e "  ${RED}✗${NC} Delta Lake pipeline: NOT RUNNING"
    fi

    # Iceberg pipeline
    if [ -f "${ICEBERG_PID_FILE}" ]; then
        local pid=$(cat "${ICEBERG_PID_FILE}")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "  ${GREEN}✓${NC} Iceberg pipeline: RUNNING (PID: $pid)"
            echo "    Log: ${ICEBERG_LOG_FILE}"
        else
            echo -e "  ${RED}✗${NC} Iceberg pipeline: NOT RUNNING (stale PID file)"
        fi
    else
        echo -e "  ${RED}✗${NC} Iceberg pipeline: NOT RUNNING"
    fi
}

logs_delta() {
    if [ -f "${DELTA_LOG_FILE}" ]; then
        tail -f "${DELTA_LOG_FILE}"
    else
        log_error "Delta log file not found: ${DELTA_LOG_FILE}"
    fi
}

logs_iceberg() {
    if [ -f "${ICEBERG_LOG_FILE}" ]; then
        tail -f "${ICEBERG_LOG_FILE}"
    else
        log_error "Iceberg log file not found: ${ICEBERG_LOG_FILE}"
    fi
}

usage() {
    cat <<EOF
Usage: $0 {start|stop|restart|status|logs} [delta|iceberg|all]

Commands:
  start [all|delta|iceberg]    Start streaming pipeline(s) (default: all)
  stop [all|delta|iceberg]     Stop streaming pipeline(s) (default: all)
  restart [all|delta|iceberg]  Restart streaming pipeline(s) (default: all)
  status                       Show status of all pipelines
  logs [delta|iceberg]         Tail logs for pipeline

Examples:
  $0 start              # Start all pipelines
  $0 start delta        # Start only Delta Lake pipeline
  $0 stop all           # Stop all pipelines
  $0 status             # Show status
  $0 logs delta         # Tail Delta pipeline logs

EOF
}

main() {
    local command="${1:-}"
    local target="${2:-all}"

    case "$command" in
        start)
            check_prerequisites
            case "$target" in
                all)
                    start_delta_pipeline
                    start_iceberg_pipeline
                    ;;
                delta)
                    start_delta_pipeline
                    ;;
                iceberg)
                    start_iceberg_pipeline
                    ;;
                *)
                    log_error "Invalid target: $target"
                    usage
                    exit 1
                    ;;
            esac
            ;;
        stop)
            case "$target" in
                all)
                    stop_delta_pipeline
                    stop_iceberg_pipeline
                    ;;
                delta)
                    stop_delta_pipeline
                    ;;
                iceberg)
                    stop_iceberg_pipeline
                    ;;
                *)
                    log_error "Invalid target: $target"
                    usage
                    exit 1
                    ;;
            esac
            ;;
        restart)
            case "$target" in
                all)
                    stop_delta_pipeline
                    stop_iceberg_pipeline
                    sleep 2
                    check_prerequisites
                    start_delta_pipeline
                    start_iceberg_pipeline
                    ;;
                delta)
                    stop_delta_pipeline
                    sleep 2
                    check_prerequisites
                    start_delta_pipeline
                    ;;
                iceberg)
                    stop_iceberg_pipeline
                    sleep 2
                    check_prerequisites
                    start_iceberg_pipeline
                    ;;
                *)
                    log_error "Invalid target: $target"
                    usage
                    exit 1
                    ;;
            esac
            ;;
        status)
            status_pipelines
            ;;
        logs)
            case "$target" in
                delta)
                    logs_delta
                    ;;
                iceberg)
                    logs_iceberg
                    ;;
                *)
                    log_error "Invalid target: $target"
                    usage
                    exit 1
                    ;;
            esac
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            log_error "Invalid command: $command"
            usage
            exit 1
            ;;
    esac
}

main "$@"
