#!/usr/bin/env bash
# ops.sh — start/stop/status for all services
# not fancy, but you can read it at 3am

set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="/var/log/defi-hft"
PID_DIR="/var/run/defi-hft"

mkdir -p "$LOG_DIR" "$PID_DIR"

die() { echo "ERROR: $*" >&2; exit 1; }

start_go_service() {
    local name=$1
    local bin=$2
    local pid_file="$PID_DIR/${name}.pid"

    [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null && {
        echo "$name already running ($(cat "$pid_file"))"; return 0
    }

    cd "$REPO"
    "$bin" >> "$LOG_DIR/${name}.log" 2>&1 &
    echo $! > "$pid_file"
    echo "started $name ($(cat "$pid_file"))"
}

start_python_service() {
    local name=$1
    local script=$2
    local pid_file="$PID_DIR/${name}.pid"

    [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" 2>/dev/null && {
        echo "$name already running ($(cat "$pid_file"))"; return 0
    }

    cd "$REPO"
    python3 "$script" >> "$LOG_DIR/${name}.log" 2>&1 &
    echo $! > "$pid_file"
    echo "started $name ($(cat "$pid_file"))"
}

stop_service() {
    local name=$1
    local pid_file="$PID_DIR/${name}.pid"

    [[ -f "$pid_file" ]] || { echo "$name not running"; return 0; }
    local pid
    pid=$(cat "$pid_file")
    kill "$pid" 2>/dev/null && echo "stopped $name ($pid)" || echo "$name already dead"
    rm -f "$pid_file"
}

status_all() {
    for pid_file in "$PID_DIR"/*.pid; do
        [[ -f "$pid_file" ]] || continue
        local name
        name=$(basename "$pid_file" .pid)
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            echo "  ✓ $name ($pid)"
        else
            echo "  ✗ $name ($pid — dead)"
        fi
    done
}

build_go() {
    echo "building go services..."
    cd "$REPO"
    go build -o bin/indexer    ./cmd/indexer/
    go build -o bin/aggregator ./cmd/aggregator/
    echo "done"
}

case "${1:-help}" in
    build)
        build_go ;;
    start)
        [[ -d "$REPO/bin" ]] || { echo "run ./scripts/ops.sh build first"; exit 1; }
        start_go_service     indexer    "$REPO/bin/indexer"
        start_go_service     aggregator "$REPO/bin/aggregator"
        start_python_service risk       "$REPO/cmd/risk/risk_engine.py"
        start_python_service arb        "$REPO/cmd/arb/arb_engine.py"
        ;;
    stop)
        stop_service indexer
        stop_service aggregator
        stop_service risk
        stop_service arb
        ;;
    restart)
        "$0" stop
        sleep 1
        "$0" start
        ;;
    status)
        echo "service status:"
        status_all ;;
    logs)
        local svc="${2:?usage: ops.sh logs <service>}"
        tail -f "$LOG_DIR/${svc}.log" ;;
    *)
        echo "usage: $0 {build|start|stop|restart|status|logs <svc>}" ;;
esac
