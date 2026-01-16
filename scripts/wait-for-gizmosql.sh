#!/usr/bin/env bash
# Wait for GizmoSQL to be ready

set -e

MAX_RETRIES=30
RETRY_INTERVAL=2
PORT=31337

probe_port() {
    local port=$1
    nc -z localhost "$port" 2>/dev/null
}

echo "Waiting for GizmoSQL to be ready on port $PORT..."

for i in $(seq 1 $MAX_RETRIES); do
    if probe_port $PORT; then
        echo "GizmoSQL port $PORT is available"
        # Give it a few more seconds for the server to fully initialize
        sleep 3
        echo "GizmoSQL is ready!"
        exit 0
    fi
    echo "Attempt $i/$MAX_RETRIES: GizmoSQL not ready yet, waiting ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
done

echo "ERROR: GizmoSQL did not become ready in time"
exit 1
