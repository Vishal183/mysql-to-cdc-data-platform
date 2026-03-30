#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo "========================================="
echo "  CDC Pipeline - Full Stack Startup"
echo "========================================="

# Step 1: Start all Docker services
echo ""
echo "[1/4] Starting Docker services..."
docker compose up -d
echo "Docker services started."

# Step 2: Wait for Kafka Connect to be healthy
echo ""
echo "[2/4] Waiting for Kafka Connect to be ready..."
MAX_RETRIES=40
RETRY_COUNT=0
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "ERROR: Kafka Connect did not become ready after $MAX_RETRIES attempts."
        echo "Check logs: docker compose logs kafka-connect"
        exit 1
    fi
    echo "  Attempt $RETRY_COUNT/$MAX_RETRIES - waiting 5s..."
    sleep 5
done
echo "Kafka Connect is ready."

# Step 3: Register Debezium connector (idempotent - skip if already exists)
echo ""
echo "[3/4] Registering Debezium connector..."
EXISTING=$(curl -s http://localhost:8083/connectors | python -c "import sys,json; print('ecommerce-mysql-connector' in json.load(sys.stdin))" 2>/dev/null || echo "False")

if [ "$EXISTING" = "True" ]; then
    echo "Connector already registered. Checking status..."
else
    curl -s -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @configs/debezium-mysql-connector.json > /dev/null
    echo "Connector registered. Waiting for initialization..."
    sleep 5
fi

curl -s http://localhost:8083/connectors/ecommerce-mysql-connector/status | python -m json.tool

# Step 4: Start PySpark consumer
echo ""
echo "[4/4] Starting PySpark CDC consumer..."
echo "  Delta tables will be written to: $PROJECT_ROOT/delta/"
echo "  Press Ctrl+C to stop the consumer."
echo ""
cd consumer
python cdc_consumer.py
