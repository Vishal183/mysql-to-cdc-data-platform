#!/bin/bash
set -e

echo "=== CDC Consumer Startup ==="

# Wait for Kafka Connect
echo "Waiting for Kafka Connect..."
until curl -s http://kafka-connect:8083/ > /dev/null 2>&1; do
    sleep 5
done
echo "Kafka Connect is up."

# Wait for the Debezium connector to be RUNNING
echo "Waiting for Debezium connector..."
for i in $(seq 1 60); do
    STATUS=$(curl -s http://kafka-connect:8083/connectors/ecommerce-mysql-connector/status 2>/dev/null \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('connector',{}).get('state',''))" 2>/dev/null || echo "")
    if [ "$STATUS" = "RUNNING" ]; then
        echo "Connector is RUNNING."
        break
    fi
    echo "  Status: '$STATUS' ($i/60)"
    sleep 5
done

# Wait for all 4 CDC topics to appear
REQUIRED_TOPICS="dbserver1.ecommerce.customers dbserver1.ecommerce.products dbserver1.ecommerce.orders dbserver1.ecommerce.order_items"
echo "Waiting for all CDC topics..."
for topic in $REQUIRED_TOPICS; do
    for i in $(seq 1 120); do
        # Check if topic exists via Kafka Connect's internal consumer group offsets
        TOPICS=$(curl -s "http://kafka-connect:8083/connectors/ecommerce-mysql-connector/topics" 2>/dev/null \
            | python3 -c "import sys,json; d=json.load(sys.stdin); print(' '.join(d.get('ecommerce-mysql-connector',{}).get('topics',[])))" 2>/dev/null || echo "")
        if echo "$TOPICS" | grep -q "$topic"; then
            echo "  Topic ready: $topic"
            break
        fi
        if [ $((i % 6)) -eq 0 ]; then
            echo "  Waiting for $topic... ($i)"
        fi
        sleep 5
    done
done

echo "All topics ready. Starting PySpark CDC consumer..."
exec python3 cdc_consumer.py
