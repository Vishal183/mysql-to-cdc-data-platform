#!/bin/bash
set -e

echo "Waiting for Kafka Connect to be ready..."
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    echo "  Kafka Connect not ready yet, retrying in 3s..."
    sleep 3
done

echo "Kafka Connect is ready."
echo "Registering Debezium MySQL connector..."

curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @configs/debezium-mysql-connector.json

echo ""
echo "Waiting for connector to initialize..."
sleep 5

echo "Connector status:"
curl -s http://localhost:8083/connectors/ecommerce-mysql-connector/status | python -m json.tool
