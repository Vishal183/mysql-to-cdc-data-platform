#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "=== Starting MySQL ==="
docker compose up -d mysql

echo "Waiting for MySQL to be ready..."
until docker compose exec mysql mysqladmin ping -h localhost -u root -prootpass --silent 2>/dev/null; do
    echo "  MySQL not ready, waiting 2s..."
    sleep 2
done
echo "MySQL is ready!"

echo ""
echo "=== Building CDC Producer ==="
cd cdc-producer
mvn clean package -q -DskipTests
cd ..

echo ""
echo "=== Running Traffic Simulator ==="
echo "Press Ctrl+C to stop"
echo ""
java -jar cdc-producer/target/cdc-producer-1.0.0-shaded.jar
