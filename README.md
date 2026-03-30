# MySQL CDC to Delta Lake Pipeline

A production-style Change Data Capture (CDC) pipeline that streams real-time database changes from MySQL into Delta Lake using Debezium, Kafka, and PySpark Structured Streaming.

## Architecture

```
+-----------+       +------------+       +---------+       +-------------------+       +------------+
|  MySQL    | ----> |  Debezium  | ----> |  Kafka  | ----> | PySpark Structured| ----> | Delta Lake |
| (binlog)  |       | (Kafka     |       | (4 CDC  |       | Streaming         |       | (MERGE     |
|           |       |  Connect)  |       |  topics)|       | (foreachBatch)    |       |  upsert/   |
|           |       |            |       |         |       |                   |       |  delete)   |
+-----------+       +------------+       +---------+       +-------------------+       +------------+
      ^
      |
+------------+
| Java       |
| Traffic    |
| Simulator  |
+------------+
```

**Data flow:**
1. A Java traffic simulator continuously generates realistic e-commerce transactions (INSERTs, UPDATEs, DELETEs) against MySQL
2. MySQL binary log captures every row-level change
3. Debezium (running as a Kafka Connect source connector) reads the binlog and publishes CDC events to Kafka topics
4. PySpark Structured Streaming consumes events from Kafka, parses the Debezium JSON envelope, and writes to Delta Lake using MERGE (upsert + delete) logic

## How CDC Works

Change Data Capture captures row-level changes (INSERT, UPDATE, DELETE) from a database's transaction log (MySQL binlog) without polling the tables. Debezium reads the binlog, wraps each change in a JSON envelope containing:

- `op`: operation type -- `c` (create), `u` (update), `d` (delete), `r` (snapshot read)
- `before`: the row state before the change (null for inserts)
- `after`: the row state after the change (null for deletes)
- `ts_ms`: event timestamp
- `source`: metadata (database, table, binlog position, GTID)

This gives us a complete audit trail of every change, enabling real-time data replication without impacting the source database.

## Project Structure

```
mysql-to-cdc-data-platform/
  docker-compose.yml                  # All 5 services
  sql/
    init.sql                          # MySQL schema + seed data + Debezium user
  cdc-producer/                       # Java traffic simulator (Maven)
    src/main/java/com/cdcpipeline/producer/
      ProducerApp.java                # Entry point
      config/DatabaseConfig.java      # HikariCP connection pool
      generator/
        CustomerGenerator.java        # INSERT/UPDATE customers
        OrderGenerator.java           # INSERT/UPDATE/DELETE orders
        OrderItemGenerator.java       # INSERT order items
      simulator/TrafficSimulator.java # Weighted random operations
  configs/
    debezium-mysql-connector.json     # Debezium connector registration payload
  consumer/                           # PySpark CDC consumer (Dockerized)
    Dockerfile                        # python:3.11-slim + JDK 21 + PySpark
    requirements.txt                  # pyspark, delta-spark
    entrypoint.sh                     # Waits for connector + topics before starting
    cdc_consumer.py                   # Main: creates SparkSession, wires 4 streams
    kafka_reader.py                   # Reads Kafka, parses Debezium envelope
    delta_writer.py                   # foreachBatch MERGE handler
    schemas.py                        # Spark StructTypes + table config
  scripts/
    run-producer.sh                   # Build + run Java traffic simulator
    register-connector.sh             # Register Debezium connector via REST
    run-pipeline.sh                   # Full pipeline orchestration
  delta/                              # (placeholder -- Delta tables written inside container volume)
```

## Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| mysql | mysql:8.0 | 3306 | Source database with binlog enabled (ROW format, GTID) |
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092 | Event streaming (single broker) |
| kafka-connect | debezium/connect:2.5 | 8083 | Hosts Debezium MySQL connector |
| spark-consumer | custom (python:3.11 + PySpark) | -- | Reads Kafka, writes Delta Lake |

## Database Schema (E-commerce)

4 tables with foreign key relationships and order lifecycle:

- **customers** -- customer profiles (name, email, phone, address)
- **products** -- product catalog (16 seed products across 4 categories)
- **orders** -- order records with status lifecycle: CREATED -> PROCESSING -> SHIPPED -> DELIVERED/CANCELLED
- **order_items** -- line items (CASCADE delete when parent order is deleted)

## Debezium Connector Configuration

- Full Debezium envelope (no SMT unwrap) -- retains `before`/`after` images
- JSON converters with `schemas.enable=false` -- plain JSON, no schema wrapper
- `decimal.handling.mode=double` -- DECIMAL columns come as numeric values, not base64
- Captures all 4 tables via `table.include.list`
- Topic prefix: `dbserver1` -- produces topics like `dbserver1.ecommerce.customers`

## Delta Lake Write Strategy

Each micro-batch uses a single atomic MERGE operation:

```
1. Coalesce PK: use 'after' PK for upserts, 'before' PK for deletes
2. Deduplicate within batch: keep latest event per PK (by ts_ms)
3. MERGE into Delta:
   - whenMatchedDelete(condition="source.__op = 'd'")
   - whenMatchedUpdate(condition="source.__op != 'd'")
   - whenNotMatchedInsert(condition="source.__op != 'd'")
```

This handles INSERT, UPDATE, DELETE, and initial snapshot (`op=r`) events correctly in one atomic operation.

## Setup Instructions

### Prerequisites
- Docker Desktop
- Java 17+ (for the traffic simulator)
- Maven

### Step 1: Start Infrastructure

```bash
docker compose up -d mysql zookeeper kafka kafka-connect
```

Wait for all services to be healthy:
```bash
docker compose ps
```

### Step 2: Register Debezium Connector

```bash
# Wait for Kafka Connect to be ready
curl http://localhost:8083/connectors

# Register the connector
cat configs/debezium-mysql-connector.json | \
  curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" -d @-

# Verify it's running
curl -s http://localhost:8083/connectors/ecommerce-mysql-connector/status | python -m json.tool
```

### Step 3: Start the Traffic Simulator

```bash
# Build and run
cd cdc-producer && mvn clean package -q -DskipTests && cd ..
java -jar cdc-producer/target/cdc-producer-1.0.0-shaded.jar
```

This generates INSERTs, UPDATEs, and DELETEs across all 4 tables every 2 seconds.

### Step 4: Start the Spark Consumer

```bash
docker compose up -d spark-consumer
```

The consumer's entrypoint script waits for the Debezium connector to be running and all 4 CDC topics to exist before starting PySpark.

### Step 5: Verify

Check consumer logs:
```bash
docker logs -f spark-consumer
```

You should see batches being merged:
```
delta_writer - Processing batch 3: 4 events for /opt/spark-data/delta/orders
delta_writer - Batch 3: merged into /opt/spark-data/delta/orders
```

Query Delta tables:
```bash
docker exec spark-consumer python3 -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('verify').master('local[1]') \
    .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension') \
    .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog') \
    .config('spark.jars.packages','io.delta:delta-spark_2.12:3.1.0').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
for t in ['customers','products','orders','order_items']:
    df = spark.read.format('delta').load(f'/opt/spark-data/delta/{t}')
    print(f'{t}: {df.count()} rows')
    df.show(5, truncate=False)
spark.stop()
"
```

### Useful Commands

```bash
# List Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Peek at a CDC message
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.ecommerce.customers \
  --from-beginning --max-messages 1

# Check connector status
curl -s http://localhost:8083/connectors/ecommerce-mysql-connector/status | python -m json.tool

# Stop everything
docker compose down
```

## Tech Stack

- **MySQL 8.0** -- source database with ROW-based binlog + GTID
- **Debezium 2.5** -- CDC connector for MySQL via Kafka Connect
- **Apache Kafka 7.5** (Confluent) -- event streaming
- **PySpark 3.5.1** -- Structured Streaming with foreachBatch
- **Delta Lake 3.1.0** -- ACID-compliant lakehouse storage with MERGE support
- **Java 17 + Datafaker** -- realistic traffic simulation
- **Docker Compose** -- orchestrates all 5 services
