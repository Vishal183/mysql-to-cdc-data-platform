# Clickstream Capture & Recommendations API

A sub-project that extends the CDC data platform with clickstream event capture, user interest scoring, and a REST API for product recommendations.

## Architecture

```
MySQL (clickstream table)
  → Debezium (CDC via binlog)
    → Kafka (dbserver1.ecommerce.clickstream topic)
      → PySpark Structured Streaming
        → Delta Lake (clickstream events)
          → Spark Batch Job (interests computation)
            → Delta Lake (user_category_interests, user_product_interests)
              → FastAPI (query service on port 8000)
```

## Components

### 1. Clickstream Table (`sql/clickstream.sql`)
MySQL table capturing user browsing behavior:
- **VIEW** -- user viewed a product
- **ADD_TO_CART** -- user added product to cart
- **REMOVE_FROM_CART** -- user removed product from cart
- **WISHLIST** -- user added product to wishlist
- **SEARCH** -- user searched for something

Each event has a `session_id` (UUID) grouping events into browsing sessions.

### 2. Clickstream Generator (`generator/ClickstreamGenerator.java`)
Simulates realistic browsing sessions:
- Picks a random customer, generates a session
- Browses 3-8 products (VIEW events)
- 30% of views lead to ADD_TO_CART
- 10% of views lead to WISHLIST
- 15% chance of a SEARCH event per session

Integrated into the existing Java traffic simulator (25% of all actions).

### 3. CDC Consumer (`consumer/`)
PySpark Structured Streaming consumer for the clickstream Kafka topic. Same architecture as the main CDC consumer -- reads Debezium events, parses the envelope, writes to Delta Lake via MERGE.

### 4. Interests Batch Job (`consumer/interests_job.py`)
Spark batch job that computes per-user interest scores:

| Signal | Weight |
|--------|--------|
| VIEW | 1.0 |
| ADD_TO_CART | 3.0 |
| WISHLIST | 2.0 |
| SEARCH | 0.5 |
| PURCHASE (from orders) | 5.0 |

Outputs two Delta tables:
- `user_category_interests` -- per-user category scores (normalized 0-1)
- `user_product_interests` -- per-user product scores (normalized 0-1)

### 5. FastAPI Query Service (`api/`)
REST API using `deltalake` (delta-rs) for lightweight Delta reads (no JVM).

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /users/{id}/interests` | Top interest categories for a user |
| `GET /users/{id}/clickstream?limit=50` | Recent clickstream events |
| `GET /products/{id}/viewers` | Users who viewed a product |
| `GET /products/{id}/engagement` | Engagement breakdown (views, carts, wishlists) |
| `GET /recommendations/{user_id}` | Recommended products based on interests |

## Setup

### Prerequisites
The main CDC pipeline must be running first:
```bash
# From project root
docker compose up -d
# Register Debezium connector, start traffic simulator, etc.
```

### Step 1: Create Clickstream Table
```bash
docker exec -i mysql mysql -uroot -prootpass ecommerce < clickstream-api/sql/clickstream.sql
```

### Step 2: Update Debezium Connector
The connector config already includes `ecommerce.clickstream`. Re-register it:
```bash
curl -s -X DELETE http://localhost:8083/connectors/ecommerce-mysql-connector
cat configs/debezium-mysql-connector.json | \
  curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" -d @-
```

### Step 3: Rebuild and Run Traffic Simulator
```bash
cd cdc-producer && mvn clean package -q -DskipTests && cd ..
java -jar cdc-producer/target/cdc-producer-1.0.0-shaded.jar
```

### Step 4: Start Clickstream Consumer and API
```bash
cd clickstream-api
docker compose up -d clickstream-consumer query-api
```

### Step 5: Run Interests Batch Job
After some clickstream data has accumulated:
```bash
docker compose --profile batch run interests-job
```

### Step 6: Query the API
```bash
# User interests
curl http://localhost:8000/users/1/interests

# User clickstream
curl http://localhost:8000/users/1/clickstream

# Product viewers
curl http://localhost:8000/products/1/viewers

# Product engagement
curl http://localhost:8000/products/1/engagement

# Recommendations
curl http://localhost:8000/recommendations/1
```

Interactive docs available at: http://localhost:8000/docs (Swagger UI)

## Tech Stack
- **PySpark 3.5.1** -- CDC streaming + batch interests computation
- **Delta Lake 3.1.0** -- ACID storage for clickstream + interests
- **FastAPI** -- REST API framework
- **deltalake (delta-rs)** -- Lightweight Delta reader for the API (no JVM)
- **Java + Datafaker** -- Clickstream event simulation
