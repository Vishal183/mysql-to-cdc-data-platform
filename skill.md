You are a senior data engineer. Build a production-style project called **"mysql-cdc-to-delta-lake"**.

## 🎯 Goal

Simulate a real-world Change Data Capture (CDC) pipeline using:

* MySQL (source database with binlog enabled)
* Debezium (via Kafka Connect)
* Kafka (event streaming)
* Python consumer (initial version; Spark later)
* Delta Lake (final storage layer, local filesystem is fine)

The system must simulate real transactional data with INSERT, UPDATE, DELETE events.

---

## 🧱 Step 1: MySQL Schema (E-commerce Domain)

Create SQL schema with 4 tables:

1. customers
2. products
3. orders
4. order_items

Requirements:

* Proper primary keys
* Foreign key relationships
* Timestamp fields for updates
* Orders must have a lifecycle (CREATED → PROCESSING → SHIPPED → DELIVERED / CANCELLED)

Provide full SQL DDL.

---

## 🚀 Step 2: Data Generator (Python Producer)

Build a Python script that connects to MySQL and continuously simulates traffic.

Requirements:

* Random but realistic data generation
* Simulate:

  * New customers
  * New orders
  * Order status updates
  * Occasional deletes (e.g., cancelled orders)
* Maintain referential integrity
* Run in loop with configurable delay
* Use clean modular functions:

  * create_customer()
  * create_order()
  * update_order_status()
  * delete_order()

Make it runnable with:
python producer.py

---

## ⚙️ Step 3: Docker Compose Setup

Provide docker-compose.yml including:

* MySQL (with binlog enabled)
* Zookeeper
* Kafka
* Kafka Connect (with Debezium)

Requirements:

* Proper environment variables
* MySQL configured for CDC (binlog_format=ROW, etc.)
* Kafka Connect ready for connector registration

---

## 🔌 Step 4: Debezium Connector Config

Provide JSON config to register MySQL connector.

Requirements:

* Capture all 4 tables
* Include database history topic
* Proper server ID and connection config

---

## 📥 Step 5: Kafka Consumer (Python)

Build a Python consumer that:

* Reads CDC events from Kafka topic
* Parses Debezium message format
* Extracts:

  * operation (c, u, d)
  * before / after values
* Logs structured output

---

## 💾 Step 6: Write to Delta Lake

Extend consumer to:

* Convert events into upsert/delete logic
* Store final state in Delta Lake (using PySpark or delta-rs)

Requirements:

* Handle:

  * INSERT → add row
  * UPDATE → upsert
  * DELETE → remove row
* Maintain latest state table

---

## 📁 Project Structure

mysql-cdc-to-delta-lake/
├── docker-compose.yml
├── producer/
├── consumer/
├── configs/
├── sql/
├── delta/
├── README.md

---

## 📘 README Requirements

Include:

* Architecture diagram (text-based is fine)
* Setup instructions (step-by-step)
* How CDC works (brief explanation)
* Sample output

---

## ⚠️ Constraints

* Keep it minimal but realistic
* No unnecessary UI
* Focus on correctness of CDC flow
* Code should be clean and modular

---

## 🔥 Bonus (if time permits)

* Add simple logging/metrics
* Add schema evolution handling
* Add retry handling in consumer

---

Generate complete working code and configs step-by-step.
Do not skip setup details.
