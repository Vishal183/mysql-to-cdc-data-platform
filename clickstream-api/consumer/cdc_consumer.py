"""PySpark Structured Streaming consumer for clickstream CDC events."""

import os
import logging

from pyspark.sql import SparkSession

from schemas import TABLE_CONFIG
from kafka_reader import create_kafka_stream
from delta_writer import make_foreach_batch_handler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("clickstream_consumer")

DELTA_ROOT = os.environ.get("DELTA_ROOT", "/opt/spark-data")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Clickstream-CDC-Consumer")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    logger.info("Starting clickstream CDC consumer...")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} | Trigger: {TRIGGER_INTERVAL}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    queries = []

    for topic, config in TABLE_CONFIG.items():
        delta_path = os.path.join(DELTA_ROOT, config["delta_path"])
        checkpoint_path = os.path.join(DELTA_ROOT, "delta", "_checkpoints", topic)

        logger.info(f"Starting stream: {topic} -> {delta_path}")

        stream = create_kafka_stream(
            spark=spark,
            topic=topic,
            table_schema=config["schema"],
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        )

        handler = make_foreach_batch_handler(
            spark=spark,
            delta_path=delta_path,
            primary_key=config["primary_key"],
        )

        query = (
            stream.writeStream
            .foreachBatch(handler)
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=TRIGGER_INTERVAL)
            .queryName(f"cdc_{topic.split('.')[-1]}")
            .start()
        )

        queries.append(query)
        logger.info(f"Stream started: {query.name}")

    logger.info(f"All {len(queries)} streams running. Awaiting termination...")

    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for q in queries:
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
