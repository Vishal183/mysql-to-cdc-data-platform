"""Reads CDC events from Kafka and parses Debezium envelope JSON."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType

from schemas import build_envelope_schema


def create_kafka_stream(
    spark: SparkSession,
    topic: str,
    table_schema: StructType,
    kafka_bootstrap_servers: str = "localhost:9092",
) -> DataFrame:
    """Create a Spark Structured Streaming DataFrame from a Kafka CDC topic.

    Reads raw Kafka messages, parses the Debezium envelope with typed
    before/after fields, and flattens into table columns + CDC metadata.

    Returns a DataFrame with all table columns plus __op, __ts_ms, __before_pk.
    """
    # Build envelope schema with typed before/after matching the table
    envelope_schema = build_envelope_schema(table_schema)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "30000")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Cast binary Kafka value to string and parse full Debezium envelope
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), envelope_schema).alias("env"))
        .select(
            col("env.op").alias("__op"),
            col("env.ts_ms").alias("__ts_ms"),
            col("env.after"),
            col("env.before"),
        )
    )

    # Flatten: extract all table columns from 'after', and PK from 'before' for deletes
    pk = table_schema.fields[0].name  # first field is always the PK
    table_columns = [field.name for field in table_schema.fields]

    select_exprs = [col("__op"), col("__ts_ms")]
    for c in table_columns:
        select_exprs.append(col(f"after.{c}").alias(c))
    # Carry the before PK for delete events (after is null on deletes)
    select_exprs.append(col(f"before.{pk}").alias("__before_pk"))

    return parsed.select(*select_exprs)
