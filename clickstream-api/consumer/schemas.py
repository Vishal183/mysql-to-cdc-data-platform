"""Spark StructType schemas for clickstream CDC events."""

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType
)


CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", LongType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("event_type", StringType(), False),
    StructField("search_query", StringType(), True),
    StructField("session_id", StringType(), False),
    StructField("created_at", StringType(), True),
])


def build_envelope_schema(table_schema):
    """Build a Debezium envelope schema with typed before/after fields."""
    source_schema = StructType([
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("table", StringType(), True),
        StructField("server_id", LongType(), True),
        StructField("gtid", StringType(), True),
        StructField("file", StringType(), True),
        StructField("pos", LongType(), True),
        StructField("row", IntegerType(), True),
    ])

    return StructType([
        StructField("before", table_schema, True),
        StructField("after", table_schema, True),
        StructField("source", source_schema, True),
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
    ])


TABLE_CONFIG = {
    "dbserver1.ecommerce.clickstream": {
        "schema": CLICKSTREAM_SCHEMA,
        "primary_key": "event_id",
        "delta_path": "delta/clickstream",
    },
}
