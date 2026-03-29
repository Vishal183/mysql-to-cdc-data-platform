"""Spark StructType schemas for Debezium CDC events per table."""

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, LongType, DoubleType
)

# --- Table-specific schemas for the 'after'/'before' payload ---
# Debezium emits:
#   - DECIMAL as double (decimal.handling.mode=double)
#   - TIMESTAMP as ISO string (io.debezium.time.ZonedTimestamp)

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("stock_quantity", IntegerType(), False),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("created_at", StringType(), True),
])


def build_envelope_schema(table_schema):
    """Build a Debezium envelope schema with typed before/after fields.

    The envelope has: before (table row or null), after (table row or null),
    op (string), ts_ms (long), source (struct), transaction (struct).
    """
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


# Table configuration: maps Kafka topic -> schema + primary key + delta path
TABLE_CONFIG = {
    "dbserver1.ecommerce.customers": {
        "schema": CUSTOMERS_SCHEMA,
        "primary_key": "customer_id",
        "delta_path": "delta/customers",
    },
    "dbserver1.ecommerce.products": {
        "schema": PRODUCTS_SCHEMA,
        "primary_key": "product_id",
        "delta_path": "delta/products",
    },
    "dbserver1.ecommerce.orders": {
        "schema": ORDERS_SCHEMA,
        "primary_key": "order_id",
        "delta_path": "delta/orders",
    },
    "dbserver1.ecommerce.order_items": {
        "schema": ORDER_ITEMS_SCHEMA,
        "primary_key": "order_item_id",
        "delta_path": "delta/order_items",
    },
}
