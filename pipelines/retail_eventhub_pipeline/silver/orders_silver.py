from __future__ import annotations
   
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    from_json,
    lit,
    sha2,
    concat_ws,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


ENVIRONMENT = spark.conf.get("ENVIRONMENT", "dev")

catalog_name = spark.conf.get("CATALOG_NAME")
bronze_schema = spark.conf.get("BRONZE_SCHEMA")
silver_schema = spark.conf.get("SILVER_SCHEMA")

BRONZE_ORDERS_TABLE = f"{catalog_name}.{bronze_schema}.orders_events_raw"
SILVER_ORDERS_TABLE = f"{catalog_name}.{silver_schema}.orders"
SILVER_ORDER_ITEMS_TABLE = f"{catalog_name}.{silver_schema}.order_items"


order_item_schema = StructType(
    [
        StructField("order_item_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("line_total", DoubleType(), True),
    ]
)

order_payload_schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("order_channel", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("order_total", DoubleType(), True),
        StructField("items", ArrayType(order_item_schema), True),
    ]
)

order_event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_version", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("payload", order_payload_schema, True),
    ]
)


@dp.table(
    name=SILVER_ORDERS_TABLE,
    comment="Silver parsed order headers from order_created events.",
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
@dp.expect_or_drop("valid_order_total", "order_total >= 0")
@dp.expect_or_drop("valid_event_type", "event_type = 'order_created'")
def orders():
    bronze_df = spark.readStream.table(BRONZE_ORDERS_TABLE)

    parsed_df = (
        bronze_df
        .withColumn("parsed_event", from_json(col("raw_payload"), order_event_schema))
        .select(
            col("parsed_event.event_id").alias("event_id"),
            col("parsed_event.event_type").alias("event_type"),
            col("parsed_event.event_version").alias("event_version"),
            to_timestamp(col("parsed_event.event_timestamp")).alias("event_timestamp"),
            col("parsed_event.source_system").alias("source_system"),
            col("parsed_event.correlation_id").alias("correlation_id"),
            col("parsed_event.payload.order_id").alias("order_id"),
            col("parsed_event.payload.customer_id").alias("customer_id"),
            col("parsed_event.payload.store_id").alias("store_id"),
            col("parsed_event.payload.order_channel").alias("order_channel"),
            col("parsed_event.payload.order_status").alias("order_status"),
            col("parsed_event.payload.payment_method").alias("payment_method"),
            col("parsed_event.payload.currency").alias("currency"),
            col("parsed_event.payload.order_total").alias("order_total"),
            col("eventhub_name"),
            col("eventhub_partition"),
            col("eventhub_offset"),
            col("eventhub_enqueued_timestamp"),
            col("_ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        )
        .withColumn(
            "event_record_hash",
            sha2(concat_ws("||", col("event_id"), col("order_id")), 256),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return (
        parsed_df
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id"])
    )


@dp.table(
    name=SILVER_ORDER_ITEMS_TABLE,
    comment="Silver parsed order line items from order_created events.",
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("valid_order_item_id", "order_item_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_line_total", "line_total >= 0")
def order_items():
    bronze_df = spark.readStream.table(BRONZE_ORDERS_TABLE)

    parsed_df = (
        bronze_df
        .withColumn("parsed_event", from_json(col("raw_payload"), order_event_schema))
        .withColumn("item", explode(col("parsed_event.payload.items")))
        .select(
            col("parsed_event.event_id").alias("event_id"),
            col("parsed_event.event_type").alias("event_type"),
            col("parsed_event.event_version").alias("event_version"),
            to_timestamp(col("parsed_event.event_timestamp")).alias("event_timestamp"),
            col("parsed_event.source_system").alias("source_system"),
            col("parsed_event.correlation_id").alias("correlation_id"),
            col("parsed_event.payload.order_id").alias("order_id"),
            col("parsed_event.payload.customer_id").alias("customer_id"),
            col("parsed_event.payload.store_id").alias("store_id"),
            col("item.order_item_id").alias("order_item_id"),
            col("item.product_id").alias("product_id"),
            col("item.quantity").alias("quantity"),
            col("item.unit_price").alias("unit_price"),
            col("item.discount_pct").alias("discount_pct"),
            col("item.line_total").alias("line_total"),
            col("eventhub_name"),
            col("eventhub_partition"),
            col("eventhub_offset"),
            col("eventhub_enqueued_timestamp"),
            col("_ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        )
        .withColumn(
            "order_item_record_hash",
            sha2(concat_ws("||", col("event_id"), col("order_item_id")), 256),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return (
        parsed_df
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id", "order_item_id"])
    )