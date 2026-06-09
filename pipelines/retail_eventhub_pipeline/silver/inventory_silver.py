from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    sha2,
    to_timestamp,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pipelines.retail_eventhub_pipeline.utils.config import (
    get_catalog_name,
    get_schema_name,
)


ENVIRONMENT = spark.conf.get("ENVIRONMENT", "dev")

catalog_name = get_catalog_name(ENVIRONMENT)
bronze_schema = get_schema_name("bronze", ENVIRONMENT)
silver_schema = get_schema_name("silver", ENVIRONMENT)

BRONZE_INVENTORY_TABLE = f"{catalog_name}.{bronze_schema}.inventory_events_raw"
SILVER_INVENTORY_MOVEMENTS_TABLE = f"{catalog_name}.{silver_schema}.inventory_movements"


inventory_payload_schema = StructType(
    [
        StructField("inventory_event_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("location_type", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("movement_type", StringType(), True),
        StructField("quantity_change", IntegerType(), True),
        StructField("stock_on_hand_after", IntegerType(), True),
        StructField("reserved_stock_after", IntegerType(), True),
        StructField("reorder_point", IntegerType(), True),
        StructField("safety_stock", IntegerType(), True),
    ]
)

inventory_event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_version", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("payload", inventory_payload_schema, True),
    ]
)


@dp.table(
    name=SILVER_INVENTORY_MOVEMENTS_TABLE,
    comment="Silver parsed inventory movement events from inventory_updated Event Hub stream.",
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_inventory_event_id", "inventory_event_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_location_type", "location_type IN ('warehouse', 'store')")
@dp.expect_or_drop("valid_location_id", "location_id IS NOT NULL")
@dp.expect_or_drop("valid_movement_type", "movement_type IS NOT NULL")
@dp.expect_or_drop("valid_stock_on_hand_after", "stock_on_hand_after >= 0")
@dp.expect_or_drop("valid_reserved_stock_after", "reserved_stock_after >= 0")
@dp.expect_or_drop("valid_reorder_point", "reorder_point >= 0")
@dp.expect_or_drop("valid_safety_stock", "safety_stock >= 0")
@dp.expect_or_drop("valid_event_type", "event_type = 'inventory_updated'")
def inventory_movements():
    bronze_df = spark.readStream.table(BRONZE_INVENTORY_TABLE)

    parsed_df = (
        bronze_df
        .withColumn("parsed_event", from_json(col("raw_payload"), inventory_event_schema))
        .select(
            col("parsed_event.event_id").alias("event_id"),
            col("parsed_event.event_type").alias("event_type"),
            col("parsed_event.event_version").alias("event_version"),
            to_timestamp(col("parsed_event.event_timestamp")).alias("event_timestamp"),
            col("parsed_event.source_system").alias("source_system"),
            col("parsed_event.correlation_id").alias("correlation_id"),
            col("parsed_event.payload.inventory_event_id").alias("inventory_event_id"),
            col("parsed_event.payload.product_id").alias("product_id"),
            col("parsed_event.payload.location_type").alias("location_type"),
            col("parsed_event.payload.location_id").alias("location_id"),
            col("parsed_event.payload.movement_type").alias("movement_type"),
            col("parsed_event.payload.quantity_change").alias("quantity_change"),
            col("parsed_event.payload.stock_on_hand_after").alias("stock_on_hand_after"),
            col("parsed_event.payload.reserved_stock_after").alias("reserved_stock_after"),
            col("parsed_event.payload.reorder_point").alias("reorder_point"),
            col("parsed_event.payload.safety_stock").alias("safety_stock"),
            col("eventhub_name"),
            col("eventhub_partition"),
            col("eventhub_offset"),
            col("eventhub_enqueued_timestamp"),
            col("_ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        )
        .withColumn(
            "inventory_record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("event_id"),
                    col("inventory_event_id"),
                    col("product_id"),
                    col("location_type"),
                    col("location_id"),
                ),
                256,
            ),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return (
        parsed_df
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["event_id"])
    )