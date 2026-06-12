from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    sha2,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


ENVIRONMENT = spark.conf.get("ENVIRONMENT", "dev")

catalog_name = spark.conf.get("CATALOG_NAME")
bronze_schema = spark.conf.get("BRONZE_SCHEMA")
silver_schema = spark.conf.get("SILVER_SCHEMA")

BRONZE_SHIPMENT_TABLE = f"{catalog_name}.{bronze_schema}.shipment_events_raw"
SILVER_SHIPMENTS_TABLE = f"{catalog_name}.{silver_schema}.shipments"


shipment_payload_schema = StructType(
    [
        StructField("shipment_id", StringType(), True),
        StructField("shipment_type", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("source_id", StringType(), True),
        StructField("destination_type", StringType(), True),
        StructField("destination_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("shipment_date", StringType(), True),
        StructField("expected_delivery_date", StringType(), True),
        StructField("actual_delivery_date", StringType(), True),
        StructField("shipment_status", StringType(), True),
        StructField("carrier", StringType(), True),
        StructField("delay_days", IntegerType(), True),
    ]
)

shipment_event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_version", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("payload", shipment_payload_schema, True),
    ]
)


@dp.table(
    name=SILVER_SHIPMENTS_TABLE,
    comment="Silver parsed shipment events from shipment_created Event Hub stream.",
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_shipment_id", "shipment_id IS NOT NULL")
@dp.expect_or_drop("valid_event_type", "event_type = 'shipment_created'")
@dp.expect_or_drop(
    "valid_shipment_type",
    "shipment_type IN ('supplier_to_warehouse', 'warehouse_to_store')",
)
@dp.expect_or_drop(
    "valid_source_type",
    "source_type IN ('supplier', 'warehouse')",
)
@dp.expect_or_drop(
    "valid_destination_type",
    "destination_type IN ('warehouse', 'store')",
)
@dp.expect_or_drop("valid_source_id", "source_id IS NOT NULL")
@dp.expect_or_drop("valid_destination_id", "destination_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop(
    "valid_shipment_status",
    "shipment_status IN ('created', 'in_transit', 'delivered', 'delayed')",
)
@dp.expect_or_drop("valid_delay_days", "delay_days >= 0")
@dp.expect_or_drop(
    "valid_supplier_to_warehouse_flow",
    """
    shipment_type != 'supplier_to_warehouse'
    OR (source_type = 'supplier' AND destination_type = 'warehouse')
    """,
)
@dp.expect_or_drop(
    "valid_warehouse_to_store_flow",
    """
    shipment_type != 'warehouse_to_store'
    OR (source_type = 'warehouse' AND destination_type = 'store')
    """,
)
def shipments():
    bronze_df = spark.readStream.table(BRONZE_SHIPMENT_TABLE)

    parsed_df = (
        bronze_df
        .withColumn("parsed_event", from_json(col("raw_payload"), shipment_event_schema))
        .select(
            col("parsed_event.event_id").alias("event_id"),
            col("parsed_event.event_type").alias("event_type"),
            col("parsed_event.event_version").alias("event_version"),
            to_timestamp(col("parsed_event.event_timestamp")).alias("event_timestamp"),
            col("parsed_event.source_system").alias("source_system"),
            col("parsed_event.correlation_id").alias("correlation_id"),
            col("parsed_event.payload.shipment_id").alias("shipment_id"),
            col("parsed_event.payload.shipment_type").alias("shipment_type"),
            col("parsed_event.payload.source_type").alias("source_type"),
            col("parsed_event.payload.source_id").alias("source_id"),
            col("parsed_event.payload.destination_type").alias("destination_type"),
            col("parsed_event.payload.destination_id").alias("destination_id"),
            col("parsed_event.payload.product_id").alias("product_id"),
            col("parsed_event.payload.quantity").alias("quantity"),
            to_date(col("parsed_event.payload.shipment_date")).alias("shipment_date"),
            to_date(col("parsed_event.payload.expected_delivery_date")).alias(
                "expected_delivery_date"
            ),
            to_date(col("parsed_event.payload.actual_delivery_date")).alias(
                "actual_delivery_date"
            ),
            col("parsed_event.payload.shipment_status").alias("shipment_status"),
            col("parsed_event.payload.carrier").alias("carrier"),
            col("parsed_event.payload.delay_days").alias("delay_days"),
            col("eventhub_name"),
            col("eventhub_partition"),
            col("eventhub_offset"),
            col("eventhub_enqueued_timestamp"),
            col("_ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        )
        .withColumn(
            "shipment_record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("event_id"),
                    col("shipment_id"),
                    col("product_id"),
                    col("source_id"),
                    col("destination_id"),
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