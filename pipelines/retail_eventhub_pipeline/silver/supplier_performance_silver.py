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
    DoubleType,
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

BRONZE_SUPPLIER_PERFORMANCE_TABLE = (
    f"{catalog_name}.{bronze_schema}.supplier_performance_events_raw"
)

SILVER_SUPPLIER_PERFORMANCE_TABLE = (
    f"{catalog_name}.{silver_schema}.supplier_performance"
)


supplier_performance_payload_schema = StructType(
    [
        StructField("performance_id", StringType(), True),
        StructField("supplier_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("performance_month", StringType(), True),
        StructField("on_time_delivery_rate", DoubleType(), True),
        StructField("defect_rate", DoubleType(), True),
        StructField("avg_delay_days", DoubleType(), True),
        StructField("fulfilled_quantity", IntegerType(), True),
        StructField("rejected_quantity", IntegerType(), True),
        StructField("risk_score", DoubleType(), True),
    ]
)

supplier_performance_event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_version", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("correlation_id", StringType(), True),
        StructField("payload", supplier_performance_payload_schema, True),
    ]
)


@dp.table(
    name=SILVER_SUPPLIER_PERFORMANCE_TABLE,
    comment="Silver parsed supplier performance events from supplier_performance Event Hub stream.",
)
@dp.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dp.expect_or_drop("valid_performance_id", "performance_id IS NOT NULL")
@dp.expect_or_drop("valid_supplier_id", "supplier_id IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("valid_event_type", "event_type = 'supplier_performance'")
@dp.expect_or_drop(
    "valid_performance_month",
    "performance_month RLIKE '^[0-9]{4}-[0-9]{2}$'",
)
@dp.expect_or_drop(
    "valid_on_time_delivery_rate",
    "on_time_delivery_rate >= 0 AND on_time_delivery_rate <= 1",
)
@dp.expect_or_drop(
    "valid_defect_rate",
    "defect_rate >= 0 AND defect_rate <= 1",
)
@dp.expect_or_drop(
    "valid_avg_delay_days",
    "avg_delay_days >= 0",
)
@dp.expect_or_drop(
    "valid_fulfilled_quantity",
    "fulfilled_quantity >= 0",
)
@dp.expect_or_drop(
    "valid_rejected_quantity",
    "rejected_quantity >= 0",
)
@dp.expect_or_drop(
    "valid_risk_score",
    "risk_score >= 0 AND risk_score <= 1",
)
def supplier_performance():
    bronze_df = spark.readStream.table(BRONZE_SUPPLIER_PERFORMANCE_TABLE)

    parsed_df = (
        bronze_df
        .withColumn(
            "parsed_event",
            from_json(col("raw_payload"), supplier_performance_event_schema),
        )
        .select(
            col("parsed_event.event_id").alias("event_id"),
            col("parsed_event.event_type").alias("event_type"),
            col("parsed_event.event_version").alias("event_version"),
            to_timestamp(col("parsed_event.event_timestamp")).alias("event_timestamp"),
            col("parsed_event.source_system").alias("source_system"),
            col("parsed_event.correlation_id").alias("correlation_id"),
            col("parsed_event.payload.performance_id").alias("performance_id"),
            col("parsed_event.payload.supplier_id").alias("supplier_id"),
            col("parsed_event.payload.product_id").alias("product_id"),
            col("parsed_event.payload.performance_month").alias("performance_month"),
            col("parsed_event.payload.on_time_delivery_rate").alias(
                "on_time_delivery_rate"
            ),
            col("parsed_event.payload.defect_rate").alias("defect_rate"),
            col("parsed_event.payload.avg_delay_days").alias("avg_delay_days"),
            col("parsed_event.payload.fulfilled_quantity").alias("fulfilled_quantity"),
            col("parsed_event.payload.rejected_quantity").alias("rejected_quantity"),
            col("parsed_event.payload.risk_score").alias("risk_score"),
            col("eventhub_name"),
            col("eventhub_partition"),
            col("eventhub_offset"),
            col("eventhub_enqueued_timestamp"),
            col("_ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        )
        .withColumn(
            "supplier_performance_record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("event_id"),
                    col("performance_id"),
                    col("supplier_id"),
                    col("product_id"),
                    col("performance_month"),
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