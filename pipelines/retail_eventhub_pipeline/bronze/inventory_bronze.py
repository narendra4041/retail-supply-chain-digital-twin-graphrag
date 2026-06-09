from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit

from pipelines.retail_eventhub_pipeline.utils.config import (
    get_catalog_name,
    get_eventhub_config,
    get_schema_name,
)
from pipelines.retail_eventhub_pipeline.utils.eventhub import get_eventhub_kafka_options


ENVIRONMENT = "dev"

catalog_name = get_catalog_name(ENVIRONMENT)
bronze_schema = get_schema_name("bronze", ENVIRONMENT)

eventhub_config = get_eventhub_config("inventory_updated", ENVIRONMENT)

from pipelines.retail_eventhub_pipeline.utils.config import get_secret_config

secret_config = get_secret_config(ENVIRONMENT)

SECRET_SCOPE = secret_config["scope"]
SECRET_KEY = secret_config["keys"]["eventhub_consumer_connection_string"]


@dp.table(
    name=f"{catalog_name}.{bronze_schema}.inventory_events_raw",
    comment="Raw inventory_updated events ingested from Azure Event Hubs.",
)
def inventory_events_raw():
    kafka_options = get_eventhub_kafka_options(
        fully_qualified_namespace=eventhub_config["fully_qualified_namespace"],
        eventhub_name=eventhub_config["eventhub_name"],
        consumer_group=eventhub_config["consumer_group"],
        connection_string_secret_scope=SECRET_SCOPE,
        connection_string_secret_key=SECRET_KEY,
    )

    return (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .load()
        .select(
            col("key").cast("string").alias("event_key"),
            col("value").cast("string").alias("raw_payload"),
            col("topic").alias("eventhub_name"),
            col("partition").alias("eventhub_partition"),
            col("offset").alias("eventhub_offset"),
            col("timestamp").alias("eventhub_enqueued_timestamp"),
            col("timestampType").alias("eventhub_timestamp_type"),
        )
        .withColumn("_event_type", lit("inventory_updated"))
        .withColumn("_source_system", lit("azure_event_hubs"))
        .withColumn("_ingestion_timestamp", current_timestamp())
    )