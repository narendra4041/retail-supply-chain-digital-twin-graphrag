from __future__ import annotations

    
from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit

ENVIRONMENT = spark.conf.get("ENVIRONMENT", "dev")

catalog_name = spark.conf.get("CATALOG_NAME")
bronze_schema = spark.conf.get("BRONZE_SCHEMA")

EVENT_HUB_NAMESPACE = spark.conf.get("EVENT_HUB_NAMESPACE")
EVENT_HUB_NAME = spark.conf.get("EVENT_HUB_ORDERS_NAME")
EVENT_HUB_CONSUMER_GROUP = spark.conf.get("EVENT_HUB_ORDERS_CONSUMER_GROUP")

EVENT_HUB_SECRET_SCOPE = spark.conf.get("EVENT_HUB_SECRET_SCOPE")
EVENT_HUB_CONNECTION_SECRET_KEY = spark.conf.get("EVENT_HUB_CONNECTION_SECRET_KEY")

event_hub_connection_string = dbutils.secrets.get(
    scope=EVENT_HUB_SECRET_SCOPE,
    key=EVENT_HUB_CONNECTION_SECRET_KEY,
)


def build_eventhub_kafka_jaas_config(connection_string: str) -> str:
    return (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        "required "
        'username="$ConnectionString" '
        f'password="{connection_string}";'
    )


def get_eventhub_kafka_options(
    fully_qualified_namespace: str,
    eventhub_name: str,
    consumer_group: str,
    kafka_jaas_config: str,
) -> dict[str, str]:
    return {
        "kafka.bootstrap.servers": f"{fully_qualified_namespace}:9093",
        "subscribe": eventhub_name,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": kafka_jaas_config,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
        "kafka.group.id": consumer_group,
    }

kafka_jaas_config = build_eventhub_kafka_jaas_config(event_hub_connection_string)

@dp.table(
    name=f"{catalog_name}.{bronze_schema}.orders_events_raw",
    comment="Raw order_created events ingested from Azure Event Hubs.",
)
def orders_events_raw():
    kafka_options = get_eventhub_kafka_options(
        fully_qualified_namespace=EVENT_HUB_NAMESPACE,
        eventhub_name=EVENT_HUB_NAME,
        consumer_group=EVENT_HUB_CONSUMER_GROUP,
        kafka_jaas_config=kafka_jaas_config,
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
        .withColumn("_event_type", lit("order_created"))
        .withColumn("_source_system", lit("azure_event_hubs"))
        .withColumn("_ingestion_timestamp", current_timestamp())
    )