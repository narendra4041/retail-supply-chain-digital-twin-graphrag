from __future__ import annotations

from typing import Dict


def get_eventhub_kafka_bootstrap_servers(fully_qualified_namespace: str) -> str:
    return f"{fully_qualified_namespace}:9093"


def get_eventhub_kafka_options(
    fully_qualified_namespace: str,
    eventhub_name: str,
    consumer_group: str,
    connection_string_secret_scope: str,
    connection_string_secret_key: str,
) -> Dict[str, str]:
    """
    Build Kafka options for Azure Event Hubs Kafka-compatible endpoint.

    Current Databricks Lakeflow Event Hubs docs use the Kafka-compatible endpoint.
    The official documented authentication pattern for Lakeflow is SAS-based.

    The connection string must be stored in a Databricks secret scope, not hardcoded.
    """

    connection_string = dbutils.secrets.get(
        scope=connection_string_secret_scope,
        key=connection_string_secret_key,
    )

    jaas_config = (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        f'required username="$ConnectionString" password="{connection_string}";'
    )

    return {
        "kafka.bootstrap.servers": get_eventhub_kafka_bootstrap_servers(
            fully_qualified_namespace
        ),
        "subscribe": eventhub_name,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas_config,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
        "kafka.group.id": consumer_group,
    }