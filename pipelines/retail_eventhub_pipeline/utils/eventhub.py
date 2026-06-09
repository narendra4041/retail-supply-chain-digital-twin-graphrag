from __future__ import annotations

from typing import Dict


def get_eventhub_kafka_bootstrap_servers(fully_qualified_namespace: str) -> str:
    return f"{fully_qualified_namespace}:9093"


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
) -> Dict[str, str]:
    return {
        "kafka.bootstrap.servers": get_eventhub_kafka_bootstrap_servers(
            fully_qualified_namespace
        ),
        "subscribe": eventhub_name,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": kafka_jaas_config,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
        "kafka.group.id": consumer_group,
    }