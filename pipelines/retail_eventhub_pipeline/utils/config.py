from __future__ import annotations

from src.common.config_loader import load_config


def get_pipeline_config(environment: str = "dev") -> dict:
    return load_config(environment)


def get_catalog_name(environment: str = "dev") -> str:
    config = get_pipeline_config(environment)
    return config["unity_catalog"]["catalog"]


def get_schema_name(schema_key: str, environment: str = "dev") -> str:
    config = get_pipeline_config(environment)
    return config["unity_catalog"]["schemas"][schema_key]


def get_eventhub_config(event_type: str, environment: str = "dev") -> dict:
    config = get_pipeline_config(environment)

    event_hubs = config["event_hubs"]
    event_config = event_hubs["events"][event_type]

    return {
        "fully_qualified_namespace": event_hubs["fully_qualified_namespace"],
        "eventhub_name": event_config["eventhub_name"],
        "consumer_group": event_hubs["consumer_groups"][event_type],
    }

def get_secret_config(environment: str = "dev") -> dict:
    config = get_pipeline_config(environment)
    return config["secrets"]