from __future__ import annotations


def get_required_conf(key: str) -> str:
    value = spark.conf.get(key, None)

    if value is None or str(value).strip() == "":
        raise ValueError(f"Required pipeline configuration is missing: {key}")

    return value


def get_catalog_name() -> str:
    return get_required_conf("CATALOG_NAME")


def get_schema_name(schema_key: str) -> str:
    mapping = {
        "bronze": "BRONZE_SCHEMA",
        "silver": "SILVER_SCHEMA",
        "gold": "GOLD_SCHEMA",
        "monitoring": "MONITORING_SCHEMA",
    }

    if schema_key not in mapping:
        raise ValueError(f"Unsupported schema key: {schema_key}")

    return get_required_conf(mapping[schema_key])


def get_eventhub_config(event_type: str) -> dict:
    mapping = {
        "order_created": {
            "eventhub_name": "EVENT_HUB_ORDERS_NAME",
            "consumer_group": "EVENT_HUB_ORDERS_CONSUMER_GROUP",
        },
        "inventory_updated": {
            "eventhub_name": "EVENT_HUB_INVENTORY_NAME",
            "consumer_group": "EVENT_HUB_INVENTORY_CONSUMER_GROUP",
        },
        "shipment_created": {
            "eventhub_name": "EVENT_HUB_SHIPMENTS_NAME",
            "consumer_group": "EVENT_HUB_SHIPMENTS_CONSUMER_GROUP",
        },
        "supplier_performance": {
            "eventhub_name": "EVENT_HUB_SUPPLIER_PERFORMANCE_NAME",
            "consumer_group": "EVENT_HUB_SUPPLIER_PERFORMANCE_CONSUMER_GROUP",
        },
    }

    if event_type not in mapping:
        raise ValueError(f"Unsupported event type: {event_type}")

    event_mapping = mapping[event_type]

    return {
        "fully_qualified_namespace": get_required_conf("EVENT_HUB_NAMESPACE"),
        "eventhub_name": get_required_conf(event_mapping["eventhub_name"]),
        "consumer_group": get_required_conf(event_mapping["consumer_group"]),
    }