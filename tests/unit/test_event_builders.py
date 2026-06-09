from pathlib import Path

from src.common.schema_validator import load_json_file, validate_json_event
from src.data_generation.retail_event_producer import (
    build_events,
)


MASTER_DATA_DIR = Path("data/synthetic/master")


def test_build_order_created_events_are_schema_valid() -> None:
    schema = load_json_file("schemas/events/order_created.schema.json")

    events = build_events(
        event_type="order_created",
        master_data_dir=MASTER_DATA_DIR,
        num_events=5,
        seed=42,
    )

    assert len(events) == 5

    for event in events:
        validate_json_event(event, schema)
        assert event["event_type"] == "order_created"
        assert event["payload"]["items"]


def test_build_inventory_updated_events_are_schema_valid() -> None:
    schema = load_json_file("schemas/events/inventory_updated.schema.json")

    events = build_events(
        event_type="inventory_updated",
        master_data_dir=MASTER_DATA_DIR,
        num_events=5,
        seed=42,
    )

    assert len(events) == 5

    for event in events:
        validate_json_event(event, schema)
        assert event["event_type"] == "inventory_updated"


def test_build_shipment_created_events_are_schema_valid() -> None:
    schema = load_json_file("schemas/events/shipment_created.schema.json")

    events = build_events(
        event_type="shipment_created",
        master_data_dir=MASTER_DATA_DIR,
        num_events=5,
        seed=42,
    )

    assert len(events) == 5

    for event in events:
        validate_json_event(event, schema)
        assert event["event_type"] == "shipment_created"


def test_build_supplier_performance_events_are_schema_valid() -> None:
    schema = load_json_file("schemas/events/supplier_performance.schema.json")

    events = build_events(
        event_type="supplier_performance",
        master_data_dir=MASTER_DATA_DIR,
        num_events=5,
        seed=42,
    )

    assert len(events) == 5

    for event in events:
        validate_json_event(event, schema)
        assert event["event_type"] == "supplier_performance"