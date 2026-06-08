import json
from pathlib import Path
from typing import Any, Dict, Set

import pandas as pd

from src.common.schema_validator import load_json_file, validate_json_event


def load_master_ids(master_data_dir: Path) -> Dict[str, Set[str]]:
    customers = pd.read_parquet(master_data_dir / "customers" / "customers.parquet")
    stores = pd.read_parquet(master_data_dir / "stores" / "stores.parquet")
    products = pd.read_parquet(master_data_dir / "products" / "products.parquet")

    return {
        "customer_ids": set(customers["customer_id"].astype(str)),
        "store_ids": set(stores["store_id"].astype(str)),
        "product_ids": set(products["product_id"].astype(str)),
    }


def validate_references(event: Dict[str, Any], master_ids: Dict[str, Set[str]]) -> None:
    payload = event["payload"]

    customer_id = payload["customer_id"]
    store_id = payload["store_id"]

    if customer_id not in master_ids["customer_ids"]:
        raise ValueError(f"Invalid customer_id: {customer_id}")

    if store_id not in master_ids["store_ids"]:
        raise ValueError(f"Invalid store_id: {store_id}")

    for item in payload["items"]:
        product_id = item["product_id"]

        if product_id not in master_ids["product_ids"]:
            raise ValueError(f"Invalid product_id: {product_id}")


def main() -> None:
    master_data_dir = Path("data/synthetic/master")
    event_file = Path("data/synthetic/events/orders/order_created_events.jsonl")
    schema_file = Path("schemas/events/order_created.schema.json")

    if not event_file.exists():
        raise FileNotFoundError(f"Event file not found: {event_file}")

    schema = load_json_file(str(schema_file))
    master_ids = load_master_ids(master_data_dir)

    event_count = 0

    with event_file.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            event = json.loads(line)

            try:
                validate_json_event(event, schema)
                validate_references(event, master_ids)
            except Exception as error:
                raise ValueError(f"Validation failed at line {line_number}: {error}") from error

            event_count += 1

    print(f"Validated {event_count:,} order_created events successfully.")
    print("Schema validation passed.")
    print("Referential integrity validation passed.")


if __name__ == "__main__":
    main()