import json
from pathlib import Path
from typing import Any, Dict, Set

import pandas as pd

from src.common.schema_validator import load_json_file, validate_json_event


def load_master_ids(master_data_dir: Path) -> Dict[str, Set[str]]:
    suppliers = pd.read_parquet(master_data_dir / "suppliers" / "suppliers.parquet")
    products = pd.read_parquet(master_data_dir / "products" / "products.parquet")

    return {
        "supplier_ids": set(suppliers["supplier_id"].astype(str)),
        "product_ids": set(products["product_id"].astype(str)),
    }


def load_product_supplier_map(master_data_dir: Path) -> Dict[str, str]:
    products = pd.read_parquet(master_data_dir / "products" / "products.parquet")

    return dict(
        zip(
            products["product_id"].astype(str),
            products["supplier_id"].astype(str),
        )
    )


def validate_references(
    event: Dict[str, Any],
    master_ids: Dict[str, Set[str]],
    product_supplier_map: Dict[str, str],
) -> None:
    payload = event["payload"]

    supplier_id = payload["supplier_id"]
    product_id = payload["product_id"]

    if supplier_id not in master_ids["supplier_ids"]:
        raise ValueError(f"Invalid supplier_id: {supplier_id}")

    if product_id not in master_ids["product_ids"]:
        raise ValueError(f"Invalid product_id: {product_id}")

    expected_supplier_id = product_supplier_map[product_id]

    if supplier_id != expected_supplier_id:
        raise ValueError(
            f"Supplier mismatch for product_id={product_id}. "
            f"Expected supplier_id={expected_supplier_id}, got supplier_id={supplier_id}"
        )


def main() -> None:
    master_data_dir = Path("data/synthetic/master")
    event_file = Path(
        "data/synthetic/events/supplier_performance/supplier_performance_events.jsonl"
    )
    schema_file = Path("schemas/events/supplier_performance.schema.json")

    if not event_file.exists():
        raise FileNotFoundError(f"Event file not found: {event_file}")

    schema = load_json_file(str(schema_file))
    master_ids = load_master_ids(master_data_dir)
    product_supplier_map = load_product_supplier_map(master_data_dir)

    event_count = 0

    with event_file.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            event = json.loads(line)

            try:
                validate_json_event(event, schema)
                validate_references(event, master_ids, product_supplier_map)
            except Exception as error:
                raise ValueError(f"Validation failed at line {line_number}: {error}") from error

            event_count += 1

    print(f"Validated {event_count:,} supplier_performance events successfully.")
    print("Schema validation passed.")
    print("Referential integrity validation passed.")
    print("Product-to-supplier relationship validation passed.")


if __name__ == "__main__":
    main()