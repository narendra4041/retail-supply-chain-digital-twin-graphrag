from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.common.schema_validator import load_json_file, validate_json_event


ORDER_CHANNELS = ["store", "ecommerce", "mobile_app"]
ORDER_STATUSES = ["created", "paid"]
PAYMENT_METHODS = ["card", "invoice", "wallet", "gift_card"]
CURRENCIES = ["SEK", "EUR"]
INVENTORY_MOVEMENT_TYPES = [
    "sale",
    "return",
    "replenishment",
    "shipment_out",
    "shipment_in",
    "adjustment",
    "damage",
    "reservation",
    "reservation_release",
]
CARRIERS = ["DHL", "PostNord", "DB Schenker", "Maersk", "UPS", "FedEx"]
SHIPMENT_STATUSES = ["created", "in_transit", "delivered", "delayed"]

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def current_month() -> str:
    today = date.today()
    return f"{today.year}-{today.month:02d}"

def load_master_dataset(master_data_dir: Path, dataset_name: str) -> pd.DataFrame:
    file_path = master_data_dir / dataset_name / f"{dataset_name}.parquet"

    if not file_path.exists():
        raise FileNotFoundError(f"Master data file not found: {file_path}")

    return pd.read_parquet(file_path)


def load_master_data(master_data_dir: Path) -> Dict[str, pd.DataFrame]:
    return {
        "customers": load_master_dataset(master_data_dir, "customers"),
        "products": load_master_dataset(master_data_dir, "products"),
        "stores": load_master_dataset(master_data_dir, "stores"),
        "warehouses": load_master_dataset(master_data_dir, "warehouses"),
        "suppliers": load_master_dataset(master_data_dir, "suppliers"),
    }


def choose_random_products(products: pd.DataFrame, min_items: int = 1, max_items: int = 5) -> pd.DataFrame:
    active_products = products[products["active_flag"] == True]

    if active_products.empty:
        raise ValueError("No active products found in master data.")

    number_of_items = random.randint(min_items, max_items)
    number_of_items = min(number_of_items, len(active_products))

    return active_products.sample(n=number_of_items)


def build_order_items(selected_products: pd.DataFrame, order_sequence: int) -> List[Dict[str, Any]]:
    items = []

    for item_index, (_, product) in enumerate(selected_products.iterrows(), start=1):
        quantity = random.randint(1, 4)
        unit_price = float(product["unit_price"])
        discount_pct = random.choice([0.0, 0.0, 0.05, 0.10, 0.15])

        line_total = round(quantity * unit_price * (1 - discount_pct), 2)

        items.append(
            {
                "order_item_id": f"OI{order_sequence:010d}_{item_index:02d}",
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_pct": discount_pct,
                "line_total": line_total,
            }
        )

    return items


def build_order_created_event(
    customers: pd.DataFrame,
    stores: pd.DataFrame,
    products: pd.DataFrame,
    order_sequence: int,
) -> Dict[str, Any]:
    customer = customers.sample(n=1).iloc[0]
    store = stores.sample(n=1).iloc[0]
    selected_products = choose_random_products(products)

    items = build_order_items(selected_products, order_sequence)
    order_total = round(sum(item["line_total"] for item in items), 2)

    order_id = f"ORD{order_sequence:010d}"
    event_id = f"evt_{uuid.uuid4()}"
    correlation_id = f"corr_{uuid.uuid4()}"

    order_channel = random.choice(ORDER_CHANNELS)

    event = {
        "event_id": event_id,
        "event_type": "order_created",
        "event_version": "1.0",
        "event_timestamp": utc_now_iso(),
        "source_system": "ecommerce" if order_channel in ["ecommerce", "mobile_app"] else "pos",
        "correlation_id": correlation_id,
        "payload": {
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "store_id": store["store_id"],
            "order_channel": order_channel,
            "order_status": random.choice(ORDER_STATUSES),
            "payment_method": random.choice(PAYMENT_METHODS),
            "currency": random.choice(CURRENCIES),
            "order_total": order_total,
            "items": items,
        },
    }

    return event

def build_inventory_updated_event(
    products: pd.DataFrame,
    warehouses: pd.DataFrame,
    stores: pd.DataFrame,
    inventory_sequence: int,
) -> Dict[str, Any]:
    active_products = products[products["active_flag"] == True]

    if active_products.empty:
        raise ValueError("No active products found in master data.")

    product = active_products.sample(n=1).iloc[0]

    location_type = random.choice(["warehouse", "store"])

    if location_type == "warehouse":
        location = warehouses.sample(n=1).iloc[0]
        location_id = location["warehouse_id"]
        source_system = "warehouse_management_system"
        base_stock = random.randint(500, 20000)
        reorder_point = random.randint(300, 1500)
        safety_stock = random.randint(200, 1000)
    else:
        location = stores.sample(n=1).iloc[0]
        location_id = location["store_id"]
        source_system = "store_inventory_system"
        base_stock = random.randint(10, 1000)
        reorder_point = random.randint(10, 100)
        safety_stock = random.randint(5, 80)

    movement_type = random.choice(INVENTORY_MOVEMENT_TYPES)

    if movement_type in ["sale", "shipment_out", "damage", "reservation"]:
        quantity_change = -random.randint(1, 50)
    elif movement_type in ["return", "replenishment", "shipment_in", "reservation_release"]:
        quantity_change = random.randint(1, 300)
    else:
        quantity_change = random.randint(-50, 50)

    stock_on_hand_after = max(0, base_stock + quantity_change)
    reserved_stock_after = random.randint(0, min(100, stock_on_hand_after))

    event = {
        "event_id": f"evt_{uuid.uuid4()}",
        "event_type": "inventory_updated",
        "event_version": "1.0",
        "event_timestamp": utc_now_iso(),
        "source_system": source_system,
        "correlation_id": f"corr_{uuid.uuid4()}",
        "payload": {
            "inventory_event_id": f"INV_EVT{inventory_sequence:010d}",
            "product_id": product["product_id"],
            "location_type": location_type,
            "location_id": location_id,
            "movement_type": movement_type,
            "quantity_change": quantity_change,
            "stock_on_hand_after": stock_on_hand_after,
            "reserved_stock_after": reserved_stock_after,
            "reorder_point": reorder_point,
            "safety_stock": safety_stock,
        },
    }

    return event

def build_shipment_created_event(
    suppliers: pd.DataFrame,
    products: pd.DataFrame,
    warehouses: pd.DataFrame,
    stores: pd.DataFrame,
    shipment_sequence: int,
) -> Dict[str, Any]:
    active_products = products[products["active_flag"] == True]

    if active_products.empty:
        raise ValueError("No active products found in master data.")

    shipment_type = random.choice(["supplier_to_warehouse", "warehouse_to_store"])
    product = active_products.sample(n=1).iloc[0]

    shipment_date = date.today()
    expected_delivery_date = shipment_date + timedelta(days=random.randint(2, 14))

    shipment_status = random.choice(SHIPMENT_STATUSES)

    delay_days = 0
    actual_delivery_date = None

    if shipment_status == "delivered":
        delay_days = random.choice([0, 0, 0, 1, 2, 3])
        actual_delivery_date = expected_delivery_date + timedelta(days=delay_days)

    elif shipment_status == "delayed":
        delay_days = random.randint(1, 10)
        actual_delivery_date = None

    if shipment_type == "supplier_to_warehouse":
        # Important: product already has a supplier_id in master data.
        # So we use the linked supplier instead of choosing any random supplier.
        supplier_id = product["supplier_id"]

        if supplier_id not in set(suppliers["supplier_id"].astype(str)):
            raise ValueError(f"Product has invalid supplier_id: {supplier_id}")

        destination_warehouse = warehouses.sample(n=1).iloc[0]

        source_type = "supplier"
        source_id = supplier_id
        destination_type = "warehouse"
        destination_id = destination_warehouse["warehouse_id"]
        quantity = random.randint(100, 5000)

    else:
        source_warehouse = warehouses.sample(n=1).iloc[0]
        destination_store = stores.sample(n=1).iloc[0]

        source_type = "warehouse"
        source_id = source_warehouse["warehouse_id"]
        destination_type = "store"
        destination_id = destination_store["store_id"]
        quantity = random.randint(10, 1000)

    event = {
        "event_id": f"evt_{uuid.uuid4()}",
        "event_type": "shipment_created",
        "event_version": "1.0",
        "event_timestamp": utc_now_iso(),
        "source_system": "transport_management_system",
        "correlation_id": f"corr_{uuid.uuid4()}",
        "payload": {
            "shipment_id": f"SHIP{shipment_sequence:010d}",
            "shipment_type": shipment_type,
            "source_type": source_type,
            "source_id": source_id,
            "destination_type": destination_type,
            "destination_id": destination_id,
            "product_id": product["product_id"],
            "quantity": quantity,
            "shipment_date": shipment_date.isoformat(),
            "expected_delivery_date": expected_delivery_date.isoformat(),
            "actual_delivery_date": actual_delivery_date.isoformat() if actual_delivery_date else None,
            "shipment_status": shipment_status,
            "carrier": random.choice(CARRIERS),
            "delay_days": delay_days,
        },
    }

    return event

def build_supplier_performance_event(
    suppliers: pd.DataFrame,
    products: pd.DataFrame,
    performance_sequence: int,
) -> Dict[str, Any]:
    active_products = products[products["active_flag"] == True]

    if active_products.empty:
        raise ValueError("No active products found in master data.")

    product = active_products.sample(n=1).iloc[0]
    supplier_id = product["supplier_id"]

    supplier_ids = set(suppliers["supplier_id"].astype(str))

    if supplier_id not in supplier_ids:
        raise ValueError(f"Product has invalid supplier_id: {supplier_id}")

    on_time_delivery_rate = round(random.uniform(0.60, 0.99), 4)
    defect_rate = round(random.uniform(0.001, 0.12), 4)
    avg_delay_days = round(random.uniform(0.0, 12.0), 2)

    fulfilled_quantity = random.randint(500, 50000)
    rejected_quantity = int(fulfilled_quantity * defect_rate)

    # Simple risk formula for synthetic data.
    # Higher delay, higher defect rate, and lower on-time rate increase risk.
    risk_score = (
        (1 - on_time_delivery_rate) * 0.45
        + min(avg_delay_days / 15, 1) * 0.35
        + min(defect_rate / 0.15, 1) * 0.20
    )
    risk_score = round(min(max(risk_score, 0), 1), 4)

    event = {
        "event_id": f"evt_{uuid.uuid4()}",
        "event_type": "supplier_performance",
        "event_version": "1.0",
        "event_timestamp": utc_now_iso(),
        "source_system": random.choice(
            [
                "supplier_management_system",
                "procurement_system",
                "quality_management_system",
            ]
        ),
        "correlation_id": f"corr_{uuid.uuid4()}",
        "payload": {
            "performance_id": f"SUP_PERF{performance_sequence:010d}",
            "supplier_id": supplier_id,
            "product_id": product["product_id"],
            "performance_month": current_month(),
            "on_time_delivery_rate": on_time_delivery_rate,
            "defect_rate": defect_rate,
            "avg_delay_days": avg_delay_days,
            "fulfilled_quantity": fulfilled_quantity,
            "rejected_quantity": rejected_quantity,
            "risk_score": risk_score,
        },
    }

    return event

def write_jsonl_event(event: Dict[str, Any], output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event) + "\n")


def generate_order_events_local(
    master_data_dir: Path,
    output_file: Path,
    schema_file: Path,
    num_events: int,
    seed: int,
) -> None:
    random.seed(seed)

    master_data = load_master_data(master_data_dir)
    schema = load_json_file(str(schema_file))

    for sequence in range(1, num_events + 1):
        event = build_order_created_event(
            customers=master_data["customers"],
            stores=master_data["stores"],
            products=master_data["products"],
            order_sequence=sequence,
        )

        validate_json_event(event, schema)
        write_jsonl_event(event, output_file)

    print(f"Wrote {num_events:,} order_created events to {output_file}")

def generate_inventory_events_local(
    master_data_dir: Path,
    output_file: Path,
    schema_file: Path,
    num_events: int,
    seed: int,
) -> None:
    random.seed(seed)

    master_data = load_master_data(master_data_dir)
    schema = load_json_file(str(schema_file))

    for sequence in range(1, num_events + 1):
        event = build_inventory_updated_event(
            products=master_data["products"],
            warehouses=master_data["warehouses"],
            stores=master_data["stores"],
            inventory_sequence=sequence,
        )

        validate_json_event(event, schema)
        write_jsonl_event(event, output_file)

    print(f"Wrote {num_events:,} inventory_updated events to {output_file}")

def generate_shipment_events_local(
    master_data_dir: Path,
    output_file: Path,
    schema_file: Path,
    num_events: int,
    seed: int,
) -> None:
    random.seed(seed)

    master_data = load_master_data(master_data_dir)
    schema = load_json_file(str(schema_file))

    for sequence in range(1, num_events + 1):
        event = build_shipment_created_event(
            suppliers=master_data["suppliers"],
            products=master_data["products"],
            warehouses=master_data["warehouses"],
            stores=master_data["stores"],
            shipment_sequence=sequence,
        )

        validate_json_event(event, schema)
        write_jsonl_event(event, output_file)

    print(f"Wrote {num_events:,} shipment_created events to {output_file}")

def generate_supplier_performance_events_local(
    master_data_dir: Path,
    output_file: Path,
    schema_file: Path,
    num_events: int,
    seed: int,
) -> None:
    random.seed(seed)

    master_data = load_master_data(master_data_dir)
    schema = load_json_file(str(schema_file))

    for sequence in range(1, num_events + 1):
        event = build_supplier_performance_event(
            suppliers=master_data["suppliers"],
            products=master_data["products"],
            performance_sequence=sequence,
        )

        validate_json_event(event, schema)
        write_jsonl_event(event, output_file)

    print(f"Wrote {num_events:,} supplier_performance events to {output_file}")

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate production-style retail transactional events."
    )

    parser.add_argument(
        "--mode",
        choices=["local"],
        default="local",
        help="Producer mode. Currently supports local JSONL output.",
    )
    parser.add_argument(
        "--master-data-dir",
        default="data/synthetic/master",
        help="Directory containing generated master data Parquet files.",
    )
    parser.add_argument(
        "--output-file",
        default="data/synthetic/events/orders/order_created_events.jsonl",
        help="Output JSONL file for local mode.",
    )
    parser.add_argument(
        "--schema-file",
        default="schemas/events/order_created.schema.json",
        help="JSON schema file for order_created events.",
    )
    parser.add_argument(
        "--num-events",
        type=int,
        default=100,
        help="Number of events to generate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--event-type",
        choices=[
            "order_created",
            "inventory_updated",
            "shipment_created",
            "supplier_performance",
        ],
        default="order_created",
        help="Type of event to generate.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.mode != "local":
        raise ValueError("Only local mode is currently supported.")

    if args.event_type == "order_created":
        generate_order_events_local(
            master_data_dir=Path(args.master_data_dir),
            output_file=Path(args.output_file),
            schema_file=Path(args.schema_file),
            num_events=args.num_events,
            seed=args.seed,
        )

    elif args.event_type == "inventory_updated":
        generate_inventory_events_local(
            master_data_dir=Path(args.master_data_dir),
            output_file=Path(args.output_file),
            schema_file=Path(args.schema_file),
            num_events=args.num_events,
            seed=args.seed,
        )

    elif args.event_type == "shipment_created":
        generate_shipment_events_local(
            master_data_dir=Path(args.master_data_dir),
            output_file=Path(args.output_file),
            schema_file=Path(args.schema_file),
            num_events=args.num_events,
            seed=args.seed,
        )
    
    elif args.event_type == "supplier_performance":
        generate_supplier_performance_events_local(
            master_data_dir=Path(args.master_data_dir),
            output_file=Path(args.output_file),
            schema_file=Path(args.schema_file),
            num_events=args.num_events,
            seed=args.seed,
        )


if __name__ == "__main__":
    main()