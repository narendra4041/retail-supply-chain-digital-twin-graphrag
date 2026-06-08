from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
from faker import Faker


fake = Faker()


COUNTRIES = [
    {"country": "Sweden", "region": "Nordics", "cities": ["Stockholm", "Gothenburg", "Malmö", "Helsingborg"]},
    {"country": "Norway", "region": "Nordics", "cities": ["Oslo", "Bergen", "Trondheim"]},
    {"country": "Denmark", "region": "Nordics", "cities": ["Copenhagen", "Aarhus", "Odense"]},
    {"country": "Germany", "region": "Europe", "cities": ["Berlin", "Hamburg", "Munich"]},
    {"country": "Netherlands", "region": "Europe", "cities": ["Amsterdam", "Rotterdam", "Utrecht"]},
    {"country": "Poland", "region": "Europe", "cities": ["Warsaw", "Krakow", "Gdansk"]},
]

PRODUCT_CATEGORIES = {
    "Furniture": ["Chairs", "Tables", "Sofas", "Beds", "Wardrobes"],
    "Home Decor": ["Lighting", "Rugs", "Curtains", "Mirrors", "Wall Art"],
    "Kitchen": ["Cookware", "Dinnerware", "Storage", "Appliances"],
    "Office": ["Desks", "Office Chairs", "Shelving", "Accessories"],
    "Outdoor": ["Garden Chairs", "Outdoor Tables", "BBQ", "Planters"],
}

BRANDS = [
    "NordicLiving",
    "HomeEase",
    "UrbanNest",
    "ScandiSpace",
    "EcoHome",
    "ModernHouse",
]

SUPPLIER_TYPES = ["manufacturer", "distributor", "wholesaler"]
CUSTOMER_SEGMENTS = ["household", "student", "small_business", "interior_designer"]
LOYALTY_TIERS = ["bronze", "silver", "gold", "platinum"]
STORE_TYPES = ["flagship", "standard", "outlet", "small_format"]


def ensure_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)


def random_country_city() -> Dict[str, str]:
    country_info = random.choice(COUNTRIES)
    return {
        "country": country_info["country"],
        "region": country_info["region"],
        "city": random.choice(country_info["cities"]),
    }


def generate_suppliers(num_suppliers: int) -> pd.DataFrame:
    rows = []

    for index in range(1, num_suppliers + 1):
        location = random_country_city()
        supplier_id = f"SUP{index:05d}"

        contract_start = fake.date_between(start_date="-3y", end_date="-1y")
        contract_end = contract_start + timedelta(days=random.randint(730, 1825))

        rows.append(
            {
                "supplier_id": supplier_id,
                "supplier_name": fake.company(),
                "country": location["country"],
                "region": location["region"],
                "supplier_type": random.choice(SUPPLIER_TYPES),
                "lead_time_days": random.randint(3, 45),
                "reliability_score": round(random.uniform(0.70, 0.99), 4),
                "quality_score": round(random.uniform(0.75, 0.99), 4),
                "contract_start_date": contract_start,
                "contract_end_date": contract_end,
                "preferred_supplier_flag": random.choice([True, False, False]),
                "created_at": pd.Timestamp.utcnow(),
                "updated_at": pd.Timestamp.utcnow(),
            }
        )

    return pd.DataFrame(rows)


def generate_products(num_products: int, suppliers: pd.DataFrame) -> pd.DataFrame:
    rows = []
    supplier_ids = suppliers["supplier_id"].tolist()

    for index in range(1, num_products + 1):
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        sub_category = random.choice(PRODUCT_CATEGORIES[category])

        unit_cost = round(random.uniform(40, 5000), 2)
        margin_multiplier = random.uniform(1.25, 2.8)
        unit_price = round(unit_cost * margin_multiplier, 2)

        product_id = f"PROD{index:06d}"

        rows.append(
            {
                "product_id": product_id,
                "product_name": f"{random.choice(BRANDS)} {sub_category} {index}",
                "category": category,
                "sub_category": sub_category,
                "brand": random.choice(BRANDS),
                "unit_price": unit_price,
                "unit_cost": unit_cost,
                "supplier_id": random.choice(supplier_ids),
                "weight_kg": round(random.uniform(0.2, 80.0), 3),
                "volume_m3": round(random.uniform(0.01, 4.0), 4),
                "active_flag": random.choice([True, True, True, False]),
                "created_at": pd.Timestamp.utcnow(),
                "updated_at": pd.Timestamp.utcnow(),
            }
        )

    return pd.DataFrame(rows)


def generate_warehouses(num_warehouses: int) -> pd.DataFrame:
    rows = []

    for index in range(1, num_warehouses + 1):
        location = random_country_city()
        warehouse_id = f"WH{index:04d}"

        rows.append(
            {
                "warehouse_id": warehouse_id,
                "warehouse_name": f"{location['city']} Distribution Center",
                "country": location["country"],
                "city": location["city"],
                "region": location["region"],
                "capacity_units": random.randint(100_000, 1_500_000),
                "current_utilization_pct": round(random.uniform(0.35, 0.92), 4),
                "created_at": pd.Timestamp.utcnow(),
                "updated_at": pd.Timestamp.utcnow(),
            }
        )

    return pd.DataFrame(rows)


def generate_stores(num_stores: int, warehouses: pd.DataFrame) -> pd.DataFrame:
    rows = []
    warehouse_ids = warehouses["warehouse_id"].tolist()

    for index in range(1, num_stores + 1):
        location = random_country_city()
        store_id = f"ST{index:04d}"

        rows.append(
            {
                "store_id": store_id,
                "store_name": f"{location['city']} Retail Store {index}",
                "country": location["country"],
                "city": location["city"],
                "region": location["region"],
                "store_type": random.choice(STORE_TYPES),
                "size_sq_m": random.randint(800, 18_000),
                "warehouse_id": random.choice(warehouse_ids),
                "created_at": pd.Timestamp.utcnow(),
                "updated_at": pd.Timestamp.utcnow(),
            }
        )

    return pd.DataFrame(rows)


def generate_customers(num_customers: int) -> pd.DataFrame:
    rows = []

    for index in range(1, num_customers + 1):
        location = random_country_city()
        customer_id = f"CUST{index:07d}"

        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": fake.name(),
                "country": location["country"],
                "city": location["city"],
                "customer_segment": random.choice(CUSTOMER_SEGMENTS),
                "loyalty_tier": random.choice(LOYALTY_TIERS),
                "signup_date": fake.date_between(start_date="-5y", end_date="today"),
                "created_at": pd.Timestamp.utcnow(),
                "updated_at": pd.Timestamp.utcnow(),
            }
        )

    return pd.DataFrame(rows)


def write_parquet(df: pd.DataFrame, output_dir: Path, dataset_name: str) -> None:
    dataset_dir = output_dir / dataset_name
    dataset_dir.mkdir(parents=True, exist_ok=True)

    output_file = dataset_dir / f"{dataset_name}.parquet"
    df.to_parquet(output_file, index=False, engine="pyarrow")

    print(f"Wrote {len(df):,} records to {output_file}")


def generate_all_master_data(
    output_dir: Path,
    num_suppliers: int,
    num_products: int,
    num_warehouses: int,
    num_stores: int,
    num_customers: int,
    seed: int,
) -> None:
    random.seed(seed)
    Faker.seed(seed)

    ensure_output_dir(output_dir)

    suppliers = generate_suppliers(num_suppliers)
    products = generate_products(num_products, suppliers)
    warehouses = generate_warehouses(num_warehouses)
    stores = generate_stores(num_stores, warehouses)
    customers = generate_customers(num_customers)

    write_parquet(suppliers, output_dir, "suppliers")
    write_parquet(products, output_dir, "products")
    write_parquet(warehouses, output_dir, "warehouses")
    write_parquet(stores, output_dir, "stores")
    write_parquet(customers, output_dir, "customers")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic retail supply chain master data as Parquet files."
    )

    parser.add_argument(
        "--output-dir",
        default="data/synthetic/master",
        help="Output directory for generated Parquet files.",
    )
    parser.add_argument("--num-suppliers", type=int, default=50)
    parser.add_argument("--num-products", type=int, default=500)
    parser.add_argument("--num-warehouses", type=int, default=10)
    parser.add_argument("--num-stores", type=int, default=100)
    parser.add_argument("--num-customers", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    generate_all_master_data(
        output_dir=Path(args.output_dir),
        num_suppliers=args.num_suppliers,
        num_products=args.num_products,
        num_warehouses=args.num_warehouses,
        num_stores=args.num_stores,
        num_customers=args.num_customers,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()