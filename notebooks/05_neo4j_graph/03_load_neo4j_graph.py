# Databricks notebook source
# MAGIC %pip install neo4j

# COMMAND ----------

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List
import math

from neo4j import GraphDatabase
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

# This notebook performs a full-snapshot UPSERT load from Databricks to Neo4j.
#
# Production source pattern:
# - Silver master tables are the source of truth for stable business entities.
# - Gold views are used for metrics, risks, dependencies, and relationships.
#
# Important:
# - This version is idempotent because it uses MERGE.
# - This version is not incremental yet.
# - This version does not soft-delete missing entities yet.
# - Incremental load and soft delete will be added later as separate production enhancements.

dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")
dbutils.widgets.text("secret_scope", "")
dbutils.widgets.text("batch_size", "500")

# COMMAND ----------

def get_required_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"Required notebook parameter '{name}' is missing.")
    return value


catalog_name = get_required_widget("catalog_name")
silver_schema = get_required_widget("silver_schema")
gold_schema = get_required_widget("gold_schema")
secret_scope = get_required_widget("secret_scope")
batch_size = int(get_required_widget("batch_size"))

print(f"Catalog: {catalog_name}")
print(f"Silver schema: {silver_schema}")
print(f"Gold schema: {gold_schema}")
print(f"Batch size: {batch_size}")

# COMMAND ----------

neo4j_uri = dbutils.secrets.get(scope=secret_scope, key="neo4j-uri")
neo4j_username = dbutils.secrets.get(scope=secret_scope, key="neo4j-username")
neo4j_password = dbutils.secrets.get(scope=secret_scope, key="neo4j-password")
neo4j_database = dbutils.secrets.get(scope=secret_scope, key="neo4j-database")

print("Neo4j secrets loaded successfully.")
print(f"Neo4j database: {neo4j_database}")

# COMMAND ----------

driver = GraphDatabase.driver(
    neo4j_uri,
    auth=(neo4j_username, neo4j_password),
)

driver.verify_connectivity()
print("Neo4j connectivity verified.")

# COMMAND ----------

def silver_table_name(table: str) -> str:
    return f"{catalog_name}.{silver_schema}.{table}"


def gold_table_name(table: str) -> str:
    return f"{catalog_name}.{gold_schema}.{table}"


def read_silver_table(table: str) -> DataFrame:
    full_name = silver_table_name(table)
    print(f"Reading Silver table: {full_name}")
    return spark.table(full_name)


def read_gold_table(table: str) -> DataFrame:
    full_name = gold_table_name(table)
    print(f"Reading Gold table: {full_name}")
    return spark.table(full_name)


def ensure_columns(df: DataFrame, required_columns: List[str]) -> DataFrame:
    result_df = df
    for column_name in required_columns:
        if column_name not in result_df.columns:
            result_df = result_df.withColumn(column_name, F.lit(None))
    return result_df.select(*required_columns)


def rename_if_present(df: DataFrame, old_name: str, new_name: str) -> DataFrame:
    if old_name in df.columns and new_name not in df.columns:
        return df.withColumnRenamed(old_name, new_name)
    return df


def normalize_warehouse_columns(df: DataFrame) -> DataFrame:
    # Gold view uses business-friendly names:
    # serving_warehouse_id / serving_warehouse_name.
    # Neo4j graph uses generic Warehouse properties:
    # warehouse_id / warehouse_name.
    result_df = rename_if_present(df, "serving_warehouse_id", "warehouse_id")
    result_df = rename_if_present(result_df, "serving_warehouse_name", "warehouse_name")
    return result_df


def add_alias_if_present(
    df: DataFrame,
    source_column: str,
    target_column: str,
) -> DataFrame:
    """Create target_column from source_column when target_column is missing."""
    if source_column in df.columns and target_column not in df.columns:
        return df.withColumn(target_column, F.col(source_column))
    return df


def normalize_supplier_master_columns(df: DataFrame) -> DataFrame:
    # silver.suppliers uses country / region.
    # Neo4j Supplier keeps these as country / region.
    result_df = add_alias_if_present(df, "supplier_country", "country")
    result_df = add_alias_if_present(result_df, "supplier_region", "region")
    return result_df


def normalize_store_master_columns(df: DataFrame) -> DataFrame:
    # silver.stores usually uses generic location columns:
    # country / city / region.
    # Neo4j Store properties use store_country / store_city / store_region
    # to avoid ambiguity when nodes are viewed together.
    result_df = add_alias_if_present(df, "country", "store_country")
    result_df = add_alias_if_present(result_df, "city", "store_city")
    result_df = add_alias_if_present(result_df, "region", "store_region")
    return result_df


def normalize_warehouse_master_columns(df: DataFrame) -> DataFrame:
    # silver.warehouses usually uses generic location columns:
    # country / city / region.
    # Neo4j Warehouse properties use warehouse_country / warehouse_city / warehouse_region.
    result_df = add_alias_if_present(df, "country", "warehouse_country")
    result_df = add_alias_if_present(result_df, "city", "warehouse_city")
    result_df = add_alias_if_present(result_df, "region", "warehouse_region")
    return result_df


def normalize_product_master_columns(df: DataFrame) -> DataFrame:
    # Keep compatibility with common alternative naming patterns.
    result_df = add_alias_if_present(df, "subcategory", "sub_category")
    result_df = add_alias_if_present(result_df, "product_category", "category")
    result_df = add_alias_if_present(result_df, "product_sub_category", "sub_category")
    return result_df


def normalize_customer_master_columns(df: DataFrame) -> DataFrame:
    # If customer_name is not present, we build it later from first_name + last_name.
    return df


def normalize_supplier_risk_columns(df: DataFrame) -> DataFrame:
    # Gold supplier_risk_score should use risk_band, but support supplier_risk_band too.
    result_df = add_alias_if_present(df, "supplier_risk_band", "risk_band")
    result_df = add_alias_if_present(result_df, "supplier_risk_score", "computed_supplier_risk_score")
    return result_df


def normalize_stockout_columns(df: DataFrame) -> DataFrame:
    # Some versions may expose store_id / warehouse_id instead of location_id.
    result_df = df
    if "location_id" not in result_df.columns:
        if "store_id" in result_df.columns:
            result_df = result_df.withColumn("location_id", F.col("store_id"))
        elif "warehouse_id" in result_df.columns:
            result_df = result_df.withColumn("location_id", F.col("warehouse_id"))
    return result_df


def sanitize_value(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (datetime, date)):
        return value.isoformat()

    return value


def sanitize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {key: sanitize_value(value) for key, value in row.items()}


def write_batch(tx, cypher: str, rows: List[Dict[str, Any]]) -> None:
    tx.run(cypher, rows=rows).consume()


def write_df_to_neo4j(
    df: DataFrame,
    cypher: str,
    operation_name: str,
    batch_size: int = 500,
) -> int:
    total_rows = 0
    current_batch: List[Dict[str, Any]] = []

    with driver.session(database=neo4j_database) as session:
        for row in df.toLocalIterator():
            current_batch.append(sanitize_row(row.asDict(recursive=True)))

            if len(current_batch) >= batch_size:
                session.execute_write(write_batch, cypher, current_batch)
                total_rows += len(current_batch)
                print(f"{operation_name}: written {total_rows} rows")
                current_batch = []

        if current_batch:
            session.execute_write(write_batch, cypher, current_batch)
            total_rows += len(current_batch)

    print(f"{operation_name}: completed. Total rows written: {total_rows}")
    return total_rows

# COMMAND ----------

# Silver master/entity tables.
# These are the base source of truth for Neo4j nodes.

suppliers_master_df = normalize_supplier_master_columns(read_silver_table("suppliers"))
products_master_df = normalize_product_master_columns(read_silver_table("products"))
warehouses_master_df = normalize_warehouse_master_columns(read_silver_table("warehouses"))
stores_master_df = normalize_store_master_columns(read_silver_table("stores"))
customers_master_df = normalize_customer_master_columns(read_silver_table("customers"))

# Silver transactional table for shipment base entities.
shipments_silver_df = read_silver_table("shipments")

# COMMAND ----------

# Gold analytics/risk/enrichment views.
# These provide metrics, risks, dependencies, and business relationships.

supplier_product_dependency_df = read_gold_table("supplier_product_dependency")
supplier_risk_score_df = normalize_supplier_risk_columns(read_gold_table("supplier_risk_score"))
product_demand_summary_df = read_gold_table("product_demand_summary")
stockout_risk_df = normalize_stockout_columns(read_gold_table("stockout_risk"))
warehouse_store_replenishment_df = normalize_warehouse_columns(
    read_gold_table("warehouse_store_replenishment_view")
)
shipment_delay_impact_df = read_gold_table("shipment_delay_impact")
digital_twin_entity_health_df = read_gold_table("digital_twin_entity_health")
customer_order_summary_df = read_gold_table("customer_order_summary")
store_sales_summary_df = read_gold_table("store_sales_summary")

# COMMAND ----------

# Lightweight schema visibility.
# This helps catch naming issues before the graph write starts.

source_dataframes = {
    "silver.suppliers": suppliers_master_df,
    "silver.products": products_master_df,
    "silver.warehouses": warehouses_master_df,
    "silver.stores": stores_master_df,
    "silver.customers": customers_master_df,
    "silver.shipments": shipments_silver_df,
    "gold.supplier_product_dependency": supplier_product_dependency_df,
    "gold.supplier_risk_score": supplier_risk_score_df,
    "gold.product_demand_summary": product_demand_summary_df,
    "gold.stockout_risk": stockout_risk_df,
    "gold.warehouse_store_replenishment_view": warehouse_store_replenishment_df,
    "gold.shipment_delay_impact": shipment_delay_impact_df,
    "gold.digital_twin_entity_health": digital_twin_entity_health_df,
    "gold.customer_order_summary": customer_order_summary_df,
    "gold.store_sales_summary": store_sales_summary_df,
}

for source_name, source_df in source_dataframes.items():
    print(f"{source_name}: {source_df.columns}")

# COMMAND ----------

# Supplier nodes
#
# Base entity: silver.suppliers
# Enrichment: gold.supplier_risk_score

supplier_master_nodes_df = ensure_columns(
    suppliers_master_df,
    [
        "supplier_id",
        "supplier_name",
        "country",
        "region",
        "supplier_type",
        "preferred_supplier_flag",
    ],
)

supplier_risk_metrics_df = ensure_columns(
    supplier_risk_score_df,
    [
        "supplier_id",
        "risk_band",
        "computed_supplier_risk_score",
        "primary_risk_reason",
    ],
)

supplier_nodes_df = (
    supplier_master_nodes_df.alias("s")
    .join(supplier_risk_metrics_df.alias("r"), on="supplier_id", how="left")
    .select(
        F.col("supplier_id"),
        F.col("s.supplier_name").alias("supplier_name"),
        F.col("s.country").alias("country"),
        F.col("s.region").alias("region"),
        F.col("s.supplier_type").alias("supplier_type"),
        F.col("s.preferred_supplier_flag").alias("preferred_supplier_flag"),
        F.col("r.risk_band").alias("risk_band"),
        F.col("r.computed_supplier_risk_score").alias("computed_supplier_risk_score"),
        F.col("r.primary_risk_reason").alias("primary_risk_reason"),
    )
    .dropna(subset=["supplier_id"])
    .dropDuplicates(["supplier_id"])
)

display(supplier_nodes_df.limit(10))

# COMMAND ----------

supplier_node_cypher = """
UNWIND $rows AS row
MERGE (s:Supplier {supplier_id: row.supplier_id})
SET
    s.supplier_name = row.supplier_name,
    s.country = row.country,
    s.region = row.region,
    s.supplier_type = row.supplier_type,
    s.preferred_supplier_flag = row.preferred_supplier_flag,
    s.risk_band = row.risk_band,
    s.computed_supplier_risk_score = row.computed_supplier_risk_score,
    s.primary_risk_reason = row.primary_risk_reason,
    s.updated_at = datetime()
"""

write_df_to_neo4j(
    supplier_nodes_df,
    supplier_node_cypher,
    "Supplier nodes",
    batch_size,
)

# COMMAND ----------

# Product nodes
#
# Base entity: silver.products
# Enrichment:
# - gold.product_demand_summary
# - gold.stockout_risk

product_master_nodes_df = ensure_columns(
    products_master_df,
    [
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "brand",
        "supplier_id",
        "unit_price",
        "unit_cost",
        "gross_margin",
        "gross_margin_pct",
        "active_flag",
    ],
)

product_demand_metrics_df = ensure_columns(
    product_demand_summary_df,
    [
        "product_id",
        "demand_band",
        "total_quantity_sold",
        "total_revenue",
    ],
).dropDuplicates(["product_id"])

stockout_base_df = ensure_columns(
    stockout_risk_df,
    [
        "product_id",
        "location_id",
        "stockout_risk_band",
        "inventory_status",
        "recommended_action",
    ],
).filter(F.col("product_id").isNotNull())

stockout_metrics_df = (
    stockout_base_df
    .withColumn(
        "stockout_severity_score",
        F.when(F.lower(F.col("stockout_risk_band")).isin("critical", "high"), F.lit(3))
        .when(F.lower(F.col("stockout_risk_band")) == "medium", F.lit(2))
        .when(F.lower(F.col("stockout_risk_band")) == "low", F.lit(1))
        .otherwise(F.lit(0)),
    )
    .groupBy("product_id")
    .agg(
        F.countDistinct("location_id").alias("stockout_location_count"),
        F.sum(F.when(F.lower(F.col("stockout_risk_band")).isin("critical", "high"), 1).otherwise(0)).alias("high_stockout_location_count"),
        F.max("stockout_severity_score").alias("max_stockout_severity_score"),
        F.first("inventory_status", ignorenulls=True).alias("sample_inventory_status"),
        F.first("recommended_action", ignorenulls=True).alias("sample_stockout_action"),
    )
    .withColumn(
        "highest_stockout_risk_band",
        F.when(F.col("max_stockout_severity_score") == 3, F.lit("high"))
        .when(F.col("max_stockout_severity_score") == 2, F.lit("medium"))
        .when(F.col("max_stockout_severity_score") == 1, F.lit("low"))
        .otherwise(F.lit(None)),
    )
)

product_nodes_df = (
    product_master_nodes_df.alias("p")
    .join(product_demand_metrics_df.alias("d"), on="product_id", how="left")
    .join(stockout_metrics_df.alias("sr"), on="product_id", how="left")
    .select(
        F.col("product_id"),
        F.col("p.product_name").alias("product_name"),
        F.col("p.category").alias("category"),
        F.col("p.sub_category").alias("sub_category"),
        F.col("p.brand").alias("brand"),
        F.col("p.supplier_id").alias("supplier_id"),
        F.col("p.unit_price").alias("unit_price"),
        F.col("p.unit_cost").alias("unit_cost"),
        F.col("p.gross_margin").alias("gross_margin"),
        F.col("p.gross_margin_pct").alias("gross_margin_pct"),
        F.col("p.active_flag").alias("active_flag"),
        F.col("d.demand_band").alias("demand_band"),
        F.col("d.total_quantity_sold").alias("total_quantity_sold"),
        F.col("d.total_revenue").alias("total_revenue"),
        F.col("sr.stockout_location_count").alias("stockout_location_count"),
        F.col("sr.high_stockout_location_count").alias("high_stockout_location_count"),
        F.col("sr.highest_stockout_risk_band").alias("highest_stockout_risk_band"),
        F.col("sr.sample_inventory_status").alias("sample_inventory_status"),
        F.col("sr.sample_stockout_action").alias("sample_stockout_action"),
    )
    .dropna(subset=["product_id"])
    .dropDuplicates(["product_id"])
)

display(product_nodes_df.limit(10))

# COMMAND ----------

product_node_cypher = """
UNWIND $rows AS row
MERGE (p:Product {product_id: row.product_id})
SET
    p.product_name = row.product_name,
    p.category = row.category,
    p.sub_category = row.sub_category,
    p.brand = row.brand,
    p.supplier_id = row.supplier_id,
    p.unit_price = row.unit_price,
    p.unit_cost = row.unit_cost,
    p.gross_margin = row.gross_margin,
    p.gross_margin_pct = row.gross_margin_pct,
    p.active_flag = row.active_flag,
    p.demand_band = row.demand_band,
    p.total_quantity_sold = row.total_quantity_sold,
    p.total_revenue = row.total_revenue,
    p.stockout_location_count = row.stockout_location_count,
    p.high_stockout_location_count = row.high_stockout_location_count,
    p.highest_stockout_risk_band = row.highest_stockout_risk_band,
    p.sample_inventory_status = row.sample_inventory_status,
    p.sample_stockout_action = row.sample_stockout_action,
    p.updated_at = datetime()
"""

write_df_to_neo4j(
    product_nodes_df,
    product_node_cypher,
    "Product nodes",
    batch_size,
)

# COMMAND ----------

# Store nodes
#
# Base entity: silver.stores
# Enrichment:
# - gold.store_sales_summary
# - gold.customer_order_summary

store_master_nodes_df = ensure_columns(
    stores_master_df,
    [
        "store_id",
        "store_name",
        "store_country",
        "store_city",
        "store_region",
        "store_type",
        "store_size_band",
        "active_flag",
    ],
)

store_sales_metrics_df = ensure_columns(
    store_sales_summary_df,
    [
        "store_id",
        "total_revenue",
        "total_quantity_sold",
        "order_count",
    ],
).dropDuplicates(["store_id"])

customer_store_metrics_df = (
    ensure_columns(
        customer_order_summary_df,
        [
            "store_id",
            "customer_id",
            "total_spend",
        ],
    )
    .filter(F.col("store_id").isNotNull())
    .groupBy("store_id")
    .agg(
        F.countDistinct("customer_id").alias("customer_count"),
        F.sum("total_spend").alias("customer_total_spend"),
    )
)

store_nodes_df = (
    store_master_nodes_df.alias("s")
    .join(store_sales_metrics_df.alias("ss"), on="store_id", how="left")
    .join(customer_store_metrics_df.alias("cm"), on="store_id", how="left")
    .select(
        F.col("store_id"),
        F.col("s.store_name").alias("store_name"),
        F.col("s.store_country").alias("store_country"),
        F.col("s.store_city").alias("store_city"),
        F.col("s.store_region").alias("store_region"),
        F.col("s.store_type").alias("store_type"),
        F.col("s.store_size_band").alias("store_size_band"),
        F.col("s.active_flag").alias("active_flag"),
        F.col("ss.total_revenue").alias("total_revenue"),
        F.col("ss.total_quantity_sold").alias("total_quantity_sold"),
        F.col("ss.order_count").alias("order_count"),
        F.col("cm.customer_count").alias("customer_count"),
        F.col("cm.customer_total_spend").alias("customer_total_spend"),
    )
    .dropna(subset=["store_id"])
    .dropDuplicates(["store_id"])
)

display(store_nodes_df.limit(10))

# COMMAND ----------

store_node_cypher = """
UNWIND $rows AS row
MERGE (s:Store {store_id: row.store_id})
SET
    s.store_name = row.store_name,
    s.store_country = row.store_country,
    s.store_city = row.store_city,
    s.store_region = row.store_region,
    s.store_type = row.store_type,
    s.store_size_band = row.store_size_band,
    s.active_flag = row.active_flag,
    s.total_revenue = row.total_revenue,
    s.total_quantity_sold = row.total_quantity_sold,
    s.order_count = row.order_count,
    s.customer_count = row.customer_count,
    s.customer_total_spend = row.customer_total_spend,
    s.updated_at = datetime()
"""

write_df_to_neo4j(
    store_nodes_df,
    store_node_cypher,
    "Store nodes",
    batch_size,
)

# COMMAND ----------

# Warehouse nodes
#
# Base entity: silver.warehouses
# Enrichment:
# - gold.warehouse_store_replenishment_view

warehouse_master_nodes_df = ensure_columns(
    warehouses_master_df,
    [
        "warehouse_id",
        "warehouse_name",
        "warehouse_country",
        "warehouse_city",
        "warehouse_region",
        "capacity_units",
        "active_flag",
    ],
)

warehouse_enrichment_df = (
    ensure_columns(
        warehouse_store_replenishment_df,
        [
            "warehouse_id",
            "warehouse_utilization_pct",
            "warehouse_utilization_band",
            "store_id",
        ],
    )
    .filter(F.col("warehouse_id").isNotNull())
    .groupBy("warehouse_id")
    .agg(
        F.max("warehouse_utilization_pct").alias("warehouse_utilization_pct"),
        F.first("warehouse_utilization_band", ignorenulls=True).alias("warehouse_utilization_band"),
        F.countDistinct("store_id").alias("served_store_count"),
    )
)

warehouse_nodes_df = (
    warehouse_master_nodes_df.alias("w")
    .join(warehouse_enrichment_df.alias("e"), on="warehouse_id", how="left")
    .select(
        F.col("warehouse_id"),
        F.col("w.warehouse_name").alias("warehouse_name"),
        F.col("w.warehouse_country").alias("warehouse_country"),
        F.col("w.warehouse_city").alias("warehouse_city"),
        F.col("w.warehouse_region").alias("warehouse_region"),
        F.col("w.capacity_units").alias("capacity_units"),
        F.col("w.active_flag").alias("active_flag"),
        F.col("e.warehouse_utilization_pct").alias("warehouse_utilization_pct"),
        F.col("e.warehouse_utilization_band").alias("warehouse_utilization_band"),
        F.col("e.served_store_count").alias("served_store_count"),
    )
    .dropna(subset=["warehouse_id"])
    .dropDuplicates(["warehouse_id"])
)

display(warehouse_nodes_df.limit(10))

# COMMAND ----------

warehouse_node_cypher = """
UNWIND $rows AS row
MERGE (w:Warehouse {warehouse_id: row.warehouse_id})
SET
    w.warehouse_name = row.warehouse_name,
    w.warehouse_country = row.warehouse_country,
    w.warehouse_city = row.warehouse_city,
    w.warehouse_region = row.warehouse_region,
    w.capacity_units = row.capacity_units,
    w.active_flag = row.active_flag,
    w.warehouse_utilization_pct = row.warehouse_utilization_pct,
    w.warehouse_utilization_band = row.warehouse_utilization_band,
    w.served_store_count = row.served_store_count,
    w.updated_at = datetime()
"""

write_df_to_neo4j(
    warehouse_nodes_df,
    warehouse_node_cypher,
    "Warehouse nodes",
    batch_size,
)

# COMMAND ----------

# Customer nodes
#
# Base entity: silver.customers
# Enrichment:
# - gold.customer_order_summary

customer_master_nodes_df = ensure_columns(
    customers_master_df,
    [
        "customer_id",
        "customer_name",
        "first_name",
        "last_name",
        "country",
        "city",
        "customer_segment",
        "loyalty_tier",
        "active_flag",
    ],
)

customer_order_metrics_df = ensure_columns(
    customer_order_summary_df,
    [
        "customer_id",
        "order_count",
        "total_spend",
        "customer_value_band",
    ],
).dropDuplicates(["customer_id"])

customer_nodes_df = (
    customer_master_nodes_df.alias("c")
    .join(customer_order_metrics_df.alias("o"), on="customer_id", how="left")
    .select(
        F.col("customer_id"),
        F.coalesce(
            F.col("c.customer_name"),
            F.concat_ws(" ", F.col("c.first_name"), F.col("c.last_name")),
            F.col("customer_id"),
        ).alias("customer_name"),
        F.col("c.country").alias("country"),
        F.col("c.city").alias("city"),
        F.col("c.customer_segment").alias("customer_segment"),
        F.col("c.loyalty_tier").alias("loyalty_tier"),
        F.col("c.active_flag").alias("active_flag"),
        F.col("o.order_count").alias("order_count"),
        F.col("o.total_spend").alias("total_spend"),
        F.col("o.customer_value_band").alias("customer_value_band"),
    )
    .dropna(subset=["customer_id"])
    .dropDuplicates(["customer_id"])
)

display(customer_nodes_df.limit(10))

# COMMAND ----------

customer_node_cypher = """
UNWIND $rows AS row
MERGE (c:Customer {customer_id: row.customer_id})
SET
    c.customer_name = row.customer_name,
    c.country = row.country,
    c.city = row.city,
    c.customer_segment = row.customer_segment,
    c.loyalty_tier = row.loyalty_tier,
    c.active_flag = row.active_flag,
    c.order_count = row.order_count,
    c.total_spend = row.total_spend,
    c.customer_value_band = row.customer_value_band,
    c.updated_at = datetime()
"""

write_df_to_neo4j(
    customer_nodes_df,
    customer_node_cypher,
    "Customer nodes",
    batch_size,
)

# COMMAND ----------

# Shipment nodes
#
# Base entity: silver.shipments
# Enrichment:
# - gold.shipment_delay_impact

shipment_master_nodes_df = ensure_columns(
    shipments_silver_df,
    [
        "shipment_id",
        "order_id",
        "product_id",
        "shipment_type",
        "shipment_status",
        "carrier",
        "source_type",
        "source_id",
        "destination_type",
        "destination_id",
        "quantity",
    ],
)

shipment_delay_metrics_df = ensure_columns(
    shipment_delay_impact_df,
    [
        "shipment_id",
        "is_delayed",
        "delay_days",
        "delay_severity",
        "impact_band",
        "recommended_action",
        "estimated_shipment_retail_value",
        "estimated_shipment_cost_value",
    ],
).dropDuplicates(["shipment_id"])

shipment_nodes_df = (
    shipment_master_nodes_df.alias("s")
    .join(shipment_delay_metrics_df.alias("d"), on="shipment_id", how="left")
    .select(
        F.col("shipment_id"),
        F.col("s.order_id").alias("order_id"),
        F.col("s.product_id").alias("product_id"),
        F.col("s.shipment_type").alias("shipment_type"),
        F.col("s.shipment_status").alias("shipment_status"),
        F.col("s.carrier").alias("carrier"),
        F.col("s.source_type").alias("source_type"),
        F.col("s.source_id").alias("source_id"),
        F.col("s.destination_type").alias("destination_type"),
        F.col("s.destination_id").alias("destination_id"),
        F.col("s.quantity").alias("quantity"),
        F.col("d.is_delayed").alias("is_delayed"),
        F.col("d.delay_days").alias("delay_days"),
        F.col("d.delay_severity").alias("delay_severity"),
        F.col("d.impact_band").alias("impact_band"),
        F.col("d.recommended_action").alias("recommended_action"),
        F.col("d.estimated_shipment_retail_value").alias("estimated_shipment_retail_value"),
        F.col("d.estimated_shipment_cost_value").alias("estimated_shipment_cost_value"),
    )
    .dropna(subset=["shipment_id"])
    .dropDuplicates(["shipment_id"])
)

display(shipment_nodes_df.limit(10))

# COMMAND ----------

shipment_node_cypher = """
UNWIND $rows AS row
MERGE (sh:Shipment {shipment_id: row.shipment_id})
SET
    sh.order_id = row.order_id,
    sh.product_id = row.product_id,
    sh.shipment_type = row.shipment_type,
    sh.shipment_status = row.shipment_status,
    sh.carrier = row.carrier,
    sh.source_type = row.source_type,
    sh.source_id = row.source_id,
    sh.destination_type = row.destination_type,
    sh.destination_id = row.destination_id,
    sh.quantity = row.quantity,
    sh.is_delayed = row.is_delayed,
    sh.delay_days = row.delay_days,
    sh.delay_severity = row.delay_severity,
    sh.impact_band = row.impact_band,
    sh.recommended_action = row.recommended_action,
    sh.estimated_shipment_retail_value = row.estimated_shipment_retail_value,
    sh.estimated_shipment_cost_value = row.estimated_shipment_cost_value,
    sh.updated_at = datetime()
"""

write_df_to_neo4j(
    shipment_nodes_df,
    shipment_node_cypher,
    "Shipment nodes",
    batch_size,
)

# COMMAND ----------

# SUPPLIES relationship
#
# Source: gold.supplier_product_dependency
#
# Pattern:
# (:Supplier)-[:SUPPLIES]->(:Product)

supplies_relationship_df = ensure_columns(
    supplier_product_dependency_df,
    [
        "supplier_id",
        "product_id",
        "dependency_risk_band",
        "total_units_sold",
        "total_revenue",
        "primary_risk_reason",
    ],
).dropna(subset=["supplier_id", "product_id"]).dropDuplicates(["supplier_id", "product_id"])

display(supplies_relationship_df.limit(10))

# COMMAND ----------

supplies_relationship_cypher = """
UNWIND $rows AS row
MATCH (s:Supplier {supplier_id: row.supplier_id})
MATCH (p:Product {product_id: row.product_id})
MERGE (s)-[r:SUPPLIES]->(p)
SET
    r.dependency_risk_band = row.dependency_risk_band,
    r.total_units_sold = row.total_units_sold,
    r.total_revenue = row.total_revenue,
    r.primary_risk_reason = row.primary_risk_reason,
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    supplies_relationship_df,
    supplies_relationship_cypher,
    "SUPPLIES relationships",
    batch_size,
)

# COMMAND ----------

# SERVES relationship
#
# Source: gold.warehouse_store_replenishment_view
#
# Pattern:
# (:Warehouse)-[:SERVES]->(:Store)

serves_relationship_df = ensure_columns(
    warehouse_store_replenishment_df,
    [
        "warehouse_id",
        "store_id",
        "warehouse_utilization_pct",
        "warehouse_utilization_band",
        "store_region",
    ],
).dropna(subset=["warehouse_id", "store_id"]).dropDuplicates(["warehouse_id", "store_id"])

display(serves_relationship_df.limit(10))

# COMMAND ----------

serves_relationship_cypher = """
UNWIND $rows AS row
MATCH (w:Warehouse {warehouse_id: row.warehouse_id})
MATCH (s:Store {store_id: row.store_id})
MERGE (w)-[r:SERVES]->(s)
SET
    r.warehouse_utilization_pct = row.warehouse_utilization_pct,
    r.warehouse_utilization_band = row.warehouse_utilization_band,
    r.store_region = row.store_region,
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    serves_relationship_df,
    serves_relationship_cypher,
    "SERVES relationships",
    batch_size,
)

# COMMAND ----------

# STOCKS relationship
#
# Source: gold.warehouse_store_replenishment_view
#
# Pattern:
# (:Store)-[:STOCKS]->(:Product)

store_stocks_relationship_df = ensure_columns(
    warehouse_store_replenishment_df,
    [
        "store_id",
        "product_id",
        "stock_on_hand_after",
        "available_stock",
        "reorder_point",
        "safety_stock",
        "inventory_status",
        "stockout_risk_band",
        "replenishment_priority",
        "warehouse_action",
    ],
).dropna(subset=["store_id", "product_id"]).dropDuplicates(["store_id", "product_id"])

display(store_stocks_relationship_df.limit(10))

# COMMAND ----------

store_stocks_relationship_cypher = """
UNWIND $rows AS row
MATCH (s:Store {store_id: row.store_id})
MATCH (p:Product {product_id: row.product_id})
MERGE (s)-[r:STOCKS]->(p)
SET
    r.stock_on_hand_after = row.stock_on_hand_after,
    r.available_stock = row.available_stock,
    r.reorder_point = row.reorder_point,
    r.safety_stock = row.safety_stock,
    r.inventory_status = row.inventory_status,
    r.stockout_risk_band = row.stockout_risk_band,
    r.replenishment_priority = row.replenishment_priority,
    r.warehouse_action = row.warehouse_action,
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    store_stocks_relationship_df,
    store_stocks_relationship_cypher,
    "Store STOCKS relationships",
    batch_size,
)

# COMMAND ----------

# MOVES relationship
#
# Source: gold.shipment_delay_impact
#
# Pattern:
# (:Shipment)-[:MOVES]->(:Product)

shipment_moves_relationship_df = ensure_columns(
    shipment_delay_impact_df,
    [
        "shipment_id",
        "product_id",
        "quantity",
        "shipment_status",
        "delay_days",
        "delay_severity",
        "impact_band",
    ],
).dropna(subset=["shipment_id", "product_id"]).dropDuplicates(["shipment_id", "product_id"])

display(shipment_moves_relationship_df.limit(10))

# COMMAND ----------

shipment_moves_relationship_cypher = """
UNWIND $rows AS row
MATCH (sh:Shipment {shipment_id: row.shipment_id})
MATCH (p:Product {product_id: row.product_id})
MERGE (sh)-[r:MOVES]->(p)
SET
    r.quantity = row.quantity,
    r.shipment_status = row.shipment_status,
    r.delay_days = row.delay_days,
    r.delay_severity = row.delay_severity,
    r.impact_band = row.impact_band,
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    shipment_moves_relationship_df,
    shipment_moves_relationship_cypher,
    "MOVES relationships",
    batch_size,
)

# COMMAND ----------

# FROM_SUPPLIER relationship
#
# Pattern:
# (:Shipment)-[:FROM_SUPPLIER]->(:Supplier)

shipment_from_supplier_df = (
    ensure_columns(
        shipment_delay_impact_df,
        [
            "shipment_id",
            "source_type",
            "source_id",
            "shipment_type",
            "carrier",
            "delay_days",
            "impact_band",
        ],
    )
    .filter(F.lower(F.col("source_type")) == "supplier")
    .dropna(subset=["shipment_id", "source_id"])
    .dropDuplicates(["shipment_id", "source_id"])
)

display(shipment_from_supplier_df.limit(10))

# COMMAND ----------

shipment_from_supplier_cypher = """
UNWIND $rows AS row
MATCH (sh:Shipment {shipment_id: row.shipment_id})
MATCH (s:Supplier {supplier_id: row.source_id})
MERGE (sh)-[r:FROM_SUPPLIER]->(s)
SET
    r.shipment_type = row.shipment_type,
    r.carrier = row.carrier,
    r.delay_days = row.delay_days,
    r.impact_band = row.impact_band,
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    shipment_from_supplier_df,
    shipment_from_supplier_cypher,
    "FROM_SUPPLIER relationships",
    batch_size,
)

# COMMAND ----------

# FROM_WAREHOUSE relationship
#
# Pattern:
# (:Shipment)-[:FROM_WAREHOUSE]->(:Warehouse)

shipment_from_warehouse_df = (
    ensure_columns(
        shipment_delay_impact_df,
        [
            "shipment_id",
            "source_type",
            "source_id",
            "shipment_type",
            "carrier",
            "delay_days",
            "impact_band",
        ],
    )
    .filter(F.lower(F.col("source_type")) == "warehouse")
    .dropna(subset=["shipment_id", "source_id"])
    .dropDuplicates(["shipment_id", "source_id"])
)

display(shipment_from_warehouse_df.limit(10))

# COMMAND ----------

shipment_from_warehouse_cypher = """
UNWIND $rows AS row
MATCH (sh:Shipment {shipment_id: row.shipment_id})
MATCH (w:Warehouse {warehouse_id: row.source_id})
MERGE (sh)-[r:FROM_WAREHOUSE]->(w)
SET
    r.shipment_type = row.shipment_type,
    r.carrier = row.carrier,
    r.delay_days = row.delay_days,
    r.impact_band = row.impact_band,
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    shipment_from_warehouse_df,
    shipment_from_warehouse_cypher,
    "FROM_WAREHOUSE relationships",
    batch_size,
)

# COMMAND ----------

# TO_STORE relationship
#
# Pattern:
# (:Shipment)-[:TO_STORE]->(:Store)

shipment_to_store_df = (
    ensure_columns(
        shipment_delay_impact_df,
        [
            "shipment_id",
            "destination_type",
            "destination_id",
            "shipment_type",
            "carrier",
            "delay_days",
            "impact_band",
        ],
    )
    .filter(F.lower(F.col("destination_type")) == "store")
    .dropna(subset=["shipment_id", "destination_id"])
    .dropDuplicates(["shipment_id", "destination_id"])
)

display(shipment_to_store_df.limit(10))

# COMMAND ----------

shipment_to_store_cypher = """
UNWIND $rows AS row
MATCH (sh:Shipment {shipment_id: row.shipment_id})
MATCH (s:Store {store_id: row.destination_id})
MERGE (sh)-[r:TO_STORE]->(s)
SET
    r.shipment_type = row.shipment_type,
    r.carrier = row.carrier,
    r.delay_days = row.delay_days,
    r.impact_band = row.impact_band,
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    shipment_to_store_df,
    shipment_to_store_cypher,
    "TO_STORE relationships",
    batch_size,
)

# COMMAND ----------

# TO_WAREHOUSE relationship
#
# Pattern:
# (:Shipment)-[:TO_WAREHOUSE]->(:Warehouse)

shipment_to_warehouse_df = (
    ensure_columns(
        shipment_delay_impact_df,
        [
            "shipment_id",
            "destination_type",
            "destination_id",
            "shipment_type",
            "carrier",
            "delay_days",
            "impact_band",
        ],
    )
    .filter(F.lower(F.col("destination_type")) == "warehouse")
    .dropna(subset=["shipment_id", "destination_id"])
    .dropDuplicates(["shipment_id", "destination_id"])
)

display(shipment_to_warehouse_df.limit(10))

# COMMAND ----------

shipment_to_warehouse_cypher = """
UNWIND $rows AS row
MATCH (sh:Shipment {shipment_id: row.shipment_id})
MATCH (w:Warehouse {warehouse_id: row.destination_id})
MERGE (sh)-[r:TO_WAREHOUSE]->(w)
SET
    r.shipment_type = row.shipment_type,
    r.carrier = row.carrier,
    r.delay_days = row.delay_days,
    r.impact_band = row.impact_band,
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    shipment_to_warehouse_df,
    shipment_to_warehouse_cypher,
    "TO_WAREHOUSE relationships",
    batch_size,
)

# COMMAND ----------

# RiskSignal nodes
#
# Source: gold.digital_twin_entity_health
#
# Pattern:
# (:RiskSignal {risk_signal_id: ...})

entity_health_df = (
    ensure_columns(
        digital_twin_entity_health_df,
        [
            "entity_type",
            "entity_id",
            "health_score",
            "risk_band",
            "main_risk_reason",
            "recommended_action",
        ],
    )
    .dropna(subset=["entity_type", "entity_id"])
    .withColumn("entity_type_normalized", F.lower(F.col("entity_type")))
    .withColumn(
        "risk_signal_id",
        F.concat(
            F.lit("entity_health_risk|"),
            F.col("entity_type_normalized"),
            F.lit("|"),
            F.col("entity_id"),
        ),
    )
    .dropDuplicates(["risk_signal_id"])
)

risk_signal_nodes_df = entity_health_df.select(
    "risk_signal_id",
    "entity_type_normalized",
    "entity_id",
    "health_score",
    "risk_band",
    "main_risk_reason",
    "recommended_action",
)

display(risk_signal_nodes_df.limit(10))

# COMMAND ----------

risk_signal_node_cypher = """
UNWIND $rows AS row
MERGE (r:RiskSignal {risk_signal_id: row.risk_signal_id})
SET
    r.risk_type = 'entity_health_risk',
    r.entity_type = row.entity_type_normalized,
    r.entity_id = row.entity_id,
    r.health_score = row.health_score,
    r.risk_band = row.risk_band,
    r.risk_reason = row.main_risk_reason,
    r.recommended_action = row.recommended_action,
    r.source_gold_view = 'digital_twin_entity_health',
    r.updated_at = datetime()
"""

write_df_to_neo4j(
    risk_signal_nodes_df,
    risk_signal_node_cypher,
    "RiskSignal nodes",
    batch_size,
)

# COMMAND ----------

# Product HAS_HEALTH_STATUS relationship

product_health_df = risk_signal_nodes_df.filter(F.col("entity_type_normalized") == "product")

product_health_relationship_cypher = """
UNWIND $rows AS row
MATCH (p:Product {product_id: row.entity_id})
MATCH (r:RiskSignal {risk_signal_id: row.risk_signal_id})
MERGE (p)-[rel:HAS_HEALTH_STATUS]->(r)
SET
    rel.health_score = row.health_score,
    rel.risk_band = row.risk_band,
    rel.main_risk_reason = row.main_risk_reason,
    rel.updated_at = datetime()
"""

write_df_to_neo4j(
    product_health_df,
    product_health_relationship_cypher,
    "Product HAS_HEALTH_STATUS relationships",
    batch_size,
)

# COMMAND ----------

# Supplier HAS_HEALTH_STATUS relationship

supplier_health_df = risk_signal_nodes_df.filter(F.col("entity_type_normalized") == "supplier")

supplier_health_relationship_cypher = """
UNWIND $rows AS row
MATCH (s:Supplier {supplier_id: row.entity_id})
MATCH (r:RiskSignal {risk_signal_id: row.risk_signal_id})
MERGE (s)-[rel:HAS_HEALTH_STATUS]->(r)
SET
    rel.health_score = row.health_score,
    rel.risk_band = row.risk_band,
    rel.main_risk_reason = row.main_risk_reason,
    rel.updated_at = datetime()
"""

write_df_to_neo4j(
    supplier_health_df,
    supplier_health_relationship_cypher,
    "Supplier HAS_HEALTH_STATUS relationships",
    batch_size,
)

# COMMAND ----------

# Store HAS_HEALTH_STATUS relationship

store_health_df = risk_signal_nodes_df.filter(F.col("entity_type_normalized") == "store")

store_health_relationship_cypher = """
UNWIND $rows AS row
MATCH (s:Store {store_id: row.entity_id})
MATCH (r:RiskSignal {risk_signal_id: row.risk_signal_id})
MERGE (s)-[rel:HAS_HEALTH_STATUS]->(r)
SET
    rel.health_score = row.health_score,
    rel.risk_band = row.risk_band,
    rel.main_risk_reason = row.main_risk_reason,
    rel.updated_at = datetime()
"""

write_df_to_neo4j(
    store_health_df,
    store_health_relationship_cypher,
    "Store HAS_HEALTH_STATUS relationships",
    batch_size,
)

# COMMAND ----------

# Warehouse HAS_HEALTH_STATUS relationship

warehouse_health_df = risk_signal_nodes_df.filter(F.col("entity_type_normalized") == "warehouse")

warehouse_health_relationship_cypher = """
UNWIND $rows AS row
MATCH (w:Warehouse {warehouse_id: row.entity_id})
MATCH (r:RiskSignal {risk_signal_id: row.risk_signal_id})
MERGE (w)-[rel:HAS_HEALTH_STATUS]->(r)
SET
    rel.health_score = row.health_score,
    rel.risk_band = row.risk_band,
    rel.main_risk_reason = row.main_risk_reason,
    rel.updated_at = datetime()
"""

write_df_to_neo4j(
    warehouse_health_df,
    warehouse_health_relationship_cypher,
    "Warehouse HAS_HEALTH_STATUS relationships",
    batch_size,
)

# COMMAND ----------

# Customer HAS_HEALTH_STATUS relationship
#
# This will write zero rows if digital_twin_entity_health does not contain customer health signals.

customer_health_df = risk_signal_nodes_df.filter(F.col("entity_type_normalized") == "customer")

customer_health_relationship_cypher = """
UNWIND $rows AS row
MATCH (c:Customer {customer_id: row.entity_id})
MATCH (r:RiskSignal {risk_signal_id: row.risk_signal_id})
MERGE (c)-[rel:HAS_HEALTH_STATUS]->(r)
SET
    rel.health_score = row.health_score,
    rel.risk_band = row.risk_band,
    rel.main_risk_reason = row.main_risk_reason,
    rel.updated_at = datetime()
"""

write_df_to_neo4j(
    customer_health_df,
    customer_health_relationship_cypher,
    "Customer HAS_HEALTH_STATUS relationships",
    batch_size,
)

# COMMAND ----------

# Validation queries

validation_queries = {
    "Supplier nodes": "MATCH (n:Supplier) RETURN count(n) AS count",
    "Product nodes": "MATCH (n:Product) RETURN count(n) AS count",
    "Store nodes": "MATCH (n:Store) RETURN count(n) AS count",
    "Warehouse nodes": "MATCH (n:Warehouse) RETURN count(n) AS count",
    "Customer nodes": "MATCH (n:Customer) RETURN count(n) AS count",
    "Shipment nodes": "MATCH (n:Shipment) RETURN count(n) AS count",
    "RiskSignal nodes": "MATCH (n:RiskSignal) RETURN count(n) AS count",
    "SUPPLIES relationships": "MATCH (:Supplier)-[r:SUPPLIES]->(:Product) RETURN count(r) AS count",
    "SERVES relationships": "MATCH (:Warehouse)-[r:SERVES]->(:Store) RETURN count(r) AS count",
    "STOCKS relationships": "MATCH (:Store)-[r:STOCKS]->(:Product) RETURN count(r) AS count",
    "MOVES relationships": "MATCH (:Shipment)-[r:MOVES]->(:Product) RETURN count(r) AS count",
    "FROM_SUPPLIER relationships": "MATCH (:Shipment)-[r:FROM_SUPPLIER]->(:Supplier) RETURN count(r) AS count",
    "FROM_WAREHOUSE relationships": "MATCH (:Shipment)-[r:FROM_WAREHOUSE]->(:Warehouse) RETURN count(r) AS count",
    "TO_STORE relationships": "MATCH (:Shipment)-[r:TO_STORE]->(:Store) RETURN count(r) AS count",
    "TO_WAREHOUSE relationships": "MATCH (:Shipment)-[r:TO_WAREHOUSE]->(:Warehouse) RETURN count(r) AS count",
    "HAS_HEALTH_STATUS relationships": "MATCH ()-[r:HAS_HEALTH_STATUS]->(:RiskSignal) RETURN count(r) AS count",
}

with driver.session(database=neo4j_database) as session:
    for name, query in validation_queries.items():
        result = session.run(query)
        count_value = result.single()["count"]
        print(f"{name}: {count_value}")

# COMMAND ----------

driver.close()
print("Neo4j graph load completed successfully.")
