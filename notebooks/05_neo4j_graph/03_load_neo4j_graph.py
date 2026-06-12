# Databricks notebook source
# MAGIC %pip install neo4j

# COMMAND ----------

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List

from neo4j import GraphDatabase
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("catalog_name", "")
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
gold_schema = get_required_widget("gold_schema")
secret_scope = get_required_widget("secret_scope")
batch_size = int(get_required_widget("batch_size"))

print(f"Catalog: {catalog_name}")
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

def table_name(table: str) -> str:
    return f"{catalog_name}.{gold_schema}.{table}"


def read_gold_table(table: str) -> DataFrame:
    full_name = table_name(table)
    print(f"Reading {full_name}")
    return spark.table(full_name)


def ensure_columns(df: DataFrame, required_columns: List[str]) -> DataFrame:
    result_df = df
    for column_name in required_columns:
        if column_name not in result_df.columns:
            result_df = result_df.withColumn(column_name, F.lit(None))
    return result_df.select(*required_columns)


def sanitize_value(value: Any) -> Any:
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

supplier_product_dependency_df = read_gold_table("supplier_product_dependency")
supplier_risk_score_df = read_gold_table("supplier_risk_score")
product_demand_summary_df = read_gold_table("product_demand_summary")
warehouse_store_replenishment_df = read_gold_table("warehouse_store_replenishment_view")
shipment_delay_impact_df = read_gold_table("shipment_delay_impact")
digital_twin_entity_health_df = read_gold_table("digital_twin_entity_health")

# COMMAND ----------

supplier_nodes_df = (
    supplier_product_dependency_df
    .select(
        "supplier_id",
        "supplier_name",
    )
    .unionByName(
        ensure_columns(
            supplier_risk_score_df,
            [
                "supplier_id",
                "supplier_name",
            ],
        ),
        allowMissingColumns=True,
    )
    .dropna(subset=["supplier_id"])
    .dropDuplicates(["supplier_id"])
)

supplier_nodes_df = ensure_columns(
    supplier_nodes_df,
    [
        "supplier_id",
        "supplier_name",
    ],
)

display(supplier_nodes_df.limit(10))

# COMMAND ----------

supplier_node_cypher = """
UNWIND $rows AS row
MERGE (s:Supplier {supplier_id: row.supplier_id})
SET
    s.supplier_name = row.supplier_name,
    s.updated_at = datetime()
"""

write_df_to_neo4j(
    supplier_nodes_df,
    supplier_node_cypher,
    "Supplier nodes",
    batch_size,
)

# COMMAND ----------

product_nodes_df = ensure_columns(
    product_demand_summary_df,
    [
        "product_id",
        "product_name",
        "category",
        "sub_category",
        "brand",
        "supplier_id",
        "demand_band",
        "total_quantity_sold",
        "total_revenue",
        "active_flag",
    ],
).dropna(subset=["product_id"]).dropDuplicates(["product_id"])

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
    p.demand_band = row.demand_band,
    p.total_quantity_sold = row.total_quantity_sold,
    p.total_revenue = row.total_revenue,
    p.active_flag = row.active_flag,
    p.updated_at = datetime()
"""

write_df_to_neo4j(
    product_nodes_df,
    product_node_cypher,
    "Product nodes",
    batch_size,
)

# COMMAND ----------

store_nodes_df = ensure_columns(
    warehouse_store_replenishment_df,
    [
        "store_id",
        "store_name",
        "store_country",
        "store_city",
        "store_region",
    ],
).dropna(subset=["store_id"]).dropDuplicates(["store_id"])

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
    s.updated_at = datetime()
"""

write_df_to_neo4j(
    store_nodes_df,
    store_node_cypher,
    "Store nodes",
    batch_size,
)

# COMMAND ----------

warehouse_nodes_df = ensure_columns(
    warehouse_store_replenishment_df,
    [
        "warehouse_id",
        "warehouse_name",
        "warehouse_country",
        "warehouse_city",
        "warehouse_region",
        "warehouse_utilization_pct",
        "warehouse_utilization_band",
    ],
).dropna(subset=["warehouse_id"]).dropDuplicates(["warehouse_id"])

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
    w.warehouse_utilization_pct = row.warehouse_utilization_pct,
    w.warehouse_utilization_band = row.warehouse_utilization_band,
    w.updated_at = datetime()
"""

write_df_to_neo4j(
    warehouse_nodes_df,
    warehouse_node_cypher,
    "Warehouse nodes",
    batch_size,
)

# COMMAND ----------

shipment_nodes_df = ensure_columns(
    shipment_delay_impact_df,
    [
        "shipment_id",
        "shipment_type",
        "shipment_status",
        "carrier",
        "delay_days",
        "delay_severity",
        "impact_band",
        "recommended_action",
        "estimated_shipment_retail_value",
        "estimated_shipment_cost_value",
    ],
).dropna(subset=["shipment_id"]).dropDuplicates(["shipment_id"])

display(shipment_nodes_df.limit(10))

# COMMAND ----------

shipment_node_cypher = """
UNWIND $rows AS row
MERGE (sh:Shipment {shipment_id: row.shipment_id})
SET
    sh.shipment_type = row.shipment_type,
    sh.shipment_status = row.shipment_status,
    sh.carrier = row.carrier,
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

validation_queries = {
    "Supplier nodes": "MATCH (n:Supplier) RETURN count(n) AS count",
    "Product nodes": "MATCH (n:Product) RETURN count(n) AS count",
    "Store nodes": "MATCH (n:Store) RETURN count(n) AS count",
    "Warehouse nodes": "MATCH (n:Warehouse) RETURN count(n) AS count",
    "Shipment nodes": "MATCH (n:Shipment) RETURN count(n) AS count",
    "RiskSignal nodes": "MATCH (n:RiskSignal) RETURN count(n) AS count",
    "SUPPLIES relationships": "MATCH (:Supplier)-[r:SUPPLIES]->(:Product) RETURN count(r) AS count",
    "SERVES relationships": "MATCH (:Warehouse)-[r:SERVES]->(:Store) RETURN count(r) AS count",
    "STOCKS relationships": "MATCH (:Store)-[r:STOCKS]->(:Product) RETURN count(r) AS count",
    "MOVES relationships": "MATCH (:Shipment)-[r:MOVES]->(:Product) RETURN count(r) AS count",
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