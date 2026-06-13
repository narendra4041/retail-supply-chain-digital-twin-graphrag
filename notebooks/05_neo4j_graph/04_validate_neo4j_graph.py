# Databricks notebook source
# MAGIC %pip install neo4j

# COMMAND ----------

from typing import Any, Dict, List

from neo4j import GraphDatabase

# COMMAND ----------

# Purpose:
# Validate the Neo4j graph after the Databricks-to-Neo4j graph load.
#
# This notebook performs:
# 1. Technical validation: node counts and relationship counts
# 2. Required object checks: fail when core graph objects are missing
# 3. Business validation: sample supply-chain graph paths
# 4. GraphRAG readiness checks: whether the graph can answer risk-context questions
#
# This notebook does not modify the graph.

dbutils.widgets.text("secret_scope", "")

# COMMAND ----------

def get_required_widget(name: str) -> str:
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"Required notebook parameter '{name}' is missing.")
    return value


secret_scope = get_required_widget("secret_scope")

print(f"Secret scope: {secret_scope}")

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

def run_cypher(query: str, parameters: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    if parameters is None:
        parameters = {}

    with driver.session(database=neo4j_database) as session:
        result = session.run(query, **parameters)
        return [record.data() for record in result]


def run_scalar_count(query: str) -> int:
    rows = run_cypher(query)
    if not rows:
        return 0

    first_row = rows[0]
    if "count" in first_row:
        return int(first_row["count"])

    return int(next(iter(first_row.values())))


def print_rows(title: str, rows: List[Dict[str, Any]], limit: int = 10) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)

    if not rows:
        print("No rows returned.")
        return

    for index, row in enumerate(rows[:limit], start=1):
        print(f"{index}. {row}")

    if len(rows) > limit:
        print(f"... {len(rows) - limit} more rows not printed")


def fail_if_empty(name: str, count_value: int, failures: List[str]) -> None:
    if count_value <= 0:
        failures.append(f"{name} count is zero.")

# COMMAND ----------

node_count_query = """
MATCH (n)
RETURN
    labels(n) AS labels,
    count(n) AS count
ORDER BY labels
"""

node_counts = run_cypher(node_count_query)
print_rows("Node counts by label", node_counts, limit=50)

# COMMAND ----------

relationship_count_query = """
MATCH ()-[r]->()
RETURN
    type(r) AS relationship_type,
    count(r) AS count
ORDER BY relationship_type
"""

relationship_counts = run_cypher(relationship_count_query)
print_rows("Relationship counts by type", relationship_counts, limit=50)

# COMMAND ----------

required_node_checks = {
    "Supplier nodes": "MATCH (n:Supplier) RETURN count(n) AS count",
    "Product nodes": "MATCH (n:Product) RETURN count(n) AS count",
    "Store nodes": "MATCH (n:Store) RETURN count(n) AS count",
    "Warehouse nodes": "MATCH (n:Warehouse) RETURN count(n) AS count",
    "Customer nodes": "MATCH (n:Customer) RETURN count(n) AS count",
    "Shipment nodes": "MATCH (n:Shipment) RETURN count(n) AS count",
    "RiskSignal nodes": "MATCH (n:RiskSignal) RETURN count(n) AS count",
}

required_relationship_checks = {
    "SUPPLIES relationships": "MATCH (:Supplier)-[r:SUPPLIES]->(:Product) RETURN count(r) AS count",
    "SERVES relationships": "MATCH (:Warehouse)-[r:SERVES]->(:Store) RETURN count(r) AS count",
    "STOCKS relationships": "MATCH (:Store)-[r:STOCKS]->(:Product) RETURN count(r) AS count",
    "MOVES relationships": "MATCH (:Shipment)-[r:MOVES]->(:Product) RETURN count(r) AS count",
    "HAS_HEALTH_STATUS relationships": "MATCH ()-[r:HAS_HEALTH_STATUS]->(:RiskSignal) RETURN count(r) AS count",
}

optional_relationship_checks = {
    "FROM_SUPPLIER relationships": "MATCH (:Shipment)-[r:FROM_SUPPLIER]->(:Supplier) RETURN count(r) AS count",
    "FROM_WAREHOUSE relationships": "MATCH (:Shipment)-[r:FROM_WAREHOUSE]->(:Warehouse) RETURN count(r) AS count",
    "TO_STORE relationships": "MATCH (:Shipment)-[r:TO_STORE]->(:Store) RETURN count(r) AS count",
    "TO_WAREHOUSE relationships": "MATCH (:Shipment)-[r:TO_WAREHOUSE]->(:Warehouse) RETURN count(r) AS count",
}

validation_failures: List[str] = []

print("\n" + "=" * 100)
print("Required node checks")
print("=" * 100)

for check_name, check_query in required_node_checks.items():
    count_value = run_scalar_count(check_query)
    print(f"{check_name}: {count_value}")
    fail_if_empty(check_name, count_value, validation_failures)

print("\n" + "=" * 100)
print("Required relationship checks")
print("=" * 100)

for check_name, check_query in required_relationship_checks.items():
    count_value = run_scalar_count(check_query)
    print(f"{check_name}: {count_value}")
    fail_if_empty(check_name, count_value, validation_failures)

print("\n" + "=" * 100)
print("Optional relationship checks")
print("=" * 100)

for check_name, check_query in optional_relationship_checks.items():
    count_value = run_scalar_count(check_query)
    print(f"{check_name}: {count_value}")
    if count_value <= 0:
        print(f"WARNING: {check_name} is zero. This may be valid depending on shipment source/destination mix.")

# COMMAND ----------

orphan_checks = {
    "Products without supplier": """
        MATCH (p:Product)
        WHERE NOT EXISTS {
            MATCH (:Supplier)-[:SUPPLIES]->(p)
        }
        RETURN count(p) AS count
    """,
    "Stores without serving warehouse": """
        MATCH (s:Store)
        WHERE NOT EXISTS {
            MATCH (:Warehouse)-[:SERVES]->(s)
        }
        RETURN count(s) AS count
    """,
    "Shipments without product": """
        MATCH (sh:Shipment)
        WHERE NOT EXISTS {
            MATCH (sh)-[:MOVES]->(:Product)
        }
        RETURN count(sh) AS count
    """,
    "RiskSignals without owning entity": """
        MATCH (r:RiskSignal)
        WHERE NOT EXISTS {
            MATCH ()-[:HAS_HEALTH_STATUS]->(r)
        }
        RETURN count(r) AS count
    """,
}

print("\n" + "=" * 100)
print("Orphan checks")
print("=" * 100)

for check_name, check_query in orphan_checks.items():
    count_value = run_scalar_count(check_query)
    print(f"{check_name}: {count_value}")

# COMMAND ----------

supplier_impact_query = """
MATCH (s:Supplier)-[rel:SUPPLIES]->(p:Product)
WHERE lower(coalesce(s.risk_band, '')) IN ['high', 'critical']
   OR lower(coalesce(rel.dependency_risk_band, '')) IN ['high', 'critical']
RETURN
    s.supplier_id AS supplier_id,
    s.supplier_name AS supplier_name,
    s.risk_band AS supplier_risk_band,
    p.product_id AS product_id,
    p.product_name AS product_name,
    p.category AS product_category,
    rel.dependency_risk_band AS dependency_risk_band,
    rel.total_revenue AS total_revenue,
    rel.primary_risk_reason AS primary_risk_reason
ORDER BY rel.total_revenue DESC
LIMIT 20
"""

supplier_impact_rows = run_cypher(supplier_impact_query)
print_rows("Business validation: high-risk suppliers and impacted products", supplier_impact_rows, limit=20)

# COMMAND ----------

urgent_replenishment_query = """
MATCH (w:Warehouse)-[:SERVES]->(st:Store)-[stock:STOCKS]->(p:Product)
WHERE lower(coalesce(stock.replenishment_priority, '')) IN ['urgent', 'high']
   OR lower(coalesce(stock.stockout_risk_band, '')) IN ['high', 'critical']
RETURN
    w.warehouse_id AS warehouse_id,
    w.warehouse_name AS warehouse_name,
    st.store_id AS store_id,
    st.store_name AS store_name,
    p.product_id AS product_id,
    p.product_name AS product_name,
    stock.available_stock AS available_stock,
    stock.reorder_point AS reorder_point,
    stock.stockout_risk_band AS stockout_risk_band,
    stock.replenishment_priority AS replenishment_priority,
    stock.warehouse_action AS warehouse_action
LIMIT 20
"""

urgent_replenishment_rows = run_cypher(urgent_replenishment_query)
print_rows("Business validation: stores needing urgent replenishment", urgent_replenishment_rows, limit=20)

# COMMAND ----------

delayed_shipment_query = """
MATCH (sh:Shipment)-[:MOVES]->(p:Product)
OPTIONAL MATCH (sh)-[:TO_STORE]->(st:Store)
OPTIONAL MATCH (sh)-[:TO_WAREHOUSE]->(w:Warehouse)
WHERE coalesce(sh.is_delayed, false) = true
   OR lower(coalesce(sh.impact_band, '')) IN ['high', 'critical']
RETURN
    sh.shipment_id AS shipment_id,
    sh.carrier AS carrier,
    sh.delay_days AS delay_days,
    sh.delay_severity AS delay_severity,
    sh.impact_band AS impact_band,
    p.product_id AS product_id,
    p.product_name AS product_name,
    st.store_id AS destination_store_id,
    st.store_name AS destination_store_name,
    w.warehouse_id AS destination_warehouse_id,
    w.warehouse_name AS destination_warehouse_name
ORDER BY sh.delay_days DESC
LIMIT 20
"""

delayed_shipment_rows = run_cypher(delayed_shipment_query)
print_rows("Business validation: delayed shipments impacting products and locations", delayed_shipment_rows, limit=20)

# COMMAND ----------

end_to_end_risk_query = """
MATCH (sup:Supplier)-[supplies:SUPPLIES]->(p:Product)<-[stock:STOCKS]-(st:Store)<-[:SERVES]-(w:Warehouse)
OPTIONAL MATCH (sh:Shipment)-[:MOVES]->(p)
OPTIONAL MATCH (sh)-[:TO_STORE]->(st)
WHERE lower(coalesce(p.highest_stockout_risk_band, '')) IN ['high', 'critical']
   OR lower(coalesce(stock.stockout_risk_band, '')) IN ['high', 'critical']
   OR lower(coalesce(stock.replenishment_priority, '')) IN ['urgent', 'high']
   OR lower(coalesce(sup.risk_band, '')) IN ['high', 'critical']
   OR lower(coalesce(sh.impact_band, '')) IN ['high', 'critical']
RETURN
    sup.supplier_id AS supplier_id,
    sup.supplier_name AS supplier_name,
    sup.risk_band AS supplier_risk_band,
    p.product_id AS product_id,
    p.product_name AS product_name,
    p.category AS product_category,
    p.demand_band AS demand_band,
    p.highest_stockout_risk_band AS product_stockout_risk_band,
    st.store_id AS store_id,
    st.store_name AS store_name,
    w.warehouse_id AS warehouse_id,
    w.warehouse_name AS warehouse_name,
    stock.stockout_risk_band AS store_product_stockout_risk_band,
    stock.replenishment_priority AS replenishment_priority,
    sh.shipment_id AS shipment_id,
    sh.impact_band AS shipment_impact_band
LIMIT 25
"""

end_to_end_risk_rows = run_cypher(end_to_end_risk_query)
print_rows("Business validation: end-to-end supply chain risk paths", end_to_end_risk_rows, limit=25)

# COMMAND ----------

business_result_counts = {
    "supplier_impact_rows": len(supplier_impact_rows),
    "urgent_replenishment_rows": len(urgent_replenishment_rows),
    "delayed_shipment_rows": len(delayed_shipment_rows),
    "end_to_end_risk_rows": len(end_to_end_risk_rows),
}

print("\n" + "=" * 100)
print("GraphRAG readiness")
print("=" * 100)

for name, count_value in business_result_counts.items():
    print(f"{name}: {count_value}")

if all(count_value == 0 for count_value in business_result_counts.values()):
    validation_failures.append(
        "All business validation queries returned zero rows. Graph loaded technically, but business risk paths are not useful yet."
    )

if validation_failures:
    print("\n" + "=" * 100)
    print("VALIDATION FAILED")
    print("=" * 100)
    for failure in validation_failures:
        print(f"- {failure}")
    raise ValueError("Neo4j graph validation failed. See failures above.")

print("\n" + "=" * 100)
print("VALIDATION PASSED")
print("=" * 100)
print("Neo4j graph is technically loaded and business-query ready.")

# COMMAND ----------

driver.close()
print("Neo4j validation completed successfully.")
