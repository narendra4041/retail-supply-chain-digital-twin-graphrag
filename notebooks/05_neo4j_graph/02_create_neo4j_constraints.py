# Databricks notebook source
# MAGIC %pip install neo4j

# COMMAND ----------

from neo4j import GraphDatabase

# COMMAND ----------

SECRET_SCOPE = "retail-digital-twin-dev"

neo4j_uri = dbutils.secrets.get(scope=SECRET_SCOPE, key="neo4j-uri")
neo4j_username = dbutils.secrets.get(scope=SECRET_SCOPE, key="neo4j-username")
neo4j_password = dbutils.secrets.get(scope=SECRET_SCOPE, key="neo4j-password")
neo4j_database = dbutils.secrets.get(scope=SECRET_SCOPE, key="neo4j-database")

# COMMAND ----------

CONSTRAINTS = [
    """
    CREATE CONSTRAINT supplier_id_unique IF NOT EXISTS
    FOR (s:Supplier)
    REQUIRE s.supplier_id IS UNIQUE
    """,
    """
    CREATE CONSTRAINT product_id_unique IF NOT EXISTS
    FOR (p:Product)
    REQUIRE p.product_id IS UNIQUE
    """,
    """
    CREATE CONSTRAINT store_id_unique IF NOT EXISTS
    FOR (s:Store)
    REQUIRE s.store_id IS UNIQUE
    """,
    """
    CREATE CONSTRAINT warehouse_id_unique IF NOT EXISTS
    FOR (w:Warehouse)
    REQUIRE w.warehouse_id IS UNIQUE
    """,
    """
    CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
    FOR (c:Customer)
    REQUIRE c.customer_id IS UNIQUE
    """,
    """
    CREATE CONSTRAINT shipment_id_unique IF NOT EXISTS
    FOR (sh:Shipment)
    REQUIRE sh.shipment_id IS UNIQUE
    """,
    """
    CREATE CONSTRAINT risk_signal_id_unique IF NOT EXISTS
    FOR (r:RiskSignal)
    REQUIRE r.risk_signal_id IS UNIQUE
    """
]

# COMMAND ----------

driver = GraphDatabase.driver(
    neo4j_uri,
    auth=(neo4j_username, neo4j_password),
)

try:
    driver.verify_connectivity()

    with driver.session(database=neo4j_database) as session:
        for constraint_sql in CONSTRAINTS:
            session.run(constraint_sql)
            print("Created/verified constraint:")
            print(constraint_sql.strip())
            print("-" * 80)

finally:
    driver.close()

# COMMAND ----------

driver = GraphDatabase.driver(
    neo4j_uri,
    auth=(neo4j_username, neo4j_password),
)

try:
    with driver.session(database=neo4j_database) as session:
        result = session.run("SHOW CONSTRAINTS")
        constraints = [record.data() for record in result]

    print(f"Total constraints found: {len(constraints)}")

    for constraint in constraints:
        print(constraint)

finally:
    driver.close()