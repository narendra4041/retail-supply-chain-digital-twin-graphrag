# Databricks notebook source
# MAGIC %pip install neo4j

# COMMAND ----------

from neo4j import GraphDatabase

# COMMAND ----------

SECRET_SCOPE = "retail-digital-twin-dev"

NEO4J_URI_SECRET_KEY = "neo4j-uri"
NEO4J_USERNAME_SECRET_KEY = "neo4j-username"
NEO4J_PASSWORD_SECRET_KEY = "neo4j-password"
NEO4J_DATABASE_SECRET_KEY = "neo4j-database"

# COMMAND ----------

neo4j_uri = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key=NEO4J_URI_SECRET_KEY,
)

neo4j_username = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key=NEO4J_USERNAME_SECRET_KEY,
)

neo4j_password = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key=NEO4J_PASSWORD_SECRET_KEY,
)

neo4j_database = dbutils.secrets.get(
    scope=SECRET_SCOPE,
    key=NEO4J_DATABASE_SECRET_KEY,
)

print("Neo4j secrets loaded successfully.")
print(f"Neo4j database: {neo4j_database}")

# COMMAND ----------

driver = GraphDatabase.driver(
    neo4j_uri,
    auth=(neo4j_username, neo4j_password),
)

try:
    driver.verify_connectivity()
    print("Neo4j connectivity verified successfully.")
finally:
    driver.close()

# COMMAND ----------

driver = GraphDatabase.driver(
    neo4j_uri,
    auth=(neo4j_username, neo4j_password),
)

try:
    with driver.session(database=neo4j_database) as session:
        result = session.run("""
            RETURN
                1 AS connection_test,
                datetime() AS neo4j_server_time
        """)
        row = result.single()

    print("Connection test result:", row["connection_test"])
    print("Neo4j server time:", row["neo4j_server_time"])

finally:
    driver.close()