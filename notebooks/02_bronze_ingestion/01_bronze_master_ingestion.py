# Databricks notebook source

# COMMAND ----------

from src.common.config_loader import load_config

config = load_config("dev")

catalog_name = config["unity_catalog"]["catalog"]
bronze_schema = config["unity_catalog"]["schemas"]["bronze"]
landing_base_path = config["paths"]["landing_base"]

master_data_config = config["master_data"]

datasets = {
    dataset_name: f"{landing_base_path}/{relative_path}"
    for dataset_name, relative_path in master_data_config.items()
}

print(f"Catalog: {catalog_name}")
print(f"Bronze schema: {bronze_schema}")
print(f"Landing base path: {landing_base_path}")
print(datasets)

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {bronze_schema}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit


def read_master_parquet(dataset_name: str, source_path: str):
    return (
        spark.read
        .format("parquet")
        .load(source_path)
        .withColumn("_source_system", lit("synthetic_master_data"))
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_dataset_name", lit(dataset_name))
    )


def write_bronze_table(dataset_name: str, source_path: str) -> None:
    table_name = f"{catalog_name}.{bronze_schema}.{dataset_name}_raw"

    df = read_master_parquet(dataset_name, source_path)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    record_count = df.count()

    print("=" * 80)
    print(f"Created Bronze table: {table_name}")
    print(f"Source path: {source_path}")
    print(f"Record count: {record_count:,}")


# COMMAND ----------

for dataset_name, source_path in datasets.items():
    write_bronze_table(dataset_name, source_path)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT table_catalog, table_schema, table_name
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog_name}'
          AND table_schema = '{bronze_schema}'
        ORDER BY table_name
        """
    )
)