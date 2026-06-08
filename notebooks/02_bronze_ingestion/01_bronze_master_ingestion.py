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

for dataset_name, source_path in datasets.items():
    print(f"{dataset_name}: {source_path}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {bronze_schema}")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit


def read_master_parquet(dataset_name: str, source_path: str) -> DataFrame:
    """
    Reads master Parquet files from ADLS landing.

    Unity Catalog does not support input_file_name().
    Use the hidden _metadata column instead.
    """
    raw_df = (
        spark.read
        .format("parquet")
        .load(source_path)
    )

    return (
        raw_df
        .select(
            "*",
            col("_metadata.file_path").alias("_source_file_path"),
            col("_metadata.file_name").alias("_source_file_name"),
            col("_metadata.file_size").alias("_source_file_size"),
            col("_metadata.file_modification_time").alias("_source_file_modification_time"),
        )
        .withColumn("_source_system", lit("synthetic_master_data"))
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_dataset_name", lit(dataset_name))
    )


def write_bronze_table(dataset_name: str, source_path: str) -> None:
    table_name = f"{catalog_name}.{bronze_schema}.{dataset_name}_raw"

    df = read_master_parquet(dataset_name, source_path)

    record_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print("=" * 100)
    print(f"Created Bronze table: {table_name}")
    print(f"Source path: {source_path}")
    print(f"Record count: {record_count:,}")


# COMMAND ----------

for dataset_name, source_path in datasets.items():
    write_bronze_table(dataset_name, source_path)

# COMMAND ----------

bronze_tables_df = spark.sql(
    f"""
    SELECT
        table_catalog,
        table_schema,
        table_name
    FROM system.information_schema.tables
    WHERE table_catalog = '{catalog_name}'
      AND table_schema = '{bronze_schema}'
    ORDER BY table_name
    """
)

display(bronze_tables_df)

# COMMAND ----------

for dataset_name in datasets.keys():
    table_name = f"{catalog_name}.{bronze_schema}.{dataset_name}_raw"
    print("=" * 100)
    print(table_name)
    display(spark.table(table_name).limit(5))