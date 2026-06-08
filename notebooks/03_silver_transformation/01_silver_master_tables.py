# Databricks notebook source

# COMMAND ----------

from src.common.config_loader import load_config

config = load_config("dev")

catalog_name = config["unity_catalog"]["catalog"]
bronze_schema = config["unity_catalog"]["schemas"]["bronze"]
silver_schema = config["unity_catalog"]["schemas"]["silver"]

print(f"Catalog: {catalog_name}")
print(f"Bronze schema: {bronze_schema}")
print(f"Silver schema: {silver_schema}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {silver_schema}")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lower,
    row_number,
    trim,
    when,
)
from pyspark.sql.window import Window


def deduplicate_by_key(df: DataFrame, key_column: str) -> DataFrame:
    """
    Deduplicate records by business key.
    Keeps the latest record based on _ingestion_timestamp.
    """
    window_spec = Window.partitionBy(key_column).orderBy(col("_ingestion_timestamp").desc())

    return (
        df.withColumn("_row_number", row_number().over(window_spec))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )


def write_silver_table(df: DataFrame, table_name: str) -> None:
    """
    Writes a Silver table as Delta.
    """
    full_table_name = f"{catalog_name}.{silver_schema}.{table_name}"

    record_count = df.count()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )

    print("=" * 100)
    print(f"Created Silver table: {full_table_name}")
    print(f"Record count: {record_count:,}")


# COMMAND ----------

def build_silver_suppliers() -> DataFrame:
    source_table = f"{catalog_name}.{bronze_schema}.suppliers_raw"

    df = spark.table(source_table)

    cleaned_df = (
        df.select(
            trim(col("supplier_id")).alias("supplier_id"),
            trim(col("supplier_name")).alias("supplier_name"),
            trim(col("country")).alias("country"),
            trim(col("region")).alias("region"),
            lower(trim(col("supplier_type"))).alias("supplier_type"),
            col("lead_time_days").cast("int").alias("lead_time_days"),
            col("reliability_score").cast("double").alias("reliability_score"),
            col("quality_score").cast("double").alias("quality_score"),
            col("contract_start_date").cast("date").alias("contract_start_date"),
            col("contract_end_date").cast("date").alias("contract_end_date"),
            col("preferred_supplier_flag").cast("boolean").alias("preferred_supplier_flag"),
            col("created_at").cast("timestamp").alias("created_at"),
            col("updated_at").cast("timestamp").alias("updated_at"),
            col("_source_file_path"),
            col("_source_file_name"),
            col("_ingestion_timestamp"),
        )
        .filter(col("supplier_id").isNotNull())
        .filter(col("supplier_name").isNotNull())
        .filter(col("lead_time_days") >= 0)
        .filter(col("reliability_score").between(0, 1))
        .filter(col("quality_score").between(0, 1))
        .withColumn(
            "supplier_risk_band",
            when(col("reliability_score") < 0.80, lit("high"))
            .when(col("reliability_score") < 0.90, lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return deduplicate_by_key(cleaned_df, "supplier_id")


def build_silver_products() -> DataFrame:
    source_table = f"{catalog_name}.{bronze_schema}.products_raw"

    df = spark.table(source_table)

    cleaned_df = (
        df.select(
            trim(col("product_id")).alias("product_id"),
            trim(col("product_name")).alias("product_name"),
            trim(col("category")).alias("category"),
            trim(col("sub_category")).alias("sub_category"),
            trim(col("brand")).alias("brand"),
            col("unit_price").cast("double").alias("unit_price"),
            col("unit_cost").cast("double").alias("unit_cost"),
            trim(col("supplier_id")).alias("supplier_id"),
            col("weight_kg").cast("double").alias("weight_kg"),
            col("volume_m3").cast("double").alias("volume_m3"),
            col("active_flag").cast("boolean").alias("active_flag"),
            col("created_at").cast("timestamp").alias("created_at"),
            col("updated_at").cast("timestamp").alias("updated_at"),
            col("_source_file_path"),
            col("_source_file_name"),
            col("_ingestion_timestamp"),
        )
        .filter(col("product_id").isNotNull())
        .filter(col("product_name").isNotNull())
        .filter(col("supplier_id").isNotNull())
        .filter(col("unit_price") >= 0)
        .filter(col("unit_cost") >= 0)
        .withColumn("gross_margin", col("unit_price") - col("unit_cost"))
        .withColumn(
            "gross_margin_pct",
            when(col("unit_price") > 0, (col("unit_price") - col("unit_cost")) / col("unit_price"))
            .otherwise(lit(0.0)),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return deduplicate_by_key(cleaned_df, "product_id")


def build_silver_warehouses() -> DataFrame:
    source_table = f"{catalog_name}.{bronze_schema}.warehouses_raw"

    df = spark.table(source_table)

    cleaned_df = (
        df.select(
            trim(col("warehouse_id")).alias("warehouse_id"),
            trim(col("warehouse_name")).alias("warehouse_name"),
            trim(col("country")).alias("country"),
            trim(col("city")).alias("city"),
            trim(col("region")).alias("region"),
            col("capacity_units").cast("int").alias("capacity_units"),
            col("current_utilization_pct").cast("double").alias("current_utilization_pct"),
            col("created_at").cast("timestamp").alias("created_at"),
            col("updated_at").cast("timestamp").alias("updated_at"),
            col("_source_file_path"),
            col("_source_file_name"),
            col("_ingestion_timestamp"),
        )
        .filter(col("warehouse_id").isNotNull())
        .filter(col("warehouse_name").isNotNull())
        .filter(col("capacity_units") > 0)
        .filter(col("current_utilization_pct").between(0, 1))
        .withColumn(
            "utilization_band",
            when(col("current_utilization_pct") >= 0.85, lit("high"))
            .when(col("current_utilization_pct") >= 0.60, lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return deduplicate_by_key(cleaned_df, "warehouse_id")


def build_silver_stores() -> DataFrame:
    source_table = f"{catalog_name}.{bronze_schema}.stores_raw"

    df = spark.table(source_table)

    cleaned_df = (
        df.select(
            trim(col("store_id")).alias("store_id"),
            trim(col("store_name")).alias("store_name"),
            trim(col("country")).alias("country"),
            trim(col("city")).alias("city"),
            trim(col("region")).alias("region"),
            lower(trim(col("store_type"))).alias("store_type"),
            col("size_sq_m").cast("int").alias("size_sq_m"),
            trim(col("warehouse_id")).alias("warehouse_id"),
            col("created_at").cast("timestamp").alias("created_at"),
            col("updated_at").cast("timestamp").alias("updated_at"),
            col("_source_file_path"),
            col("_source_file_name"),
            col("_ingestion_timestamp"),
        )
        .filter(col("store_id").isNotNull())
        .filter(col("store_name").isNotNull())
        .filter(col("warehouse_id").isNotNull())
        .filter(col("size_sq_m") > 0)
        .withColumn(
            "store_size_band",
            when(col("size_sq_m") >= 10000, lit("large"))
            .when(col("size_sq_m") >= 3000, lit("medium"))
            .otherwise(lit("small")),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return deduplicate_by_key(cleaned_df, "store_id")


def build_silver_customers() -> DataFrame:
    source_table = f"{catalog_name}.{bronze_schema}.customers_raw"

    df = spark.table(source_table)

    cleaned_df = (
        df.select(
            trim(col("customer_id")).alias("customer_id"),
            trim(col("customer_name")).alias("customer_name"),
            trim(col("country")).alias("country"),
            trim(col("city")).alias("city"),
            lower(trim(col("customer_segment"))).alias("customer_segment"),
            lower(trim(col("loyalty_tier"))).alias("loyalty_tier"),
            col("signup_date").cast("date").alias("signup_date"),
            col("created_at").cast("timestamp").alias("created_at"),
            col("updated_at").cast("timestamp").alias("updated_at"),
            col("_source_file_path"),
            col("_source_file_name"),
            col("_ingestion_timestamp"),
        )
        .filter(col("customer_id").isNotNull())
        .filter(col("customer_name").isNotNull())
        .filter(col("signup_date").isNotNull())
        .withColumn(
            "loyalty_rank",
            when(col("loyalty_tier") == "bronze", lit(1))
            .when(col("loyalty_tier") == "silver", lit(2))
            .when(col("loyalty_tier") == "gold", lit(3))
            .when(col("loyalty_tier") == "platinum", lit(4))
            .otherwise(lit(0)),
        )
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return deduplicate_by_key(cleaned_df, "customer_id")


# COMMAND ----------

silver_suppliers = build_silver_suppliers()
silver_products = build_silver_products()
silver_warehouses = build_silver_warehouses()
silver_stores = build_silver_stores()
silver_customers = build_silver_customers()

# COMMAND ----------

write_silver_table(silver_suppliers, "suppliers")
write_silver_table(silver_products, "products")
write_silver_table(silver_warehouses, "warehouses")
write_silver_table(silver_stores, "stores")
write_silver_table(silver_customers, "customers")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            table_catalog,
            table_schema,
            table_name
        FROM system.information_schema.tables
        WHERE table_catalog = '{catalog_name}'
          AND table_schema = '{silver_schema}'
        ORDER BY table_name
        """
    )
)

# COMMAND ----------

for table_name in ["suppliers", "products", "warehouses", "stores", "customers"]:
    full_table_name = f"{catalog_name}.{silver_schema}.{table_name}"
    print("=" * 100)
    print(full_table_name)
    display(spark.table(full_table_name).limit(5))