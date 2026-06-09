# Databricks notebook source

# COMMAND ----------

from src.common.config_loader import load_config

config = load_config("dev")

catalog_name = config["unity_catalog"]["catalog"]
silver_schema = config["unity_catalog"]["schemas"]["silver"]
monitoring_schema = config["unity_catalog"]["schemas"]["monitoring"]

dq_table = f"{catalog_name}.{monitoring_schema}.data_quality_results"

print(f"Catalog: {catalog_name}")
print(f"Silver schema: {silver_schema}")
print(f"Monitoring schema: {monitoring_schema}")
print(f"DQ result table: {dq_table}")

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit


run_id = datetime.now(timezone.utc).strftime("dq_run_%Y%m%d_%H%M%S")


def table_name(schema: str, name: str) -> str:
    return f"{catalog_name}.{schema}.{name}"


def count_rows(df: DataFrame) -> int:
    return df.count()


def create_dq_result(
    rule_name: str,
    table_name_value: str,
    rule_type: str,
    severity: str,
    status: str,
    failed_count: int,
    total_count: int,
    description: str,
) -> Dict[str, object]:
    return {
        "run_id": run_id,
        "rule_name": rule_name,
        "table_name": table_name_value,
        "rule_type": rule_type,
        "severity": severity,
        "status": status,
        "failed_count": failed_count,
        "total_count": total_count,
        "description": description,
    }


dq_results: List[Dict[str, object]] = []

# COMMAND ----------

def validate_not_null(
    table: str,
    column_name: str,
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()
    failed_count = df.filter(col(column_name).isNull()).count()

    status = "PASS" if failed_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.{column_name}.not_null",
            table_name_value=table,
            rule_type="not_null",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=f"Column {column_name} must not be null.",
        )
    )


def validate_unique_key(
    table: str,
    key_column: str,
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()

    duplicate_count = (
        df.groupBy(key_column)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    status = "PASS" if duplicate_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.{key_column}.unique",
            table_name_value=table,
            rule_type="unique_key",
            severity=severity,
            status=status,
            failed_count=duplicate_count,
            total_count=total_count,
            description=f"Column {key_column} must be unique.",
        )
    )


def validate_numeric_range(
    table: str,
    column_name: str,
    min_value: float,
    max_value: float,
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()

    failed_count = (
        df.filter(
            col(column_name).isNull()
            | (col(column_name) < min_value)
            | (col(column_name) > max_value)
        )
        .count()
    )

    status = "PASS" if failed_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.{column_name}.between_{min_value}_{max_value}",
            table_name_value=table,
            rule_type="numeric_range",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=f"Column {column_name} must be between {min_value} and {max_value}.",
        )
    )


def validate_non_negative(
    table: str,
    column_name: str,
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()
    failed_count = df.filter(col(column_name).isNull() | (col(column_name) < 0)).count()

    status = "PASS" if failed_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.{column_name}.non_negative",
            table_name_value=table,
            rule_type="non_negative",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=f"Column {column_name} must be non-negative.",
        )
    )


def validate_referential_integrity(
    child_table: str,
    child_column: str,
    parent_table: str,
    parent_column: str,
    severity: str = "critical",
) -> None:
    child_df = spark.table(child_table).select(col(child_column).alias("child_key"))
    parent_df = spark.table(parent_table).select(col(parent_column).alias("parent_key")).distinct()

    total_count = child_df.count()

    failed_count = (
        child_df.join(
            parent_df,
            child_df["child_key"] == parent_df["parent_key"],
            "left_anti",
        )
        .filter(col("child_key").isNotNull())
        .count()
    )

    status = "PASS" if failed_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{child_table}.{child_column}.references.{parent_table}.{parent_column}",
            table_name_value=child_table,
            rule_type="referential_integrity",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=f"{child_table}.{child_column} must exist in {parent_table}.{parent_column}.",
        )
    )


def validate_row_count_minimum(
    table: str,
    minimum_count: int,
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()
    failed_count = 0 if total_count >= minimum_count else 1

    status = "PASS" if failed_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.row_count.minimum_{minimum_count}",
            table_name_value=table,
            rule_type="row_count",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=f"Table must contain at least {minimum_count} rows.",
        )
    )

# COMMAND ----------

suppliers_table = table_name(silver_schema, "suppliers")
products_table = table_name(silver_schema, "products")
warehouses_table = table_name(silver_schema, "warehouses")
stores_table = table_name(silver_schema, "stores")
customers_table = table_name(silver_schema, "customers")

# COMMAND ----------

# Row count checks
validate_row_count_minimum(suppliers_table, 1)
validate_row_count_minimum(products_table, 1)
validate_row_count_minimum(warehouses_table, 1)
validate_row_count_minimum(stores_table, 1)
validate_row_count_minimum(customers_table, 1)

# COMMAND ----------

# Primary key checks
validate_not_null(suppliers_table, "supplier_id")
validate_unique_key(suppliers_table, "supplier_id")

validate_not_null(products_table, "product_id")
validate_unique_key(products_table, "product_id")

validate_not_null(warehouses_table, "warehouse_id")
validate_unique_key(warehouses_table, "warehouse_id")

validate_not_null(stores_table, "store_id")
validate_unique_key(stores_table, "store_id")

validate_not_null(customers_table, "customer_id")
validate_unique_key(customers_table, "customer_id")

# COMMAND ----------

# Business field checks
validate_numeric_range(suppliers_table, "reliability_score", 0, 1)
validate_numeric_range(suppliers_table, "quality_score", 0, 1)
validate_non_negative(suppliers_table, "lead_time_days")

validate_non_negative(products_table, "unit_price")
validate_non_negative(products_table, "unit_cost")
validate_non_negative(products_table, "weight_kg")
validate_non_negative(products_table, "volume_m3")

validate_non_negative(warehouses_table, "capacity_units")
validate_numeric_range(warehouses_table, "current_utilization_pct", 0, 1)

validate_non_negative(stores_table, "size_sq_m")

# COMMAND ----------

# Referential integrity checks
validate_referential_integrity(
    child_table=products_table,
    child_column="supplier_id",
    parent_table=suppliers_table,
    parent_column="supplier_id",
)

validate_referential_integrity(
    child_table=stores_table,
    child_column="warehouse_id",
    parent_table=warehouses_table,
    parent_column="warehouse_id",
)

# COMMAND ----------

dq_results_df = spark.createDataFrame(dq_results)

dq_results_df = (
    dq_results_df
    .withColumn("environment", lit(config["environment"]))
    .withColumn("validated_at", current_timestamp())
)

display(dq_results_df.orderBy("severity", "status", "table_name", "rule_name"))

# COMMAND ----------

(
    dq_results_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(dq_table)
)

print(f"Written data quality results to: {dq_table}")

# COMMAND ----------

critical_failures = (
    dq_results_df
    .filter((col("severity") == "critical") & (col("status") == "FAIL"))
    .count()
)

warning_failures = (
    dq_results_df
    .filter((col("severity") == "warning") & (col("status") == "FAIL"))
    .count()
)

print(f"Critical failures: {critical_failures}")
print(f"Warning failures: {warning_failures}")

if critical_failures > 0:
    failed_rules = (
        dq_results_df
        .filter((col("severity") == "critical") & (col("status") == "FAIL"))
        .select("rule_name", "failed_count", "description")
        .collect()
    )

    failed_rule_text = "\n".join(
        [
            f"- {row['rule_name']} | failed_count={row['failed_count']} | {row['description']}"
            for row in failed_rules
        ]
    )

    raise Exception(
        "Critical data quality checks failed:\n"
        + failed_rule_text
    )

print("All critical Silver master data quality checks passed.")