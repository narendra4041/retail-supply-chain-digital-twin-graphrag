# Databricks notebook source

# COMMAND ----------

environment = dbutils.widgets.get("environment")
catalog_name = dbutils.widgets.get("catalog_name")
silver_schema = dbutils.widgets.get("silver_schema")
monitoring_schema = dbutils.widgets.get("monitoring_schema")

dq_table = f"{catalog_name}.{monitoring_schema}.data_quality_results"

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")
print(f"Silver schema: {silver_schema}")
print(f"Monitoring schema: {monitoring_schema}")
print(f"DQ result table: {dq_table}")

# COMMAND ----------

from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

run_id = datetime.now(timezone.utc).strftime("silver_event_dq_run_%Y%m%d_%H%M%S")

dq_results: List[Dict[str, object]] = []


def table_name(name: str) -> str:
    return f"{catalog_name}.{silver_schema}.{name}"


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


# COMMAND ----------

def validate_table_exists(table: str, severity: str = "critical") -> bool:
    try:
        spark.table(table).limit(1).count()
        status = "PASS"
        failed_count = 0
        exists = True
    except Exception:
        status = "FAIL"
        failed_count = 1
        exists = False

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.exists",
            table_name_value=table,
            rule_type="table_exists",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=1,
            description=f"Table {table} must exist and be readable.",
        )
    )

    return exists


def validate_minimum_row_count(
    table: str,
    minimum_records: int = 1,
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()
    failed_count = 0 if total_count >= minimum_records else 1
    status = "PASS" if failed_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.row_count.minimum_{minimum_records}",
            table_name_value=table,
            rule_type="row_count",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=f"Table must contain at least {minimum_records} records.",
        )
    )


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
    key_columns: list[str],
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()

    duplicate_count = (
        df.groupBy(*key_columns)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    status = "PASS" if duplicate_count == 0 else "FAIL"

    key_name = "_".join(key_columns)

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.{key_name}.unique",
            table_name_value=table,
            rule_type="unique_key",
            severity=severity,
            status=status,
            failed_count=duplicate_count,
            total_count=total_count,
            description=f"Columns {key_columns} must be unique together.",
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


def validate_allowed_values(
    table: str,
    column_name: str,
    allowed_values: list[str],
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()
    failed_count = (
        df.filter(col(column_name).isNull() | (~col(column_name).isin(allowed_values)))
        .count()
    )

    status = "PASS" if failed_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.{column_name}.allowed_values",
            table_name_value=table,
            rule_type="allowed_values",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=f"Column {column_name} must be one of {allowed_values}.",
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
    parent_df = (
        spark.table(parent_table)
        .select(col(parent_column).alias("parent_key"))
        .distinct()
    )

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


def validate_conditional_referential_integrity(
    child_table: str,
    child_column: str,
    child_filter_column: str,
    child_filter_value: str,
    parent_table: str,
    parent_column: str,
    severity: str = "critical",
) -> None:
    child_df = (
        spark.table(child_table)
        .filter(col(child_filter_column) == child_filter_value)
        .select(col(child_column).alias("child_key"))
    )

    parent_df = (
        spark.table(parent_table)
        .select(col(parent_column).alias("parent_key"))
        .distinct()
    )

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
            rule_name=(
                f"{child_table}.{child_column}.when_{child_filter_column}_"
                f"{child_filter_value}.references.{parent_table}.{parent_column}"
            ),
            table_name_value=child_table,
            rule_type="conditional_referential_integrity",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=(
                f"When {child_filter_column} = {child_filter_value}, "
                f"{child_table}.{child_column} must exist in {parent_table}.{parent_column}."
            ),
        )
    )


# COMMAND ----------

orders_table = table_name("orders")
order_items_table = table_name("order_items")
inventory_table = table_name("inventory_movements")
shipments_table = table_name("shipments")
supplier_performance_table = table_name("supplier_performance")

customers_table = table_name("customers")
stores_table = table_name("stores")
products_table = table_name("products")
suppliers_table = table_name("suppliers")
warehouses_table = table_name("warehouses")

silver_event_tables = [
    orders_table,
    order_items_table,
    inventory_table,
    shipments_table,
    supplier_performance_table,
]

# COMMAND ----------

# Table existence and minimum row checks
for table in silver_event_tables:
    if validate_table_exists(table):
        validate_minimum_row_count(table, minimum_records=1)

# COMMAND ----------

# Primary/event key checks
validate_not_null(orders_table, "event_id")
validate_not_null(orders_table, "order_id")
validate_unique_key(orders_table, ["event_id"])

validate_not_null(order_items_table, "event_id")
validate_not_null(order_items_table, "order_id")
validate_not_null(order_items_table, "order_item_id")
validate_unique_key(order_items_table, ["event_id", "order_item_id"])

validate_not_null(inventory_table, "event_id")
validate_not_null(inventory_table, "inventory_event_id")
validate_unique_key(inventory_table, ["event_id"])

validate_not_null(shipments_table, "event_id")
validate_not_null(shipments_table, "shipment_id")
validate_unique_key(shipments_table, ["event_id"])

validate_not_null(supplier_performance_table, "event_id")
validate_not_null(supplier_performance_table, "performance_id")
validate_unique_key(supplier_performance_table, ["event_id"])

# COMMAND ----------

# Business rule checks
validate_non_negative(orders_table, "order_total")

validate_non_negative(order_items_table, "quantity")
validate_non_negative(order_items_table, "unit_price")
validate_non_negative(order_items_table, "line_total")

validate_non_negative(inventory_table, "stock_on_hand_after")
validate_non_negative(inventory_table, "reserved_stock_after")
validate_non_negative(inventory_table, "reorder_point")
validate_non_negative(inventory_table, "safety_stock")
validate_allowed_values(inventory_table, "location_type", ["warehouse", "store"])

validate_non_negative(shipments_table, "quantity")
validate_non_negative(shipments_table, "delay_days")
validate_allowed_values(
    shipments_table,
    "shipment_status",
    ["created", "in_transit", "delivered", "delayed"],
)
validate_allowed_values(
    shipments_table,
    "shipment_type",
    ["supplier_to_warehouse", "warehouse_to_store"],
)

validate_numeric_range(supplier_performance_table, "on_time_delivery_rate", 0, 1)
validate_numeric_range(supplier_performance_table, "defect_rate", 0, 1)
validate_numeric_range(supplier_performance_table, "risk_score", 0, 1)
validate_non_negative(supplier_performance_table, "avg_delay_days")
validate_non_negative(supplier_performance_table, "fulfilled_quantity")
validate_non_negative(supplier_performance_table, "rejected_quantity")

# COMMAND ----------

# Referential integrity checks to Silver master tables
validate_referential_integrity(
    child_table=orders_table,
    child_column="customer_id",
    parent_table=customers_table,
    parent_column="customer_id",
)

validate_referential_integrity(
    child_table=orders_table,
    child_column="store_id",
    parent_table=stores_table,
    parent_column="store_id",
)

validate_referential_integrity(
    child_table=order_items_table,
    child_column="product_id",
    parent_table=products_table,
    parent_column="product_id",
)

validate_referential_integrity(
    child_table=inventory_table,
    child_column="product_id",
    parent_table=products_table,
    parent_column="product_id",
)

validate_referential_integrity(
    child_table=shipments_table,
    child_column="product_id",
    parent_table=products_table,
    parent_column="product_id",
)

validate_referential_integrity(
    child_table=supplier_performance_table,
    child_column="supplier_id",
    parent_table=suppliers_table,
    parent_column="supplier_id",
)

validate_referential_integrity(
    child_table=supplier_performance_table,
    child_column="product_id",
    parent_table=products_table,
    parent_column="product_id",
)

# COMMAND ----------

# Conditional referential integrity for inventory location_id
validate_conditional_referential_integrity(
    child_table=inventory_table,
    child_column="location_id",
    child_filter_column="location_type",
    child_filter_value="store",
    parent_table=stores_table,
    parent_column="store_id",
)

validate_conditional_referential_integrity(
    child_table=inventory_table,
    child_column="location_id",
    child_filter_column="location_type",
    child_filter_value="warehouse",
    parent_table=warehouses_table,
    parent_column="warehouse_id",
)

# Conditional referential integrity for shipment source/destination
validate_conditional_referential_integrity(
    child_table=shipments_table,
    child_column="source_id",
    child_filter_column="source_type",
    child_filter_value="supplier",
    parent_table=suppliers_table,
    parent_column="supplier_id",
)

validate_conditional_referential_integrity(
    child_table=shipments_table,
    child_column="source_id",
    child_filter_column="source_type",
    child_filter_value="warehouse",
    parent_table=warehouses_table,
    parent_column="warehouse_id",
)

validate_conditional_referential_integrity(
    child_table=shipments_table,
    child_column="destination_id",
    child_filter_column="destination_type",
    child_filter_value="warehouse",
    parent_table=warehouses_table,
    parent_column="warehouse_id",
)

validate_conditional_referential_integrity(
    child_table=shipments_table,
    child_column="destination_id",
    child_filter_column="destination_type",
    child_filter_value="store",
    parent_table=stores_table,
    parent_column="store_id",
)

# COMMAND ----------

dq_results_df = spark.createDataFrame(dq_results)

dq_results_df = (
    dq_results_df
    .withColumn("environment", lit(environment))
    .withColumn("pipeline_layer", lit("silver_events"))
    .withColumn("validated_at", current_timestamp())
)

display(dq_results_df.orderBy("status", "severity", "table_name", "rule_name"))

# COMMAND ----------

(
    dq_results_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(dq_table)
)

print(f"Written Silver event data quality results to: {dq_table}")

# COMMAND ----------

critical_failures = (
    dq_results_df
    .filter((col("severity") == "critical") & (col("status") == "FAIL"))
    .count()
)

print(f"Critical failures: {critical_failures}")

if critical_failures > 0:
    failed_rules = (
        dq_results_df
        .filter((col("severity") == "critical") & (col("status") == "FAIL"))
        .select("rule_name", "table_name", "failed_count", "description")
        .collect()
    )

    failed_rule_text = "\n".join(
        [
            f"- {row['rule_name']} | table={row['table_name']} | "
            f"failed_count={row['failed_count']} | {row['description']}"
            for row in failed_rules
        ]
    )

    raise Exception(
        "Critical Silver event data quality checks failed:\n"
        + failed_rule_text
    )

print("All critical Silver event data quality checks passed.")