# Databricks notebook source

# COMMAND ----------

environment = dbutils.widgets.get("environment")
catalog_name = dbutils.widgets.get("catalog_name")
gold_schema = dbutils.widgets.get("gold_schema")
monitoring_schema = dbutils.widgets.get("monitoring_schema")

dq_table = f"{catalog_name}.{monitoring_schema}.data_quality_results"

print(f"Environment: {environment}")
print(f"Catalog: {catalog_name}")
print(f"Gold schema: {gold_schema}")
print(f"Monitoring schema: {monitoring_schema}")
print(f"DQ result table: {dq_table}")

# COMMAND ----------

from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql.functions import col, current_timestamp, lit

run_id = datetime.now(timezone.utc).strftime("gold_dq_run_%Y%m%d_%H%M%S")

dq_results: List[Dict[str, object]] = []


def gold_table(name: str) -> str:
    return f"{catalog_name}.{gold_schema}.{name}"


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
            description=f"Gold table/view {table} must exist and be readable.",
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
            description=f"Gold table/view must contain at least {minimum_records} rows.",
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


def validate_allowed_values(
    table: str,
    column_name: str,
    allowed_values: list[str],
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()

    failed_count = (
        df.filter(
            col(column_name).isNull()
            | (~col(column_name).isin(allowed_values))
        )
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

    failed_count = (
        df.filter(
            col(column_name).isNotNull()
            & (col(column_name) < 0)
        )
        .count()
    )

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
            description=f"Column {column_name} must not be negative when present.",
        )
    )


# COMMAND ----------

gold_tables = [
    "inventory_position",
    "stockout_risk",
    "supplier_risk_score",
    "supplier_product_dependency",
    "shipment_delay_impact",
    "product_demand_summary",
    "store_sales_summary",
    "customer_order_summary",
    "warehouse_store_replenishment_view",
    "digital_twin_entity_health",
]

existing_tables = []

for table_short_name in gold_tables:
    table = gold_table(table_short_name)

    print(f"Validating existence and row count: {table}")

    if validate_table_exists(table):
        existing_tables.append(table_short_name)
        validate_minimum_row_count(table, minimum_records=1)

# COMMAND ----------

# inventory_position
inventory_position_table = gold_table("inventory_position")

if "inventory_position" in existing_tables:
    validate_not_null(inventory_position_table, "product_id")
    validate_not_null(inventory_position_table, "location_type")
    validate_not_null(inventory_position_table, "location_id")
    validate_allowed_values(
        inventory_position_table,
        "location_type",
        ["store", "warehouse"],
    )
    validate_allowed_values(
        inventory_position_table,
        "inventory_status",
        ["critical", "reorder", "healthy"],
    )
    validate_non_negative(inventory_position_table, "stock_on_hand_after")
    validate_non_negative(inventory_position_table, "reserved_stock_after")
    validate_non_negative(inventory_position_table, "available_stock")
    validate_unique_key(
        inventory_position_table,
        ["product_id", "location_type", "location_id"],
    )

# COMMAND ----------

# stockout_risk
stockout_risk_table = gold_table("stockout_risk")

if "stockout_risk" in existing_tables:
    validate_not_null(stockout_risk_table, "product_id")
    validate_not_null(stockout_risk_table, "location_type")
    validate_not_null(stockout_risk_table, "location_id")
    validate_allowed_values(
        stockout_risk_table,
        "stockout_risk_band",
        ["low", "medium", "high"],
    )
    validate_allowed_values(
        stockout_risk_table,
        "recommended_action",
        ["monitor", "plan_replenishment", "urgent_replenishment"],
    )
    validate_non_negative(stockout_risk_table, "replenishment_gap")
    validate_unique_key(
        stockout_risk_table,
        ["product_id", "location_type", "location_id"],
    )

# COMMAND ----------

# supplier_risk_score
supplier_risk_table = gold_table("supplier_risk_score")

if "supplier_risk_score" in existing_tables:
    validate_not_null(supplier_risk_table, "supplier_id")
    validate_unique_key(supplier_risk_table, ["supplier_id"])
    validate_numeric_range(
        supplier_risk_table,
        "computed_supplier_risk_score",
        0,
        1,
    )
    validate_allowed_values(
        supplier_risk_table,
        "risk_band",
        ["low", "medium", "high"],
    )
    validate_non_negative(supplier_risk_table, "total_fulfilled_quantity")
    validate_non_negative(supplier_risk_table, "total_rejected_quantity")

# COMMAND ----------

# supplier_product_dependency
supplier_product_dependency_table = gold_table("supplier_product_dependency")

if "supplier_product_dependency" in existing_tables:
    validate_not_null(supplier_product_dependency_table, "product_id")
    validate_not_null(supplier_product_dependency_table, "supplier_id")
    validate_unique_key(supplier_product_dependency_table, ["product_id"])
    validate_allowed_values(
        supplier_product_dependency_table,
        "dependency_risk_band",
        ["low", "medium", "high", "critical"],
    )
    validate_non_negative(supplier_product_dependency_table, "total_units_sold")
    validate_non_negative(supplier_product_dependency_table, "total_revenue")

# COMMAND ----------

# shipment_delay_impact
shipment_delay_impact_table = gold_table("shipment_delay_impact")

if "shipment_delay_impact" in existing_tables:
    validate_not_null(shipment_delay_impact_table, "shipment_id")
    validate_not_null(shipment_delay_impact_table, "product_id")
    validate_unique_key(shipment_delay_impact_table, ["shipment_id"])
    validate_allowed_values(
        shipment_delay_impact_table,
        "delay_severity",
        ["none", "minor", "moderate", "severe"],
    )
    validate_allowed_values(
        shipment_delay_impact_table,
        "impact_band",
        ["none", "low", "medium", "high", "critical"],
    )
    validate_non_negative(shipment_delay_impact_table, "delay_days")
    validate_non_negative(shipment_delay_impact_table, "estimated_shipment_retail_value")
    validate_non_negative(shipment_delay_impact_table, "estimated_shipment_cost_value")

# COMMAND ----------

# product_demand_summary
product_demand_table = gold_table("product_demand_summary")

if "product_demand_summary" in existing_tables:
    validate_not_null(product_demand_table, "product_id")
    validate_unique_key(product_demand_table, ["product_id"])
    validate_allowed_values(
        product_demand_table,
        "demand_band",
        ["low", "medium", "high"],
    )
    validate_non_negative(product_demand_table, "order_count")
    validate_non_negative(product_demand_table, "customer_count")
    validate_non_negative(product_demand_table, "total_quantity_sold")
    validate_non_negative(product_demand_table, "total_revenue")

# COMMAND ----------

# store_sales_summary
store_sales_table = gold_table("store_sales_summary")

if "store_sales_summary" in existing_tables:
    validate_not_null(store_sales_table, "store_id")
    validate_allowed_values(
        store_sales_table,
        "store_performance_band",
        ["low", "medium", "high"],
    )
    validate_non_negative(store_sales_table, "order_count")
    validate_non_negative(store_sales_table, "customer_count")
    validate_non_negative(store_sales_table, "total_items_sold")
    validate_non_negative(store_sales_table, "total_revenue")

# COMMAND ----------

# customer_order_summary
customer_order_table = gold_table("customer_order_summary")

if "customer_order_summary" in existing_tables:
    validate_not_null(customer_order_table, "customer_id")
    validate_unique_key(customer_order_table, ["customer_id"])
    validate_allowed_values(
        customer_order_table,
        "customer_value_band",
        ["low_value", "medium_value", "high_value"],
    )
    validate_non_negative(customer_order_table, "order_count")
    validate_non_negative(customer_order_table, "total_items_purchased")
    validate_non_negative(customer_order_table, "total_spend")

# COMMAND ----------

# warehouse_store_replenishment_view
replenishment_table = gold_table("warehouse_store_replenishment_view")

if "warehouse_store_replenishment_view" in existing_tables:
    validate_not_null(replenishment_table, "store_id")
    validate_not_null(replenishment_table, "product_id")
    validate_not_null(replenishment_table, "serving_warehouse_id")
    validate_allowed_values(
        replenishment_table,
        "replenishment_priority",
        ["urgent", "planned", "monitor_delay", "normal"],
    )
    validate_allowed_values(
        replenishment_table,
        "warehouse_action",
        [
            "rebalance_or_expedite_from_alternate_warehouse",
            "replenish_from_serving_warehouse",
            "schedule_replenishment",
            "track_inbound_shipment",
            "no_action",
        ],
    )
    validate_non_negative(replenishment_table, "stock_on_hand_after")
    validate_non_negative(replenishment_table, "available_stock")
    validate_non_negative(replenishment_table, "replenishment_gap")

# COMMAND ----------

# digital_twin_entity_health
entity_health_table = gold_table("digital_twin_entity_health")

if "digital_twin_entity_health" in existing_tables:
    validate_not_null(entity_health_table, "entity_type")
    validate_not_null(entity_health_table, "entity_id")
    validate_not_null(entity_health_table, "entity_name")
    validate_unique_key(entity_health_table, ["entity_type", "entity_id"])
    validate_allowed_values(
        entity_health_table,
        "entity_type",
        ["product", "supplier", "store", "warehouse"],
    )
    validate_allowed_values(
        entity_health_table,
        "risk_band",
        ["low", "medium", "high"],
    )
    validate_numeric_range(entity_health_table, "health_score", 0, 100)

# COMMAND ----------

dq_results_df = spark.createDataFrame(dq_results)

dq_results_df = (
    dq_results_df
    .withColumn("environment", lit(environment))
    .withColumn("pipeline_layer", lit("gold"))
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

print(f"Written Gold data quality results to: {dq_table}")

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
        "Critical Gold data quality checks failed:\n"
        + failed_rule_text
    )

print("All critical Gold data quality checks passed.")