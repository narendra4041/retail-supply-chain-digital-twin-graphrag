# Databricks notebook source

# COMMAND ----------

from src.common.config_loader import load_config

config = load_config("dev")

catalog_name = config["unity_catalog"]["catalog"]
bronze_schema = config["unity_catalog"]["schemas"]["bronze"]
monitoring_schema = config["unity_catalog"]["schemas"]["monitoring"]

dq_table = f"{catalog_name}.{monitoring_schema}.data_quality_results"

print(f"Catalog: {catalog_name}")
print(f"Bronze schema: {bronze_schema}")
print(f"Monitoring schema: {monitoring_schema}")
print(f"DQ result table: {dq_table}")

# COMMAND ----------

from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

run_id = datetime.now(timezone.utc).strftime("bronze_event_dq_run_%Y%m%d_%H%M%S")

bronze_event_tables = {
    "orders_events_raw": {
        "expected_event_type": "order_created",
        "minimum_records": 1,
    },
    "inventory_events_raw": {
        "expected_event_type": "inventory_updated",
        "minimum_records": 1,
    },
    "shipment_events_raw": {
        "expected_event_type": "shipment_created",
        "minimum_records": 1,
    },
    "supplier_performance_events_raw": {
        "expected_event_type": "supplier_performance",
        "minimum_records": 1,
    },
}

dq_results: List[Dict[str, object]] = []


def full_table_name(table_name: str) -> str:
    return f"{catalog_name}.{bronze_schema}.{table_name}"


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
        table_exists = True
    except Exception:
        status = "FAIL"
        failed_count = 1
        table_exists = False

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

    return table_exists


def validate_minimum_row_count(
    table: str,
    minimum_records: int,
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


def validate_expected_event_type(
    table: str,
    expected_event_type: str,
    severity: str = "critical",
) -> None:
    df = spark.table(table)

    total_count = df.count()
    failed_count = (
        df.filter(
            col("_event_type").isNull()
            | (col("_event_type") != lit(expected_event_type))
        )
        .count()
    )

    status = "PASS" if failed_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}._event_type.equals_{expected_event_type}",
            table_name_value=table,
            rule_type="expected_value",
            severity=severity,
            status=status,
            failed_count=failed_count,
            total_count=total_count,
            description=f"_event_type must equal {expected_event_type}.",
        )
    )


def validate_unique_eventhub_position(
    table: str,
    severity: str = "critical",
) -> None:
    """
    Each Event Hub message should be unique by eventhub_name + partition + offset.
    """
    df = spark.table(table)

    total_count = df.count()

    duplicate_count = (
        df.groupBy("eventhub_name", "eventhub_partition", "eventhub_offset")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    status = "PASS" if duplicate_count == 0 else "FAIL"

    dq_results.append(
        create_dq_result(
            rule_name=f"{table}.eventhub_position.unique",
            table_name_value=table,
            rule_type="unique_eventhub_position",
            severity=severity,
            status=status,
            failed_count=duplicate_count,
            total_count=total_count,
            description="eventhub_name + eventhub_partition + eventhub_offset must be unique.",
        )
    )


# COMMAND ----------

for table_short_name, table_config in bronze_event_tables.items():
    table = full_table_name(table_short_name)

    print(f"Validating {table}")

    table_exists = validate_table_exists(table)

    if not table_exists:
        continue

    validate_minimum_row_count(
        table=table,
        minimum_records=table_config["minimum_records"],
    )

    validate_not_null(table, "raw_payload")
    validate_not_null(table, "eventhub_name")
    validate_not_null(table, "eventhub_partition")
    validate_not_null(table, "eventhub_offset")
    validate_not_null(table, "eventhub_enqueued_timestamp")
    validate_not_null(table, "_ingestion_timestamp")

    validate_expected_event_type(
        table=table,
        expected_event_type=table_config["expected_event_type"],
    )

    validate_unique_eventhub_position(table)

# COMMAND ----------

dq_results_df = spark.createDataFrame(dq_results)

dq_results_df = (
    dq_results_df
    .withColumn("environment", lit(config["environment"]))
    .withColumn("pipeline_layer", lit("bronze"))
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

print(f"Written Bronze event data quality results to: {dq_table}")

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
        "Critical Bronze event data quality checks failed:\n"
        + failed_rule_text
    )

print("All critical Bronze event data quality checks passed.")