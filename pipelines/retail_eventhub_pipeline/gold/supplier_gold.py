from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    lit,
    max as spark_max,
    sum as spark_sum,
    when,
)

ENVIRONMENT = spark.conf.get("ENVIRONMENT", "dev")

catalog_name = spark.conf.get("CATALOG_NAME")
silver_schema = spark.conf.get("SILVER_SCHEMA")
gold_schema = spark.conf.get("GOLD_SCHEMA")

SILVER_SUPPLIER_PERFORMANCE_TABLE = (
    f"{catalog_name}.{silver_schema}.supplier_performance"
)
SILVER_SUPPLIERS_TABLE = f"{catalog_name}.{silver_schema}.suppliers"
SILVER_PRODUCTS_TABLE = f"{catalog_name}.{silver_schema}.products"
SILVER_ORDER_ITEMS_TABLE = f"{catalog_name}.{silver_schema}.order_items"

GOLD_SUPPLIER_RISK_SCORE_TABLE = f"{catalog_name}.{gold_schema}.supplier_risk_score"
GOLD_SUPPLIER_PRODUCT_DEPENDENCY_TABLE = (
    f"{catalog_name}.{gold_schema}.supplier_product_dependency"
)


@dp.materialized_view(
    name=GOLD_SUPPLIER_RISK_SCORE_TABLE,
    comment=(
        "Supplier-level risk score combining delivery performance, quality, "
        "fulfilled/rejected quantities, and product dependency signals."
    ),
)
def supplier_risk_score():
    supplier_performance_df = spark.read.table(SILVER_SUPPLIER_PERFORMANCE_TABLE)
    suppliers_df = spark.read.table(SILVER_SUPPLIERS_TABLE)
    products_df = spark.read.table(SILVER_PRODUCTS_TABLE)
    order_items_df = spark.read.table(SILVER_ORDER_ITEMS_TABLE)

    supplier_product_count_df = (
        products_df
        .groupBy("supplier_id")
        .agg(
            count("product_id").alias("product_count"),
        )
    )

    supplier_demand_df = (
        order_items_df
        .join(
            products_df.select("product_id", "supplier_id"),
            "product_id",
            "left",
        )
        .groupBy("supplier_id")
        .agg(
            spark_sum("quantity").alias("total_units_sold"),
            spark_sum("line_total").alias("total_revenue"),
            count("order_id").alias("order_line_count"),
        )
    )

    supplier_perf_agg_df = (
        supplier_performance_df
        .groupBy("supplier_id")
        .agg(
            avg("on_time_delivery_rate").alias("avg_on_time_delivery_rate"),
            avg("defect_rate").alias("avg_defect_rate"),
            avg("avg_delay_days").alias("avg_delay_days"),
            avg("risk_score").alias("avg_source_risk_score"),
            spark_sum("fulfilled_quantity").alias("total_fulfilled_quantity"),
            spark_sum("rejected_quantity").alias("total_rejected_quantity"),
            count("performance_id").alias("performance_observation_count"),
            spark_max("event_timestamp").alias("latest_performance_event_timestamp"),
        )
    )

    return (
        supplier_perf_agg_df.alias("perf")
        .join(
            suppliers_df.select(
                "supplier_id",
                "supplier_name",
                "country",
                "region",
                "supplier_type",
                "lead_time_days",
                "reliability_score",
                "quality_score",
                "preferred_supplier_flag",
                "supplier_risk_band",
            ).alias("sup"),
            "supplier_id",
            "left",
        )
        .join(
            supplier_product_count_df.alias("prod"),
            "supplier_id",
            "left",
        )
        .join(
            supplier_demand_df.alias("demand"),
            "supplier_id",
            "left",
        )
        .withColumn(
            "computed_supplier_risk_score",
            (
                (1 - col("avg_on_time_delivery_rate")) * lit(0.30)
                + col("avg_defect_rate") * lit(0.25)
                + when(col("avg_delay_days") >= 15, lit(1.0))
                    .otherwise(col("avg_delay_days") / lit(15.0)) * lit(0.20)
                + (1 - col("reliability_score")) * lit(0.15)
                + (1 - col("quality_score")) * lit(0.10)
            ),
        )
        .withColumn(
            "risk_band",
            when(col("computed_supplier_risk_score") >= 0.60, lit("high"))
            .when(col("computed_supplier_risk_score") >= 0.30, lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn(
            "primary_risk_reason",
            when(col("avg_on_time_delivery_rate") < 0.80, lit("poor_on_time_delivery"))
            .when(col("avg_defect_rate") > 0.05, lit("high_defect_rate"))
            .when(col("avg_delay_days") > 5, lit("high_average_delay"))
            .when(col("reliability_score") < 0.80, lit("low_supplier_reliability"))
            .when(col("quality_score") < 0.80, lit("low_quality_score"))
            .otherwise(lit("healthy")),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )


@dp.materialized_view(
    name=GOLD_SUPPLIER_PRODUCT_DEPENDENCY_TABLE,
    comment=(
        "Product-to-supplier dependency view enriched with demand and supplier risk. "
        "Used for graph relationships and supplier concentration analysis."
    ),
)
def supplier_product_dependency():
    suppliers_df = spark.read.table(SILVER_SUPPLIERS_TABLE)
    products_df = spark.read.table(SILVER_PRODUCTS_TABLE)
    order_items_df = spark.read.table(SILVER_ORDER_ITEMS_TABLE)
    supplier_risk_df = spark.read.table(GOLD_SUPPLIER_RISK_SCORE_TABLE)

    product_demand_df = (
        order_items_df
        .groupBy("product_id")
        .agg(
            spark_sum("quantity").alias("total_units_sold"),
            spark_sum("line_total").alias("total_revenue"),
            count("order_id").alias("order_line_count"),
            spark_max("event_timestamp").alias("latest_order_event_timestamp"),
        )
    )

    return (
        products_df.alias("p")
        .join(
            suppliers_df.select(
                "supplier_id",
                "supplier_name",
                "country",
                "region",
                "supplier_type",
                "preferred_supplier_flag",
            ).alias("s"),
            "supplier_id",
            "left",
        )
        .join(product_demand_df.alias("d"), "product_id", "left")
        .join(
            supplier_risk_df.select(
                "supplier_id",
                "computed_supplier_risk_score",
                "risk_band",
                "primary_risk_reason",
            ).alias("r"),
            "supplier_id",
            "left",
        )
        .select(
            col("product_id"),
            col("product_name"),
            col("category"),
            col("sub_category"),
            col("brand"),
            col("active_flag"),
            col("unit_price"),
            col("unit_cost"),
            col("gross_margin"),
            col("gross_margin_pct"),
            col("supplier_id"),
            col("supplier_name"),
            col("country").alias("supplier_country"),
            col("region").alias("supplier_region"),
            col("supplier_type"),
            col("preferred_supplier_flag"),
            col("total_units_sold"),
            col("total_revenue"),
            col("order_line_count"),
            col("latest_order_event_timestamp"),
            col("computed_supplier_risk_score"),
            col("risk_band"),
            col("primary_risk_reason"),
        )
        .withColumn(
            "dependency_risk_band",
            when(
                (col("risk_band") == "high") & (col("total_revenue") > 0),
                lit("critical"),
            )
            .when(col("risk_band") == "high", lit("high"))
            .when(col("risk_band") == "medium", lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )