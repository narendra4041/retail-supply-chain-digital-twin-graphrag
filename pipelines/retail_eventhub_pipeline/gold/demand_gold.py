from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    lit,
    max as spark_max,
    sum as spark_sum,
    when,
)

from pipelines.retail_eventhub_pipeline.utils.config import (
    get_catalog_name,
    get_schema_name,
)


ENVIRONMENT = spark.conf.get("ENVIRONMENT", "dev")

catalog_name = get_catalog_name(ENVIRONMENT)
silver_schema = get_schema_name("silver", ENVIRONMENT)
gold_schema = get_schema_name("gold", ENVIRONMENT)

SILVER_ORDERS_TABLE = f"{catalog_name}.{silver_schema}.orders"
SILVER_ORDER_ITEMS_TABLE = f"{catalog_name}.{silver_schema}.order_items"
SILVER_PRODUCTS_TABLE = f"{catalog_name}.{silver_schema}.products"
SILVER_STORES_TABLE = f"{catalog_name}.{silver_schema}.stores"
SILVER_CUSTOMERS_TABLE = f"{catalog_name}.{silver_schema}.customers"

GOLD_PRODUCT_DEMAND_SUMMARY_TABLE = f"{catalog_name}.{gold_schema}.product_demand_summary"
GOLD_STORE_SALES_SUMMARY_TABLE = f"{catalog_name}.{gold_schema}.store_sales_summary"
GOLD_CUSTOMER_ORDER_SUMMARY_TABLE = f"{catalog_name}.{gold_schema}.customer_order_summary"


@dp.materialized_view(
    name=GOLD_PRODUCT_DEMAND_SUMMARY_TABLE,
    comment=(
        "Product-level demand and revenue summary enriched with product, "
        "supplier, and margin attributes."
    ),
)
def product_demand_summary():
    orders_df = spark.read.table(SILVER_ORDERS_TABLE)
    order_items_df = spark.read.table(SILVER_ORDER_ITEMS_TABLE)
    products_df = spark.read.table(SILVER_PRODUCTS_TABLE)

    demand_df = (
        order_items_df.alias("oi")
        .join(
            orders_df.select(
                col("order_id").alias("o_order_id"),
                col("customer_id").alias("o_customer_id"),
                col("store_id").alias("o_store_id"),
                col("order_channel").alias("o_order_channel"),
                col("order_status").alias("o_order_status"),
                col("payment_method").alias("o_payment_method"),
                col("event_timestamp").alias("o_event_timestamp"),
            ).alias("o"),
            col("oi.order_id") == col("o.o_order_id"),
            "left",
        )
        .join(
            products_df.select(
                col("product_id").alias("p_product_id"),
                col("product_name"),
                col("category"),
                col("sub_category"),
                col("brand"),
                col("supplier_id"),
                col("unit_price").alias("product_unit_price"),
                col("unit_cost"),
                col("gross_margin"),
                col("gross_margin_pct"),
                col("active_flag"),
            ).alias("p"),
            col("oi.product_id") == col("p.p_product_id"),
            "left",
        )
        .groupBy(
            col("oi.product_id"),
            col("p.product_name"),
            col("p.category"),
            col("p.sub_category"),
            col("p.brand"),
            col("p.supplier_id"),
            col("p.active_flag"),
        )
        .agg(
            countDistinct(col("oi.order_id")).alias("order_count"),
            countDistinct(col("o.o_customer_id")).alias("customer_count"),
            countDistinct(col("o.o_store_id")).alias("store_count"),
            spark_sum(col("oi.quantity")).alias("total_quantity_sold"),
            spark_sum(col("oi.line_total")).alias("total_revenue"),
            avg(col("oi.unit_price")).alias("avg_sold_unit_price"),
            avg(col("oi.discount_pct")).alias("avg_discount_pct"),
            spark_max(col("o.o_event_timestamp")).alias("latest_order_timestamp"),
        )
        .withColumn(
            "demand_band",
            when(col("total_quantity_sold") >= 1000, lit("high"))
            .when(col("total_quantity_sold") >= 100, lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )

    return demand_df


@dp.materialized_view(
    name=GOLD_STORE_SALES_SUMMARY_TABLE,
    comment="Store and sales-channel level performance summary.",
)
def store_sales_summary():
    orders_df = spark.read.table(SILVER_ORDERS_TABLE)
    order_items_df = spark.read.table(SILVER_ORDER_ITEMS_TABLE)
    stores_df = spark.read.table(SILVER_STORES_TABLE)

    store_sales_df = (
        orders_df.alias("o")
        .join(
            order_items_df.select(
                "order_id",
                "quantity",
                "line_total",
                "product_id",
            ).alias("oi"),
            "order_id",
            "left",
        )
        .join(
            stores_df.select(
                "store_id",
                "store_name",
                "country",
                "city",
                "region",
                "store_type",
                "store_size_band",
                "warehouse_id",
            ).alias("s"),
            "store_id",
            "left",
        )
        .groupBy(
            "store_id",
            "store_name",
            "country",
            "city",
            "region",
            "store_type",
            "store_size_band",
            "warehouse_id",
            "order_channel",
        )
        .agg(
            countDistinct(col("o.order_id")).alias("order_count"),
            countDistinct(col("o.customer_id")).alias("customer_count"),
            countDistinct(col("oi.product_id")).alias("distinct_product_count"),
            spark_sum(col("oi.quantity")).alias("total_items_sold"),
            spark_sum(col("oi.line_total")).alias("total_revenue"),
            avg(col("o.order_total")).alias("avg_order_value"),
            spark_max(col("o.event_timestamp")).alias("latest_order_timestamp"),
        )
        .withColumn(
            "store_performance_band",
            when(col("total_revenue") >= 500000, lit("high"))
            .when(col("total_revenue") >= 100000, lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )

    return store_sales_df


@dp.materialized_view(
    name=GOLD_CUSTOMER_ORDER_SUMMARY_TABLE,
    comment="Customer 360 order and spend summary.",
)
def customer_order_summary():
    orders_df = spark.read.table(SILVER_ORDERS_TABLE)
    order_items_df = spark.read.table(SILVER_ORDER_ITEMS_TABLE)
    customers_df = spark.read.table(SILVER_CUSTOMERS_TABLE)

    customer_summary_df = (
        orders_df.alias("o")
        .join(
            order_items_df.select(
                "order_id",
                "product_id",
                "quantity",
                "line_total",
            ).alias("oi"),
            "order_id",
            "left",
        )
        .join(
            customers_df.select(
                "customer_id",
                "customer_name",
                "country",
                "city",
                "customer_segment",
                "loyalty_tier",
                "loyalty_rank",
                "signup_date",
            ).alias("c"),
            "customer_id",
            "left",
        )
        .groupBy(
            "customer_id",
            "customer_name",
            "country",
            "city",
            "customer_segment",
            "loyalty_tier",
            "loyalty_rank",
        )
        .agg(
            countDistinct(col("o.order_id")).alias("order_count"),
            countDistinct(col("oi.product_id")).alias("distinct_product_count"),
            spark_sum(col("oi.quantity")).alias("total_items_purchased"),
            spark_sum(col("oi.line_total")).alias("total_spend"),
            avg(col("o.order_total")).alias("avg_order_value"),
            spark_max(col("o.event_timestamp")).alias("latest_order_timestamp"),
        )
        .withColumn(
            "customer_value_band",
            when(col("total_spend") >= 10000, lit("high_value"))
            .when(col("total_spend") >= 1000, lit("medium_value"))
            .otherwise(lit("low_value")),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )

    return customer_summary_df