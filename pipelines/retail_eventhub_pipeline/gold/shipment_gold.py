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

SILVER_SHIPMENTS_TABLE = f"{catalog_name}.{silver_schema}.shipments"
SILVER_PRODUCTS_TABLE = f"{catalog_name}.{silver_schema}.products"
SILVER_SUPPLIERS_TABLE = f"{catalog_name}.{silver_schema}.suppliers"
SILVER_STORES_TABLE = f"{catalog_name}.{silver_schema}.stores"
SILVER_WAREHOUSES_TABLE = f"{catalog_name}.{silver_schema}.warehouses"

GOLD_SUPPLIER_RISK_SCORE_TABLE = f"{catalog_name}.{gold_schema}.supplier_risk_score"
GOLD_SHIPMENT_DELAY_IMPACT_TABLE = f"{catalog_name}.{gold_schema}.shipment_delay_impact"


@dp.materialized_view(
    name=GOLD_SHIPMENT_DELAY_IMPACT_TABLE,
    comment=(
        "Business-ready view of shipment delays and their impact on products, "
        "suppliers, stores, warehouses, and carriers."
    ),
)
def shipment_delay_impact():
    shipments_df = spark.read.table(SILVER_SHIPMENTS_TABLE)
    products_df = spark.read.table(SILVER_PRODUCTS_TABLE)
    suppliers_df = spark.read.table(SILVER_SUPPLIERS_TABLE)
    stores_df = spark.read.table(SILVER_STORES_TABLE)
    warehouses_df = spark.read.table(SILVER_WAREHOUSES_TABLE)
    supplier_risk_df = spark.read.table(GOLD_SUPPLIER_RISK_SCORE_TABLE)

    product_supplier_df = (
        products_df
        .select(
            "product_id",
            "product_name",
            "category",
            "sub_category",
            "brand",
            "supplier_id",
            "unit_price",
            "unit_cost",
        )
    )

    supplier_enriched_df = (
        suppliers_df
        .select(
            "supplier_id",
            "supplier_name",
            "country",
            "region",
            "supplier_type",
            "preferred_supplier_flag",
            "supplier_risk_band",
        )
    )

    store_location_df = (
        stores_df
        .select(
            col("store_id").alias("location_id"),
            col("store_name").alias("location_name"),
            col("country").alias("location_country"),
            col("city").alias("location_city"),
            col("region").alias("location_region"),
            lit("store").alias("location_type"),
        )
    )

    warehouse_location_df = (
        warehouses_df
        .select(
            col("warehouse_id").alias("location_id"),
            col("warehouse_name").alias("location_name"),
            col("country").alias("location_country"),
            col("city").alias("location_city"),
            col("region").alias("location_region"),
            lit("warehouse").alias("location_type"),
        )
    )

    location_df = store_location_df.unionByName(warehouse_location_df)

    enriched_shipments_df = (
        shipments_df.alias("sh")
        .join(product_supplier_df.alias("p"), "product_id", "left")
        .join(supplier_enriched_df.alias("s"), "supplier_id", "left")
        .join(
            supplier_risk_df.select(
                "supplier_id",
                "computed_supplier_risk_score",
                "risk_band",
                "primary_risk_reason",
            ).alias("sr"),
            "supplier_id",
            "left",
        )
        .join(
            location_df.alias("src_loc"),
            (
                (col("sh.source_id") == col("src_loc.location_id"))
                & (col("sh.source_type") == col("src_loc.location_type"))
            ),
            "left",
        )
        .join(
            location_df.alias("dst_loc"),
            (
                (col("sh.destination_id") == col("dst_loc.location_id"))
                & (col("sh.destination_type") == col("dst_loc.location_type"))
            ),
            "left",
        )
        .select(
            col("sh.shipment_id"),
            col("sh.event_id"),
            col("sh.event_timestamp"),
            col("sh.shipment_type"),
            col("sh.source_type"),
            col("sh.source_id"),
            col("src_loc.location_name").alias("source_name"),
            col("src_loc.location_country").alias("source_country"),
            col("src_loc.location_city").alias("source_city"),
            col("src_loc.location_region").alias("source_region"),
            col("sh.destination_type"),
            col("sh.destination_id"),
            col("dst_loc.location_name").alias("destination_name"),
            col("dst_loc.location_country").alias("destination_country"),
            col("dst_loc.location_city").alias("destination_city"),
            col("dst_loc.location_region").alias("destination_region"),
            col("sh.product_id"),
            col("p.product_name"),
            col("p.category"),
            col("p.sub_category"),
            col("p.brand"),
            col("p.supplier_id"),
            col("s.supplier_name"),
            col("s.country").alias("supplier_country"),
            col("s.region").alias("supplier_region"),
            col("s.supplier_type"),
            col("s.preferred_supplier_flag"),
            col("s.supplier_risk_band").alias("master_supplier_risk_band"),
            col("sr.computed_supplier_risk_score"),
            col("sr.risk_band").alias("computed_supplier_risk_band"),
            col("sr.primary_risk_reason"),
            col("sh.quantity"),
            col("p.unit_price"),
            col("p.unit_cost"),
            col("sh.shipment_date"),
            col("sh.expected_delivery_date"),
            col("sh.actual_delivery_date"),
            col("sh.shipment_status"),
            col("sh.carrier"),
            col("sh.delay_days"),
        )
        .withColumn(
            "estimated_shipment_retail_value",
            col("quantity") * col("unit_price"),
        )
        .withColumn(
            "estimated_shipment_cost_value",
            col("quantity") * col("unit_cost"),
        )
        .withColumn(
            "is_delayed",
            when(
                (col("shipment_status") == "delayed")
                | (col("delay_days") > 0),
                lit(True),
            ).otherwise(lit(False)),
        )
        .withColumn(
            "delay_severity",
            when(col("delay_days") >= 7, lit("severe"))
            .when(col("delay_days") >= 3, lit("moderate"))
            .when(col("delay_days") > 0, lit("minor"))
            .otherwise(lit("none")),
        )
        .withColumn(
            "impact_band",
            when(
                (col("delay_severity") == "severe")
                & (col("estimated_shipment_retail_value") >= 50000),
                lit("critical"),
            )
            .when(col("delay_severity") == "severe", lit("high"))
            .when(col("delay_severity") == "moderate", lit("medium"))
            .when(col("delay_severity") == "minor", lit("low"))
            .otherwise(lit("none")),
        )
        .withColumn(
            "recommended_action",
            when(col("impact_band") == "critical", lit("escalate_supplier_and_carrier"))
            .when(col("impact_band") == "high", lit("expedite_or_reroute"))
            .when(col("impact_band") == "medium", lit("monitor_and_update_eta"))
            .when(col("impact_band") == "low", lit("monitor"))
            .otherwise(lit("none")),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )

    return enriched_shipments_df