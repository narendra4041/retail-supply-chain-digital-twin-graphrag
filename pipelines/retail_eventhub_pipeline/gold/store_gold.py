from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    count,
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

SILVER_STORES_TABLE = f"{catalog_name}.{silver_schema}.stores"
SILVER_WAREHOUSES_TABLE = f"{catalog_name}.{silver_schema}.warehouses"

GOLD_INVENTORY_POSITION_TABLE = f"{catalog_name}.{gold_schema}.inventory_position"
GOLD_STOCKOUT_RISK_TABLE = f"{catalog_name}.{gold_schema}.stockout_risk"
GOLD_STORE_SALES_SUMMARY_TABLE = f"{catalog_name}.{gold_schema}.store_sales_summary"
GOLD_SHIPMENT_DELAY_IMPACT_TABLE = f"{catalog_name}.{gold_schema}.shipment_delay_impact"

GOLD_WAREHOUSE_STORE_REPLENISHMENT_TABLE = (
    f"{catalog_name}.{gold_schema}.warehouse_store_replenishment_view"
)


@dp.materialized_view(
    name=GOLD_WAREHOUSE_STORE_REPLENISHMENT_TABLE,
    comment=(
        "Warehouse-store replenishment view combining store demand, inventory position, "
        "stockout risk, serving warehouse, and shipment delay signals."
    ),
)
def warehouse_store_replenishment_view():
    stores_df = spark.read.table(SILVER_STORES_TABLE)
    warehouses_df = spark.read.table(SILVER_WAREHOUSES_TABLE)
    inventory_position_df = spark.read.table(GOLD_INVENTORY_POSITION_TABLE)
    stockout_risk_df = spark.read.table(GOLD_STOCKOUT_RISK_TABLE)
    store_sales_df = spark.read.table(GOLD_STORE_SALES_SUMMARY_TABLE)
    shipment_delay_df = spark.read.table(GOLD_SHIPMENT_DELAY_IMPACT_TABLE)

    store_inventory_df = (
        inventory_position_df
        .filter(col("location_type") == "store")
        .select(
            col("location_id").alias("store_id"),
            col("product_id"),
            col("product_name"),
            col("category"),
            col("sub_category"),
            col("brand"),
            col("supplier_id"),
            col("stock_on_hand_after"),
            col("reserved_stock_after"),
            col("available_stock"),
            col("reorder_point"),
            col("safety_stock"),
            col("inventory_status"),
            col("latest_inventory_event_timestamp"),
            col("inventory_value_at_cost"),
            col("inventory_value_at_retail"),
        )
    )

    store_stockout_df = (
        stockout_risk_df
        .filter(col("location_type") == "store")
        .select(
            col("location_id").alias("store_id"),
            col("product_id"),
            col("stockout_risk_band"),
            col("recommended_action").alias("stockout_recommended_action"),
            col("replenishment_gap"),
            col("total_outflow_quantity"),
            col("total_inflow_quantity"),
            col("movement_event_count"),
            col("last_movement_timestamp"),
        )
    )

    store_sales_agg_df = (
        store_sales_df
        .groupBy("store_id")
        .agg(
            spark_sum("total_revenue").alias("store_total_revenue"),
            spark_sum("total_items_sold").alias("store_total_items_sold"),
            spark_sum("order_count").alias("store_order_count"),
            spark_max("latest_order_timestamp").alias("store_latest_order_timestamp"),
        )
    )

    store_delay_impact_df = (
        shipment_delay_df
        .filter(col("destination_type") == "store")
        .groupBy(col("destination_id").alias("store_id"), "product_id")
        .agg(
            count("*").alias("inbound_shipment_count"),
            spark_sum(when(col("is_delayed") == True, lit(1)).otherwise(lit(0))).alias(
                "delayed_inbound_shipment_count"
            ),
            spark_sum(
                when(col("impact_band").isin("critical", "high"), lit(1)).otherwise(lit(0))
            ).alias("high_impact_inbound_delay_count"),
            spark_max("event_timestamp").alias("latest_inbound_shipment_event_timestamp"),
        )
    )

    return (
        store_inventory_df.alias("inv")
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
            ).alias("st"),
            "store_id",
            "left",
        )
        .join(
            warehouses_df.select(
                "warehouse_id",
                "warehouse_name",
                col("country").alias("warehouse_country"),
                col("city").alias("warehouse_city"),
                col("region").alias("warehouse_region"),
                "capacity_units",
                "current_utilization_pct",
                "utilization_band",
            ).alias("wh"),
            "warehouse_id",
            "left",
        )
        .join(
            store_stockout_df.alias("risk"),
            (
                (col("inv.store_id") == col("risk.store_id"))
                & (col("inv.product_id") == col("risk.product_id"))
            ),
            "left",
        )
        .join(
            store_sales_agg_df.alias("sales"),
            "store_id",
            "left",
        )
        .join(
            store_delay_impact_df.alias("delay"),
            (
                (col("inv.store_id") == col("delay.store_id"))
                & (col("inv.product_id") == col("delay.product_id"))
            ),
            "left",
        )
        .select(
            col("inv.store_id"),
            col("st.store_name"),
            col("st.country").alias("store_country"),
            col("st.city").alias("store_city"),
            col("st.region").alias("store_region"),
            col("st.store_type"),
            col("st.store_size_band"),
            col("st.warehouse_id").alias("serving_warehouse_id"),
            col("wh.warehouse_name").alias("serving_warehouse_name"),
            col("wh.warehouse_country"),
            col("wh.warehouse_city"),
            col("wh.warehouse_region"),
            col("wh.capacity_units").alias("warehouse_capacity_units"),
            col("wh.current_utilization_pct").alias("warehouse_utilization_pct"),
            col("wh.utilization_band").alias("warehouse_utilization_band"),
            col("inv.product_id"),
            col("inv.product_name"),
            col("inv.category"),
            col("inv.sub_category"),
            col("inv.brand"),
            col("inv.supplier_id"),
            col("inv.stock_on_hand_after"),
            col("inv.reserved_stock_after"),
            col("inv.available_stock"),
            col("inv.reorder_point"),
            col("inv.safety_stock"),
            col("inv.inventory_status"),
            col("risk.stockout_risk_band"),
            col("risk.stockout_recommended_action"),
            col("risk.replenishment_gap"),
            col("risk.total_outflow_quantity"),
            col("risk.total_inflow_quantity"),
            col("risk.movement_event_count"),
            col("risk.last_movement_timestamp"),
            col("delay.inbound_shipment_count"),
            col("delay.delayed_inbound_shipment_count"),
            col("delay.high_impact_inbound_delay_count"),
            col("delay.latest_inbound_shipment_event_timestamp"),
            col("sales.store_total_revenue"),
            col("sales.store_total_items_sold"),
            col("sales.store_order_count"),
            col("sales.store_latest_order_timestamp"),
            col("inv.inventory_value_at_cost"),
            col("inv.inventory_value_at_retail"),
            col("inv.latest_inventory_event_timestamp"),
        )
        .withColumn(
            "replenishment_priority",
            when(col("stockout_risk_band") == "high", lit("urgent"))
            .when(col("stockout_risk_band") == "medium", lit("planned"))
            .when(col("high_impact_inbound_delay_count") > 0, lit("monitor_delay"))
            .otherwise(lit("normal")),
        )
        .withColumn(
            "warehouse_action",
            when(
                (col("replenishment_priority") == "urgent")
                & (col("warehouse_utilization_band") == "high"),
                lit("rebalance_or_expedite_from_alternate_warehouse"),
            )
            .when(col("replenishment_priority") == "urgent", lit("replenish_from_serving_warehouse"))
            .when(col("replenishment_priority") == "planned", lit("schedule_replenishment"))
            .when(col("replenishment_priority") == "monitor_delay", lit("track_inbound_shipment"))
            .otherwise(lit("no_action")),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )