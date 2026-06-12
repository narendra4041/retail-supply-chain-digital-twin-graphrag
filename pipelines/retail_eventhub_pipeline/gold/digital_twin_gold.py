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

SILVER_PRODUCTS_TABLE = f"{catalog_name}.{silver_schema}.products"
SILVER_SUPPLIERS_TABLE = f"{catalog_name}.{silver_schema}.suppliers"
SILVER_STORES_TABLE = f"{catalog_name}.{silver_schema}.stores"
SILVER_WAREHOUSES_TABLE = f"{catalog_name}.{silver_schema}.warehouses"

GOLD_INVENTORY_POSITION_TABLE = f"{catalog_name}.{gold_schema}.inventory_position"
GOLD_STOCKOUT_RISK_TABLE = f"{catalog_name}.{gold_schema}.stockout_risk"
GOLD_SUPPLIER_RISK_SCORE_TABLE = f"{catalog_name}.{gold_schema}.supplier_risk_score"
GOLD_SHIPMENT_DELAY_IMPACT_TABLE = f"{catalog_name}.{gold_schema}.shipment_delay_impact"
GOLD_PRODUCT_DEMAND_SUMMARY_TABLE = f"{catalog_name}.{gold_schema}.product_demand_summary"
GOLD_STORE_SALES_SUMMARY_TABLE = f"{catalog_name}.{gold_schema}.store_sales_summary"
GOLD_WAREHOUSE_STORE_REPLENISHMENT_TABLE = (
    f"{catalog_name}.{gold_schema}.warehouse_store_replenishment_view"
)

GOLD_DIGITAL_TWIN_ENTITY_HEALTH_TABLE = (
    f"{catalog_name}.{gold_schema}.digital_twin_entity_health"
)


@dp.materialized_view(
    name=GOLD_DIGITAL_TWIN_ENTITY_HEALTH_TABLE,
    comment=(
        "Unified digital twin health view for products, suppliers, stores, and warehouses. "
        "Combines inventory, demand, shipment delay, and supplier risk signals."
    ),
)
def digital_twin_entity_health():
    products_df = spark.read.table(SILVER_PRODUCTS_TABLE)
    suppliers_df = spark.read.table(SILVER_SUPPLIERS_TABLE)
    stores_df = spark.read.table(SILVER_STORES_TABLE)
    warehouses_df = spark.read.table(SILVER_WAREHOUSES_TABLE)

    inventory_position_df = spark.read.table(GOLD_INVENTORY_POSITION_TABLE)
    stockout_risk_df = spark.read.table(GOLD_STOCKOUT_RISK_TABLE)
    supplier_risk_df = spark.read.table(GOLD_SUPPLIER_RISK_SCORE_TABLE)
    shipment_delay_df = spark.read.table(GOLD_SHIPMENT_DELAY_IMPACT_TABLE)
    product_demand_df = spark.read.table(GOLD_PRODUCT_DEMAND_SUMMARY_TABLE)
    store_sales_df = spark.read.table(GOLD_STORE_SALES_SUMMARY_TABLE)
    warehouse_store_replenishment_df = spark.read.table(GOLD_WAREHOUSE_STORE_REPLENISHMENT_TABLE)

    # ---------------------------------------------------------------------
    # Product health
    # ---------------------------------------------------------------------
    product_inventory_summary_df = (
        inventory_position_df
        .groupBy("product_id")
        .agg(
            spark_sum("stock_on_hand_after").alias("total_stock_on_hand"),
            spark_sum("available_stock").alias("total_available_stock"),
            spark_sum("inventory_value_at_cost").alias("inventory_value_at_cost"),
            count("*").alias("inventory_location_count"),
            spark_max("latest_inventory_event_timestamp").alias(
                "latest_inventory_event_timestamp"
            ),
        )
    )

    product_stockout_summary_df = (
        stockout_risk_df
        .groupBy("product_id")
        .agg(
            spark_sum(
                when(col("stockout_risk_band") == "high", lit(1)).otherwise(lit(0))
            ).alias("high_stockout_location_count"),
            spark_sum(
                when(col("stockout_risk_band") == "medium", lit(1)).otherwise(lit(0))
            ).alias("medium_stockout_location_count"),
            count("*").alias("stockout_location_count"),
        )
    )

    product_delay_summary_df = (
        shipment_delay_df
        .groupBy("product_id")
        .agg(
            spark_sum(when(col("is_delayed") == True, lit(1)).otherwise(lit(0))).alias(
                "delayed_shipment_count"
            ),
            spark_sum(
                when(col("impact_band").isin("critical", "high"), lit(1)).otherwise(lit(0))
            ).alias("high_impact_delay_count"),
            spark_max("event_timestamp").alias("latest_shipment_event_timestamp"),
        )
    )

    product_health_df = (
        products_df.alias("p")
        .join(product_inventory_summary_df.alias("inv"), "product_id", "left")
        .join(product_stockout_summary_df.alias("stock"), "product_id", "left")
        .join(product_delay_summary_df.alias("delay"), "product_id", "left")
        .join(product_demand_df.alias("demand"), "product_id", "left")
        .select(
            lit("product").alias("entity_type"),
            col("p.product_id").alias("entity_id"),
            col("p.product_name").alias("entity_name"),
            col("p.category").alias("entity_category"),
            col("p.supplier_id").alias("related_entity_id"),
            col("p.active_flag").alias("is_active"),
            col("inv.total_stock_on_hand"),
            col("inv.total_available_stock"),
            col("stock.high_stockout_location_count"),
            col("stock.medium_stockout_location_count"),
            col("delay.delayed_shipment_count"),
            col("delay.high_impact_delay_count"),
            col("demand.total_quantity_sold"),
            col("demand.total_revenue"),
            col("demand.demand_band"),
        )
        .withColumn(
            "health_score",
            when(col("high_stockout_location_count") > 0, lit(40))
            .when(col("high_impact_delay_count") > 0, lit(50))
            .when(col("medium_stockout_location_count") > 0, lit(70))
            .otherwise(lit(90)),
        )
        .withColumn(
            "risk_band",
            when(col("health_score") < 50, lit("high"))
            .when(col("health_score") < 75, lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn(
            "main_risk_reason",
            when(col("high_stockout_location_count") > 0, lit("stockout_risk"))
            .when(col("high_impact_delay_count") > 0, lit("shipment_delay"))
            .when(col("medium_stockout_location_count") > 0, lit("reorder_risk"))
            .otherwise(lit("healthy")),
        )
    )

    # ---------------------------------------------------------------------
    # Supplier health
    # ---------------------------------------------------------------------
    supplier_delay_summary_df = (
        shipment_delay_df
        .groupBy("supplier_id")
        .agg(
            spark_sum(when(col("is_delayed") == True, lit(1)).otherwise(lit(0))).alias(
                "delayed_shipment_count"
            ),
            spark_sum(
                when(col("impact_band").isin("critical", "high"), lit(1)).otherwise(lit(0))
            ).alias("high_impact_delay_count"),
            count("*").alias("shipment_count"),
        )
    )

    supplier_health_df = (
        suppliers_df.alias("s")
        .join(supplier_risk_df.alias("risk"), "supplier_id", "left")
        .join(supplier_delay_summary_df.alias("delay"), "supplier_id", "left")
        .select(
            lit("supplier").alias("entity_type"),
            col("s.supplier_id").alias("entity_id"),
            col("s.supplier_name").alias("entity_name"),
            col("s.supplier_type").alias("entity_category"),
            lit(None).cast("string").alias("related_entity_id"),
            lit(True).alias("is_active"),
            lit(None).cast("long").alias("total_stock_on_hand"),
            lit(None).cast("long").alias("total_available_stock"),
            lit(None).cast("long").alias("high_stockout_location_count"),
            lit(None).cast("long").alias("medium_stockout_location_count"),
            col("delay.delayed_shipment_count"),
            col("delay.high_impact_delay_count"),
            lit(None).cast("long").alias("total_quantity_sold"),
            col("risk.total_revenue"),
            lit(None).cast("string").alias("demand_band"),
            col("risk.computed_supplier_risk_score"),
            col("risk.risk_band").alias("supplier_risk_band"),
            col("risk.primary_risk_reason"),
        )
        .withColumn(
            "health_score",
            when(col("supplier_risk_band") == "high", lit(35))
            .when(col("supplier_risk_band") == "medium", lit(65))
            .otherwise(lit(90)),
        )
        .withColumn(
            "risk_band",
            when(col("supplier_risk_band").isNotNull(), col("supplier_risk_band"))
            .otherwise(lit("low")),
        )
        .withColumn(
            "main_risk_reason",
            when(col("primary_risk_reason").isNotNull(), col("primary_risk_reason"))
            .otherwise(lit("healthy")),
        )
        .drop("computed_supplier_risk_score", "supplier_risk_band", "primary_risk_reason")
    )

    # ---------------------------------------------------------------------
    # Store health
    # ---------------------------------------------------------------------
    
    store_replenishment_summary_df = (
        warehouse_store_replenishment_df
        .groupBy("store_id")
        .agg(
            spark_sum(
                when(col("replenishment_priority") == "urgent", lit(1)).otherwise(lit(0))
            ).alias("urgent_replenishment_count"),
            spark_sum(
                when(col("replenishment_priority") == "planned", lit(1)).otherwise(lit(0))
            ).alias("planned_replenishment_count"),
            spark_sum(
                when(col("replenishment_priority") == "monitor_delay", lit(1)).otherwise(lit(0))
            ).alias("delay_monitoring_count"),
            spark_sum(
                when(col("high_impact_inbound_delay_count") > 0, lit(1)).otherwise(lit(0))
            ).alias("high_impact_inbound_delay_count"),
            spark_sum("stock_on_hand_after").alias("total_stock_on_hand"),
            spark_sum("available_stock").alias("total_available_stock"),
            spark_sum("store_total_revenue").alias("total_revenue"),
            spark_sum("store_total_items_sold").alias("total_quantity_sold"),
            spark_max("store_latest_order_timestamp").alias("latest_order_timestamp"),
            spark_max("latest_inventory_event_timestamp").alias(
                "latest_inventory_event_timestamp"
            ),
            spark_max("latest_inbound_shipment_event_timestamp").alias(
                "latest_inbound_shipment_event_timestamp"
            ),
        )
    )

    store_health_df = (
        stores_df.alias("s")
        .join(store_replenishment_summary_df.alias("rep"), "store_id", "left")
        .select(
            lit("store").alias("entity_type"),
            col("s.store_id").alias("entity_id"),
            col("s.store_name").alias("entity_name"),
            col("s.store_type").alias("entity_category"),
            col("s.warehouse_id").alias("related_entity_id"),
            lit(True).alias("is_active"),
            col("rep.total_stock_on_hand"),
            col("rep.total_available_stock"),
            col("rep.urgent_replenishment_count").alias("high_stockout_location_count"),
            col("rep.planned_replenishment_count").alias("medium_stockout_location_count"),
            col("rep.delay_monitoring_count").alias("delayed_shipment_count"),
            col("rep.high_impact_inbound_delay_count"),
            col("rep.total_quantity_sold"),
            col("rep.total_revenue"),
            lit(None).cast("string").alias("demand_band"),
        )
        .withColumn(
            "health_score",
            when(col("high_stockout_location_count") > 0, lit(40))
            .when(col("high_impact_inbound_delay_count") > 0, lit(55))
            .when(col("medium_stockout_location_count") > 0, lit(70))
            .otherwise(lit(90)),
        )
        .withColumn(
            "risk_band",
            when(col("health_score") < 50, lit("high"))
            .when(col("health_score") < 75, lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn(
            "main_risk_reason",
            when(
                col("high_stockout_location_count") > 0,
                lit("urgent_replenishment_required"),
            )
            .when(
                col("high_impact_inbound_delay_count") > 0,
                lit("inbound_shipment_delay"),
            )
            .when(
                col("medium_stockout_location_count") > 0,
                lit("planned_replenishment_required"),
            )
            .otherwise(lit("healthy")),
        )
    )

    # ---------------------------------------------------------------------
    # Warehouse health
    # ---------------------------------------------------------------------
        # ---------------------------------------------------------------------
    # Warehouse health
    # ---------------------------------------------------------------------
    warehouse_replenishment_summary_df = (
        warehouse_store_replenishment_df
        .groupBy("serving_warehouse_id")
        .agg(
            spark_sum(
                when(col("replenishment_priority") == "urgent", lit(1)).otherwise(lit(0))
            ).alias("served_store_urgent_replenishment_count"),
            spark_sum(
                when(col("replenishment_priority") == "planned", lit(1)).otherwise(lit(0))
            ).alias("served_store_planned_replenishment_count"),
            spark_sum(
                when(
                    col("warehouse_action")
                    == "rebalance_or_expedite_from_alternate_warehouse",
                    lit(1),
                ).otherwise(lit(0))
            ).alias("warehouse_capacity_pressure_count"),
            spark_sum(
                when(col("high_impact_inbound_delay_count") > 0, lit(1)).otherwise(lit(0))
            ).alias("high_impact_store_delay_count"),
            spark_sum("stock_on_hand_after").alias("total_stock_on_hand"),
            spark_sum("available_stock").alias("total_available_stock"),
            count("store_id").alias("store_product_replenishment_signal_count"),
        )
    )

    warehouse_health_df = (
        warehouses_df.alias("w")
        .join(
            warehouse_replenishment_summary_df.alias("rep"),
            col("w.warehouse_id") == col("rep.serving_warehouse_id"),
            "left",
        )
        .select(
            lit("warehouse").alias("entity_type"),
            col("w.warehouse_id").alias("entity_id"),
            col("w.warehouse_name").alias("entity_name"),
            col("w.utilization_band").alias("entity_category"),
            lit(None).cast("string").alias("related_entity_id"),
            lit(True).alias("is_active"),
            col("rep.total_stock_on_hand"),
            col("rep.total_available_stock"),
            col("rep.served_store_urgent_replenishment_count").alias(
                "high_stockout_location_count"
            ),
            col("rep.served_store_planned_replenishment_count").alias(
                "medium_stockout_location_count"
            ),
            col("rep.high_impact_store_delay_count").alias("delayed_shipment_count"),
            col("rep.warehouse_capacity_pressure_count").alias(
                "high_impact_delay_count"
            ),
            lit(None).cast("long").alias("total_quantity_sold"),
            lit(None).cast("double").alias("total_revenue"),
            lit(None).cast("string").alias("demand_band"),
        )
        .withColumn(
            "health_score",
            when(col("high_impact_delay_count") > 0, lit(45))
            .when(col("high_stockout_location_count") > 0, lit(55))
            .when(col("medium_stockout_location_count") > 0, lit(75))
            .otherwise(lit(90)),
        )
        .withColumn(
            "risk_band",
            when(col("health_score") < 60, lit("high"))
            .when(col("health_score") < 80, lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn(
            "main_risk_reason",
            when(
                col("high_impact_delay_count") > 0,
                lit("warehouse_capacity_pressure"),
            )
            .when(
                col("high_stockout_location_count") > 0,
                lit("served_store_urgent_replenishment"),
            )
            .when(
                col("medium_stockout_location_count") > 0,
                lit("served_store_planned_replenishment"),
            )
            .otherwise(lit("healthy")),
        )
    )

    return (
        product_health_df
        .unionByName(supplier_health_df, allowMissingColumns=True)
        .unionByName(store_health_df, allowMissingColumns=True)
        .unionByName(warehouse_health_df, allowMissingColumns=True)
        .withColumn("gold_updated_at", current_timestamp())
    )