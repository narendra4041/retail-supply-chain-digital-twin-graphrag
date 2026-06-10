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

from pipelines.retail_eventhub_pipeline.utils.config import (
    get_catalog_name,
    get_schema_name,
)


ENVIRONMENT = spark.conf.get("ENVIRONMENT", "dev")

catalog_name = get_catalog_name(ENVIRONMENT)
silver_schema = get_schema_name("silver", ENVIRONMENT)
gold_schema = get_schema_name("gold", ENVIRONMENT)

SILVER_INVENTORY_MOVEMENTS_TABLE = (
    f"{catalog_name}.{silver_schema}.inventory_movements"
)
SILVER_PRODUCTS_TABLE = f"{catalog_name}.{silver_schema}.products"
SILVER_STORES_TABLE = f"{catalog_name}.{silver_schema}.stores"
SILVER_WAREHOUSES_TABLE = f"{catalog_name}.{silver_schema}.warehouses"

GOLD_INVENTORY_POSITION_TABLE = f"{catalog_name}.{gold_schema}.inventory_position"
GOLD_STOCKOUT_RISK_TABLE = f"{catalog_name}.{gold_schema}.stockout_risk"


@dp.materialized_view(
    name=GOLD_INVENTORY_POSITION_TABLE,
    comment=(
        "Current inventory position by product and location. "
        "Built from latest inventory movement events."
    ),
)
def inventory_position():
    inventory_df = spark.read.table(SILVER_INVENTORY_MOVEMENTS_TABLE)
    products_df = spark.read.table(SILVER_PRODUCTS_TABLE)
    stores_df = spark.read.table(SILVER_STORES_TABLE)
    warehouses_df = spark.read.table(SILVER_WAREHOUSES_TABLE)

    latest_inventory_ts_df = (
        inventory_df
        .groupBy("product_id", "location_type", "location_id")
        .agg(
            spark_max("event_timestamp").alias("latest_event_timestamp")
        )
    )

    latest_inventory_df = (
        inventory_df.alias("inv")
        .join(
            latest_inventory_ts_df.alias("latest"),
            (
                (col("inv.product_id") == col("latest.product_id"))
                & (col("inv.location_type") == col("latest.location_type"))
                & (col("inv.location_id") == col("latest.location_id"))
                & (col("inv.event_timestamp") == col("latest.latest_event_timestamp"))
            ),
            "inner",
        )
        .select(
            col("inv.product_id"),
            col("inv.location_type"),
            col("inv.location_id"),
            col("inv.movement_type"),
            col("inv.quantity_change"),
            col("inv.stock_on_hand_after"),
            col("inv.reserved_stock_after"),
            col("inv.reorder_point"),
            col("inv.safety_stock"),
            col("inv.event_timestamp").alias("latest_inventory_event_timestamp"),
            col("inv.eventhub_enqueued_timestamp"),
            col("inv._silver_processed_at"),
        )
    )

    stores_enriched_df = (
        latest_inventory_df
        .filter(col("location_type") == "store")
        .join(
            stores_df.select(
                "store_id",
                "store_name",
                "country",
                "city",
                "region",
                "store_type",
                "warehouse_id",
            ),
            latest_inventory_df["location_id"] == stores_df["store_id"],
            "left",
        )
        .select(
            latest_inventory_df["*"],
            col("store_name").alias("location_name"),
            col("country").alias("location_country"),
            col("city").alias("location_city"),
            col("region").alias("location_region"),
            col("store_type").alias("location_subtype"),
            col("warehouse_id").alias("serving_warehouse_id"),
        )
    )

    warehouses_enriched_df = (
        latest_inventory_df
        .filter(col("location_type") == "warehouse")
        .join(
            warehouses_df.select(
                "warehouse_id",
                "warehouse_name",
                "country",
                "city",
                "region",
                "utilization_band",
            ),
            latest_inventory_df["location_id"] == warehouses_df["warehouse_id"],
            "left",
        )
        .select(
            latest_inventory_df["*"],
            col("warehouse_name").alias("location_name"),
            col("country").alias("location_country"),
            col("city").alias("location_city"),
            col("region").alias("location_region"),
            col("utilization_band").alias("location_subtype"),
            lit(None).cast("string").alias("serving_warehouse_id"),
        )
    )

    unified_location_df = stores_enriched_df.unionByName(warehouses_enriched_df)

    return (
        unified_location_df
        .join(
            products_df.select(
                "product_id",
                "product_name",
                "category",
                "sub_category",
                "brand",
                "supplier_id",
                "unit_price",
                "unit_cost",
                "active_flag",
            ),
            "product_id",
            "left",
        )
        .withColumn(
            "available_stock",
            col("stock_on_hand_after") - col("reserved_stock_after"),
        )
        .withColumn(
            "inventory_value_at_cost",
            col("stock_on_hand_after") * col("unit_cost"),
        )
        .withColumn(
            "inventory_value_at_retail",
            col("stock_on_hand_after") * col("unit_price"),
        )
        .withColumn(
            "inventory_status",
            when(col("stock_on_hand_after") <= col("safety_stock"), lit("critical"))
            .when(col("stock_on_hand_after") <= col("reorder_point"), lit("reorder"))
            .otherwise(lit("healthy")),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )


@dp.materialized_view(
    name=GOLD_STOCKOUT_RISK_TABLE,
    comment=(
        "Stockout risk view by product and location using inventory position, "
        "recent movement signals, and reorder/safety stock thresholds."
    ),
)
def stockout_risk():
    inventory_position_df = spark.read.table(GOLD_INVENTORY_POSITION_TABLE)
    inventory_movements_df = spark.read.table(SILVER_INVENTORY_MOVEMENTS_TABLE)

    movement_summary_df = (
        inventory_movements_df
        .groupBy("product_id", "location_type", "location_id")
        .agg(
            count("*").alias("movement_event_count"),
            spark_sum(
                when(col("quantity_change") < 0, -col("quantity_change")).otherwise(0)
            ).alias("total_outflow_quantity"),
            spark_sum(
                when(col("quantity_change") > 0, col("quantity_change")).otherwise(0)
            ).alias("total_inflow_quantity"),
            avg("quantity_change").alias("avg_quantity_change"),
            spark_max("event_timestamp").alias("last_movement_timestamp"),
        )
    )

    risk_df = (
        inventory_position_df.alias("pos")
        .join(
            movement_summary_df.alias("mov"),
            (
                (col("pos.product_id") == col("mov.product_id"))
                & (col("pos.location_type") == col("mov.location_type"))
                & (col("pos.location_id") == col("mov.location_id"))
            ),
            "left",
        )
        .select(
            col("pos.product_id"),
            col("pos.product_name"),
            col("pos.category"),
            col("pos.sub_category"),
            col("pos.brand"),
            col("pos.supplier_id"),
            col("pos.location_type"),
            col("pos.location_id"),
            col("pos.location_name"),
            col("pos.location_country"),
            col("pos.location_city"),
            col("pos.location_region"),
            col("pos.stock_on_hand_after"),
            col("pos.reserved_stock_after"),
            col("pos.available_stock"),
            col("pos.reorder_point"),
            col("pos.safety_stock"),
            col("pos.inventory_status"),
            col("mov.movement_event_count"),
            col("mov.total_outflow_quantity"),
            col("mov.total_inflow_quantity"),
            col("mov.avg_quantity_change"),
            col("mov.last_movement_timestamp"),
        )
        .withColumn(
            "stockout_risk_band",
            when(col("stock_on_hand_after") <= col("safety_stock"), lit("high"))
            .when(col("stock_on_hand_after") <= col("reorder_point"), lit("medium"))
            .otherwise(lit("low")),
        )
        .withColumn(
            "recommended_action",
            when(col("stockout_risk_band") == "high", lit("urgent_replenishment"))
            .when(col("stockout_risk_band") == "medium", lit("plan_replenishment"))
            .otherwise(lit("monitor")),
        )
        .withColumn(
            "replenishment_gap",
            when(
                col("stock_on_hand_after") < col("reorder_point"),
                col("reorder_point") - col("stock_on_hand_after"),
            ).otherwise(lit(0)),
        )
        .withColumn("gold_updated_at", current_timestamp())
    )

    return risk_df