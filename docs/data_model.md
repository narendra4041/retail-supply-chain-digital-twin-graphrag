# Retail Supply Chain Data Model

## Overview

This document defines the core data model for the Retail Supply Chain Digital Twin with GraphRAG project.

The model supports inventory intelligence, demand forecasting, supplier risk analysis, shipment impact analysis, fraud detection, and graph-based AI reasoning.

## Core Entities

### suppliers

| Column | Description |
|---|---|
| supplier_id | Unique supplier identifier |
| supplier_name | Supplier name |
| country | Supplier country |
| region | Supplier region |
| supplier_type | Type of supplier |
| lead_time_days | Standard lead time in days |
| reliability_score | Supplier reliability score from 0 to 1 |
| quality_score | Supplier quality score from 0 to 1 |
| contract_start_date | Contract start date |
| contract_end_date | Contract end date |
| preferred_supplier_flag | Whether supplier is preferred |

### products

| Column | Description |
|---|---|
| product_id | Unique product identifier |
| product_name | Product name |
| category | Product category |
| sub_category | Product sub-category |
| brand | Brand name |
| unit_price | Retail selling price |
| unit_cost | Product cost |
| supplier_id | Linked supplier |
| weight_kg | Product weight |
| volume_m3 | Product volume |
| active_flag | Whether product is active |

### warehouses

| Column | Description |
|---|---|
| warehouse_id | Unique warehouse identifier |
| warehouse_name | Warehouse name |
| country | Warehouse country |
| city | Warehouse city |
| region | Business region |
| capacity_units | Maximum inventory capacity |
| current_utilization_pct | Current capacity usage |

### stores

| Column | Description |
|---|---|
| store_id | Unique store identifier |
| store_name | Store name |
| country | Store country |
| city | Store city |
| region | Business region |
| store_type | Store format |
| size_sq_m | Store size in square meters |
| warehouse_id | Main supplying warehouse |

### customers

| Column | Description |
|---|---|
| customer_id | Unique customer identifier |
| customer_name | Customer name |
| country | Customer country |
| city | Customer city |
| customer_segment | Customer segment |
| loyalty_tier | Loyalty level |
| signup_date | Customer signup date |

### orders

| Column | Description |
|---|---|
| order_id | Unique order identifier |
| customer_id | Linked customer |
| store_id | Linked store |
| order_date | Order date |
| order_channel | Store or ecommerce |
| order_status | Order status |
| payment_method | Payment method |
| total_amount | Total order amount |

### order_items

| Column | Description |
|---|---|
| order_item_id | Unique order item identifier |
| order_id | Linked order |
| product_id | Linked product |
| quantity | Quantity purchased |
| unit_price | Selling price |
| discount_pct | Discount percentage |
| line_total | Total value for the item |

### inventory

| Column | Description |
|---|---|
| inventory_id | Unique inventory record |
| product_id | Linked product |
| warehouse_id | Warehouse identifier, nullable |
| store_id | Store identifier, nullable |
| stock_on_hand | Available stock |
| reserved_stock | Reserved stock |
| reorder_point | Reorder threshold |
| safety_stock | Minimum safety stock |
| last_updated | Last inventory update timestamp |

### shipments

| Column | Description |
|---|---|
| shipment_id | Unique shipment identifier |
| source_type | supplier or warehouse |
| source_id | Source entity ID |
| destination_type | warehouse or store |
| destination_id | Destination entity ID |
| product_id | Product being shipped |
| quantity | Quantity shipped |
| shipment_date | Shipment start date |
| expected_delivery_date | Expected delivery date |
| actual_delivery_date | Actual delivery date |
| shipment_status | in_transit, delivered, delayed, cancelled |
| carrier | Logistics carrier |
| delay_days | Number of delayed days |

### returns

| Column | Description |
|---|---|
| return_id | Unique return identifier |
| order_id | Linked order |
| product_id | Returned product |
| customer_id | Customer returning product |
| store_id | Store handling return |
| return_date | Return date |
| return_reason | Reason for return |
| refund_amount | Refund amount |
| return_status | approved, rejected, pending |

### supplier_performance

| Column | Description |
|---|---|
| performance_id | Unique performance record |
| supplier_id | Linked supplier |
| product_id | Linked product |
| month | Performance month |
| on_time_delivery_rate | Delivery success rate |
| defect_rate | Product defect rate |
| avg_delay_days | Average delay |
| fulfilled_quantity | Quantity fulfilled |
| rejected_quantity | Quantity rejected |
| risk_score | Supplier risk score |

## Model Purpose

This model supports:

- Bronze, Silver, and Gold lakehouse pipelines
- Neo4j knowledge graph creation
- Demand forecasting
- Stockout prediction
- Supplier risk scoring
- Shipment impact analysis
- GraphRAG AI agents
- Power BI dashboards