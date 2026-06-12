# Neo4j Graph Model — Retail Supply Chain Digital Twin

## 1. Purpose

This document defines the Neo4j knowledge graph model for the Retail Supply Chain Digital Twin project.

The graph connects suppliers, products, stores, warehouses, shipments, customers, inventory risk, supplier risk, shipment delay risk, replenishment signals, and digital twin health signals.

The goal is to support:

- Supply chain impact analysis
- Supplier risk investigation
- Store and warehouse replenishment analysis
- Product dependency analysis
- GraphRAG and AI agent reasoning
- Executive-level supply chain explanations

---

## 2. Source Layer

Neo4j should load only from curated Databricks Gold views, not raw Bronze or Silver tables.

Primary Gold sources:

```text
retail_digital_twin_dev.gold.supplier_product_dependency
retail_digital_twin_dev.gold.warehouse_store_replenishment_view
retail_digital_twin_dev.gold.shipment_delay_impact
retail_digital_twin_dev.gold.product_demand_summary
retail_digital_twin_dev.gold.customer_order_summary
retail_digital_twin_dev.gold.digital_twin_entity_health
retail_digital_twin_dev.gold.inventory_position
retail_digital_twin_dev.gold.stockout_risk
retail_digital_twin_dev.gold.supplier_risk_score
```

Supporting Silver sources if required later:

```text
retail_digital_twin_dev.silver.orders
retail_digital_twin_dev.silver.order_items
retail_digital_twin_dev.silver.shipments
retail_digital_twin_dev.silver.customers
```

---

## 3. Core Node Labels

### 3.1 Supplier

Represents a product supplier.

Source:

```text
gold.supplier_product_dependency
gold.supplier_risk_score
gold.digital_twin_entity_health
```

Unique key:

```text
supplier_id
```

Important properties:

```text
supplier_id
supplier_name
supplier_country
supplier_region
supplier_type
preferred_supplier_flag
risk_band
primary_risk_reason
computed_supplier_risk_score
health_score
```

---

### 3.2 Product

Represents a sellable retail product.

Source:

```text
gold.supplier_product_dependency
gold.product_demand_summary
gold.stockout_risk
gold.digital_twin_entity_health
```

Unique key:

```text
product_id
```

Important properties:

```text
product_id
product_name
category
sub_category
brand
active_flag
unit_price
unit_cost
gross_margin
gross_margin_pct
demand_band
total_quantity_sold
total_revenue
health_score
risk_band
main_risk_reason
```

---

### 3.3 Store

Represents a retail store.

Source:

```text
gold.warehouse_store_replenishment_view
gold.store_sales_summary
gold.digital_twin_entity_health
```

Unique key:

```text
store_id
```

Important properties:

```text
store_id
store_name
store_country
store_city
store_region
store_type
store_size_band
total_revenue
total_quantity_sold
health_score
risk_band
main_risk_reason
```

---

### 3.4 Warehouse

Represents a warehouse or distribution center.

Source:

```text
gold.warehouse_store_replenishment_view
gold.inventory_position
gold.digital_twin_entity_health
```

Unique key:

```text
warehouse_id
```

Important properties:

```text
warehouse_id
warehouse_name
warehouse_country
warehouse_city
warehouse_region
capacity_units
warehouse_utilization_pct
warehouse_utilization_band
health_score
risk_band
main_risk_reason
```

---

### 3.5 Customer

Represents a customer summary entity.

Source:

```text
gold.customer_order_summary
```

Unique key:

```text
customer_id
```

Important properties:

```text
customer_id
customer_name
country
city
customer_segment
loyalty_tier
loyalty_rank
order_count
total_spend
customer_value_band
```

---

### 3.6 Shipment

Represents a shipment movement.

Source:

```text
gold.shipment_delay_impact
```

Unique key:

```text
shipment_id
```

Important properties:

```text
shipment_id
shipment_type
shipment_status
carrier
delay_days
delay_severity
impact_band
recommended_action
estimated_shipment_retail_value
estimated_shipment_cost_value
```

---

### 3.7 RiskSignal

Represents a reusable risk signal attached to a business entity.

Risk types:

```text
stockout_risk
supplier_risk
shipment_delay
replenishment_risk
entity_health_risk
```

Unique key:

```text
risk_signal_id
```

Recommended generated key pattern:

```text
<risk_type>|<entity_type>|<entity_id>
```

Example:

```text
stockout_risk|product|PROD0001
supplier_risk|supplier|SUP0001
```

Important properties:

```text
risk_signal_id
risk_type
entity_type
entity_id
risk_band
risk_score
risk_reason
recommended_action
source_gold_view
last_updated_at
```

---

## 4. Core Relationships

### 4.1 Supplier supplies Product

```cypher
(:Supplier)-[:SUPPLIES]->(:Product)
```

Source:

```text
gold.supplier_product_dependency
```

Relationship properties:

```text
dependency_risk_band
total_units_sold
total_revenue
primary_risk_reason
```

Business meaning:

A supplier provides a product. This relationship helps identify supplier dependency and product supply risk.

---

### 4.2 Warehouse serves Store

```cypher
(:Warehouse)-[:SERVES]->(:Store)
```

Source:

```text
gold.warehouse_store_replenishment_view
```

Relationship properties:

```text
warehouse_utilization_pct
warehouse_utilization_band
store_region
```

Business meaning:

A warehouse is responsible for replenishing a store.

---

### 4.3 Store stocks Product

```cypher
(:Store)-[:STOCKS]->(:Product)
```

Source:

```text
gold.warehouse_store_replenishment_view
```

Relationship properties:

```text
stock_on_hand_after
available_stock
reorder_point
safety_stock
inventory_status
stockout_risk_band
replenishment_priority
warehouse_action
```

Business meaning:

A store currently carries a product and may need replenishment.

---

### 4.4 Warehouse stocks Product

```cypher
(:Warehouse)-[:STOCKS]->(:Product)
```

Source:

```text
gold.inventory_position
```

Condition:

```text
location_type = 'warehouse'
```

Relationship properties:

```text
stock_on_hand_after
available_stock
reorder_point
safety_stock
inventory_status
inventory_value_at_cost
inventory_value_at_retail
```

Business meaning:

A warehouse carries stock for a product.

---

### 4.5 Shipment moves Product

```cypher
(:Shipment)-[:MOVES]->(:Product)
```

Source:

```text
gold.shipment_delay_impact
```

Relationship properties:

```text
quantity
shipment_status
delay_days
delay_severity
impact_band
```

Business meaning:

A shipment is moving a product through the supply chain.

---

### 4.6 Shipment from Supplier

```cypher
(:Shipment)-[:FROM_SUPPLIER]->(:Supplier)
```

Condition:

```text
source_type = 'supplier'
```

Source:

```text
gold.shipment_delay_impact
```

Relationship properties:

```text
shipment_type
carrier
delay_days
impact_band
```

---

### 4.7 Shipment from Warehouse

```cypher
(:Shipment)-[:FROM_WAREHOUSE]->(:Warehouse)
```

Condition:

```text
source_type = 'warehouse'
```

Source:

```text
gold.shipment_delay_impact
```

Relationship properties:

```text
shipment_type
carrier
delay_days
impact_band
```

---

### 4.8 Shipment to Warehouse

```cypher
(:Shipment)-[:TO_WAREHOUSE]->(:Warehouse)
```

Condition:

```text
destination_type = 'warehouse'
```

Source:

```text
gold.shipment_delay_impact
```

Relationship properties:

```text
shipment_type
carrier
delay_days
impact_band
```

---

### 4.9 Shipment to Store

```cypher
(:Shipment)-[:TO_STORE]->(:Store)
```

Condition:

```text
destination_type = 'store'
```

Source:

```text
gold.shipment_delay_impact
```

Relationship properties:

```text
shipment_type
carrier
delay_days
impact_band
```

---

### 4.10 Customer has demand profile

For the first graph version, we avoid loading every order as a node to keep the graph focused and compact.

Initial relationship:

```cypher
(:Customer)-[:HAS_DEMAND_PROFILE]->(:RiskSignal)
```

Later detailed model:

```cypher
(:Customer)-[:PLACED]->(:Order)
(:Order)-[:CONTAINS]->(:Product)
```

The detailed order graph can be added in a later phase if needed.

---

### 4.11 Entity has health status

```cypher
(:Product)-[:HAS_HEALTH_STATUS]->(:RiskSignal)
(:Supplier)-[:HAS_HEALTH_STATUS]->(:RiskSignal)
(:Store)-[:HAS_HEALTH_STATUS]->(:RiskSignal)
(:Warehouse)-[:HAS_HEALTH_STATUS]->(:RiskSignal)
```

Source:

```text
gold.digital_twin_entity_health
```

Relationship properties:

```text
health_score
risk_band
main_risk_reason
```

Business meaning:

Every major supply chain entity has a digital twin health signal.

---

## 5. Graph Loading Order

Recommended load order:

```text
1. Constraints and indexes
2. Supplier nodes
3. Product nodes
4. Store nodes
5. Warehouse nodes
6. Customer nodes
7. Shipment nodes
8. SUPPLIES relationships
9. SERVES relationships
10. STOCKS relationships
11. Shipment movement relationships
12. RiskSignal nodes
13. HAS_HEALTH_STATUS relationships
```

Reason:

Nodes should exist before relationships are created.

---

## 6. Neo4j Constraints

Recommended constraints:

```cypher
CREATE CONSTRAINT supplier_id_unique IF NOT EXISTS
FOR (s:Supplier)
REQUIRE s.supplier_id IS UNIQUE;

CREATE CONSTRAINT product_id_unique IF NOT EXISTS
FOR (p:Product)
REQUIRE p.product_id IS UNIQUE;

CREATE CONSTRAINT store_id_unique IF NOT EXISTS
FOR (s:Store)
REQUIRE s.store_id IS UNIQUE;

CREATE CONSTRAINT warehouse_id_unique IF NOT EXISTS
FOR (w:Warehouse)
REQUIRE w.warehouse_id IS UNIQUE;

CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer)
REQUIRE c.customer_id IS UNIQUE;

CREATE CONSTRAINT shipment_id_unique IF NOT EXISTS
FOR (sh:Shipment)
REQUIRE sh.shipment_id IS UNIQUE;

CREATE CONSTRAINT risk_signal_id_unique IF NOT EXISTS
FOR (r:RiskSignal)
REQUIRE r.risk_signal_id IS UNIQUE;
```

---

## 7. Example Business Questions

The graph should answer:

```text
Which high-demand products depend on high-risk suppliers?

Which stores need urgent replenishment and which warehouse serves them?

Which delayed shipments impact high-demand products?

Which supplier delays are affecting which stores?

Which products are connected to both stockout risk and shipment delay?

Which warehouses serve the most at-risk stores?

Which supply chain entities have the worst digital twin health scores?
```

---

## 8. Example Cypher Queries

### 8.1 High-risk suppliers and impacted products

```cypher
MATCH (s:Supplier)-[r:SUPPLIES]->(p:Product)
WHERE s.risk_band = 'high' OR r.dependency_risk_band IN ['high', 'critical']
RETURN
  s.supplier_id,
  s.supplier_name,
  s.risk_band,
  p.product_id,
  p.product_name,
  p.category,
  r.dependency_risk_band,
  r.total_revenue
ORDER BY r.total_revenue DESC
LIMIT 20;
```

---

### 8.2 Stores needing urgent replenishment

```cypher
MATCH (w:Warehouse)-[:SERVES]->(st:Store)-[stock:STOCKS]->(p:Product)
WHERE stock.replenishment_priority = 'urgent'
RETURN
  w.warehouse_name,
  st.store_name,
  p.product_name,
  stock.available_stock,
  stock.reorder_point,
  stock.stockout_risk_band,
  stock.warehouse_action
ORDER BY stock.reorder_point DESC
LIMIT 20;
```

---

### 8.3 Delayed shipments impacting stores

```cypher
MATCH (sh:Shipment)-[:MOVES]->(p:Product)
MATCH (sh)-[:TO_STORE]->(st:Store)
WHERE sh.impact_band IN ['high', 'critical']
RETURN
  sh.shipment_id,
  sh.carrier,
  sh.delay_days,
  sh.impact_band,
  p.product_name,
  st.store_name
ORDER BY sh.delay_days DESC
LIMIT 20;
```

---

### 8.4 Entity health overview

```cypher
MATCH (r:RiskSignal)
WHERE r.risk_type = 'entity_health_risk'
RETURN
  r.entity_type,
  r.risk_band,
  count(*) AS entity_count
ORDER BY r.entity_type, r.risk_band;
```

---

## 9. GraphRAG Usage

The graph will support GraphRAG by providing:

```text
Entity relationships
Supply chain dependency paths
Risk propagation paths
Structured facts for LLM grounding
```

Example GraphRAG question:

```text
Why is product PROD123 at risk, and which suppliers, shipments, warehouses, and stores are connected to that risk?
```

Expected retrieval path:

```text
Product
  <- SUPPLIES - Supplier
  <- MOVES - Shipment
  <- STOCKS - Store/Warehouse
  <- HAS_HEALTH_STATUS - RiskSignal
```

---

## 10. First Implementation Scope

Initial graph load should include:

```text
Supplier
Product
Store
Warehouse
Shipment
RiskSignal

SUPPLIES
SERVES
STOCKS
MOVES
FROM_SUPPLIER
FROM_WAREHOUSE
TO_WAREHOUSE
TO_STORE
HAS_HEALTH_STATUS
```

Customer and detailed Order graph can be added later after the first graph layer is validated.

---

## 11. Production Notes

- Neo4j credentials must be stored in Azure Key Vault.
- Databricks must access Neo4j credentials through a Key Vault-backed secret scope.
- Graph loading must be idempotent using `MERGE`.
- Use constraints before loading data.
- Do not delete and reload the whole graph unless explicitly doing a full rebuild.
- Graph load jobs should write validation metrics back to Databricks monitoring tables.
- Deployment should be managed through Databricks Asset Bundles.
- Service principals should use least-privilege access.
- The graph loader should read only from curated Gold views unless there is a documented reason to use Silver.
