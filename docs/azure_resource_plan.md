# Azure Resource Plan

## Overview

This document defines the Azure resources required for the Retail Supply Chain Digital Twin with GraphRAG project.

The goal is to create a production-style Azure foundation for:

- Batch master data ingestion
- Streaming transactional event ingestion
- Azure Databricks lakehouse processing
- Bronze, Silver, and Gold Delta layers
- Event Hub based streaming
- Secure secret management
- Future Neo4j and GraphRAG integration

---

## Environment

Initial environment:

```text
dev
```

Future environments:

```text
test
prod
```

---

## Naming Convention

Use this pattern:

```text
<resource-type>-<project-name>-<environment>
```

Examples:

```text
rg-retail-digital-twin-dev
ehns-retail-digital-twin-dev
dbw-retail-digital-twin-dev
kv-retail-digital-twin-dev
```

For globally unique resources such as storage accounts, use a short suffix.

Example:

```text
stretaildtdev001
```

---

## Azure Region

Recommended region:

```text
swedencentral
```

Alternative regions:

```text
westeurope
northeurope
```

Use one region consistently for the initial dev environment.

---

## 1. Resource Group

### Purpose

The resource group will contain all Azure resources for the dev environment.

### Name

```text
rg-retail-digital-twin-dev
```

### Resources inside this group

```text
ADLS Gen2 storage account
Azure Event Hubs namespace
Azure Databricks workspace
Azure Key Vault
Log Analytics workspace
Application Insights
Managed identities
Private endpoints later
```

---

## 2. ADLS Gen2 Storage Account

### Purpose

ADLS Gen2 is the main storage layer for landing data, Delta tables, checkpoints, logs, and configuration files.

### Name

```text
stretaildtdev001
```

Storage account names must be globally unique and use only lowercase letters and numbers.

### Required settings

```text
Performance: Standard
Replication: LRS for dev
Hierarchical namespace: Enabled
Access tier: Hot
Minimum TLS version: TLS 1.2
Public access: Disabled where possible
```

### Containers

Create these containers:

```text
landing
bronze
silver
gold
checkpoints
configs
logs
archive
quarantine
```

### Container purpose

| Container | Purpose |
|---|---|
| landing | Raw batch files and raw event snapshots |
| bronze | Raw Delta tables |
| silver | Cleaned and validated Delta tables |
| gold | Business-ready analytics tables |
| checkpoints | Structured Streaming checkpoints |
| configs | Pipeline configuration files |
| logs | Pipeline and producer logs |
| archive | Archived processed files |
| quarantine | Bad records and rejected files |

---

## 3. Landing Zone Folder Structure

### Batch master data

```text
landing/retail/master/suppliers/
landing/retail/master/products/
landing/retail/master/warehouses/
landing/retail/master/stores/
landing/retail/master/customers/
```

### Event landing snapshot folders

Even though Event Hub is the main streaming source, we may also store raw event snapshots for replay and debugging.

```text
landing/retail/events/orders/
landing/retail/events/inventory/
landing/retail/events/shipments/
landing/retail/events/supplier_performance/
```

### Quarantine folders

```text
quarantine/retail/master/
quarantine/retail/events/
```

---

## 4. Azure Event Hubs Namespace

### Purpose

Azure Event Hubs will simulate production-style event streaming for transactional retail events.

### Namespace name

```text
ehns-retail-digital-twin-dev
```

### Pricing tier

For development:

```text
Standard
```

For production later:

```text
Premium or Dedicated
```

---

## 5. Event Hubs

Create separate Event Hubs by domain.

```text
retail-orders-events
retail-inventory-events
retail-shipments-events
retail-supplier-performance-events
```

### Event Hub purpose

| Event Hub | Event type |
|---|---|
| retail-orders-events | order_created |
| retail-inventory-events | inventory_updated |
| retail-shipments-events | shipment_created |
| retail-supplier-performance-events | supplier_performance |

### Recommended dev settings

```text
Partition count: 2
Message retention: 1 day
Capture: Optional for dev
```

### Recommended production settings later

```text
Partition count: Based on throughput
Message retention: 3 to 7 days
Capture: Enabled
Private endpoint: Enabled
Managed identity/RBAC: Preferred
```

---

## 6. Event Hub Consumer Groups

Create consumer groups for Databricks.

```text
cg-databricks-orders
cg-databricks-inventory
cg-databricks-shipments
cg-databricks-supplier-performance
```

Optional future consumer groups:

```text
cg-monitoring
cg-replay
cg-debug
```

---

## 7. Azure Databricks Workspace

### Purpose

Azure Databricks will be used for:

- Batch ingestion
- Streaming ingestion
- Bronze, Silver, Gold Delta processing
- Data quality checks
- Databricks Workflows
- MLflow
- Feature engineering
- Future GraphRAG and AI pipelines

### Workspace name

```text
dbw-retail-digital-twin-dev
```

### Pricing tier

```text
Premium
```

Premium is recommended because we will later use Unity Catalog, governance, and enterprise features.

---

## 8. Unity Catalog Design

### Metastore

Use an existing Unity Catalog metastore if available.

If not available, create one for the region.

### Catalogs

For dev:

```text
retail_digital_twin_dev
```

Future:

```text
retail_digital_twin_test
retail_digital_twin_prod
```

### Schemas

Inside the dev catalog:

```text
bronze
silver
gold
monitoring
ml
graph
```

### Example table names

```text
retail_digital_twin_dev.bronze.orders_events_raw
retail_digital_twin_dev.bronze.inventory_events_raw
retail_digital_twin_dev.bronze.shipment_events_raw
retail_digital_twin_dev.bronze.supplier_performance_events_raw

retail_digital_twin_dev.silver.orders
retail_digital_twin_dev.silver.order_items
retail_digital_twin_dev.silver.inventory_movements
retail_digital_twin_dev.silver.shipments
retail_digital_twin_dev.silver.supplier_performance

retail_digital_twin_dev.gold.inventory_position
retail_digital_twin_dev.gold.stockout_risk
retail_digital_twin_dev.gold.supplier_risk_score
retail_digital_twin_dev.gold.shipment_delay_impact
```

---

## 9. External Locations

Create Unity Catalog external locations for storage access.

Recommended external locations:

```text
extloc_landing_retail_dt_dev
extloc_bronze_retail_dt_dev
extloc_silver_retail_dt_dev
extloc_gold_retail_dt_dev
extloc_checkpoints_retail_dt_dev
```

Each external location maps to an ADLS path.

Example:

```text
abfss://landing@stretaildtdev001.dfs.core.windows.net/
abfss://bronze@stretaildtdev001.dfs.core.windows.net/
abfss://silver@stretaildtdev001.dfs.core.windows.net/
abfss://gold@stretaildtdev001.dfs.core.windows.net/
abfss://checkpoints@stretaildtdev001.dfs.core.windows.net/
```

---