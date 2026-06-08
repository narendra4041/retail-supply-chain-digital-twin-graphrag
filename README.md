# Retail Supply Chain Digital Twin with GraphRAG

## Overview

This project builds an enterprise-grade Retail Supply Chain Digital Twin using Azure Databricks, Delta Lake, Unity Catalog, Neo4j, and GraphRAG.

The platform ingests retail, inventory, supplier, shipment, customer, and order data into a governed lakehouse. It then creates a supply-chain knowledge graph in Neo4j and uses AI agents to answer business questions about inventory risk, supplier delays, stockouts, demand forecasting, and operational impact.

## Business Problem

Retail supply chains are complex and highly connected. A delay from one supplier can impact products, warehouses, stores, customers, and revenue. Traditional data warehouses can show historical reports, but they cannot easily explain connected impact across the full supply chain.

This project solves that problem by combining:

- Lakehouse data engineering
- Knowledge graph modeling
- Machine learning
- GraphRAG
- Enterprise AI agents

## Core Capabilities

- Batch and streaming ingestion
- Bronze, Silver, Gold Delta Lake architecture
- Retail supply chain data model
- Supplier, product, warehouse, store, and customer graph
- Demand forecasting
- Stockout risk prediction
- Supplier risk scoring
- Inventory movement recommendations
- GraphRAG AI agent
- Dashboard-ready Gold tables
- Production-grade CI/CD and governance design

## Technology Stack

- Azure Databricks
- Delta Lake
- ADLS Gen2
- Unity Catalog
- Neo4j
- Azure OpenAI
- MLflow
- PySpark
- Python
- Power BI
- Terraform
- GitHub Actions
- Databricks Asset Bundles

## Main Use Cases

1. Inventory Intelligence
2. Supplier Risk Analysis
3. Demand Forecasting
4. Shipment Delay Impact Analysis
5. Fraud and Return Anomaly Detection
6. Executive Supply Chain Assistant