# Production Ingestion Design

## Overview

This project uses a production-style ingestion architecture for a Retail Supply Chain Digital Twin.

The ingestion design separates master/reference data from transactional event data.

## Data Categories

### Batch Master Data

Master data changes slowly and is loaded from files into ADLS Gen2.

Datasets:

- suppliers
- products
- warehouses
- stores
- customers

Target landing path:

```text
abfss://landing@<storage-account>.dfs.core.windows.net/retail/master/