# Ecommerce Streaming Pipeline (Databricks)

End-to-end streaming data pipeline built on Databricks using 
Auto Loader, Delta Lake, Unity Catalog, and Databricks Jobs.

## Architecture

```
FakeStore API
      ↓
api_ingest.py (local)
      ↓
Manual upload to Databricks Volume
      ↓
01_bronze_ingestion    (Auto Loader → Delta bronze)
      ↓
02_silver_transformations  (clean/enrich → Delta silver)
      ↓
03_gold_aggregations   (aggregate → Delta gold)
      ↓
04_views               (SQL View on top of gold)
      ↓
Databricks Job         (scheduled pipeline)
```

## Tech Stack

| Tool                      | Purpose                    |
|---                        |---                         |
| Databricks                | Cloud data platform        |
| Auto Loader (cloudFiles)  | Incremental file ingestion |
| Delta Lake                | ACID storage format        |
| Unity Catalog             | Data governance            |
| Databricks Jobs           | Pipeline orchestration     |
| Python/PySpark            | Data processing            |

## Project Structure

```
ecommerce-streaming-databricks/
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_aggregations.py
│   └── 04_views.py
└── scripts/
    └── api_ingest.py
```

## Unity Catalog Structure

```
streaming_project (catalog)
└── ecommerce (schema)
    ├── bronze_orders
    ├── bronze_customers
    ├── bronze_products
    ├── silver_orders
    ├── silver_customers
    ├── silver_products
    ├── gold_customer_revenue
    ├── gold_popular_products
    ├── gold_category_revenue
    └── vw_order_summary (view)
```

## Databricks Volume Path

```
/Volumes/streaming_project/ecommerce/stream_volume/
├── orders/          ← JSON files uploaded here
├── customers/
├── products/
└── checkpoints/     ← Auto Loader checkpoints
    ├── bronze_orders_schema/
    ├── bronze_customers_schema/
    ├── bronze_products_schema/
    ├── bronze_orders/
    ├── bronze_customers/
    ├── bronze_products/
    ├── silver_orders/
    ├── silver_customers/
    └── silver_products/
```

## Setup Steps

### 1. Run API ingestion locally
```bash
pip install requests
python scripts/api_ingest.py
```

### 2. Upload JSON files to Databricks Volume
```
Databricks UI → Catalog → streaming_project → ecommerce
→ stream_volume → orders/customers/products folders
→ Upload generated JSON files
```

### 3. Create Unity Catalog resources
```sql
CREATE CATALOG IF NOT EXISTS streaming_project;
CREATE SCHEMA IF NOT EXISTS streaming_project.ecommerce;
CREATE VOLUME IF NOT EXISTS streaming_project.ecommerce.stream_volume;
```

### 4. Run notebooks in order
```
01_bronze_ingestion       → creates bronze tables
02_silver_transformations → creates silver tables
03_gold_aggregations      → creates gold tables
04_views                  → creates SQL view
```

### 5. Set up Databricks Job
```
Workflows → Create Job → Add 4 tasks in sequence
Schedule: every 1 hour
```

## Databricks Job Pipeline

```
Task 1: bronze_ingestion
    ↓ depends on Task 1
Task 2: silver_transformations
    ↓ depends on Task 2
Task 3: gold_aggregations
    ↓ depends on Task 3
Task 4: views
```

## Key Concepts Used

### Auto Loader
Automatically detects new files in Volume using cloudFiles format.
Tracks processed files via schemaLocation checkpoint.

### Delta Lake
ACID transactions on Parquet files. Enables time travel,
schema evolution, and streaming reads/writes.

### Unity Catalog
Three-level namespace: catalog.schema.table
Provides data governance, lineage, and access control.

### Materialized Views (attempted)
Slow on Serverless 2XS cluster due to compute limitations.
Replaced with regular SQL view for performance.