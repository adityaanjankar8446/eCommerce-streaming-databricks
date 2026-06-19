# eCommerce Streaming Pipeline on Databricks

An end-to-end e-commerce data pipeline built on **Databricks**, using **Auto Loader-style incremental ingestion**, **Delta Lake**, **Unity Catalog**, and **Databricks Jobs & Pipelines** to implement a Bronze → Silver → Gold medallion architecture, complete with materialized views and reporting views.

## Architecture

```
External API ──> Python script (local) ──> JSON files ──> manual upload ──> Databricks Volume
                                                                                    │
                                                                                    ▼
                                                                  ┌──────────────────────────────┐
                                                                  │  01_bronze_ingestion         │
                                                                  │  (Structured Streaming,      │
                                                                  │   trigger=AvailableNow)      │
                                                                  └──────────────────────────────┘
                                                                                    │
                                                                                    ▼
                                                                  ┌──────────────────────────────┐
                                                                  │  02_silver_transformations   │
                                                                  │  (Structured Streaming,      │
                                                                  │   trigger=AvailableNow)      │
                                                                  └──────────────────────────────┘
                                                                                    │
                                                                                    ▼
                                                                  ┌──────────────────────────────┐
                                                                  │  03_gold_aggregations        │
                                                                  └──────────────────────────────┘
                                                                                    │
                                                                                    ▼
                                                                  ┌──────────────────────────────┐
                                                                  │  04_materialized_views       │
                                                                  │  05_views                    │
                                                                  └──────────────────────────────┘

                Orchestrated end-to-end by a Databricks Job: Ecommerce_Streaming_Job
```

### Medallion layers

| Layer | Tables | Purpose |
|-------|--------|---------|
| **Bronze** | `bronze_customers`, `bronze_orders`, `bronze_products` | Raw JSON landed from the Volume as Delta tables, minimal transformation |
| **Silver** | `silver_customers`, `silver_orders`, `silver_products` | Parsed, typed, and flattened records per entity |
| **Gold** | `gold_category_revenue`, `gold_customer_revenue`, `gold_popular_products` | Business-level aggregations for analytics and reporting |
| **Materialized Views** | `mv_customer_360` | Pre-computed, refreshable cross-entity view (customer 360) |
| **Views** | `vw_order_summary` | Lightweight reporting view on top of curated tables |

## Tech Stack

- **Databricks** (Free Edition / Serverless compute)
- **Delta Lake** — ACID storage format for all bronze/silver/gold tables
- **Unity Catalog** — governance layer (`streaming_project.ecommerce` catalog/schema)
- **Databricks Volumes** — landing zone for raw JSON files (`stream_volume`)
- **Spark Structured Streaming** — incremental processing for Bronze and Silver layers, using `trigger(availableNow=True)` for batch-style streaming runs
- **Databricks Jobs & Pipelines** — orchestration of the notebook chain
- **Python** (`requests`) — local data generation script
- **SQL / PySpark** — transformations, aggregations, materialized views, and views

## Data Flow

1. **Data generation** — A standalone Python script (run locally, outside Databricks) calls an external API and writes the resulting JSON payloads to local files, organized by entity (orders, customers, products).
2. **Manual upload to Volume** — The generated JSON files are manually uploaded into the Databricks Unity Catalog Volume (`stream_volume`), which acts as the landing zone that downstream ingestion reads from.
3. **Bronze ingestion** (`01_bronze_ingestion.ipynb`) — Reads the JSON files from the Volume path using Spark, and writes them as Delta tables (`bronze_customers`, `bronze_orders`, `bronze_products`) using Structured Streaming with `trigger(availableNow=True)`, so each run processes whatever new files have landed since the last run and then stops.
4. **Silver transformations** (`02_silver_transformations.ipynb`) — Reads each Bronze Delta table as a stream, parses and flattens the raw JSON into typed columns (e.g. flattening nested address/geolocation fields, exploding order line items), and writes to Silver Delta tables, also using `trigger(availableNow=True)`.
5. **Gold aggregations** (`03_gold_aggregations.ipynb`) — Reads Silver tables and computes business aggregates:
   - **`gold_category_revenue`** — revenue rolled up by product category
   - **`gold_customer_revenue`** — total spend and order count per customer
   - **`gold_popular_products`** — best-selling products by quantity/revenue
6. **Materialized views** (`04_materialized_views.ipynb`) — Builds `mv_customer_360`, a pre-computed cross-entity view combining customer, order, and product data for fast downstream querying.
7. **Views** (`05_views.ipynb`) — Creates `vw_order_summary`, a lightweight SQL view for reporting on top of the curated Silver/Gold tables.
8. **Orchestration** — A Databricks Job (`Ecommerce_Streaming_Job`) chains all four stages as serverless tasks in sequence:

   ```
   Streaming_data_Ingestion_at_Bronze → Data_Transformations_at_Silver → Aggregations_at_Gold → View_order_summary
   ```

## Unity Catalog Structure

```
streaming_project (catalog)
└── ecommerce (schema)
    ├── Tables
    │   ├── bronze_customers
    │   ├── bronze_orders
    │   ├── bronze_products
    │   ├── silver_customers
    │   ├── silver_orders
    │   ├── silver_products
    │   ├── gold_category_revenue
    │   ├── gold_customer_revenue
    │   ├── gold_popular_products
    │   ├── mv_customer_360        (materialized view)
    │   └── vw_order_summary       (view)
    └── Volumes
        └── stream_volume          (landing zone for raw JSON)
```

## Project Structure

```
Streaming Project - Databricks/
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformations.ipynb
│   ├── 03_gold_aggregations.ipynb
│   ├── 04_materialized_views.ipynb
│   └── 05_views.ipynb
├── data/
│   └── landing/              # Locally generated JSON files before upload (gitignored)
├── scripts/
│   └── api_data_generator.py # Pulls data from API, writes JSON locally
├── venv/                     # Python virtual environment (gitignored)
└── README.md
```

## Prerequisites

- A Databricks workspace (Free Edition or paid tier) with Unity Catalog enabled
- A Unity Catalog Volume created for landing raw JSON files
- Python 3.8+ locally (for running the data generation script)
- `requests` library (`pip install requests`)

## Setup & Running

1. **Clone the repository**
   ```bash
   git clone https://github.com/<your-username>/eCommerce-streaming-databricks.git
   cd eCommerce-streaming-databricks
   ```

2. **Generate data locally**
   Run the data generation script to pull fresh records from the API and write them as JSON files:
   ```bash
   python scripts/api_data_generator.py
   ```

3. **Upload JSON files to the Databricks Volume**
   In the Databricks workspace, navigate to **Catalog → streaming_project → ecommerce → Volumes → stream_volume**, and manually upload the generated JSON files (or use the Databricks CLI: `databricks fs cp <local_path> dbfs:/Volumes/streaming_project/ecommerce/stream_volume/`).

4. **Import the notebooks**
   Import the five notebooks from `notebooks/` into your Databricks Workspace.

5. **Run the Job**
   Trigger the `Ecommerce_Streaming_Job` (or run notebooks individually in order: Bronze → Silver → Gold → Views) from **Jobs & Pipelines**.

6. **Verify output**
   Check the resulting tables under **Catalog → streaming_project → ecommerce → Tables**, and query `vw_order_summary` or `mv_customer_360` directly via the SQL Editor.

## Databricks Job Overview

Job name: `Ecommerce_Streaming_Job`

| Task | Notebook | Description |
|------|----------|-------------|
| `Streaming_data_Ingestion_at_Bronze` | `01_bronze_ingestion` | Reads JSON from the Volume, writes Bronze Delta tables via Structured Streaming (`availableNow` trigger) |
| `Data_Transformations_at_Silver` | `02_silver_transformations` | Parses, types, and flattens Bronze data into Silver Delta tables via Structured Streaming (`availableNow` trigger) |
| `Aggregations_at_Gold` | `03_gold_aggregations` | Computes Gold-layer business aggregations |
| `View_order_summary` | `05_views` | Creates/refreshes the `vw_order_summary` reporting view |

Tasks run sequentially on **Serverless** compute, each depending on the successful completion of the previous task.

## Notes

- Ingestion is intentionally manual at the landing stage (local script → manual upload to Volume) to simulate an external system dropping files; in a production setup this step would be automated (e.g. via Auto Loader watching a cloud storage path directly, or a proper file-drop integration).
- Bronze and Silver use `trigger(availableNow=True)` rather than continuous streaming, so each Job run processes all new files since the last run and then completes — well suited to scheduled/triggered orchestration via Databricks Jobs rather than an always-on cluster.
- `mv_customer_360` and `vw_order_summary` sit on top of the curated Silver/Gold layers, so they should be refreshed/created only after the upstream tables are populated — this ordering is enforced by the Job's task dependencies.

## License

This project is provided as-is for educational and portfolio purposes.
