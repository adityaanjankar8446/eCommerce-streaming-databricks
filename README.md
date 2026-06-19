# eCommerce Streaming Pipeline on Databricks

An end-to-end eCommerce streaming data pipeline built on **Databricks**, leveraging **Auto Loader**, **Delta Lake**, **Unity Catalog**, and **Databricks Jobs** to implement a scalable **Bronze → Silver → Gold Medallion Architecture** for real-time analytics and reporting.

---

## Architecture

```text
FakeStore API
      ↓
api_ingest.py (Local Data Generation)
      ↓
JSON Files
      ↓
Manual Upload to Databricks Volume
      ↓
01_bronze_ingestion
(Auto Loader / Structured Streaming)
      ↓
02_silver_transformations
(Data Cleansing & Enrichment)
      ↓
03_gold_aggregations
(Business Aggregations)
      ↓
04_views
(Reporting Views)
      ↓
Databricks Job
(Automated Orchestration)
```

---

## Medallion Architecture

| Layer  | Tables                                                              | Purpose                                             |
| ------ | ------------------------------------------------------------------- | --------------------------------------------------- |
| Bronze | bronze_orders, bronze_customers, bronze_products                    | Raw data ingestion from JSON files                  |
| Silver | silver_orders, silver_customers, silver_products                    | Data cleansing, transformation, and standardization |
| Gold   | gold_customer_revenue, gold_category_revenue, gold_popular_products | Business-ready analytics and aggregations           |
| Views  | vw_order_summary                                                    | Reporting and dashboard consumption                 |

---

## Technology Stack

| Technology               | Purpose                                 |
| ------------------------ | --------------------------------------- |
| Databricks               | Unified Data & Analytics Platform       |
| Auto Loader (cloudFiles) | Incremental file ingestion              |
| Delta Lake               | ACID-compliant storage layer            |
| Unity Catalog            | Governance, lineage, and access control |
| Databricks Jobs          | Workflow orchestration                  |
| PySpark                  | Distributed data processing             |
| SQL                      | Analytics and reporting views           |
| Python                   | API ingestion and data generation       |

---

## Project Structure

```text
ecommerce-streaming-databricks/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_gold_aggregations.py
│   └── 04_views.py
│
├── scripts/
│   └── api_ingest.py
│
├── data/
│   └── landing/
│
├── requirements.txt
└── README.md
```

---

## Data Flow

### 1. API Data Generation

A Python script retrieves eCommerce data from FakeStore API and generates JSON files for:

* Customers
* Orders
* Products

```bash
pip install requests
python scripts/api_ingest.py
```

---

### 2. Landing Zone

Generated JSON files are uploaded into Databricks Unity Catalog Volume:

```text
/Volumes/streaming_project/ecommerce/stream_volume/
├── orders/
├── customers/
└── products/
```

---

### 3. Bronze Layer

The Bronze notebook uses Auto Loader / Structured Streaming to ingest new files incrementally and store them as Delta tables.

Created tables:

```text
bronze_orders
bronze_customers
bronze_products
```

---

### 4. Silver Layer

Transforms raw Bronze data by:

* Standardizing schema
* Flattening nested JSON structures
* Data quality checks
* Type conversions

Created tables:

```text
silver_orders
silver_customers
silver_products
```

---

### 5. Gold Layer

Creates business-ready aggregated datasets:

```text
gold_customer_revenue
gold_category_revenue
gold_popular_products
```

Business insights include:

* Customer revenue analysis
* Product performance analysis
* Category-wise revenue reporting

---

### 6. Reporting Layer

Creates SQL view:

```text
vw_order_summary
```

Used for:

* Dashboard reporting
* Ad-hoc analysis
* Business intelligence consumption

---

## Unity Catalog Structure

```text
streaming_project (Catalog)
└── ecommerce (Schema)

    Bronze Tables
    ├── bronze_orders
    ├── bronze_customers
    └── bronze_products

    Silver Tables
    ├── silver_orders
    ├── silver_customers
    └── silver_products

    Gold Tables
    ├── gold_customer_revenue
    ├── gold_category_revenue
    └── gold_popular_products

    Views
    └── vw_order_summary

    Volumes
    └── stream_volume
```

---

## Databricks Volume Structure

```text
/Volumes/streaming_project/ecommerce/stream_volume/

├── orders/
├── customers/
├── products/

└── checkpoints/
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

---

## Setup Instructions

### Step 1: Create Unity Catalog Resources

```sql
CREATE CATALOG IF NOT EXISTS streaming_project;

CREATE SCHEMA IF NOT EXISTS streaming_project.ecommerce;

CREATE VOLUME IF NOT EXISTS
streaming_project.ecommerce.stream_volume;
```

### Step 2: Generate Data

```bash
python scripts/api_ingest.py
```

### Step 3: Upload Files

Upload generated JSON files into the corresponding folders within the Databricks Volume.

### Step 4: Execute Notebooks

Run notebooks in the following order:

```text
01_bronze_ingestion
        ↓
02_silver_transformations
        ↓
03_gold_aggregations
        ↓
04_views
```

### Step 5: Configure Databricks Job

Create a Databricks Job with dependent tasks:

```text
bronze_ingestion
        ↓
silver_transformations
        ↓
gold_aggregations
        ↓
views
```

Schedule:

```text
Every 1 Hour
```

---

## Key Concepts Demonstrated

### Auto Loader

* Incremental file ingestion
* Schema inference
* Schema evolution
* Checkpoint management

### Structured Streaming

* Near real-time data processing
* Trigger-based execution
* Incremental pipeline execution

### Delta Lake

* ACID transactions
* Time travel
* Schema enforcement
* Efficient storage management

### Unity Catalog

* Centralized governance
* Data lineage
* Fine-grained access control
* Three-level namespace (Catalog → Schema → Table)

### Databricks Jobs

* Workflow orchestration
* Dependency management
* Scheduled execution
* Production-grade pipeline automation

---

## Business Value

This project demonstrates how modern cloud-based data engineering platforms can ingest, process, transform, and serve streaming eCommerce data through a scalable Medallion Architecture, enabling faster analytics, improved data quality, and business-ready reporting.
