# Telemetry Data Pipeline

This project simulates a modern end-to-end data platform that processes telemetry-style events through a multi-layer architecture.

The platform creates synthetic application events, ingests them via a FastAPI service, processes them through a medallion architecture using PySpark and Parquet, and transforms them with dbt, all using orchestrated workflows with Airflow.

This is not intended to be production-grade infrastructure. Instead, the project is intentionally optimized for:

1. Demonstrating conceptual fluency with modern DE tooling
2. Understanding how modern pipeline components fit together
3. Demonstrating initiative and continuous learning

---

# Goals

This project was intentionally designed to provide hands-on exposure to:

- PySpark
- Parquet
- Medallion architecture
- Apache Airflow
- dbt
- Incremental ETL
- Schema evolution
- Late-arriving data
- Event-driven analytics
- Dimensional modeling
- SCD Type 2 dimensions
- FastAPI (ingestion layer)
- Batch + streaming concepts

---

# Architecture

```text
Synthetic Event Generator
        ↓
Ingestion API Layer (FastAPI)
        ↓
Raw JSON Files
        ↓
Bronze Layer (Parquet)
        ↓
PySpark Transformations
        ↓
Silver Layer (Parquet)
        ↓
DuckDB Warehouse
        ↓
dbt Gold Models
        ↓
Analytics Tables
```  

---

# Repository Structure

```text
project/
│
├── data/
│   ├── _state/
│   ├── raw/
│       ├── events/
│       └── logs/
│   ├── bronze/
│   └── silver/
│
├── state/
│   └── scd_watermark.txt
│
├── generator/
│   └── generate_events.py
│
├── spark_jobs/
│   ├── bronze_load.py
│   ├── bronze_to_silver.py
│   └── update_dim_user_scd.py
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── marts/
│       └── source.yml
│   ├── tests/
│   └── dbt_project.yml
│
├── airflow/
│   └── dags/
│       └── telemetry_pipeline.py
│
├── api/
│   ├── main.py
│   └── routes/
│
├── scripts/
│   └── inspect_duckdb.py
│
├── requirements.txt
└── README.md
```

---

# Data Model

## Event Types

### Page View
```json
{
  "event_id": "1",
  "event_type": "page_view",
  "event_ts": "2026-05-18T10:00:00",
  "user_id": 101,
  "country": "US",
  "page": "/home"
}
```

### Search
```json
{
  "event_id": "2",
  "event_type": "search",
  "event_ts": "2026-05-18T10:01:00",
  "user_id": 101,
  "country": "US",
  "query": "firefox extensions"
}
```

### Purchase
```json
{
  "event_id": "3",
  "event_type": "purchase",
  "event_ts": "2026-05-18T10:02:00",
  "user_id": 101,
  "country": "US",
  "amount": 29.99
}
```

---

# Data Layers

## Bronze
- Raw ingestion of JSON events to preserve source fidelity
- Minimally transformed to include metadata
- Stored as Parquet
- Partitioned by event_date and event_type

## Silver

Normalized and validated event tables. It processes all events in the Bronze layer from over the past 2 days in case any events arrived last. This is a balance between completeness and minimizing compute.

### Example Silver Tables

```text
silver_page_views
silver_searches
silver_purchases
silver_errors
```

### Silver Responsibilities

- schema normalization
- deduplication
- type enforcement
- timestamp standardization
- invalid record filtering

## Warehouse (DuckDB)
- Analytical store
- Supports SCD Type 2 user dimension
- Fact tables for analytics

## dbt Marts
These tables include quality checks such as uniqueness, nullness, and categorical values. They are currently recomputed in full every time, but we could also implement a materialized='incremental' strategy on them to save compute.

- mart_dau: daily active users
- mart_revenue: revenue aggregation
- mart_funnel: conversion funnel
- mart_event_mix: event distribution
- dim_user_current: latest user snapshot

---

# Slowly Changing Dimension (Users)

- Type 2 SCD implementation
- Tracks historical changes in user attributes (e.g. country)
- Incrementally updated using ingestion watermark

---

# Airflow Pipeline

1. Bronze load (Spark)
2. Silver transform (Spark)
3. Update SCD users
4. dbt run (analytics layer)

---

# Key Design Idea: API as Ingestion Boundary

A key feature of this project is the use of a **FastAPI ingestion layer**.

Instead of writing data directly to storage, all events flow through a controlled ingestion endpoint:

```http
POST /events
```

---

## Why This Matters

This design demonstrates real-world data engineering concepts:

### 1. Controlled ingestion boundary
All data enters the system through a single validated interface.

### 2. Schema flexibility
The endpoint supports multiple event types with varying fields.

### 3. Decoupling ingestion from processing
The API is not responsible for transformations—only durable capture.

### 4. Real-world ingestion patterns
This mirrors how event tracking systems, IoT systems, and product analytics pipelines work.

---

## What the ingestion layer does NOT do

- No aggregations
- No transformations
- No analytics logic
- No dbt-style modeling

It simply:
> accepts events → persists raw data → hands off to downstream pipeline

---

# Synthetic Event Generation

The project generates realistic telemetry-style JSON events.
Different event types intentionally use different schemas to simulate realistic telemetry systems.

---

# Late-Arriving Data

The pipeline intentionally introduces delayed events.

Example:

```text
Event timestamp: 10:00
Arrival timestamp: 10:15
```

The system handles this using:

- sliding lookback windows
- reprocessing recent partitions
- event-level deduplication

Example strategy:

```python
reprocess last 1 day of data
```

Deduplication occurs on:

```text
event_id
```


---

# Schema Evolution

The project simulates evolving telemetry schemas.

Examples:

- new event attributes
- optional fields
- missing fields
- changing payload structures

This demonstrates handling semi-structured data evolution using Spark and Parquet.

---

# Key Features

- Incremental processing with watermarks
- FastAPI ingestion layer
- Deduplication across layers
- Partitioned Parquet storage
- Separation of compute and analytics
- DuckDB warehouse for lightweight analytics
- Airflow orchestration

---

# Setup Instructions

## Install dependencies

```bash
pip install -r requirements.txt
```

---

## Run FastAPI

```bash
uvicorn api.main:app --reload
```

API:
http://127.0.0.1:8000

---

## Generate synthetic events

```bash
python generator/generate_events.py
```

Output:
`data/raw/events/`

---

## Run Spark pipeline manually

### Bronze layer
```bash
python spark_jobs/bronze_load.py
```

### Silver layer
```bash
python spark_jobs/bronze_to_silver.py
```

### SCD user updates
```bash
python spark_jobs/update_dim_user_scd.py
```

---

## Run dbt

```bash
cd dbt
dbt run
dbt test
```

---

## Run Airflow

```bash
airflow standalone
```

Open UI:
http://localhost:8080

Trigger DAG:
telemetry_pipeline

---

# Future Improvements

- While the Airflow script was working earlier in development, there is currently a bug
- Streaming ingestion (Kafka)
- Schema registry
- Dockerized deployment
- Cloud storage integration (S3)
- Observability and lineage tracking

---

# Cloud Mapping (AWS Reference Architecture)

Although this project is designed to run locally for learning purposes, it closely mirrors a production-style AWS data platform. This section maps each component of the system to equivalent AWS services to help translate the architecture into real-world cloud environments.

---

## Ingestion Layer (FastAPI)

**Local Component:**
- FastAPI `POST /events` ingestion endpoint

**AWS Equivalent:**
- Amazon API Gateway (request handling / routing)
- AWS Lambda (lightweight ingestion processing, optional)
- AWS App Runner or ECS (if running containerized ingestion service)

**Purpose in AWS:**
Acts as the controlled entry point for telemetry/events into the data platform, enforcing schema validation and request structure before data lands in storage.

---

## Raw Event Storage

**Local Component:**
- JSON files in `/data/raw`

**AWS Equivalent:**
- Amazon S3 (raw data lake storage)

**Purpose in AWS:**
Immutable landing zone for all incoming events.

Typical pattern:
- `s3://bucket/raw/events/year=2026/month=05/day=18/`

This corresponds directly to the **bronze layer input zone**.

---

## Bronze Layer (Raw Data Lake)

**Local Component:**
- Parquet files stored on filesystem

**AWS Equivalent:**
- Amazon S3 (optimized Parquet lake storage)
- AWS Glue Data Catalog (optional metadata layer)

**Purpose in AWS:**
- Clean storage format (Parquet)
- Partitioned, replayable dataset
- Source of truth for downstream processing

---

## PySpark Processing Layer (Bronze → Silver)

**Local Component:**
- PySpark jobs executed locally

**AWS Equivalent:**
- AWS Glue ETL Jobs (primary equivalent)
- Amazon EMR (Spark clusters)
- AWS Glue Studio (visual ETL option)

**Purpose in AWS:**
- schema normalization
- deduplication
- handling late-arriving data
- event-type separation

This is the core distributed compute layer of the system.

---
