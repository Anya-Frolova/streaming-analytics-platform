# Architecture Overview

## Data Flow
## Layers

### Bronze Layer (Raw)
- `bronze.watch_events` — raw streaming watch events from Kafka
- `bronze.ratings_late` — late-arriving user ratings (up to 48h delay)
- `bronze.users` — user master data (batch)
- `bronze.content_catalog` — content catalog (batch)

### Silver Layer (Cleaned)
- `silver.dim_user` — SCD Type 2 user dimension
- `silver.dim_content` — content dimension
- `silver.dim_time` — time dimension
- `silver.dim_device` — device dimension
- `silver.watch_sessions` — cleaned watch events with late-arrival flag
- `silver.ratings` — validated ratings with 48h window flag

### Gold Layer (Aggregated)
- `gold.fact_watch_sessions` — fact table (star schema)
- `gold.daily_engagement` — daily KPI dashboard table (per date)
- `gold.churn_features` — ML feature table (per user per day)

## Data Quality Checks
- Null checks on all primary keys
- Duplicate checks on user_id and content_id
- Late-arriving data: flagged if event_time is 5min-48h before ingestion_time
- Schema validation on all bronze tables

## Components
- **MinIO**: S3-compatible storage at localhost:9001
- **Iceberg REST**: Table catalog at localhost:8181
- **Spark Master**: Processing at localhost:8080
- **Kafka**: Streaming at localhost:9092
- **Kafka UI**: Monitor at localhost:8090
- **Airflow**: Orchestration at localhost:8085
