# Data Model

## Star Schema

```mermaid
erDiagram
    fact_watch_sessions {
        STRING session_id PK
        BIGINT user_key FK
        STRING content_id FK
        DATE event_date FK
        STRING device_type FK
        STRING event_type
        INT watch_duration_seconds
        DOUBLE completion_percent
        BOOLEAN is_late_arrival
        TIMESTAMP ingestion_time
    }
    dim_user {
        BIGINT user_key PK
        STRING user_id
        STRING age_band
        STRING subscription_tier
        STRING country
        DATE effective_from
        DATE effective_to
        BOOLEAN is_current
    }
    dim_content {
        STRING content_id PK
        STRING title
        STRING genre
        INT duration_minutes
    }
    dim_time {
        DATE date_key PK
        INT year
        INT quarter
        INT month
        INT day_of_week
    }
    dim_device {
        STRING device_type PK
        STRING device_category
    }

    fact_watch_sessions }o--|| dim_user : "user_key"
    fact_watch_sessions }o--|| dim_content : "content_id"
    fact_watch_sessions }o--|| dim_time : "event_date"
    fact_watch_sessions }o--|| dim_device : "device_type"
```

## Gold Tables

### daily_engagement (per date)
Dashboard table for executives — aggregated by date, never cumulative.

### churn_features (per user per day)
ML feature table with high granularity: age_band, subscription_tier,
watch patterns, device preferences, late arrival counts.

## SCD Type 2
`dim_user` implements SCD Type 2:
- `effective_from` / `effective_to` date range
- `is_current = TRUE` for active record
- Historical records kept when subscription_tier or country changes
