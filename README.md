# Streaming Analytics Platform

Netflix-style streaming analytics platform.

## Stack
- Apache Iceberg + MinIO (Bronze / Silver / Gold)
- Apache Spark (batch + streaming)
- Apache Kafka (real-time events)
- Apache Airflow (orchestration)

## Quick Start

### 1. Create shared network (once)
```bash
docker network create data_platform_network
```

### 2. Start Processing (MinIO + Iceberg + Spark)
```bash
cd processing && docker compose up -d
```

### 3. Start Streaming (Kafka)
```bash
cd streaming && docker compose up -d
```

### 4. Start Orchestration (Airflow)
```bash
cd orchestration && docker compose up -d
```

## URLs
- MinIO UI:   http://localhost:9001  (minioadmin / minioadmin123)
- Spark UI:   http://localhost:8080
- Kafka UI:   http://localhost:8090
- Airflow UI: http://localhost:8085  (admin / admin)
