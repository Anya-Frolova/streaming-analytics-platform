# Streaming Analytics Platform

Netflix-style streaming analytics platform for user engagement and retention analysis.

## Stack
- Storage: Apache Iceberg + MinIO (Bronze / Silver / Gold layers)
- Processing: Apache Spark 3.5 (batch + streaming)
- Streaming: Apache Kafka (real-time events)
- Orchestration: Apache Airflow 2.9

## Quick Start

### Step 1 - Create shared network (run ONCE)
docker network create data_platform_network

### Step 2 - Start Processing (MinIO + Iceberg + Spark)
cd processing
docker compose up -d --build

### Step 3 - Start Streaming (Kafka)
cd ../streaming
docker compose up -d
docker compose logs kafka-init
(wait for: All topics created)

### Step 4 - Start Orchestration (Airflow)
cd ../orchestration
docker compose up -d
(wait ~2 min, then open http://localhost:8085)

### Step 5 - Verify everything works
cd ../processing
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/test_iceberg.py
(expected: SUCCESS: 2 rows in MinIO via Iceberg)

## Service URLs
MinIO UI   - http://localhost:9001 - minioadmin / minioadmin123
Spark UI   - http://localhost:8080
Kafka UI   - http://localhost:8090
Airflow UI - http://localhost:8085 - admin / admin

## Stopping Services
cd processing && docker compose down
cd ../streaming && docker compose down
cd ../orchestration && docker compose down

## Data Layers
Bronze - bronze/ - Raw ingested data
Silver - silver/ - Cleaned and transformed data
Gold   - gold/   - Aggregated data for BI and ML
