# Component Descriptions

- **MinIO** - S3 compatible object storage backing the Iceberg warehouse (bronze/silver/gold buckets). Console at `localhost:9001`, API at `localhost:9000`.
- **Iceberg REST catalog** - Table catalog service (`tabulario/iceberg-rest`) that Spark uses to resolve and manage Iceberg tables stored in MinIO. Runs at `localhost:8181`.
- **Spark Master** - Cluster manager and entry point for `spark-submit` jobs (streaming and batch ETL scripts). UI at `localhost:8080`.
- **Spark Worker** - Executes the tasks assigned by Spark Master; configured with 2 cores / 2GB memory.
- **Bronze Seed Init (`bronze-seed-init`)** - One-shot container that runs `bronze_seed_data.py` (idempotent) to seed `bronze.users` and `bronze.content_catalog` before anything else starts. `spark-streaming` waits for it to complete successfully.
- **Spark Streaming (`spark-streaming`)** - Dedicated container that runs `bronze_streaming.py` continuously via `spark-submit`; consumes both Kafka topics and writes to `bronze.watch_events` / `bronze.ratings_late`. Restarts automatically on failure.
- **Kafka** - Message broker that receives `watch-events` and `ratings-late` from the producer and feeds `bronze_streaming.py`. Broker at `localhost:9092` (host) / `kafka:29092` (internal).
- **Zookeeper** - Coordination service required by this Kafka broker version (`confluentinc/cp-kafka:7.6.0`).
- **Kafka UI** - Web UI for browsing Kafka topics and messages. Runs at `localhost:8090`.
- **watch-producer** - Python service that continuously generates simulated watch events and late arriving ratings and publishes them to Kafka.
- **Airflow webserver** - Web UI for viewing and triggering DAG runs. Runs at `localhost:8085`.
- **Airflow scheduler** - Schedules and triggers the `batch_pipeline` and `streaming_pipeline` DAGs according to their cron schedules.
- **Airflow postgres** - PostgreSQL database backing Airflow's metadata store (DAG runs, task state, connections).
