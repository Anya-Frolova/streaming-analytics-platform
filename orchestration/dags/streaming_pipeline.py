from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SPARK_SUBMIT = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
)

default_args = {
    "owner":           "anna",
    "depends_on_past": False,
    "start_date":      datetime(2026, 7, 27),
    "retries":         3,
    "retry_delay":     timedelta(minutes=2),
}

with DAG(
    dag_id="streaming_pipeline",
    default_args=default_args,
    description="Kafka streaming monitor and silver/gold refresh",
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["streaming", "kafka"],
) as dag:

    def check_kafka_producer(**context):
        import subprocess
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", "watch-producer"],
            capture_output=True, text=True
        )
        status = result.stdout.strip()
        if status != "running":
            raise Exception(f"watch-producer is {status}, expected running")
        print(f"watch-producer status: {status} ✅")
        return status

    check_producer = PythonOperator(
        task_id="check_kafka_producer",
        python_callable=check_kafka_producer,
    )

    refresh_silver = BashOperator(
        task_id="refresh_silver",
        bash_command=(
            SPARK_SUBMIT +
            "/opt/spark-apps/silver_etl.py"
        ),
        retries=3,
    )

    refresh_gold = BashOperator(
        task_id="refresh_gold",
        bash_command=(
            SPARK_SUBMIT +
            "/opt/spark-apps/gold_etl.py"
        ),
        retries=3,
    )

    def log_completion(**context):
        run_id = context["run_id"]
        print(f"Streaming pipeline completed — run_id: {run_id}")
        return True

    log_done = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
    )

    check_producer >> refresh_silver >> refresh_gold >> log_done
