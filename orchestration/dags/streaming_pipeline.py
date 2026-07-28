from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SPARK = "docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077"

default_args = {
    "owner":           "anna",
    "depends_on_past": False,
    "start_date":      datetime(2026, 7, 27),
    "retries":         3,
    "retry_delay":     timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="streaming_pipeline",
    default_args=default_args,
    description="Streaming monitor: check Kafka producer + refresh Silver/Gold",
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["streaming", "kafka"],
) as dag:

    def check_kafka(**context):
        import subprocess
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.State.Status}}", "watch-producer"],
            capture_output=True, text=True
        )
        status = result.stdout.strip()
        print(f"Kafka producer status: {status}")
        if status != "running":
            raise Exception(f"watch-producer container is {status}!")
        print("Kafka producer is running ✅")
        return status

    check_producer = PythonOperator(
        task_id="check_kafka_producer",
        python_callable=check_kafka,
        provide_context=True,
        retries=3,
    )

    refresh_silver = BashOperator(
        task_id="refresh_silver_layer",
        bash_command=f"{SPARK} /opt/spark-apps/silver_etl.py",
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    refresh_gold = BashOperator(
        task_id="refresh_gold_layer",
        bash_command=f"{SPARK} /opt/spark-apps/gold_etl.py",
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    def log_run(**context):
        print(f"Streaming pipeline run completed")
        print(f"Run ID: {context['run_id']}")
        return "SUCCESS"

    log_done = PythonOperator(
        task_id="log_completion",
        python_callable=log_run,
        provide_context=True,
    )

    check_producer >> refresh_silver >> refresh_gold >> log_done
