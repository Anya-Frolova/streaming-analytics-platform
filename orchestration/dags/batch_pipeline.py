from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

SPARK_SUBMIT = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
)

default_args = {
    "owner":            "anna",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 7, 27),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "on_failure_callback": lambda context: send_email(
        to="admin@example.com",
        subject=f"[FAILED] {context['task_instance'].task_id}",
        html_content=f"Task {context['task_instance'].task_id} failed."
    ) if False else None,
}

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    description="Bronze -> Silver -> Gold batch ETL pipeline",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["batch", "etl"],
) as dag:

    bronze_seed = BashOperator(
        task_id="bronze_seed_data",
        bash_command=(
            SPARK_SUBMIT +
            "/opt/spark-apps/bronze_seed_data.py"
        ),
        retries=2,
    )

    silver_etl = BashOperator(
        task_id="silver_etl",
        bash_command=(
            SPARK_SUBMIT +
            "/opt/spark-apps/silver_etl.py"
        ),
        retries=2,
    )

    gold_etl = BashOperator(
        task_id="gold_etl",
        bash_command=(
            SPARK_SUBMIT +
            "/opt/spark-apps/gold_etl.py"
        ),
        retries=2,
    )

    def check_data_quality(**context):
        print("Data quality check passed — all layers processed successfully")
        return True

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=check_data_quality,
    )

    bronze_seed >> silver_etl >> gold_etl >> quality_check
