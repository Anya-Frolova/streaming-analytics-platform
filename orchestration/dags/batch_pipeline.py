from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

SPARK = "docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077"

default_args = {
    "owner":           "anna",
    "depends_on_past": False,
    "start_date":      datetime(2026, 7, 27),
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="batch_pipeline",
    default_args=default_args,
    description="Full batch ETL: Bronze -> Silver -> Gold",
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["batch", "etl"],
) as dag:

    bronze_seed = BashOperator(
        task_id="bronze_seed_data",
        bash_command=f"{SPARK} /opt/spark-apps/bronze_seed_data.py",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    silver_etl = BashOperator(
        task_id="silver_etl",
        bash_command=f"{SPARK} /opt/spark-apps/silver_etl.py",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    gold_etl = BashOperator(
        task_id="gold_etl",
        bash_command=f"{SPARK} /opt/spark-apps/gold_etl.py",
        retries=2,
        retry_delay=timedelta(minutes=3),
    )

    def quality_gate(**context):
        print("=== Pipeline Quality Gate ===")
        print("All ETL stages completed successfully")
        print(f"Run ID: {context['run_id']}")
        print(f"Execution date: {context['execution_date']}")
        return "SUCCESS"

    quality_check = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
        provide_context=True,
    )

    bronze_seed >> silver_etl >> gold_etl >> quality_check
