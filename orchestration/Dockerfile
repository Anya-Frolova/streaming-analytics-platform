FROM apache/airflow:2.9.2-python3.11
USER root
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*
USER airflow
