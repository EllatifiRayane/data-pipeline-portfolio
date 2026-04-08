FROM apache/airflow:2.9.3

USER root

# 🔥 install git (system)
RUN apt-get update && apt-get install -y git

USER airflow

# 🔥 install python deps
RUN pip install --no-cache-dir \
    loguru \
    pandas \
    dbt-bigquery \
    google-cloud-bigquery \
    google-cloud-storage \
    faker