from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from loguru import logger


# 🔔 Alert
def notify_failure(context):
    logger.error(
        f"🚨 Failure in {context['dag'].dag_id} - {context['task_instance'].task_id}"
    )


# ⚙️ Default args
default_args = {
    "owner": "rayane",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


# 📦 DAG
with DAG(
    dag_id="dbt_transform",
    default_args=default_args,
    description="Run dbt models (staging → marts)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # 🔥 déclenché par merge
    catchup=False,
    tags=["dbt", "transform"],
) as dag:

    # 🚀 Run dbt (tout le lineage)
    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
    cd /opt/airflow/dbt &&
    dbt run --select +marts --profiles-dir /home/airflow/.dbt
    """,
    )

    dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="""
    cd /opt/airflow/dbt &&
    dbt test --profiles-dir /home/airflow/.dbt
    """,
    )

    dbt_run >> dbt_test