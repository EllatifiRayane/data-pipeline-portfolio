from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  # 🔥 NEW
from datetime import datetime, timedelta
from loguru import logger

from src.pipeline.ingester import OpenFoodFactsIngester


# 🔔 Alert
def notify_failure(context):
    logger.error(
        f"🚨 Failure in {context['dag'].dag_id} - {context['task_instance'].task_id}"
    )


# ⚙️ Default args
default_args = {
    "owner": "rayane",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}


# 🚀 Task
def extract_products(**context):
    logger.info("🚀 Starting extraction from Open Food Facts")

    ingester = OpenFoodFactsIngester()

    filepath = ingester.run(n_pages=2)

    if not filepath:
        raise ValueError("No data extracted from API ❌")

    logger.success(f"✅ Extraction completed → {filepath}")

    return str(filepath)


# 📦 DAG
with DAG(
    dag_id="extract_products",
    default_args=default_args,
    description="Extract products from Open Food Facts API",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["extract", "api"],
) as dag:

    task_extract = PythonOperator(
        task_id="extract_products",
        python_callable=extract_products,
        execution_timeout=timedelta(minutes=5),
    )

    # 🔥 NEW → trigger load DAG
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_products",
        trigger_dag_id="load_products",
        conf={
            "triggered_by": "extract_products"
        },
    )

    # 🔗 dependency
    task_extract >> trigger_load