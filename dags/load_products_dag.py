from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from loguru import logger
from pathlib import Path
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from src.pipeline.bigquery_loader import BigQueryLoader


# 🔔 Alert
def notify_failure(context):
    logger.error(
        f"🚨 Failure in {context['dag'].dag_id} - {context['task_instance'].task_id}"
    )


# ⚙️ Default args
default_args = {
    "owner": "rayane",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_failure,
}


# 🚀 Task
def load_products(**context):
    logger.info("🚀 Starting LOAD step")

    # 🔥 1. récupérer depuis trigger (si présent)
    dag_conf = context.get("dag_run").conf if context.get("dag_run") else {}

    filepath = dag_conf.get("filepath")

    # 🔁 fallback (ton système actuel)
    if not filepath:
        logger.warning("⚠️ No filepath from trigger → fallback to date-based path")

        today = datetime.utcnow()

        filepath = f"/opt/airflow/data/raw/products/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.parquet"

    filepath = Path(filepath)

    # ❌ sécurité CRITIQUE
    if not filepath:
        raise ValueError("❌ No file received")

    logger.info(f"📂 File used: {filepath}")

    # 🔥 vérifie existence
    if not filepath.exists():
        raise FileNotFoundError(f"❌ File does not exist: {filepath}")

    loader = BigQueryLoader()

    loader.run(
        filepath=filepath,
        table_name="products_temp",
        write_mode="append",
    )

    logger.success("✅ Load completed → BigQuery")


# 📦 DAG
with DAG(
    dag_id="load_products",
    default_args=default_args,
    description="Load products into BigQuery via GCS",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # 🔥 IMPORTANT → trigger only
    catchup=False,
    tags=["load", "gcs", "bigquery"],
) as dag:

    load = PythonOperator(
        task_id="load_products",
        python_callable=load_products,
        execution_timeout=timedelta(minutes=10),
    )

    trigger_merge = TriggerDagRunOperator(
    task_id="trigger_merge_products",
    trigger_dag_id="merge_products",
    )
    load >> trigger_merge