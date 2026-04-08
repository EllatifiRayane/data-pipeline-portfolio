from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from loguru import logger
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from google.cloud import bigquery
from src.pipeline.config import GCP_PROJECT_ID, BIGQUERY_DATASET


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
def merge_products():
    logger.info("🚀 Starting MERGE step")

    client = bigquery.Client(project=GCP_PROJECT_ID)

    table_final = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.products"
    table_temp = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.products_temp"

    # -----------------------------
    # ✅ 1. CHECK DATA EXISTS
    # -----------------------------
    check_query = f"""
    SELECT COUNT(*) as count
    FROM `{table_temp}`
    """

    result = client.query(check_query).result()
    row_count = list(result)[0]["count"]

    logger.info(f"📊 Rows in temp table: {row_count}")

    if row_count == 0:
        raise ValueError("❌ products_temp is empty → aborting merge")

    # -----------------------------
    # 🧱 2. CREATE TABLE IF NOT EXISTS
    # -----------------------------
    create_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_final}` AS
    SELECT * FROM `{table_temp}` WHERE 1=0
    """

    logger.info("Ensuring final table exists")
    client.query(create_query).result()

    # -----------------------------
    # 🔥 3. MERGE
    # -----------------------------
    merge_query = f"""
    MERGE `{table_final}` T
    USING (
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY last_modified_t DESC
                ) AS row_num
            FROM `{table_temp}`
        )
        WHERE row_num = 1
    ) S
    ON T.id = S.id

    WHEN MATCHED THEN
    UPDATE SET
        product_name = S.product_name,
        brands = S.brands,
        categories = S.categories,
        quantity = S.quantity,
        nutriscore_grade = S.nutriscore_grade,
        ecoscore_grade = S.ecoscore_grade,
        countries_tags = S.countries_tags,
        stores = S.stores,
        last_modified_t = S.last_modified_t,
        ingested_at = S.ingested_at

    WHEN NOT MATCHED THEN
    INSERT ROW
    """

    logger.info("🔄 Running MERGE query")
    client.query(merge_query).result()

    logger.success(f"✅ MERGE completed — {row_count} rows processed")

    # -----------------------------
    # 🧹 CLEAN TEMP
    # -----------------------------
    truncate_query = f"""
    TRUNCATE TABLE `{table_temp}`
    """

    logger.info("🧹 Cleaning temp table")
    client.query(truncate_query).result()

    logger.success("✅ Temp table cleaned")


# 📦 DAG
with DAG(
    dag_id="merge_products",
    default_args=default_args,
    description="Merge products_temp into products (dedup + upsert)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # 🔥 IMPORTANT → trigger only
    catchup=False,
    tags=["merge", "bigquery", "production"],
) as dag:

    merge = PythonOperator(
        task_id="merge_products",
        python_callable=merge_products,
        execution_timeout=timedelta(minutes=10),
    )

    trigger_dbt = TriggerDagRunOperator(
    task_id="trigger_dbt",
    trigger_dag_id="dbt_transform",
    )

    merge >> trigger_dbt