from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments applied to all tasks
default_args = {
    "owner": "rayane",
    "retries": 5,
    "retry_delay": timedelta(seconds=20),
    "email_on_failure": False,
}

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Daily e-commerce data pipeline",
    schedule="0 6 * * *",  # Every day at 6am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "daily"],
) as dag:

    # -----------------------
    # EXTRACT
    # -----------------------
    def extract_products():
        """Extract products from Open Food Facts API."""
        from src.pipeline.ingester import OpenFoodFactsIngester

        ingester = OpenFoodFactsIngester()
        return str(ingester.run(n_pages=2))

    # -----------------------
    # LOAD (TEMP TABLE)
    # -----------------------
    def load_products(**context):
        from src.pipeline.bigquery_loader import BigQueryLoader
        from loguru import logger

        filepath = context["ti"].xcom_pull(task_ids="extract_products")

        if not filepath:
            raise ValueError("No file produced by extract task")

        logger.info(f"Starting load step — file: {filepath}")

        loader = BigQueryLoader()

        logger.info("Loading data into BigQuery TEMP table: products_temp")

        loader.run(filepath=filepath, table_name="products_temp")

        logger.success("Load step completed successfully")

    # -----------------------
    # MERGE (UPSERT)
    # -----------------------
    def merge_products():
        from google.cloud import bigquery
        from loguru import logger
        from src.pipeline.config import GCP_PROJECT_ID, BIGQUERY_DATASET

        client = bigquery.Client(project=GCP_PROJECT_ID)

        # ✅ Step 0 — create table if not exists
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.products` AS
        SELECT * FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.products_temp`
        WHERE 1=0
        """

        logger.info("Ensuring products table exists")
        client.query(create_table_query).result()

        # ✅ Step 1 — MERGE
        merge_query = f"""
        MERGE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.products` T
        USING (
        SELECT * EXCEPT(row_num)
        FROM (
            SELECT *,
             ROW_NUMBER() OVER (
             PARTITION BY id
             ORDER BY last_modified_t DESC
             ) AS row_num
             FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.products_temp`
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

        logger.info("Starting MERGE into products table")
        client.query(merge_query).result()
        logger.success("MERGE completed")

        # ✅ Step 2 — TRUNCATE
        truncate_query = f"""
        TRUNCATE TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.products_temp`
        """

        logger.info("Cleaning temp table")
        client.query(truncate_query).result()
        logger.success("Temp table cleaned")
    # -----------------------
    # TASKS
    # -----------------------
    task_extract = PythonOperator(
        task_id="extract_products",
        python_callable=extract_products,
    )

    task_load = PythonOperator(
        task_id="load_products",
        python_callable=load_products,
    )

    task_merge = PythonOperator(
        task_id="merge_products",
        python_callable=merge_products,
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /Users/Rayane/projects/data-pipeline-portfolio/ecommerce_analytics && poetry run dbt run",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /Users/Rayane/projects/data-pipeline-portfolio/ecommerce_analytics && poetry run dbt test",
    )

    # -----------------------
    # DEPENDENCIES
    # -----------------------
    task_extract >> task_load >> task_merge >> task_dbt_run >> task_dbt_test