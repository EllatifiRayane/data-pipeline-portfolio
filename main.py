from google.cloud import bigquery
from loguru import logger

from src.pipeline.bigquery_loader import BigQueryLoader
from src.pipeline.ingester import OpenFoodFactsIngester
from src.pipeline.orders_generator import OrdersGenerator
from src.pipeline.config import GCP_PROJECT_ID, BIGQUERY_DATASET

# ── Execution flags ───────────────────────────────────────────────────────────
RUN_INGESTION = False
RUN_ORDERS_GENERATION = True


def fetch_product_categories() -> dict[str, str]:
    """
    Fetch product_id → primary_category mapping from BigQuery dim_products.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT product_id, primary_category
        FROM `{GCP_PROJECT_ID}.ecommerce_marts.dim_products`
        WHERE product_id IS NOT NULL
    """
    df = client.query(query).to_dataframe()
    return dict(zip(df["product_id"], df["primary_category"]))


def table_exists(table_name: str) -> bool:
    """
    Check if a table exists in BigQuery.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    try:
        client.get_table(table_id)
        return True
    except Exception:
        return False


if __name__ == "__main__":
    loader = BigQueryLoader()

    # ─────────────────────────────────────────────────────────
    # PRODUCTS INGESTION (OPTIONAL)
    # ─────────────────────────────────────────────────────────
    if RUN_INGESTION:
        logger.info("Starting products ingestion")
        ingester = OpenFoodFactsIngester()
        filepath = ingester.run(n_pages=2)
        loader.run(filepath=filepath, table_name="products")
        logger.success("Products ingestion completed")

    # ─────────────────────────────────────────────────────────
    # ORDERS + CUSTOMERS GENERATION (ONE-SHOT)
    # ─────────────────────────────────────────────────────────
    if RUN_ORDERS_GENERATION:

        # Safety check
        if table_exists("orders") and table_exists("order_items") and table_exists("customers"):
            logger.warning("Orders, Order Items & Customers already exist → skipping generation")
        else:
            logger.info("Starting orders and customers generation")

            # Step 1 — Fetch product categories
            product_categories = fetch_product_categories()

            # Step 2 — Generate synthetic data
            generator = OrdersGenerator(product_categories=product_categories)
            orders_filepath, order_items_filepath, customers_filepath = generator.run()

            # Step 3 — Load into BigQuery
            logger.info("Loading orders into BigQuery")
            loader.run(filepath=orders_filepath, table_name="orders")

            logger.info("Loading order_items into BigQuery")
            loader.run(filepath=order_items_filepath, table_name="order_items")

            logger.info("Loading customers into BigQuery")
            loader.run(filepath=customers_filepath, table_name="customers")

            logger.success("Orders + Order Items + Customers successfully loaded")