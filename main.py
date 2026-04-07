from google.cloud import bigquery
from loguru import logger

from src.pipeline.bigquery_loader import BigQueryLoader
from src.pipeline.ingester import OpenFoodFactsIngester
from src.pipeline.orders_generator import OrdersGenerator
from src.pipeline.config import GCP_PROJECT_ID, BIGQUERY_DATASET

# ── Execution flags ─────────────────────────────────────────
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

    logger.info(f"Fetched {len(df)} product categories")

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
    logger.info("🚀 Starting pipeline")

    loader = BigQueryLoader()

    # ─────────────────────────────────────────────────────────
    # 1. PRODUCTS INGESTION (API → GCS → BQ)
    # ─────────────────────────────────────────────────────────
    if RUN_INGESTION:
        logger.info("📦 Step 1 — Products ingestion")

        ingester = OpenFoodFactsIngester()
        filepath = ingester.run(n_pages=2)

        if filepath:
            loader.run(filepath=filepath, table_name="products")
            logger.success("✅ Products ingestion completed")
        else:
            logger.warning("⚠️ No products ingested")

    # ─────────────────────────────────────────────────────────
    # 2. ORDERS + CUSTOMERS GENERATION
    # ─────────────────────────────────────────────────────────
    if RUN_ORDERS_GENERATION:

        logger.info("🛒 Step 2 — Orders generation")

        # Safety check
        if (
            table_exists("orders")
            and table_exists("order_items")
            and table_exists("customers")
        ):
            logger.warning("⚠️ Tables already exist → skipping generation")

        else:
            logger.info("Generating synthetic data")

            # Step 2.1 — Fetch product categories
            product_categories = fetch_product_categories()

            if not product_categories:
                raise ValueError("❌ No product categories found — aborting")

            # Step 2.2 — Generate data
            generator = OrdersGenerator(product_categories=product_categories)

            orders_filepath, order_items_filepath, customers_filepath = generator.run()

            # Step 2.3 — Load into BigQuery (via GCS)
            logger.info("⬆️ Loading orders")
            loader.run(
                filepath=orders_filepath,
                table_name="orders",
                write_mode="append",
            )

            logger.info("⬆️ Loading order_items")
            loader.run(
                filepath=order_items_filepath,
                table_name="order_items",
                write_mode="append",
            )

            logger.info("⬆️ Loading customers")
            loader.run(
                filepath=customers_filepath,
                table_name="customers",
                write_mode="append",
            )

            logger.success("✅ Orders + Items + Customers loaded")

    logger.success("🎉 Pipeline finished successfully")