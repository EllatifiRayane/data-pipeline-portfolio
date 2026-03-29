from google.cloud import bigquery

from src.pipeline.bigquery_loader import BigQueryLoader
from src.pipeline.ingester import OpenFoodFactsIngester
from src.pipeline.orders_generator import OrdersGenerator
from src.pipeline.config import GCP_PROJECT_ID

# ── Execution flags ───────────────────────────────────────────────────────────
RUN_INGESTION = False       # Extract from API + load products to BigQuery
RUN_ORDERS_GENERATION = True  # Generate synthetic orders + load to BigQuery


def fetch_product_categories() -> dict[str, str]:
    """
    Fetch product_id → primary_category mapping from BigQuery dim_products.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = """
        SELECT product_id, primary_category
        FROM `data-pipeline-portfolio.ecommerce_marts.dim_products`
        WHERE product_id IS NOT NULL
    """
    df = client.query(query).to_dataframe()
    return dict(zip(df["product_id"], df["primary_category"]))


if __name__ == "__main__":
    loader = BigQueryLoader()

    if RUN_INGESTION:
        # Step 1 — Extract products from API and store as Parquet
        ingester = OpenFoodFactsIngester()
        filepath = ingester.run(n_pages=2)

        # Step 2 — Load products Parquet into BigQuery
        loader.run(filepath=filepath, table_name="products")

    if RUN_ORDERS_GENERATION:
        # Step 3 — Fetch product categories from BigQuery
        product_categories = fetch_product_categories()

        # Step 4 — Generate synthetic orders
        generator = OrdersGenerator(product_categories=product_categories)
        orders_filepath, order_items_filepath = generator.run()

        # Step 5 — Load orders into BigQuery
        loader.run(filepath=orders_filepath, table_name="orders")
        loader.run(filepath=order_items_filepath, table_name="order_items")