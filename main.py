from src.pipeline.bigquery_loader import BigQueryLoader
from src.pipeline.ingester import OpenFoodFactsIngester


if __name__ == "__main__":
    # Step 1 — Extract data from API and store as Parquet
    ingester = OpenFoodFactsIngester()
    filepath = ingester.run(n_pages=2)

    # Step 2 — Load Parquet into BigQuery
    loader = BigQueryLoader()
    loader.run(filepath=filepath, table_name="products")