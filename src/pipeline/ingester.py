import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from loguru import logger

from src.pipeline.config import (
    API_BASE_URL,
    API_PAGE_SIZE,
    RAW_DATA_DIR,
)


class OpenFoodFactsIngester:
    def __init__(self):
        # We create a reusable HTTP session instead of calling
        # requests.get() directly — this is more efficient because the TCP
        # connection is maintained between requests
        self.session = requests.Session()

        # The API is accessed via the User-Agent
        # Open Food Facts explicitly requires this in their documentation
        # Without it → a 503 error is returned every time
        self.session.headers.update(
            {"User-Agent": "DataPipelinePortfolio/1.0 (re7trading@gmail.com)"}
        )

        # Output directory for Parquet files
        # Defined here so it can be accessed by all methods via `self`
        self.output_dir = RAW_DATA_DIR / "products"

        logger.info("Ingester initialized")

    def _fetch_with_retry(self, url: str, max_retries: int = 5) -> requests.Response:
        """
        Makes an HTTP GET request with retry and exponential backoff.

        Handles the following cases:
        - 503: server temporarily unavailable
        - Timeout: no response within the specified time limit

        Args:
            url: URL to call
            max_retries: maximum number of attempts before giving up

        Returns:
            requests.Response: the HTTP response if successful

        Raises:
            Exception: if all attempts fail
        """
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 503:
                    wait_time = 2**attempt
                    logger.warning(
                        f"503 — attempt {attempt + 1}/{max_retries}"
                        f" — waiting time {wait_time}s"
                    )
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                wait_time = 2**attempt
                logger.error(
                    f"Timeout — attempt {attempt + 1}/{max_retries}"
                    f" — waiting time {wait_time}s"
                )
                time.sleep(wait_time)
        # We only end up here if all other attempts have failed
        raise Exception(f"API unavailable after {max_retries} attempts")

    def fetch_products(self, n_pages: int = 5) -> list[dict]:
        """
        Retrieves n_pages pages of products from the Open Food Facts API.

        Args:
            n_pages: number of pages to retrieve (100 products per page)

        Returns:
            list[dict]: list of all retrieved products
        """
        all_products = []

        for page in range(1, n_pages + 1):
            logger.info(f"Fetching page {page}/{n_pages}")

            # We build the URL with the pagination parameters
            # and filter only the fields we need
            url = (
                f"{API_BASE_URL}"
                f"?action=process"
                f"&json=1"
                f"&page={page}"
                f"&page_size={API_PAGE_SIZE}"
                f"&fields=id,product_name,brands,categories,quantity,"
                f"nutriscore_grade,ecoscore_grade,countries_tags,"
                f"stores,created_t,last_modified_t"
            )

            response = self._fetch_with_retry(url)

            # .get(“products”, []) — if the “products” key is missing
            # an empty list is returned instead of causing a crash
            products = response.json().get("products", [])
            all_products.extend(products)

            logger.success(f"Page {page} : {len(products)} products retrieved")

            # Pause between each page to comply with the rate limit
            # Without this → Consistent 503 errors after a few pages
            time.sleep(2)

        logger.info(f"Total : {len(all_products)} products retrieved")
        return all_products

    def to_parquet(self, products: list[dict]) -> Path:
        """
        Writes raw data to a Parquet file partitioned by date.

        The Hive partitioning scheme (year=/month=/day/) allows BigQuery
        and Spark to read only the necessary partitions — partition pruning.

        Args:
            products: list of products retrieved from the API

        Returns:
            Path: path to the written Parquet file
        """
        now = datetime.now(timezone.utc)

        # Standard Hive partitioning — format year=YYYY/month=MM/day=DD
        # The :02d forces the display to use two digits (e.g., 03 instead of 3)
        # Without this, the alphabetical sorting of folders would be incorrect
        partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        output_path = self.output_dir / partition

        # parents=True — creates all parent folders if they do not exist
        # exist_ok=True — does not crash if the folder already exists
        output_path.mkdir(parents=True, exist_ok=True)

        filepath = output_path / "data.parquet"

        df = pd.DataFrame(products)

        # Technical metadata — when the data was ingested
        # Essential for debugging and auditing in production
        df["ingested_at"] = now.isoformat()

        # index=False — the pandas index has no business value
        # it is not written to the file
        df.to_parquet(filepath, index=False)

        logger.success(f"{len(df)} products written → {filepath}")
        return filepath

    def run(self, n_pages: int = 5) -> None:
        """
        Main ingestion entry point.
        Orchestrates fetch → validation → storage.

        Args:
            n_pages: number of pages to ingest (100 products/page)
        """
        logger.info(f"Start ingestion — {n_pages} pages ({n_pages * 100} products)")

        products = self.fetch_products(n_pages=n_pages)

        # Safety net — if the API returns 0 products, we exit gracefully
        # rather than writing an empty Parquet file
        if not products:
            logger.warning("Zero products retrieved — stop ingestion")
            return

        filepath = self.to_parquet(products)

        logger.success(f"Ingestion done — {len(products)} products → {filepath}")
