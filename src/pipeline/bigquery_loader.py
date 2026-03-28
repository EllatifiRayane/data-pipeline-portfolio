from pathlib import Path

from google.cloud import bigquery
from loguru import logger

from src.pipeline.config import BIGQUERY_DATASET, GCP_PROJECT_ID


class BigQueryLoader:
    def __init__(self):
        # Initialize the BigQuery client
        # It automatically uses the GCP credentials configured via gcloud CLI
        self.client = bigquery.Client(project=GCP_PROJECT_ID)
        self.dataset_id = BIGQUERY_DATASET

        logger.info(f"BigQuery client initialized — project: {GCP_PROJECT_ID}")

    def create_dataset_if_not_exists(self) -> None:
        """
        Creates the BigQuery dataset if it doesn't already exist.
        Safe to call multiple times — idempotent.
        """
        dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{self.dataset_id}")

        # Dataset location — EU for GDPR compliance
        dataset_ref.location = "EU"

        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except Exception:
            # Dataset doesn't exist — we create it
            self.client.create_dataset(dataset_ref)
            logger.success(f"Dataset {self.dataset_id} created")

    def load_parquet(self, filepath: Path, table_name: str) -> None:
        """
        Loads a Parquet file into a BigQuery table.
        Creates the table automatically based on the Parquet schema.

        Args:
            filepath: path to the Parquet file to load
            table_name: target BigQuery table name
        """
        table_ref = f"{GCP_PROJECT_ID}.{self.dataset_id}.{table_name}"

        # Job configuration — how BigQuery should load the data
        job_config = bigquery.LoadJobConfig(
            # Automatically detect schema from Parquet file
            autodetect=True,
            # Parquet format
            source_format=bigquery.SourceFormat.PARQUET,
            # WRITE_APPEND — adds data without deleting existing rows
            # Important for incremental loads — each day adds new rows
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        logger.info(f"Loading {filepath} → {table_ref}")

        with open(filepath, "rb") as f:
            job = self.client.load_table_from_file(
                f,
                table_ref,
                job_config=job_config,
            )

        # Wait for the job to complete — BigQuery loads are asynchronous
        job.result()

        logger.success(f"{job.output_rows} rows loaded → {table_ref}")

    def run(self, filepath: Path, table_name: str) -> None:
        """
        Main entry point for the BigQuery loader.
        Orchestrates dataset creation → data loading.

        Args:
            filepath: path to the Parquet file to load
            table_name: target BigQuery table name
        """
        logger.info(f"Starting BigQuery load — {filepath} → {table_name}")

        # Step 1 — Create dataset if it doesn't exist
        self.create_dataset_if_not_exists()

        # Step 2 — Load Parquet file into BigQuery
        self.load_parquet(filepath, table_name)

        logger.success(f"BigQuery load complete — {table_name}")
