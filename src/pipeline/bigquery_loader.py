from pathlib import Path

from google.cloud import bigquery, storage
from google.api_core.retry import Retry
from loguru import logger

from src.pipeline.config import BIGQUERY_DATASET, GCP_PROJECT_ID, GCS_BUCKET


class BigQueryLoader:
    def __init__(self):
        # BigQuery client
        self.client = bigquery.Client(project=GCP_PROJECT_ID)

        # GCS client
        self.storage_client = storage.Client(project=GCP_PROJECT_ID)
        self.bucket = self.storage_client.bucket(GCS_BUCKET)

        self.dataset_id = BIGQUERY_DATASET

        logger.info(
            f"BigQuery + GCS initialized — project: {GCP_PROJECT_ID}, bucket: {GCS_BUCKET}"
        )

    def create_dataset_if_not_exists(self) -> None:
        dataset_ref = bigquery.Dataset(f"{GCP_PROJECT_ID}.{self.dataset_id}")
        dataset_ref.location = "EU"

        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"Dataset {self.dataset_id} already exists")
        except Exception:
            self.client.create_dataset(dataset_ref)
            logger.success(f"Dataset {self.dataset_id} created")

    def upload_to_gcs(self, filepath: Path, gcs_path: str) -> str:
        """
        Upload local file to GCS using chunked upload + retry.
        """
        blob = self.bucket.blob(gcs_path)

        logger.info(
            f"Uploading {filepath} → gs://{self.bucket.name}/{gcs_path}"
        )

        # 🔥 important pour gros fichiers
        blob.chunk_size = 10 * 1024 * 1024  # 10 MB chunks

        blob.upload_from_filename(
            str(filepath),
            timeout=600,  # 10 minutes
            retry=Retry(deadline=600),  # retry robuste
        )

        gcs_uri = f"gs://{self.bucket.name}/{gcs_path}"

        logger.success(f"Uploaded → {gcs_uri}")
        return gcs_uri

    def load_parquet_from_gcs(
        self,
        gcs_uri: str,
        table_name: str,
        write_mode: str = "append",
    ) -> None:
        """
        Load data from GCS into BigQuery.
        """
        table_ref = f"{GCP_PROJECT_ID}.{self.dataset_id}.{table_name}"

        write_disposition = {
            "append": bigquery.WriteDisposition.WRITE_APPEND,
            "truncate": bigquery.WriteDisposition.WRITE_TRUNCATE,
        }[write_mode]

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
        )

        logger.info(f"Loading {gcs_uri} → {table_ref} (mode={write_mode})")

        job = self.client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config,
        )

        job.result()

        logger.success(f"{job.output_rows} rows loaded → {table_ref}")

    def load_from_local_via_gcs(
        self,
        filepath: Path,
        table_name: str,
        write_mode: str = "append",
    ) -> None:
        """
        Full pipeline:
        local parquet → GCS → BigQuery
        """

        # 🔥 garder le partitionnement (Hive-style)
        try:
            partition = filepath.relative_to(filepath.parents[3])
        except Exception:
            partition = filepath.name

        gcs_path = f"raw/{table_name}/{partition}"

        # Step 1 — upload
        gcs_uri = self.upload_to_gcs(filepath, gcs_path)

        # Step 2 — load into BQ
        self.load_parquet_from_gcs(gcs_uri, table_name, write_mode)

    def run(
        self,
        filepath: Path,
        table_name: str,
        write_mode: str = "append",
    ) -> None:
        """
        Main entry point.
        """
        logger.info(f"Starting pipeline — {filepath} → {table_name}")

        # Step 1 — dataset
        self.create_dataset_if_not_exists()

        # Step 2 — local → GCS → BigQuery
        self.load_from_local_via_gcs(filepath, table_name, write_mode)

        logger.success(f"Pipeline complete — {table_name}")