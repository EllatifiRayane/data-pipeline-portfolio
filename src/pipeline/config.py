import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ----------------------------------
# Base paths (Docker compatible)
# ----------------------------------
BASE_DIR = Path(os.getenv("AIRFLOW_HOME", Path(__file__).resolve().parents[3]))
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

# ----------------------------------
# API Open Food Facts
# ----------------------------------
API_BASE_URL = "https://world.openfoodfacts.org/cgi/search.pl"
API_PAGE_SIZE = 100

# ----------------------------------
# GCP
# ----------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "ecommerce_raw")

# ----------------------------------
# GCS
# ----------------------------------
GCS_BUCKET = os.getenv("GCS_BUCKET")