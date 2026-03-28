from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# Local Path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"

# API Open Food Facts
API_BASE_URL = "https://world.openfoodfacts.org/cgi/search.pl"
API_PAGE_SIZE = 100

# GCP
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "ecommerce_raw")
