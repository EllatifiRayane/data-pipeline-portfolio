import pandas as pd
from src.pipeline.ingester import OpenFoodFactsIngester
from unittest.mock import MagicMock


def test_ingester_initialisation():
    """
    Check that the ingester initializes correctly.
    - The HTTP session is created
    - The User-Agent is set correctly
    - The output directory is correct
    """
    # ARRANGE & ACT — We create the ingester
    ingester = OpenFoodFactsIngester()

    # ASSERT — Check of the initial state
    assert ingester.session is not None
    assert "User-Agent" in ingester.session.headers
    assert "DataPipelinePortfolio" in ingester.session.headers["User-Agent"]
    assert ingester.output_dir.name == "products"

def test_to_parquet_writes_file(tmp_path, monkeypatch):
    """
    Check that to_parquet() writes the Parquet file correctly.
    
    tmp_path — temporary directory created by pytest, deleted after the test
    monkeypatch — allows you to temporarily modify values during the test
    """
    # ARRANGE — Sample data 
    products = [
        {"id": "1", "product_name": "Coca Cola", "brands": "Coca Cola"},
        {"id": "2", "product_name": "Pepsi", "brands": "Pepsi"},
    ]

    ingester = OpenFoodFactsIngester()

    # Redirect output_dir to tmp_path
    # to avoid cluttering data/raw/ with test data
    monkeypatch.setattr(ingester, "output_dir", tmp_path)

    # ACT
    filepath = ingester.to_parquet(products)

    # ASSERT
    assert filepath.exists()
    assert filepath.suffix == ".parquet"

    df = pd.read_parquet(filepath)
    assert len(df) == 2
    assert "ingested_at" in df.columns

def test_run_full_ingestion(tmp_path, monkeypatch):
    """
    Check that run() correctly coordinates the fetch → storage process.
    We mock _fetch_with_retry so that the API isn't actually called.
    """
    # ARRANGE — We fake a fake HTTP response
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {
        "products": [
            {"id": "1", "product_name": "Coca Cola", "brands": "Coca Cola"},
            {"id": "2", "product_name": "Pepsi", "brands": "Pepsi"},
            {"id": "3", "product_name": "Orangina", "brands": "Orangina"},
        ]
    }
    # Ingester instanciation
    ingester = OpenFoodFactsIngester()
    
    # We replace `fetch_with_retry` with our fake response
    # Every time `fetch_with_retry` is called, `fake_response` will be returned
    monkeypatch.setattr(
        ingester,
        "_fetch_with_retry",
        lambda url: fake_response
    )
    
    # We redirect output_dir to tmp_path
    monkeypatch.setattr(ingester, "output_dir", tmp_path)
    # ACT — Run of the pipeline
    ingester.run(n_pages=2)

    # ASSERT — Check that the parquet file has been created
    parquet_files = list(tmp_path.rglob("*.parquet"))
     
    # Verify the list contains exactly 1 file 
    # 0 files means no data was written, 2+ files means duplicates
    assert len(parquet_files) == 1 

    df = pd.read_parquet(parquet_files[0])
    assert len(df) == 6  # 3 products × 2 pages
    assert "ingested_at" in df.columns