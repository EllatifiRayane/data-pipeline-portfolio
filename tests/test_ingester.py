import pandas as pd
from src.pipeline.ingester import OpenFoodFactsIngester
from unittest.mock import MagicMock


def test_ingester_initialisation():
    """
    Vérifie que l'ingester s'initialise correctement.
    - La session HTTP est créée
    - Le User-Agent est bien défini
    - Le répertoire de sortie est correct
    """
    # ARRANGE & ACT — on crée l'ingester
    ingester = OpenFoodFactsIngester()

    # ASSERT — on vérifie l'état initial
    assert ingester.session is not None
    assert "User-Agent" in ingester.session.headers
    assert "DataPipelinePortfolio" in ingester.session.headers["User-Agent"]
    assert ingester.output_dir.name == "products"

def test_to_parquet_writes_file(tmp_path, monkeypatch):
    """
    Vérifie que to_parquet() écrit correctement le fichier Parquet.
    
    tmp_path — dossier temporaire créé par pytest, supprimé après le test
    monkeypatch — permet de modifier temporairement des valeurs pendant le test
    """
    # ARRANGE — données de test minimalistes
    products = [
        {"id": "1", "product_name": "Coca Cola", "brands": "Coca Cola"},
        {"id": "2", "product_name": "Pepsi", "brands": "Pepsi"},
    ]

    ingester = OpenFoodFactsIngester()

    # On redirige output_dir vers tmp_path
    # pour ne pas polluer data/raw/ avec des données de test
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
    Vérifie que run() orchestre correctement fetch → stockage.
    On mocke _fetch_with_retry pour ne pas appeler vraiment l'API.
    """
    # ARRANGE — on crée une fausse réponse HTTP
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {
        "products": [
            {"id": "1", "product_name": "Coca Cola", "brands": "Coca Cola"},
            {"id": "2", "product_name": "Pepsi", "brands": "Pepsi"},
            {"id": "3", "product_name": "Orangina", "brands": "Orangina"},
        ]
    }
    # On crée l'ingester
    ingester = OpenFoodFactsIngester()
    
    # On remplace _fetch_with_retry par notre fausse réponse
    # à chaque appel de _fetch_with_retry, fake_response sera retourné
    monkeypatch.setattr(
        ingester,
        "_fetch_with_retry",
        lambda url: fake_response
    )
    
    # On redirige output_dir vers tmp_path
    monkeypatch.setattr(ingester, "output_dir", tmp_path)
    # ACT — on lance le pipeline complet
    ingester.run(n_pages=2)

    # ASSERT — on vérifie que le fichier Parquet a bien été créé
    parquet_files = list(tmp_path.rglob("*.parquet"))
    
    # On assert que cette liste contient exactement 1 fichier. 0 fichier -> Pas de data / +1 fichier = doublons
    assert len(parquet_files) == 1 

    df = pd.read_parquet(parquet_files[0])
    assert len(df) == 6  # 3 produits × 2 pages
    assert "ingested_at" in df.columns