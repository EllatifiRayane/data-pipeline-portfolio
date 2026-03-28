from src.pipeline.ingester import OpenFoodFactsIngester

if __name__ == "__main__":
    # On instancie l'ingester et on lance l'ingestion
    # n_pages=2 pour le test — 200 produits
    ingester = OpenFoodFactsIngester()
    ingester.run(n_pages=2)