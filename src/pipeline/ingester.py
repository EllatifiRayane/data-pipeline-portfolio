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
        # On crée une Session HTTP réutilisable plutôt que d'appeler
        # requests.get() directement — plus efficace car la connexion
        # TCP est maintenue entre les requêtes
        self.session = requests.Session()

        # On se présente à l'API via le User-Agent
        # Open Food Facts l'exige explicitement dans leur doc
        # Sans ça → 503 systématique
        self.session.headers.update(
            {"User-Agent": "DataPipelinePortfolio/1.0 (re7trading@gmail.com)"}
        )

        # Répertoire de sortie pour les fichiers Parquet
        # Défini ici pour être accessible par toutes les méthodes via self
        self.output_dir = RAW_DATA_DIR / "products"

        logger.info("Ingester initialisé")

    def _fetch_with_retry(self, url: str, max_retries: int = 5) -> requests.Response:
        """
        Effectue un appel HTTP GET avec retry et backoff exponentiel.

        Gère les cas suivants :
        - 503 : serveur temporairement indisponible
        - Timeout : pas de réponse dans le délai imparti

        Args:
            url: URL à appeler
            max_retries: nombre maximum de tentatives avant abandon

        Returns:
            requests.Response : la réponse HTTP si succès

        Raises:
            Exception: si toutes les tentatives échouent
        """
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 503:
                    wait_time = 2**attempt
                    logger.warning(
                        f"503 reçu — tentative {attempt + 1}/{max_retries}"
                        f" — attente {wait_time}s"
                    )
                    time.sleep(wait_time)
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                wait_time = 2**attempt
                logger.error(
                    f"Timeout — tentative {attempt + 1}/{max_retries}"
                    f" — attente {wait_time}s"
                )
                time.sleep(wait_time)
        # On arrive ici uniquement si toutes les tentatives ont échoué
        raise Exception(f"API indisponible après {max_retries} tentatives")

    def fetch_products(self, n_pages: int = 5) -> list[dict]:
        """
        Récupère n_pages pages de produits depuis l'API Open Food Facts.

        Args:
            n_pages: nombre de pages à récupérer (100 produits par page)

        Returns:
            list[dict] : liste de tous les produits récupérés
        """
        all_products = []

        for page in range(1, n_pages + 1):
            logger.info(f"Récupération page {page}/{n_pages}")

            # On construit l'URL avec les paramètres de pagination
            # et on filtre uniquement les champs dont on a besoin
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

            # .get("products", []) — si la clé "products" est absente
            # on retourne une liste vide plutôt que de planter
            products = response.json().get("products", [])
            all_products.extend(products)

            logger.success(f"Page {page} : {len(products)} produits récupérés")

            # Pause entre chaque page pour respecter le rate limit
            # Sans ça → 503 systématique après quelques pages
            time.sleep(2)

        logger.info(f"Total : {len(all_products)} produits récupérés")
        return all_products

    def to_parquet(self, products: list[dict]) -> Path:
        """
        Écrit les produits bruts en Parquet partitionné par date.

        Le partitionnement Hive (year=/month=/day/) permet à BigQuery
        et Spark de ne lire que les partitions nécessaires — partition pruning.

        Args:
            products: liste de produits récupérés depuis l'API

        Returns:
            Path : chemin du fichier Parquet écrit
        """
        now = datetime.now(timezone.utc)

        # Partitionnement Hive standard — format year=YYYY/month=MM/day=DD
        # Le :02d force l'affichage sur 2 chiffres (ex: 03 et non 3)
        # Sans ça le tri alphabétique des dossiers serait incorrect
        partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        output_path = self.output_dir / partition

        # parents=True — crée tous les dossiers parents si inexistants
        # exist_ok=True — ne plante pas si le dossier existe déjà
        output_path.mkdir(parents=True, exist_ok=True)

        filepath = output_path / "data.parquet"

        df = pd.DataFrame(products)

        # Métadonnée technique — quand la donnée a été ingérée
        # Indispensable pour le debugging et l'audit en production
        df["ingested_at"] = now.isoformat()

        # index=False — l'index pandas n'a aucune valeur métier
        # on ne l'écrit pas dans le fichier
        df.to_parquet(filepath, index=False)

        logger.success(f"{len(df)} produits écrits → {filepath}")
        return filepath

    def run(self, n_pages: int = 5) -> None:
        """
        Point d'entrée principal de l'ingester.
        Orchestre fetch → validation → stockage.

        Args:
            n_pages: nombre de pages à ingérer (100 produits/page)
        """
        logger.info(f"Démarrage ingestion — {n_pages} pages ({n_pages * 100} produits)")

        products = self.fetch_products(n_pages=n_pages)

        # Garde-fou — si l'API retourne 0 produits on arrête proprement
        # plutôt qu'écrire un fichier Parquet vide
        if not products:
            logger.warning("Aucun produit récupéré — arrêt de l'ingestion")
            return

        filepath = self.to_parquet(products)

        logger.success(f"Ingestion terminée — {len(products)} produits → {filepath}")
