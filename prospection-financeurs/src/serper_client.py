"""
Client Serper API avec retry exponentiel, cache et rate limiting.
"""
import json
import logging
import time
import sys
from pathlib import Path
from typing import Any

import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

# Ajouter le répertoire parent au path pour les imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    SERPER_API_KEY,
    SERPER_DELAY_SECONDS,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
    SERPER_TIMEOUT,
)

logger = logging.getLogger(__name__)

SERPER_URL = "https://google.serper.dev/search"


class SerperClient:
    """
    Encapsule les appels à l'API Serper avec retry, rate limiting et cache en mémoire.
    """

    def __init__(self, cache: dict[str, Any] | None = None):
        self._cache: dict[str, Any] = cache if cache is not None else {}
        self._last_call: float = 0.0
        self.request_count: int = 0
        self._client = httpx.Client(timeout=SERPER_TIMEOUT)

    def _throttle(self) -> None:
        """Attend le délai nécessaire entre deux requêtes."""
        elapsed = time.time() - self._last_call
        wait = SERPER_DELAY_SECONDS - elapsed
        if wait > 0:
            time.sleep(wait)

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=RETRY_BASE_DELAY, min=RETRY_BASE_DELAY, max=30),
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _raw_search(self, query: str, num: int = 10) -> dict[str, Any]:
        """Appel HTTP brut à l'API Serper."""
        self._throttle()
        payload = {"q": query, "num": num, "gl": "fr", "hl": "fr"}
        response = self._client.post(
            SERPER_URL,
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            content=json.dumps(payload),
        )
        response.raise_for_status()
        self._last_call = time.time()
        self.request_count += 1
        logger.debug("Serper [%d] '%s' → %d résultats", self.request_count, query, num)
        return response.json()

    def search(self, query: str, num: int = 10, use_cache: bool = True) -> dict[str, Any]:
        """
        Lance une recherche Serper. Vérifie d'abord le cache.
        Retourne le dict complet de résultats Serper.
        """
        cache_key = f"{query}||{num}"
        if use_cache and cache_key in self._cache:
            logger.debug("Cache hit pour '%s'", query)
            return self._cache[cache_key]

        result = self._raw_search(query, num)

        if use_cache:
            self._cache[cache_key] = result

        return result

    def extract_snippets(self, results: dict[str, Any]) -> list[str]:
        """
        Extrait les snippets sous forme de texte concaténé depuis un résultat Serper.
        Chaque entrée contient titre + snippet + lien.
        """
        lines: list[str] = []
        for item in results.get("organic", []):
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            link = item.get("link", "")
            lines.append(f"[{title}] ({link})\n{snippet}")
        return lines

    def search_and_extract(self, query: str, num: int = 10) -> tuple[dict, list[str]]:
        """Recherche + extraction des snippets en une seule opération."""
        results = self.search(query, num)
        snippets = self.extract_snippets(results)
        return results, snippets

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
