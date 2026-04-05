"""
Recherche des profils LinkedIn pour chaque contact identifié.

Stratégie en 3 paliers (du plus précis au moins restrictif) :
  Q1 : site:linkedin.com/in "Prénom Nom" + mot-clé entité court
  Q2 : site:linkedin.com/in "Prénom Nom"  (sans entité, mais on vérifie la cohérence)
  Q3 : linkedin.com "Prénom Nom" + type entité  (hors opérateur site:)

On ne retient un résultat que si le snippet/titre contient au moins un mot
du nom complet (vérification anti-homonymie basique).
"""
import logging
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.serper_client import SerperClient
from src.normalizer import remove_accents

logger = logging.getLogger(__name__)

_LINKEDIN_RE = re.compile(r"https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9_\-/%]+")

# Mots-clés courts associés à chaque type d'entité pour affiner la recherche
_ENTITY_KEYWORDS = {
    "departement": "département",
    "dirpjj": "PJJ",
    "ars": "ARS",
}


def _extract_linkedin_urls(results: dict) -> list[str]:
    """
    Extrait toutes les URLs LinkedIn valides depuis un résultat Serper.
    Cherche dans : organic[].link, organic[].sitelinks[].link, knowledgeGraph.
    """
    urls: list[str] = []

    def _add(url: str) -> None:
        m = _LINKEDIN_RE.search(url)
        if m:
            clean = m.group(0).rstrip("/")
            if clean not in urls:
                urls.append(clean)

    for item in results.get("organic", []):
        _add(item.get("link", ""))
        for sl in item.get("sitelinks", []):
            _add(sl.get("link", ""))

    kg = results.get("knowledgeGraph", {})
    _add(kg.get("website", ""))

    return urls


def _name_in_result(full_name: str, item: dict) -> bool:
    """
    Vérifie que l'un des mots du nom complet apparaît dans le titre ou snippet
    du résultat (vérification anti-homonymie basique, insensible à la casse).
    """
    haystack = (item.get("title", "") + " " + item.get("snippet", "")).lower()
    haystack = remove_accents(haystack)
    for word in full_name.split():
        if len(word) > 2 and remove_accents(word.lower()) in haystack:
            return True
    return False


def _best_url(results: dict, full_name: str) -> str | None:
    """
    Retourne la meilleure URL LinkedIn en privilégiant les résultats où
    le nom apparaît dans le titre/snippet.
    """
    candidates: list[tuple[int, str]] = []  # (score, url)

    for item in results.get("organic", []):
        link = item.get("link", "")
        if "linkedin.com/in/" not in link:
            continue
        m = _LINKEDIN_RE.search(link)
        if not m:
            continue
        url = m.group(0).rstrip("/")
        score = 1 + (1 if _name_in_result(full_name, item) else 0)
        candidates.append((score, url))
        for sl in item.get("sitelinks", []):
            sl_link = sl.get("link", "")
            if "linkedin.com/in/" in sl_link:
                m2 = _LINKEDIN_RE.search(sl_link)
                if m2:
                    candidates.append((1, m2.group(0).rstrip("/")))

    if not candidates:
        return None

    # Trier par score décroissant
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _entity_context(entity_name: str, entity_type: str, entity_code: str = "") -> str:
    """
    Construit la partie "contexte entité" de Q1 sous forme (A OR B OR C).

    Exemples :
      departement, "Côtes-d'Armor", "22"
        → ("département" "Côtes-d'Armor" OR "Conseil départemental" "Côtes-d'Armor" OR "CD22")
      dirpjj  → ("DIRPJJ" OR "protection judiciaire")
      ars     → ("ARS" OR "agence régionale de santé")
    """
    if entity_type == "departement" and entity_name:
        parts = [
            f'"département" "{entity_name}"',
            f'"Conseil départemental" "{entity_name}"',
        ]
        if entity_code:
            parts.append(f'"CD{entity_code}"')
        return "(" + " OR ".join(parts) + ")"
    elif entity_type == "dirpjj":
        return '("DIRPJJ" OR "protection judiciaire de la jeunesse")'
    elif entity_type == "ars":
        return '("ARS" OR "agence régionale de santé")'
    return ""


class LinkedInFinder:
    """
    Recherche les profils LinkedIn via Serper.
    Stratégie en 3 paliers avec vérification anti-homonymie.
    """

    def __init__(self, serper: SerperClient):
        self.serper = serper
        self._cache: dict[str, str | None] = {}

    def find_profile(
        self,
        prenom: str,
        nom: str,
        entity_name: str,
        entity_type: str = "",
        entity_code: str = "",
        source_nom: str = "",
    ) -> str | None:
        """
        Recherche le profil LinkedIn d'une personne.
        Retourne l'URL ou None si non trouvé.

        Bonus : si source_nom est déjà une URL linkedin.com/in,
        on l'utilise directement sans consommer de crédit Serper.
        """
        # Raccourci : source_nom est déjà un profil LinkedIn
        if source_nom:
            m = _LINKEDIN_RE.search(source_nom)
            if m:
                url = m.group(0).rstrip("/")
                logger.info("LinkedIn récupéré depuis source_nom pour %s %s : %s",
                            prenom, nom, url)
                return url

        cache_key = f"{prenom}|{nom}|{entity_name}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Normaliser la casse : "DUPONT" → "Dupont" pour la recherche
        prenom_q = prenom.strip().title() if prenom.isupper() else prenom.strip()
        nom_q = nom.strip().title() if nom.isupper() else nom.strip()
        full_name = f"{prenom_q} {nom_q}".strip()
        context = _entity_context(entity_name, entity_type, entity_code)

        queries: list[str] = []

        # Q1 : site:linkedin.com/in + nom + contexte entité élargi (OR)
        if context:
            queries.append(f'site:linkedin.com/in "{full_name}" {context}')

        # Q2 : fallback sur nom seul
        queries.append(f'site:linkedin.com/in "{full_name}"')

        for q in queries:
            try:
                results = self.serper.search(q, num=5)
                url = _best_url(results, full_name)
                if url:
                    self._cache[cache_key] = url
                    logger.info("LinkedIn trouvé pour %s (q=%s…): %s", full_name, q[:50], url)
                    return url
            except Exception as exc:
                logger.error("Erreur LinkedIn pour %s (q=%s…): %s", full_name, q[:50], exc)

        logger.debug("LinkedIn non trouvé pour %s", full_name)
        self._cache[cache_key] = None
        return None

    def enrich_contacts(
        self, contacts: list[dict[str, Any]], entity_name: str,
        entity_type: str = "", entity_code: str = ""
    ) -> list[dict[str, Any]]:
        """Enrichit une liste de contacts avec les URLs LinkedIn."""
        for contact in contacts:
            if contact.get("linkedin_url"):
                continue  # déjà connu
            url = self.find_profile(
                prenom=contact.get("prenom", ""),
                nom=contact.get("nom", ""),
                entity_name=entity_name,
                entity_type=entity_type,
                entity_code=entity_code,
                source_nom=contact.get("source_nom", ""),
            )
            contact["linkedin_url"] = url
        return contacts
