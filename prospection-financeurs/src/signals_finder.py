"""
Détection de signaux : problématiques récentes d'un département
en matière de tarification et de financement des ESSMS.

Uniquement applicable pour entity_type == 'departement'.

Stratégie :
  Q1 : "Conseil départemental {nom}" ESSMS tarification financement 2024 2025
  Q2 : "département {nom}" EHPAD SAAD financement budget médico-social
  Q3 : "département {nom}" ("dotation globale" OR "prix de journée" OR CPOM)

Le LLM synthétise les snippets en un résumé structuré avec tags et niveau d'alerte.
"""
import json
import logging
import sys
from datetime import date
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)

# Tags prédéfinis utilisés comme guide pour le LLM
TAGS_REFERENCE = [
    "retards_paiement",
    "restriction_budgetaire",
    "contentieux",
    "reforme_tarifaire",
    "tension_operateurs",
    "sous_dotation",
    "controle_ou_audit",
    "reconfiguration_cpom",
    "fermeture_etablissement",
    "crise_sectorielle",
    "coupes_budgetaires",
    "non_revalorisation",
    "surcharge_administrative",
]

# Résultat vide renvoyé si aucun résultat Serper ne permet d'analyse
_SIGNAL_VIDE = {
    "resume": "Aucun signal récent identifié sur ce département pour la tarification/financement ESSMS.",
    "tags": [],
    "niveau_alerte": "faible",
    "sources_utilisees": [],
    "confiance": "faible",
    "periode_couverte": "2023-2025",
    "nb_resultats_serper": 0,
}


class SignalsFinder:
    """
    Recherche et synthétise des signaux de tension ou d'enjeux récents
    pour un département donné autour du financement/tarification des ESSMS.
    """

    def __init__(self, serper_client, mistral_client):
        self._serper = serper_client
        self._mistral = mistral_client

    def find_signals(self, entity_name: str, code: str) -> dict[str, Any]:
        """
        Génère un signal pour un département.

        Args:
            entity_name : nom court du département (ex. "Côtes-d'Armor")
            code        : code du département (ex. "22")

        Returns:
            dict avec : resume, tags, niveau_alerte, sources_utilisees,
                        confiance, periode_couverte, nb_resultats_serper,
                        code_dept, date_generation
        """
        snippets_all: list[str] = []
        sources_all: list[str] = []

        queries = [
            f'"Conseil départemental {entity_name}" ESSMS tarification financement 2024 2025',
            f'"département {entity_name}" EHPAD SAAD financement budget médico-social',
            f'"département {entity_name}" ("dotation globale" OR "prix de journée" OR CPOM)',
        ]

        for q in queries:
            try:
                results = self._serper.search(q, num=5)
            except Exception as exc:
                logger.warning("Serper signals Q[%s] erreur : %s", q[:40], exc)
                continue

            for r in results.get("organic", [])[:5]:
                title = r.get("title", "").strip()
                snippet = r.get("snippet", "").strip()
                link = r.get("link", "")
                date_str = r.get("date", "")
                if snippet or title:
                    line = f"[{title}]"
                    if date_str:
                        line += f" ({date_str})"
                    line += f" — {snippet}"
                    snippets_all.append(line)
                if link and link not in sources_all:
                    sources_all.append(link)

        nb_results = len(snippets_all)
        logger.info(
            "Signaux dept %s (%s) : %d snippets récupérés sur %d requêtes",
            entity_name, code, nb_results, len(queries)
        )

        if nb_results == 0:
            signal = dict(_SIGNAL_VIDE)
        else:
            try:
                signal = self._mistral.analyze_signals(
                    entity_name=entity_name,
                    code=code,
                    snippets=snippets_all,
                    sources=sources_all,
                )
            except Exception as exc:
                logger.error("Mistral analyze_signals erreur pour dept %s : %s", code, exc)
                signal = dict(_SIGNAL_VIDE)

        signal["nb_resultats_serper"] = nb_results
        signal["code_dept"] = code
        signal["date_generation"] = date.today().isoformat()
        return signal
