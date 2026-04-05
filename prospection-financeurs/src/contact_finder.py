"""
Identification des contacts pour chaque entité via Serper + Mistral.
"""
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import MAX_CONTACTS_PER_ENTITY
from src.serper_client import SerperClient
from src.mistral_client import MistralClient
from src.normalizer import normalize_name, split_full_name, clean_full_name

logger = logging.getLogger(__name__)


def _build_queries_departement(nom: str, postes_all: list[str]) -> list[str]:
    """Construit les requêtes Serper pour un département."""
    return [
        f'"conseil départemental {nom}" organigramme DGA solidarités',
        f'"conseil départemental {nom}" "directeur autonomie" OR "directeur PA-PH" OR "directeur enfance"',
        f'site:linkedin.com/in "conseil départemental {nom}" "directeur" "solidarités" OR "autonomie" OR "enfance"',
    ]


def _build_queries_dirpjj(nom: str, siege: str) -> list[str]:
    return [
        f'"DIRPJJ {nom}" OR "direction interrégionale PJJ {siege}" directeur',
        f'"DIRPJJ {nom}" DEPAFI OR "affaires financières" tarification',
        f'site:linkedin.com/in "DIRPJJ" OR "protection judiciaire jeunesse" "{siege}"',
    ]


def _build_queries_ars(region: str) -> list[str]:
    return [
        f'"ARS {region}" organigramme "offre médico-sociale" OR "autonomie"',
        f'"ARS {region}" "tarification" OR "financement" médico-social "chef de service"',
        f'site:linkedin.com/in "ARS {region}" "médico-social" OR "autonomie" OR "tarification"',
    ]


def _build_queries_poste_manquant(
    entity_type: str, entity_name: str, poste: str
) -> list[str]:
    """Requête ciblée pour un poste non trouvé."""
    titre_court = poste.replace('"', "").strip()
    if entity_type == "departement":
        return [f'"conseil départemental {entity_name}" "{titre_court}"']
    elif entity_type == "dirpjj":
        return [f'"DIRPJJ {entity_name}" "{titre_court}"']
    else:
        return [f'"ARS {entity_name}" "{titre_court}"']


def _deduplicate_contacts(contacts: list[dict]) -> list[dict]:
    """Déduplique les contacts en normalisant les noms."""
    seen: set[str] = set()
    unique: list[dict] = []
    for c in contacts:
        key = normalize_name(c.get("nom_complet", ""))
        if key and key not in seen:
            seen.add(key)
            unique.append(c)
    return unique


class ContactFinder:
    """
    Orchestre la recherche et l'extraction des contacts pour une entité.
    """

    def __init__(self, serper: SerperClient, mistral: MistralClient):
        self.serper = serper
        self.mistral = mistral

    def find_contacts(
        self,
        entity: dict[str, Any],
        entity_type: str,
        target_posts: list[dict],
    ) -> list[dict[str, Any]]:
        """
        Recherche les contacts pour une entité donnée.

        Args:
            entity: Dictionnaire de l'entité (nom, code, etc.)
            entity_type: "departement", "dirpjj" ou "ars"
            target_posts: Liste de dicts {niveau, titres}

        Returns:
            Liste de contacts enrichis.
        """
        nom = entity.get("nom", "")
        siege = entity.get("siege", "")
        all_titres = [t for p in target_posts for t in p.get("titres", [])]

        # ── 1. Construire et lancer les requêtes principales ──────────────────
        if entity_type == "departement":
            queries = _build_queries_departement(nom, all_titres)
        elif entity_type == "dirpjj":
            queries = _build_queries_dirpjj(nom, siege)
        else:
            queries = _build_queries_ars(nom)

        all_snippets: list[str] = []
        for query in queries:
            try:
                _, snippets = self.serper.search_and_extract(query)
                all_snippets.extend(snippets)
                logger.info("[%s] Requête '%s' → %d snippets", nom, query[:60], len(snippets))
            except Exception as exc:
                logger.error("[%s] Erreur Serper pour '%s': %s", nom, query[:60], exc)

        # ── 2. Extraction LLM ─────────────────────────────────────────────────
        contacts: list[dict] = []
        if all_snippets:
            entity_type_label = {
                "departement": "conseil départemental",
                "dirpjj": "direction interrégionale PJJ",
                "ars": "agence régionale de santé",
            }.get(entity_type, entity_type)

            try:
                contacts = self.mistral.extract_contacts(
                    entity_type=entity_type_label,
                    entity_name=nom,
                    target_posts=all_titres,
                    snippets=all_snippets[:30],  # limiter pour ne pas dépasser le contexte
                )
                logger.info("[%s] Mistral → %d contact(s) trouvé(s)", nom, len(contacts))
            except Exception as exc:
                logger.error("[%s] Erreur Mistral: %s", nom, exc)

        # ── 3. Recherches ciblées pour les postes non couverts ────────────────
        found_niveaux = set()
        for c in contacts:
            poste = c.get("poste_exact", "").lower()
            for p in target_posts:
                niveau = p.get("niveau", "")
                if any(t.lower() in poste for t in p.get("titres", [])):
                    found_niveaux.add(niveau)

        missing_niveaux = [
            p for p in target_posts if p.get("niveau") not in found_niveaux
        ]
        for poste_def in missing_niveaux:
            for titre in poste_def.get("titres", [])[:2]:  # max 2 titres par niveau
                q_list = _build_queries_poste_manquant(entity_type, nom, titre)
                for q in q_list:
                    try:
                        _, snippets = self.serper.search_and_extract(q)
                        if snippets:
                            new_contacts = self.mistral.extract_contacts(
                                entity_type=entity_type,
                                entity_name=nom,
                                target_posts=[titre],
                                snippets=snippets[:10],
                            )
                            contacts.extend(new_contacts)
                            logger.info(
                                "[%s] Recherche ciblée '%s' → %d contacts",
                                nom, titre[:40], len(new_contacts)
                            )
                    except Exception as exc:
                        logger.error("[%s] Erreur requête ciblée: %s", nom, exc)

        # ── 4. Enrichissement et normalisation ────────────────────────────────
        enriched: list[dict] = []
        for c in contacts:
            nom_complet = clean_full_name(c.get("nom_complet", "").strip())
            if not nom_complet:
                continue
            prenom, nom_contact = split_full_name(nom_complet)

            # Déterminer le niveau
            poste_lower = c.get("poste_exact", "").lower()
            niveau = "direction"
            for p in target_posts:
                if any(t.lower() in poste_lower for t in p.get("titres", [])):
                    niveau = p.get("niveau", "direction")
                    break

            # Ignorer les contacts sans nom de famille identifiable
            if not nom_contact:
                logger.warning("Contact sans nom de famille ignoré : '%s'", nom_complet)
                continue

            enriched.append({
                "nom_complet": nom_complet,
                "prenom": prenom,
                "nom": nom_contact,
                "poste_exact": c.get("poste_exact", ""),
                "niveau": niveau,
                "source_nom": c.get("source", ""),
                "confiance_nom": c.get("confiance", "basse"),
                "linkedin_url": None,
                "email_principal": None,
                "email_variantes": [],
                "confiance_email": "inconnue",
            })

        # ── 5. Déduplication et limitation ───────────────────────────────────
        enriched = _deduplicate_contacts(enriched)
        if MAX_CONTACTS_PER_ENTITY > 0:
            enriched = enriched[:MAX_CONTACTS_PER_ENTITY]

        return enriched
