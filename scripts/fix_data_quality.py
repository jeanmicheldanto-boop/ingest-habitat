"""Correction qualité données FINESS — script multi-phases.

Corrige les problèmes identifiés dans DIAGNOSTIC-DATA-QUALITY.md :
  Phase 1   : Nettoyage contacts pourris (NULL, Dupont fantôme, noms génériques)
  Phase 2   : Purge domaines invalides (blacklist + matching sophistiqué acronyme/sigle)
  Phase 2.5 : Re-normalisation fonctions depuis fonction_brute
  Phase 3   : Dédoublonnage DG/Président + purge non-personnes
  Phase 4   : Découverte domaine officiel (Serper + LLM, parallélisable Cloud Run)
  Phase 5   : Reconstruction emails (pattern detection, sans fallback prenom.nom)

Usage local :
    # Dry-run complet
    python scripts/fix_data_quality.py --phase all --dry-run

    # Phases SQL pures sur un département test (0 API)
    python scripts/fix_data_quality.py --phase 1,2,2.5,3 --dry-run --dept 90

    # Phase 1-2-2.5-3 live sur tout
    python scripts/fix_data_quality.py --phase 1,2,2.5,3

    # Phase 4 sur un batch (pour Cloud Run parallélisé)
    python scripts/fix_data_quality.py --phase 4 --batch-offset 0 --batch-size 500

    # Phase 4+5 dry-run sur un département
    python scripts/fix_data_quality.py --phase 4,5 --dry-run --dept 47

Env vars :
    LLM_PROVIDER   (gemini|mistral, default: gemini)
    GEMINI_API_KEY / MISTRAL_API_KEY
    GEMINI_MODEL   / MISTRAL_MODEL
    SERPER_API_KEY
    DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
import re
import sys
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests

# ── Path setup ───────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from database import DatabaseManager
from enrich_finess_config import SITE_EXCLUSIONS, GEMINI_CONFIG, MISTRAL_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fix_quality")

# ── LLM provider (set in main) ──────────────────────────────────────────────
_llm_provider: str = "gemini"

# ── Domaines clairement non-organisationnels (top fréquences en base) ───────
# Ces domaines apparaissent dans site_web/domaine_mail alors qu'ils ne sont
# PAS les sites officiels des gestionnaires.
KNOWN_BAD_DOMAINS: set[str] = {
    # Annuaires / portails (confirmés par analyse de fréquence — utilisés par >10 gestionnaires différents)
    "fondationdefrance.org",
    "courdecassation.fr",
    "assoce.fr",
    "etablissements.fhf.fr",
    "ccomptes.fr",
    "documentation.ehesp.fr",
    "managersdelactionsociale.fr",
    "fhf.fr",
    "esante-paysdelaloire.fr",
    "entreprises.lagazettefrance.fr",
    "banque-france.fr",
    "fhf-hdf.fr",
    "maitredata.com",
    "theses.hal.science",
    "infonet.fr",
    "cdn.paris.fr",
    "fehap.fr",
    "numerique.banq.qc.ca",
    "yumpu.com",
    "doctrine.fr",
    "maboussoleaidants.fr",
    "adiph.org",
    "lebonehpad.com",
    "cheops-provencealpescotedazur.com",
    "kananas.com",
    "pourunautremodeledesociete.coop",
    "fsa-geneve.ch",
    "cpias-ile-de-france.fr",
    "aprc.asso.fr",
    "dcalin.fr",
    "ueem.umc-europe.org",
    "fr.kompass.com",
    "jorfsearch.steinertriples.ch",
    "zones-humides.org",
    "apps.gestionweblex.ca",
    "fondationcos.org",
    "openedition.org",
    "journals.openedition.org",
    "hal.science",
    "pleugueneuc.com",
    "lesmaisonsderetraite.fr",
    "archivesdepartementales.aude.fr",
    "psmigrants.org",
    "demo.koumoul.com",
    # Ajouts suite à calibrage data (domaines-annuaires multi-gestionnaires)
    "fondationpartageetvie.org",
    "clamart.fr",
    "societe.busyplace.fr",
    "demarchesadministratives.fr",
    "shs.cairn.info",
    "la-mairie.com",
    "dumas.ccsd.cnrs.fr",
    "famillys.fr",
    "valdemarne.fr",
    "cpias-ile-de-france.fr",
    "vie-publique.fr",
    "ville-levallois.fr",
    "solidarites.troyes.fr",
    "chartepnrmillevaches.wordpress.com",
    "chantier.qc.ca",
    "moncompte.departement13.fr",
    "societe.busyplace.fr",
    "france-assos-sante.org",
    "droits-salaries.com",
    "grassrootsjusticenetwork.org",
    "briand0493.wordpress.com",
    "suisse-rando.ch",
    "canada.ca",
    "alzheimersudaisne.e-monsite.com",
    "foyerdesapprentis.ch",
    "allie-social.fr",
    "gerontopole-paysdelaloire.fr",
    "ifms.chu-montpellier.fr",
}

# ── Acronymes sectoriels connus du médico-social ──────────────────────────
# Utilisés pour matcher domaines type "adsea77.org" → ORG "ADSEA" dept 77
SECTOR_ACRONYMS: set[str] = {
    "admr", "adapei", "adsea", "udaf", "apei", "apaei", "apajh",
    "cidff", "ccas", "cias", "cpam", "ugecam", "unapei",
    "asei", "apf", "aria", "gcsms", "ehpad", "eeap",
    "itep", "ime", "esat", "savs", "samsah", "sessad",
    "apeai", "aapei", "epms", "epsms", "epnak",
    "aftc", "adpep", "pep", "alefpa", "coallia",
    "soliha", "afiph", "adsea", "adages", "adpep",
    "alefpa", "ladapt", "afeji", "apahj",
    "unafam", "unapei", "fehap", "nexem",
}

# Noms de famille génériques souvent issus d'artefacts de scraping
GENERIC_LASTNAMES: set[str] = {
    "dupont", "martin", "durand", "bernard", "thomas",
    "robert", "richard", "petit", "moreau", "simon",
}

# ── Mapping re-normalisation fonction_brute → fonction_normalisee ─────────
# Les fonctions brutes ont été sur-normalisées (ex: "Directeur d'établissement" → "Directeur")
FONCTION_RENORM_MAP: list[tuple[str, str, str]] = [
    # (pattern SQL ILIKE, nouvelle valeur normalisée, commentaire)
    # Ordre : du plus spécifique au plus général
    ("Directeur général délégué", "Directeur Général Délégué", "DG délégué"),
    ("Directrice générale adjointe", "Directrice Générale Adjointe", ""),
    ("Directeur Général des Services", "Directeur Général des Services", "DGS"),
    ("Directeur général", "Directeur Général", "était normalisé en Directeur"),
    ("Directrice générale", "Directrice Générale", "était normalisé en Directeur"),
    ("Directeur d'établissement", "Directeur d'Établissement", "fréquent"),
    ("Directrice d'établissement", "Directrice d'Établissement", ""),
    ("Directeur de l'établissement", "Directeur d'Établissement", "variante"),
    ("directeur de l'établissement", "Directeur d'Établissement", "minuscule"),
    ("Directeur d'hôpital", "Directeur d'Hôpital", ""),
    ("Directeur de maison de retraite", "Directeur d'EHPAD", "reformulé"),
    ("Directeur de la publication", "_EXCLURE_", "pas un dirigeant opérationnel"),
    ("Directeur de publication", "_EXCLURE_", "pas un dirigeant opérationnel"),
    ("Team leadership", "_EXCLURE_", "garbage LinkedIn"),
    ("Gérant", "Gérant", "était normalisé en Directeur"),
    ("Centre Communal d'Action Sociale", "_EXCLURE_", "c'est le nom de la structure, pas une fonction"),
]

# Fonctions à exclure : ce ne sont pas des dirigeants/contacts utiles
FONCTIONS_GARBAGE: set[str] = {
    "_EXCLURE_",
}

# ═══════════════════════════════════════════════════════════════════════════════
# API HELPERS (repris de enrich_finess_dept.py pour autonomie du script)
# ═══════════════════════════════════════════════════════════════════════════════

def _clean_secret(value: str) -> str:
    if not value:
        return ""
    return value.replace("\ufeff", "").replace("\x00", "").strip()


def serper_search(query: str, *, num: int = 8, api_key: str) -> List[Dict[str, Any]]:
    if not api_key.strip():
        return []
    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": api_key.strip(), "Content-Type": "application/json"},
            json={"q": query, "num": int(num)},
            timeout=25,
        )
        if r.status_code != 200:
            return []
        data = r.json() or {}
        return [x for x in (data.get("organic") or []) if isinstance(x, dict)]
    except Exception:
        return []


def get_or_search_serper(query: str, api_key: str, cur) -> List[Dict[str, Any]]:
    query_hash = hashlib.sha256(query.encode()).hexdigest()
    cur.execute(
        "SELECT results FROM finess_cache_serper WHERE query_hash = %s AND expire_at > NOW()",
        (query_hash,),
    )
    row = cur.fetchone()
    if row:
        try:
            return json.loads(row["results"]) if isinstance(row["results"], str) else (row["results"] or [])
        except Exception:
            return []

    results = serper_search(query, num=5, api_key=api_key)
    try:
        cur.execute(
            """INSERT INTO finess_cache_serper (query_hash, query_text, results, nb_results)
               VALUES (%s, %s, %s, %s)
               ON CONFLICT (query_hash) DO UPDATE SET
                   results = EXCLUDED.results, nb_results = EXCLUDED.nb_results,
                   date_requete = NOW(), expire_at = NOW() + INTERVAL '30 days'""",
            (query_hash, query, json.dumps(results, ensure_ascii=False), len(results)),
        )
    except Exception:
        pass
    return results


def _call_gemini(api_key: str, model: str, prompt: str, max_tokens: int) -> str:
    model_name = (model or "").strip() or str(GEMINI_CONFIG["model"])
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.15, "maxOutputTokens": max_tokens},
    }
    for attempt in range(5):
        try:
            resp = requests.post(endpoint, json=payload, timeout=60)
            if resp.status_code == 429:
                time.sleep(15 * (attempt + 1) + random.uniform(0, 5))
                continue
            if resp.status_code in {500, 502, 503, 504}:
                time.sleep(2 ** attempt + random.uniform(0, 2))
                continue
            if resp.status_code != 200:
                return ""
            raw = resp.json() or {}
            return (
                raw.get("candidates", [{}])[0]
                .get("content", {}).get("parts", [{}])[0]
                .get("text", "").strip()
            )
        except Exception:
            time.sleep(2 ** attempt)
    return ""


def _call_mistral(api_key: str, model: str, prompt: str, max_tokens: int) -> str:
    model_name = (model or "").strip() or str(MISTRAL_CONFIG["model"])
    headers = {"Content-Type": "application/json; charset=utf-8", "Authorization": f"Bearer {api_key}"}
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt.replace("\ufeff", "").replace("\x00", "")}],
        "temperature": 0.15,
        "max_tokens": max_tokens,
    }
    for attempt in range(5):
        try:
            resp = requests.post(
                "https://api.mistral.ai/v1/chat/completions",
                headers=headers,
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                timeout=60,
            )
            if resp.status_code == 429:
                time.sleep(10 * (attempt + 1) + random.uniform(0, 5))
                continue
            if resp.status_code in {500, 502, 503, 504}:
                time.sleep(2 ** attempt + random.uniform(0, 2))
                continue
            if resp.status_code != 200:
                return ""
            choices = (resp.json() or {}).get("choices", [])
            return (choices[0].get("message", {}).get("content", "") or "").strip() if choices else ""
        except Exception:
            time.sleep(2 ** attempt)
    return ""


def llm_generate(api_key: str, model: str, prompt: str, max_tokens: int = 800) -> str:
    key = _clean_secret(api_key)
    if not key:
        return ""
    if _llm_provider == "gemini":
        return _call_gemini(key, model, prompt, max_tokens)
    return _call_mistral(key, model, prompt, max_tokens)


def llm_json(api_key: str, model: str, prompt: str, max_tokens: int = 800) -> Dict[str, Any]:
    text = llm_generate(api_key, model, prompt, max_tokens)
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        cleaned = re.sub(r"^```json\s*|\s*```$", "", text, flags=re.IGNORECASE).strip()
        try:
            return json.loads(cleaned)
        except Exception:
            return {}


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 : Nettoyage contacts pourris
# ═══════════════════════════════════════════════════════════════════════════════

def phase1_cleanup_contacts(conn, dry_run: bool) -> Dict[str, int]:
    """Supprime les contacts inutilisables et purge les emails reconstruits sur domaines invalides."""
    stats: Dict[str, int] = {}

    with conn.cursor() as cur:
        # 1a. Supprimer dirigeants sans nom ET sans prénom
        cur.execute(
            "SELECT COUNT(*) FROM finess_dirigeant WHERE (nom IS NULL OR LOWER(TRIM(nom)) IN ('', 'null')) "
            "AND (prenom IS NULL OR LOWER(TRIM(prenom)) IN ('', 'null'))"
        )
        stats["null_null_contacts"] = cur.fetchone()[0]

        if not dry_run:
            cur.execute(
                "DELETE FROM finess_dirigeant WHERE (nom IS NULL OR LOWER(TRIM(nom)) IN ('', 'null')) "
                "AND (prenom IS NULL OR LOWER(TRIM(prenom)) IN ('', 'null'))"
            )
            log.info("  Supprimé %d contacts null/null", cur.rowcount)

        # 1b. Supprimer les "Dupont" sans prénom (artefacts scraping — 1401 en base)
        cur.execute(
            "SELECT COUNT(*) FROM finess_dirigeant "
            "WHERE LOWER(TRIM(nom)) = 'dupont' AND (prenom IS NULL OR LOWER(TRIM(prenom)) IN ('', 'null'))"
        )
        stats["dupont_no_prenom"] = cur.fetchone()[0]

        if not dry_run:
            cur.execute(
                "DELETE FROM finess_dirigeant "
                "WHERE LOWER(TRIM(nom)) = 'dupont' AND (prenom IS NULL OR LOWER(TRIM(prenom)) IN ('', 'null'))"
            )
            log.info("  Supprimé %d 'Dupont' sans prénom", cur.rowcount)

        # 1c. Passer en confiance='basse' les noms génériques suspects
        cur.execute(
            "SELECT COUNT(*) FROM finess_dirigeant "
            "WHERE LOWER(TRIM(nom)) IN %s AND confiance != 'basse'",
            (tuple(GENERIC_LASTNAMES),),
        )
        stats["generic_names_flagged"] = cur.fetchone()[0]

        if not dry_run:
            cur.execute(
                "UPDATE finess_dirigeant SET confiance = 'basse' "
                "WHERE LOWER(TRIM(nom)) IN %s AND confiance != 'basse'",
                (tuple(GENERIC_LASTNAMES),),
            )
            log.info("  Flaggé %d noms génériques → confiance=basse", cur.rowcount)

        # 1d. Purger TOUS les email_reconstitue (construits sur domaines invalides à ~90%)
        cur.execute("SELECT COUNT(*) FROM finess_dirigeant WHERE email_reconstitue IS NOT NULL")
        stats["emails_purged"] = cur.fetchone()[0]

        if not dry_run:
            cur.execute("UPDATE finess_dirigeant SET email_reconstitue = NULL WHERE email_reconstitue IS NOT NULL")
            log.info("  Purgé %d emails reconstruits (domaines non fiables)", cur.rowcount)

        # 1e. Supprimer dirigeants avec fonction 'null'
        cur.execute("SELECT COUNT(*) FROM finess_dirigeant WHERE fonction_normalisee = 'null'")
        stats["null_function"] = cur.fetchone()[0]

        if not dry_run:
            cur.execute("DELETE FROM finess_dirigeant WHERE fonction_normalisee = 'null'")
            log.info("  Supprimé %d contacts fonction='null'", cur.rowcount)

    if not dry_run:
        conn.commit()
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 : Purge domaines invalides
# ═══════════════════════════════════════════════════════════════════════════════

def _is_bad_domain(domain: str) -> bool:
    """Vérifie si un domaine est clairement non-organisationnel."""
    if not domain:
        return True
    d = domain.lower().strip()

    # Domaines explicitement connus comme mauvais
    if d in KNOWN_BAD_DOMAINS:
        return True

    # Domaines dans la liste d'exclusions du pipeline
    if any(excl in d for excl in SITE_EXCLUSIONS):
        return True

    return False


def _is_bad_site_web(url: str) -> bool:
    """Vérifie si un site_web est un lien profond, un PDF, ou un annuaire."""
    if not url:
        return True
    u = url.lower().strip()

    # Lien direct vers un document
    if any(u.endswith(ext) for ext in (".pdf", ".ods", ".xlsx", ".xls", ".doc", ".docx", ".csv")):
        return True

    # URL avec trop de segments = deep link vers une page spécifique
    path = urlparse(u).path
    segments = [s for s in path.split("/") if s]
    if len(segments) > 3:
        return True

    # Domaine dans les exclusions
    netloc = urlparse(u).netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    if any(excl in netloc for excl in SITE_EXCLUSIONS):
        return True
    if netloc in KNOWN_BAD_DOMAINS:
        return True

    return False


def phase2_purge_domains(conn, dry_run: bool, dept_filter: str = None) -> Dict[str, int]:
    """Purge les domaines invalides avec matching sophistiqué.

    Stratégie :
      2a. Blacklist : domaines connus comme annuaires/portails -> NULL
      2b. Multi-gestionnaire sans match acronyme/nom -> NULL
      2c. Mono-gestionnaire sans match -> NULL (probablement faux aussi)
    """
    stats: Dict[str, int] = {}

    with conn.cursor() as cur:
        query = (
            "SELECT id_gestionnaire, domaine_mail, site_web, raison_sociale, sigle, departement_code, commune "
            "FROM finess_gestionnaire "
            "WHERE domaine_mail IS NOT NULL AND domaine_mail != ''"
        )
        params: list = []
        if dept_filter:
            query += " AND departement_code = %s"
            params.append(dept_filter)

        cur.execute(query, params)
        rows = cur.fetchall()
        stats["total_with_domain"] = len(rows)

        # 2a. Blacklist explicite
        blacklist_ids: list[str] = []
        remaining: list[tuple] = []
        for gid, dom, site, rs, sigle, dept, commune in rows:
            if _is_bad_domain(dom) or _is_bad_site_web(site or ""):
                blacklist_ids.append(gid)
            else:
                remaining.append((gid, dom, site, rs, sigle, dept, commune))
        stats["blacklisted"] = len(blacklist_ids)

        # 2b. Compter les domaines multi-gestionnaires
        from collections import Counter
        dom_counts = Counter(r[1].lower() for r in remaining)

        multi_good_ids: list[str] = []
        multi_bad_ids: list[str] = []
        single_good_ids: list[str] = []
        single_bad_ids: list[str] = []

        for gid, dom, site, rs, sigle, dept, commune in remaining:
            matches = _domain_matches_org(dom, rs, sigle, dept, commune=commune)
            if dom_counts[dom.lower()] > 5:
                (multi_good_ids if matches else multi_bad_ids).append(gid)
            else:
                (single_good_ids if matches else single_bad_ids).append(gid)

        stats["multi_gest_matched"] = len(multi_good_ids)
        stats["multi_gest_purged"] = len(multi_bad_ids)
        stats["single_matched"] = len(single_good_ids)
        stats["single_no_match_purged"] = len(single_bad_ids)

        purge_ids = blacklist_ids + multi_bad_ids + single_bad_ids
        stats["total_purged"] = len(purge_ids)
        stats["valid_domains_remaining"] = len(multi_good_ids) + len(single_good_ids)

        if not dry_run and purge_ids:
            batch_sz = 500
            for i in range(0, len(purge_ids), batch_sz):
                batch = purge_ids[i : i + batch_sz]
                cur.execute(
                    "UPDATE finess_gestionnaire SET domaine_mail = NULL, structure_mail = NULL, "
                    "site_web = NULL WHERE id_gestionnaire = ANY(%s)",
                    (batch,),
                )
            log.info("  Purgé %d domaines (blacklist=%d, multi_no_match=%d, single_no_match=%d)",
                     len(purge_ids), len(blacklist_ids), len(multi_bad_ids), len(single_bad_ids))

    if not dry_run:
        conn.commit()
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2.5 : Re-normalisation des fonctions depuis fonction_brute
# ═══════════════════════════════════════════════════════════════════════════════

def phase25_renormalize_functions(conn, dry_run: bool, dept_filter: str = None) -> Dict[str, int]:
    """Re-normalise les fonctions vagues ('Directeur') depuis la fonction_brute originale.

    Corrige la sur-normalisation qui a écrasé l'information utile.
    Supprime aussi les dirigeants dont la fonction est un garbage (pas un vrai rôle).
    """
    stats: Dict[str, int] = {"renormalized": 0, "excluded": 0, "unchanged": 0}

    with conn.cursor() as cur:
        # Sélectionner les dirigeants avec fonction vague
        query = """
            SELECT d.id, d.fonction_brute, d.fonction_normalisee
            FROM finess_dirigeant d
        """
        if dept_filter:
            query += """
            JOIN finess_gestionnaire g ON g.id_gestionnaire = d.id_gestionnaire
            WHERE d.fonction_normalisee IN ('Directeur', 'Directrice')
              AND d.fonction_brute IS NOT NULL
              AND g.departement_code = %s
            """
            cur.execute(query, (dept_filter,))
        else:
            query += """
            WHERE d.fonction_normalisee IN ('Directeur', 'Directrice')
              AND d.fonction_brute IS NOT NULL
            """
            cur.execute(query)

        rows = cur.fetchall()
        log.info("  %d dirigeants avec fonction vague à re-normaliser", len(rows))

        renorm_updates: list[tuple] = []  # (new_norm, id)
        exclude_ids: list[int] = []

        for did, brute, current_norm in rows:
            if not brute:
                stats["unchanged"] += 1
                continue

            brute_lower = brute.lower().strip()
            new_norm = None

            # Appliquer le mapping de re-normalisation
            for pattern, target, _comment in FONCTION_RENORM_MAP:
                if pattern.lower() in brute_lower:
                    new_norm = target
                    break

            if new_norm == "_EXCLURE_":
                exclude_ids.append(did)
                stats["excluded"] += 1
            elif new_norm and new_norm != current_norm:
                renorm_updates.append((new_norm, did))
                stats["renormalized"] += 1
            else:
                stats["unchanged"] += 1

        if not dry_run:
            # Appliquer les re-normalisations
            if renorm_updates:
                for new_norm, did in renorm_updates:
                    cur.execute(
                        "UPDATE finess_dirigeant SET fonction_normalisee = %s WHERE id = %s",
                        (new_norm, did),
                    )
                log.info("  Re-normalisé %d fonctions", len(renorm_updates))

            # Supprimer les garbage
            if exclude_ids:
                batch_sz = 500
                for i in range(0, len(exclude_ids), batch_sz):
                    batch = exclude_ids[i : i + batch_sz]
                    cur.execute(
                        "DELETE FROM finess_dirigeant WHERE id = ANY(%s)",
                        (batch,),
                    )
                log.info("  Supprimé %d entrées garbage (publication, CCAS, LinkedIn noise)", len(exclude_ids))

            conn.commit()

    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 : Dédoublonnage DG/Président
# ═══════════════════════════════════════════════════════════════════════════════

def phase3_dedup_leaders(conn, dry_run: bool, dept_filter: str = None) -> Dict[str, int]:
    """Garde un seul DG et un seul Président par gestionnaire.

    Aussi nettoie les entrées qui ne sont pas des personnes réelles
    (ex: prenom=NULL, nom="Président du Conseil départemental").
    """
    stats: Dict[str, int] = {}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 3a. Supprimer les non-personnes (nom = titre/institution, pas un nom propre)
        garbage_query = """
            SELECT d.id FROM finess_dirigeant d
        """
        if dept_filter:
            garbage_query += " JOIN finess_gestionnaire g ON g.id_gestionnaire = d.id_gestionnaire"
        garbage_query += """
            WHERE (d.prenom IS NULL OR TRIM(d.prenom) = '')
              AND d.nom IS NOT NULL
              AND (
                d.nom ILIKE '%%Président%%'
                OR d.nom ILIKE '%%Conseil%%'
                OR d.nom ILIKE '%%Université%%'
                OR d.nom ILIKE '%%Département%%'
                OR LENGTH(d.nom) > 50
              )
        """
        if dept_filter:
            garbage_query += " AND g.departement_code = %s"
            cur.execute(garbage_query, (dept_filter,))
        else:
            cur.execute(garbage_query)

        garbage = cur.fetchall()
        stats["non_person_entries"] = len(garbage)

        if not dry_run and garbage:
            gids = [g["id"] for g in garbage]
            batch_sz = 500
            for i in range(0, len(gids), batch_sz):
                batch = gids[i : i + batch_sz]
                cur.execute("DELETE FROM finess_dirigeant WHERE id = ANY(%s)", (batch,))
            log.info("  Supprimé %d entrées non-personnes", len(gids))

        # 3b. Dédupliquer pour chaque rôle clé
        for role_label, pattern in [
            ("DG", "%directeur g%"),
            ("DG_exact", "DG"),
            ("Président", "%président%"),
        ]:
            # Build query with proper parameterization (avoid raw % in SQL)
            dup_query = "SELECT d.id_gestionnaire, COUNT(*) as nb FROM finess_dirigeant d"
            if dept_filter:
                dup_query += " JOIN finess_gestionnaire g ON g.id_gestionnaire = d.id_gestionnaire"

            params: list = []
            if role_label == "DG_exact":
                dup_query += " WHERE d.fonction_normalisee = %s"
                params.append("DG")
            else:
                dup_query += " WHERE d.fonction_normalisee ILIKE %s"
                params.append(pattern)

            if dept_filter:
                dup_query += " AND g.departement_code = %s"
                params.append(dept_filter)
            dup_query += " GROUP BY d.id_gestionnaire HAVING COUNT(*) > 1"

            cur.execute(dup_query, params)

            dups = cur.fetchall()
            total_dups = sum(r["nb"] - 1 for r in dups)
            stats[f"dedup_{role_label}_gestionnaires"] = len(dups)
            stats[f"dedup_{role_label}_removed"] = total_dups

            if not dry_run and dups:
                # Pour chaque gestionnaire avec doublons, garder le meilleur
                for dup in dups:
                    gid = dup["id_gestionnaire"]

                    cur.execute("""
                        WITH ranked AS (
                            SELECT id, ROW_NUMBER() OVER (
                                ORDER BY
                                    CASE source_type
                                        WHEN 'site_officiel' THEN 1
                                        WHEN 'linkedin_serper' THEN 2
                                        WHEN 'web_serper' THEN 3
                                        ELSE 4
                                    END,
                                    CASE confiance
                                        WHEN 'haute' THEN 1
                                        WHEN 'moyenne' THEN 2
                                        WHEN 'basse' THEN 3
                                        ELSE 4
                                    END,
                                    CASE WHEN linkedin_url IS NOT NULL THEN 0 ELSE 1 END,
                                    CASE WHEN prenom IS NOT NULL AND nom IS NOT NULL THEN 0 ELSE 1 END,
                                    id ASC
                            ) as rn
                            FROM finess_dirigeant
                            WHERE id_gestionnaire = %s AND fonction_normalisee ILIKE %s
                        )
                        DELETE FROM finess_dirigeant
                        WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
                    """, (gid, "DG" if role_label == "DG_exact" else pattern))

                log.info("  Dédupliqué %s : %d gestionnaires, %d doublons supprimés",
                         role_label, len(dups), total_dups)

    if not dry_run:
        conn.commit()
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4 : Découverte domaine officiel (Serper + LLM, parallélisable)
# ═══════════════════════════════════════════════════════════════════════════════

def _extract_org_domain(url: str) -> Optional[str]:
    """Extrait le domaine d'une URL en filtrant les annuaires."""
    if not url:
        return None
    netloc = urlparse(url).netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    if not netloc or "." not in netloc:
        return None
    if _is_bad_domain(netloc):
        return None
    return netloc


def _domain_matches_org(domain: str, raison_sociale: str,
                        sigle: str = None, dept_code: str = None,
                        commune: str = None) -> bool:
    """Heuristique sophistiquée : le domaine correspond-il à l'organisation ?

    Gère :
    - Match direct (mot significatif ≥4 chars dans le domaine)
    - Acronymes sectoriels (ADSEA → adsea77.org)
    - Sigles (ATAL → atal.asso.fr)
    - Préfixes Centre Hospitalier (CH- → ch-pau.fr)
    - Acronyme construit + code département (adapei77, admr47…)
    - Noms composés (fondation-gcoulon.fr → Fondation Georges Coulon)
    """
    if not domain or not raison_sociale:
        return False

    # Normaliser
    rs_lower = raison_sociale.lower()
    rs_nfd = unicodedata.normalize("NFD", rs_lower)
    rs_ascii = "".join(c for c in rs_nfd if unicodedata.category(c) != "Mn")
    rs_clean = re.sub(r"[^a-z0-9]", "", rs_ascii)

    dom_parts = domain.split(".")
    dom_base = dom_parts[0].lower()
    dom_base_clean = re.sub(r"[^a-z0-9]", "", dom_base)
    dom_base_alpha = re.sub(r"[^a-z]", "", dom_base)
    dom_base_no_dash = dom_base.replace("-", "")

    # ── Stop words à ignorer dans la raison sociale ──
    stop_rs = {"association", "fondation", "centre", "maison", "ehpad", "foyer",
               "residence", "institut", "service", "services", "action", "sociale",
               "communal", "intercommunal", "departement", "departemental",
               "hebergement", "personnes", "agees", "handicapees",
               "les", "des", "pour", "dans", "par", "sur", "avec", "accueil",
               "etablissement", "public", "medico", "social",
               "gestion", "education", "insertion", "protection", "enfance",
               "adulte", "adultes", "jeunes", "aide", "travail", "mutuelle",
               "regionale", "regional", "nationale", "national", "generale",
               "laique", "protestante", "catholique", "solidarite",
               "habitat", "logement", "accompagnement", "readaptation",
               "prevention", "promotion", "animation", "formation"}

    # Normaliser commune pour éviter faux positifs (ex: "belfort" dans "territoiredebelfort")
    commune_words: set[str] = set()
    if commune:
        commune_nfd = unicodedata.normalize("NFD", commune.lower())
        commune_ascii = "".join(c for c in commune_nfd if unicodedata.category(c) != "Mn")
        commune_words = {w for w in re.findall(r"[a-z]{4,}", commune_ascii)}

    # ── 1. Match direct : mot significatif de la RS dans le domaine ──
    rs_words = set(re.findall(r"[a-z]{3,}", rs_ascii))
    rs_words -= stop_rs
    matched_words = []
    for word in rs_words:
        w_clean = re.sub(r"[^a-z]", "", word)
        if len(w_clean) >= 4 and w_clean in dom_base_clean:
            matched_words.append(w_clean)
    # Si seuls les mots matchés sont des noms de commune → pas de match
    if matched_words:
        non_commune = [w for w in matched_words if w not in commune_words]
        if non_commune:
            return True

    # ── 2. Match sigle (colonne sigle ou acronyme du nom) ──
    if sigle:
        sigle_nfd = unicodedata.normalize("NFD", sigle.lower())
        sigle_ascii = "".join(c for c in sigle_nfd if unicodedata.category(c) != "Mn")
        sigle_clean = re.sub(r"[^a-z0-9]", "", sigle_ascii)
        if len(sigle_clean) >= 3 and sigle_clean in dom_base_clean:
            return True
        if dept_code and len(sigle_clean) >= 3:
            if (sigle_clean + dept_code) in dom_base_clean:
                return True

    # ── 3. Acronyme construit depuis la raison sociale ──
    # Ex: "Association Départementale Sauvegarde Enfance Adolescence" → ADSEA
    words_for_acro = re.findall(r"[A-ZÀ-Ü][a-zà-ü]*|[A-ZÀ-Ü]+", raison_sociale)
    if not words_for_acro:
        words_for_acro = raison_sociale.split()
    stop_acro = {"de", "du", "des", "la", "le", "les", "l", "et", "en", "a", "au", "aux",
                 "d", "pour", "par", "sur", "dans", "avec", "un", "une", "son", "sa"}
    acronym = "".join(
        w[0].lower() for w in words_for_acro
        if w.lower() not in stop_acro and len(w) > 1
    )
    if len(acronym) >= 3:
        if acronym in dom_base_clean:
            return True
        if dept_code:
            if (acronym + dept_code) in dom_base_clean:
                return True
            if len(dept_code) >= 2 and (acronym + dept_code[:2]) in dom_base_clean:
                return True

    # ── 4. Acronymes sectoriels connus ──
    # Ex: "ADMR DE JARNAC" → domaine admr.org → match via secteur
    for sector_acro in SECTOR_ACRONYMS:
        if sector_acro in rs_ascii.split() or rs_ascii.startswith(sector_acro + " "):
            if sector_acro in dom_base_clean:
                return True
            if dept_code and (sector_acro + dept_code) in dom_base_clean:
                return True

    # ── 5. Préfixe CH- pour Centre Hospitalier ──
    ch_patterns = ["centre hospitalier", "ch "]
    if any(p in rs_ascii for p in ch_patterns):
        if dom_base.startswith("ch-") or dom_base.startswith("ch"):
            # Vérifier que le reste matche une partie du nom
            rest = dom_base_clean.replace("ch", "", 1)
            if rest and len(rest) >= 3:
                # Chercher le nom de ville dans la RS
                for word in rs_words:
                    w = re.sub(r"[^a-z]", "", word)
                    if len(w) >= 3 and w in rest:
                        return True

    # ── 6. RS préfixe dans le domaine (pour noms courts) ──
    if len(rs_clean) >= 5 and rs_clean[:8] in dom_base_clean:
        return True

    # ── 7. Nom de commune dans le domaine (CCAS DE <ville> → ville.fr) ──
    # Extraire la commune potentielle après des mots-clés
    commune_match = re.search(
        r"(?:ccas|cias|mairie|ville)\s+(?:de\s+|d['\u2019]\s*)?(.+)",
        rs_ascii, re.IGNORECASE,
    )
    if commune_match:
        commune_name = re.sub(r"[^a-z]", "", commune_match.group(1).split()[0] if commune_match.group(1).split() else "")
        if len(commune_name) >= 4 and commune_name in dom_base_clean:
            return True

    return False


PROMPT_VALIDATE_DOMAIN = """Tu dois déterminer si un site web est le site OFFICIEL d'un organisme gestionnaire ESSMS (médico-social).

Organisme : "{raison_sociale}"
Commune : {commune}
Département : {departement}
Site web candidat : {url}
Domaine : {domain}

Extrait du titre/snippet de la page trouvée sur Google :
{snippet}

Réponds UNIQUEMENT en JSON :
{{
  "is_official": true/false,
  "confiance": "haute" | "moyenne" | "basse",
  "raison": "<courte justification>"
}}

Critères :
- Le site doit être celui de L'ORGANISME LUI-MÊME (pas un annuaire, pas un article de presse, pas une fiche externe)
- Le domaine doit correspondre au nom ou sigle de l'organisme
- Un site .gouv.fr, .asso.fr avec le nom de l'organisme est bon signe
- Un PDF, un annuaire, un portail tiers = false
"""


def discover_domain_for_gestionnaire(
    gest: Dict[str, Any],
    serper_key: str,
    llm_key: str,
    llm_model: str,
    cur,
    dry_run: bool,
) -> Optional[str]:
    """Tente de découvrir le domaine officiel d'un gestionnaire."""
    gid = gest["id_gestionnaire"]
    rs = gest["raison_sociale"] or ""
    commune = gest.get("commune") or ""
    dept = gest.get("departement_nom") or ""
    sigle = gest.get("sigle") or ""
    dept_code = gest.get("departement_code") or ""

    # ── Stratégie 1 : site_web existant en base valide ──
    existing_site = gest.get("site_web") or ""
    if existing_site:
        dom = _extract_org_domain(existing_site)
        if dom and _domain_matches_org(dom, rs, sigle, dept_code, commune=commune):
            _store_domain(cur, gid, dom, "site_existant_valide", dry_run)
            return dom

    # ── Stratégie 2 : Serper simple (sans guillemets — robuste aux noms longs) ──
    query = f"{rs} {commune} site officiel".strip() if commune else f"{rs} site officiel"
    results = get_or_search_serper(query, serper_key, cur)

    candidates: list[Dict[str, Any]] = []
    for r in results:
        url = r.get("link", "")
        dom = _extract_org_domain(url)
        if dom:
            candidates.append({
                "url": url,
                "domain": dom,
                "title": r.get("title", ""),
                "snippet": r.get("snippet", ""),
                "name_match": _domain_matches_org(dom, rs, sigle, dept_code, commune=commune),
            })

    # Fast path : name_match direct → pas besoin de LLM
    for c in candidates:
        if c["name_match"]:
            _store_domain(cur, gid, c["domain"], "serper_name_match", dry_run)
            log.debug("  [%s] Domaine trouvé par name match: %s", gid, c["domain"])
            return c["domain"]

    # ── Stratégie 3 : LLM sur les 2 premiers candidats ──
    if candidates and llm_key:
        for c in candidates[:2]:
            prompt = PROMPT_VALIDATE_DOMAIN.format(
                raison_sociale=rs,
                commune=commune,
                departement=dept,
                url=c["url"],
                domain=c["domain"],
                snippet=f'{c["title"]} — {c["snippet"]}'[:500],
            )
            result = llm_json(llm_key, llm_model, prompt, max_tokens=300)
            if result.get("is_official") and result.get("confiance") in ("haute", "moyenne"):
                _store_domain(cur, gid, c["domain"], f"llm_validated_{result.get('confiance')}", dry_run)
                log.debug("  [%s] Domaine validé par LLM: %s (%s)", gid, c["domain"], result.get("raison", ""))
                return c["domain"]

    # ── Stratégie 4 : Sigle explicite si 0 candidats ──
    if sigle and len(sigle) >= 3 and not candidates:
        query2 = f"{sigle} {commune} site officiel".strip()
        results2 = get_or_search_serper(query2, serper_key, cur)
        for r in results2:
            dom = _extract_org_domain(r.get("link", ""))
            if dom and _domain_matches_org(dom, rs, sigle, dept_code, commune=commune):
                _store_domain(cur, gid, dom, "serper_sigle_match", dry_run)
                return dom

    return None


def _store_domain(cur, gid: str, domain: str, source: str, dry_run: bool):
    if dry_run:
        return
    cur.execute(
        "UPDATE finess_gestionnaire SET domaine_mail = %s, "
        "source_enrichissement = COALESCE(source_enrichissement, '') || %s, "
        "date_enrichissement = NOW() "
        "WHERE id_gestionnaire = %s",
        (domain, f" domain:{source}", gid),
    )


def phase4_discover_domains(
    conn, serper_key: str, llm_key: str, llm_model: str,
    batch_offset: int, batch_size: int, dry_run: bool,
    dept_filter: str = None,
) -> Dict[str, int]:
    """Découvre les domaines officiels pour les gestionnaires qui n'en ont pas."""
    stats: Dict[str, int] = {"processed": 0, "found": 0, "not_found": 0, "errors": 0}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        query = """
            SELECT id_gestionnaire, raison_sociale, sigle, commune,
                   departement_nom, departement_code, site_web, siren
            FROM finess_gestionnaire
            WHERE (domaine_mail IS NULL OR domaine_mail = '')
              AND enrichissement_statut = 'enrichi'
        """
        params: list = []
        if dept_filter:
            query += " AND departement_code = %s"
            params.append(dept_filter)
        query += " ORDER BY id_gestionnaire OFFSET %s LIMIT %s"
        params.extend([batch_offset, batch_size])

        cur.execute(query, params)

        gestionnaires = cur.fetchall()
        total = len(gestionnaires)
        log.info("Phase 4 : %d gestionnaires à traiter (offset=%d, batch=%d)",
                 total, batch_offset, batch_size)

        for i, gest in enumerate(gestionnaires):
            gid = gest["id_gestionnaire"]
            try:
                domain = discover_domain_for_gestionnaire(
                    gest, serper_key, llm_key, llm_model, cur, dry_run,
                )
                if domain:
                    stats["found"] += 1
                else:
                    stats["not_found"] += 1
            except Exception as e:
                log.warning("  [%s] Erreur: %s", gid, e)
                stats["errors"] += 1

            stats["processed"] += 1
            if (i + 1) % 50 == 0:
                log.info("  Phase 4 progression: %d/%d (found=%d)", i + 1, total, stats["found"])
                if not dry_run:
                    conn.commit()

    if not dry_run:
        conn.commit()
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 5 : Reconstruction emails (sans fallback prenom.nom)
# ═══════════════════════════════════════════════════════════════════════════════

def _is_person_email(email: str) -> bool:
    generic = {"contact", "info", "accueil", "direction", "secretariat",
               "admin", "rh", "communication", "compta", "comptabilite",
               "standard", "reception", "candidature", "recrutement",
               "noreply", "no-reply"}
    local = email.split("@")[0].lower().replace(".", "").replace("-", "").replace("_", "")
    return local not in generic and len(local) > 2


def _normalise_for_email(text: str) -> str:
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9]", "", text)
    return text


def _reconstituer_email(prenom: str, nom: str, structure: str, domaine: str) -> Optional[str]:
    if not prenom or not nom or not structure or not domaine:
        return None
    p = _normalise_for_email(prenom)
    n = _normalise_for_email(nom)
    if not p or not n:
        return None
    templates = {
        "prenom.nom": f"{p}.{n}@{domaine}",
        "p.nom": f"{p[0]}.{n}@{domaine}",
        "nom.prenom": f"{n}.{p}@{domaine}",
        "prenom-nom": f"{p}-{n}@{domaine}",
        "prenom_nom": f"{p}_{n}@{domaine}",
    }
    return templates.get(structure)


def detect_and_reconstruct_emails(
    gid: str,
    domaine: str,
    raison_sociale: str,
    serper_key: str,
    llm_key: str,
    llm_model: str,
    cur,
    dry_run: bool,
) -> Tuple[int, Optional[str]]:
    """Détecte le pattern email et reconstruit pour les dirigeants du gestionnaire.

    Retourne (nb_emails_reconstruits, structure_detectee).
    NE FAIT PAS de fallback prenom.nom si aucun pattern détecté.
    """
    # 5a. Chercher des emails réels "@domaine" via Serper
    found_emails: set[str] = set()
    if serper_key:
        for q in [f'"@{domaine}"', f'"@{domaine}" "{raison_sociale}"']:
            results = get_or_search_serper(q, serper_key, cur)
            for r in results:
                text = f'{r.get("snippet", "")} {r.get("title", "")} {r.get("link", "")}'
                matches = re.findall(
                    r"[a-zA-Z0-9._%+-]+@" + re.escape(domaine), text, re.IGNORECASE,
                )
                found_emails.update(m.lower() for m in matches)

    person_emails = [e for e in found_emails if _is_person_email(e)]

    # 5b. Détection pattern rule-based
    structure = None
    confiance = "basse"

    if person_emails:
        from collections import Counter
        patterns: list[str] = []
        for email in person_emails:
            local = email.split("@")[0]
            if "." in local:
                parts = local.split(".")
                if len(parts) == 2:
                    patterns.append("p.nom" if len(parts[0]) == 1 else "prenom.nom")
                else:
                    patterns.append("prenom.nom")
            elif "-" in local:
                patterns.append("prenom-nom")
            elif "_" in local:
                patterns.append("prenom_nom")

        if patterns:
            most_common = Counter(patterns).most_common(1)[0]
            structure = most_common[0]
            confiance = "haute" if most_common[1] >= 2 else "moyenne"

    # 5c. LLM qualification si exemples mais pattern ambigu
    if person_emails and llm_key and confiance == "basse":
        examples_str = "\n".join(f"  - {e}" for e in person_emails[:10])
        prompt = f"""Analyse ces adresses email trouvées au domaine @{domaine} :
{examples_str}

Identifie le modèle de construction des emails personnels.
Réponds UNIQUEMENT en JSON :
{{
  "structure": "<prenom.nom | p.nom | nom.prenom | prenom-nom | prenom_nom>",
  "confiance": "<haute | moyenne | basse>"
}}"""
        result = llm_json(llm_key, llm_model, prompt, max_tokens=200)
        llm_struct = (result.get("structure") or "").strip().lower()
        valid = {"prenom.nom", "p.nom", "nom.prenom", "prenom-nom", "prenom_nom"}
        if llm_struct in valid:
            structure = llm_struct
            confiance = result.get("confiance", "moyenne")

    # 5d. PAS DE FALLBACK — si on n'a pas de pattern fiable, on ne reconstruit pas
    if not structure or confiance == "basse":
        # Stocker quand même le domaine sans pattern
        if not dry_run:
            cur.execute(
                "UPDATE finess_gestionnaire SET structure_mail = NULL WHERE id_gestionnaire = %s",
                (gid,),
            )
        return 0, None

    # 5e. Stocker le pattern détecté
    if not dry_run:
        cur.execute(
            "UPDATE finess_gestionnaire SET structure_mail = %s WHERE id_gestionnaire = %s",
            (structure, gid),
        )

    # 5f. Reconstruire les emails des dirigeants
    cur.execute("""
        SELECT id, prenom, nom FROM finess_dirigeant
        WHERE id_gestionnaire = %s
          AND email_reconstitue IS NULL
          AND prenom IS NOT NULL AND LOWER(TRIM(prenom)) NOT IN ('', 'null')
          AND nom IS NOT NULL AND LOWER(TRIM(nom)) NOT IN ('', 'null')
          AND confiance IN ('haute', 'moyenne')
    """, (gid,))
    dirigeants = cur.fetchall()

    count = 0
    for d in dirigeants:
        email = _reconstituer_email(d[1], d[2], structure, domaine)
        if email and not dry_run:
            cur.execute(
                "UPDATE finess_dirigeant SET email_reconstitue = %s, email_organisation = %s WHERE id = %s",
                (email, f"@{domaine}", d[0]),
            )
            count += 1
        elif email:
            count += 1

    return count, structure


def phase5_reconstruct_emails(
    conn, serper_key: str, llm_key: str, llm_model: str,
    batch_offset: int, batch_size: int, dry_run: bool,
) -> Dict[str, int]:
    """Reconstruit les emails pour les gestionnaires avec domaine validé."""
    stats: Dict[str, int] = {
        "processed": 0, "with_pattern": 0, "no_pattern": 0,
        "emails_built": 0, "errors": 0,
    }

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id_gestionnaire, domaine_mail, raison_sociale
            FROM finess_gestionnaire
            WHERE domaine_mail IS NOT NULL AND domaine_mail != ''
              AND (structure_mail IS NULL OR structure_mail = '')
            ORDER BY id_gestionnaire
            OFFSET %s LIMIT %s
        """, (batch_offset, batch_size))

        gestionnaires = cur.fetchall()
        total = len(gestionnaires)
        log.info("Phase 5 : %d gestionnaires à traiter (offset=%d)", total, batch_offset)

    # Utiliser un curseur standard pour les updates
    with conn.cursor() as cur:
        for i, gest in enumerate(gestionnaires):
            gid = gest["id_gestionnaire"]
            try:
                nb, structure = detect_and_reconstruct_emails(
                    gid, gest["domaine_mail"], gest["raison_sociale"] or "",
                    serper_key, llm_key, llm_model, cur, dry_run,
                )
                if structure:
                    stats["with_pattern"] += 1
                    stats["emails_built"] += nb
                else:
                    stats["no_pattern"] += 1
            except Exception as e:
                log.warning("  [%s] Erreur phase 5: %s", gid, e)
                stats["errors"] += 1

            stats["processed"] += 1
            if (i + 1) % 50 == 0:
                log.info("  Phase 5 progression: %d/%d (emails=%d)", i + 1, total, stats["emails_built"])
                if not dry_run:
                    conn.commit()

    if not dry_run:
        conn.commit()
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Correction qualité données FINESS")
    p.add_argument("--phase", default="all",
                   help="Phase(s) à exécuter : 1,2,2.5,3,4,5 ou 'all' ou '1,2,3' (défaut: all)")
    p.add_argument("--dry-run", action="store_true",
                   help="Ne pas écrire en base, afficher les stats seulement")
    p.add_argument("--dept", default=None,
                   help="Code département pour filtrer (ex: 90, 47). Test sur un seul dept.")
    p.add_argument("--batch-offset", type=int, default=0,
                   help="Offset pour phases 4/5 (parallélisation Cloud Run)")
    p.add_argument("--batch-size", type=int, default=0,
                   help="Taille du batch pour phases 4/5 (0 = tout)")
    return p.parse_args()


def main():
    global _llm_provider
    args = parse_args()

    # Déterminer les phases à exécuter
    if args.phase.lower() == "all":
        phases = {1, 2, 2.5, 3, 4, 5}
    else:
        phases = set()
        for x in args.phase.split(","):
            x = x.strip()
            if x == "2.5":
                phases.add(2.5)
            elif x.isdigit():
                phases.add(int(x))

    dept_filter = args.dept

    # Config LLM/Serper
    _llm_provider = os.getenv("LLM_PROVIDER", "gemini").lower()
    serper_key = _clean_secret(os.getenv("SERPER_API_KEY", ""))
    if _llm_provider == "gemini":
        llm_key = _clean_secret(os.getenv("GEMINI_API_KEY", ""))
        llm_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    else:
        llm_key = _clean_secret(os.getenv("MISTRAL_API_KEY", ""))
        llm_model = os.getenv("MISTRAL_MODEL", "mistral-small-latest")

    batch_offset = args.batch_offset
    batch_size = args.batch_size if args.batch_size > 0 else 999999

    mode = "DRY-RUN" if args.dry_run else "LIVE"
    log.info("=" * 60)
    log.info("FIX DATA QUALITY — phases=%s mode=%s dept=%s", phases, mode, dept_filter or "ALL")
    log.info("  LLM=%s serper=%s batch=%d+%d", _llm_provider, "OK" if serper_key else "NONE", batch_offset, batch_size)
    log.info("=" * 60)

    db = DatabaseManager()
    all_stats: Dict[str, Any] = {}

    with db.get_connection() as conn:
        # Phase 1 : Nettoyage contacts
        if 1 in phases:
            log.info("━━━ PHASE 1 : Nettoyage contacts ━━━")
            s = phase1_cleanup_contacts(conn, args.dry_run)
            all_stats["phase1"] = s
            log.info("  Résultat: %s", json.dumps(s, indent=2))

        # Phase 2 : Purge domaines (avec matching sophistiqué)
        if 2 in phases:
            log.info("━━━ PHASE 2 : Purge domaines invalides ━━━")
            s = phase2_purge_domains(conn, args.dry_run, dept_filter)
            all_stats["phase2"] = s
            log.info("  Résultat: %s", json.dumps(s, indent=2))

        # Phase 2.5 : Re-normalisation fonctions
        if 2.5 in phases:
            log.info("━━━ PHASE 2.5 : Re-normalisation fonctions ━━━")
            s = phase25_renormalize_functions(conn, args.dry_run, dept_filter)
            all_stats["phase2_5"] = s
            log.info("  Résultat: %s", json.dumps(s, indent=2))

        # Phase 3 : Dédoublonnage
        if 3 in phases:
            log.info("━━━ PHASE 3 : Dédoublonnage DG/Président ━━━")
            s = phase3_dedup_leaders(conn, args.dry_run, dept_filter)
            all_stats["phase3"] = s
            log.info("  Résultat: %s", json.dumps(s, indent=2))

        # Phase 4 : Découverte domaines (API)
        if 4 in phases:
            if not serper_key:
                log.warning("  SERPER_API_KEY absent — phase 4 ignorée")
            else:
                log.info("━━━ PHASE 4 : Découverte domaines officiels ━━━")
                s = phase4_discover_domains(
                    conn, serper_key, llm_key, llm_model,
                    batch_offset, batch_size, args.dry_run, dept_filter,
                )
                all_stats["phase4"] = s
                log.info("  Résultat: %s", json.dumps(s, indent=2))

        # Phase 5 : Reconstruction emails (API)
        if 5 in phases:
            log.info("━━━ PHASE 5 : Reconstruction emails ━━━")
            s = phase5_reconstruct_emails(
                conn, serper_key, llm_key, llm_model,
                batch_offset, batch_size, args.dry_run,
            )
            all_stats["phase5"] = s
            log.info("  Résultat: %s", json.dumps(s, indent=2))

    # Résumé final
    log.info("=" * 60)
    log.info("RÉSUMÉ COMPLET:")
    log.info(json.dumps(all_stats, indent=2, ensure_ascii=False))
    log.info("=" * 60)


if __name__ == "__main__":
    main()
