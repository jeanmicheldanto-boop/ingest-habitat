"""Enrichissement FINESS par département — script principal.

Ce script LIT depuis Supabase et ÉCRIT directement dans Supabase.
Aucun fichier CSV n'est nécessaire. L'ingestion est une étape locale préalable.

Usage :
    # Test local Hautes-Pyrénées (dry-run)
    python scripts/enrich_finess_dept.py --departements 65 --dry-run --out-dir outputs/finess

    # Test local avec écriture en base
    python scripts/enrich_finess_dept.py --departements 65 --out-dir outputs/finess

    # Plusieurs départements
    python scripts/enrich_finess_dept.py --departements 65,31,32 --out-dir outputs/finess

    # Cloud Run batch (identique — lit/écrit directement dans Supabase)
    python scripts/enrich_finess_dept.py --departements all --out-dir /tmp/outputs

Options :
    --departements  : Code(s) département séparés par virgule, ou "all"
    --limit         : Nombre max d'établissements par département (0 = pas de limite)
    --dry-run       : Ne pas écrire en base, uniquement générer des rapports
    --out-dir       : Dossier de sortie pour les rapports CSV/JSON
    --skip-serper   : Ne pas faire les requêtes Serper (utiliser le cache uniquement)
    --skip-llm      : Ne pas appeler le LLM (règles métier + géocodage only)
    --skip-geocode  : Ne pas géocoder les adresses
    --etape         : Étape spécifique à exécuter (1-9)

Env vars :
    - LLM_PROVIDER (mistral|gemini, default: mistral)
    - MISTRAL_API_KEY (requis si LLM_PROVIDER=mistral)
    - MISTRAL_MODEL (optionnel, default: ministral-8b-latest)
    - GEMINI_API_KEY (requis si LLM_PROVIDER=gemini)
    - GEMINI_MODEL (optionnel, default: gemini-2.0-flash)
    - SERPER_API_KEY (requis pour étapes 3, 6, 8)
    - DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import re
import sys
import time
import unicodedata
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg2
import psycopg2.errors
import psycopg2.extras
import requests
import urllib3

# -- Repo root setup ----------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from database import DatabaseManager
from enrich_finess_config import (
    CATEGORIE_NORMALISEE,
    DAF_SYNONYMES,
    EXTRA_QUERIES_EHPAD,
    EXTRA_QUERIES_HANDICAP_ENFANT,
    EXTRA_QUERIES_PROTECTION_ENFANCE,
    FINANCEUR_PAR_CATEGORIE,
    GEMINI_CONFIG,
    MISTRAL_CONFIG,
    PAGES_CIBLES,
    PROMPT_EXTRACTION_DIRIGEANTS,
    PROMPT_QUALIFICATION_PUBLIC,
    PROMPT_RESEAU_FEDERAL,
    PROMPT_SIGNAUX_TENSION,
    SECTEUR_PAR_CATEGORIE,
    SITE_EXCLUSIONS,
    TARIFICATION_PAR_CATEGORIE,
)

# Optional (for reports)
try:
    import pandas as pd_  # type: ignore
except ImportError:
    pd_ = None  # type: ignore[assignment]

# Optional (for geocoding)
try:
    from geopy.geocoders import Nominatim  # type: ignore
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False


# =============================================================================
# Deadlock / lock retry helper (for parallel Cloud Run executions)
# =============================================================================

RETRIABLE_PG_ERRORS = (
    psycopg2.errors.DeadlockDetected,
    psycopg2.errors.LockNotAvailable,
    psycopg2.errors.QueryCanceled,
    psycopg2.errors.SerializationFailure,
)


def retry_on_deadlock(func, conn, max_retries: int = 5, base_delay: float = 3.0,
                      label: str = "DB operation"):
    """Retry a database operation on deadlock, lock timeout, or query canceled.

    The function `func` receives a fresh cursor and should NOT commit.
    On success, we commit. On retriable error, we rollback and retry with
    exponential backoff + jitter.
    """
    for attempt in range(max_retries):
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur2:
                # Restore session settings after potential rollback
                cur2.execute("SET statement_timeout = '900s'")
                cur2.execute("SET lock_timeout = '30s'")
                func(cur2)
                conn.commit()
                return True
        except RETRIABLE_PG_ERRORS as e:
            conn.rollback()
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, base_delay)
                print(f"  [RETRY] {label}: {type(e).__name__} (attempt {attempt+1}/{max_retries}), "
                      f"waiting {delay:.1f}s...")
                time.sleep(delay)
            else:
                print(f"  [ERROR] {label}: {type(e).__name__} after {max_retries} attempts — skipping")
                return False
        except Exception:
            conn.rollback()
            raise
    return False


def execute_with_retry(cur, conn, sql: str, params: tuple = (),
                       max_retries: int = 3, label: str = "SQL"):
    """Execute a single SQL statement with deadlock retry.

    Uses the EXISTING cursor (doesn't create a new one) but rolls back
    and re-executes on retriable errors.
    """
    for attempt in range(max_retries):
        try:
            cur.execute(sql, params)
            return True
        except RETRIABLE_PG_ERRORS as e:
            conn.rollback()
            if attempt < max_retries - 1:
                delay = 2.0 * (2 ** attempt) + random.uniform(0, 2)
                print(f"  [RETRY] {label}: {type(e).__name__} (attempt {attempt+1}/{max_retries}), "
                      f"waiting {delay:.1f}s...")
                time.sleep(delay)
                # Re-set statement_timeout after rollback clears session settings
                cur.execute("SET statement_timeout = '600s'")
                conn.commit()
            else:
                print(f"  [ERROR] {label}: {type(e).__name__} after {max_retries} attempts")
                raise
    return False


# =============================================================================
# API helpers (reused from enrich_dept_prototype.py patterns)
# =============================================================================

def serper_search(query: str, *, num: int = 8, api_key: str) -> List[Dict[str, Any]]:
    """Recherche Serper (google.serper.dev)."""
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
    """Serper search with PostgreSQL cache (30-day TTL)."""
    query_hash = hashlib.sha256(query.encode()).hexdigest()

    cur.execute("""
        SELECT results FROM finess_cache_serper
        WHERE query_hash = %s AND expire_at > NOW()
    """, (query_hash,))
    row = cur.fetchone()
    if row:
        try:
            return json.loads(row["results"]) if isinstance(row["results"], str) else (row["results"] or [])
        except Exception:
            return []

    results = serper_search(query, num=10, api_key=api_key)

    try:
        cur.execute("""
            INSERT INTO finess_cache_serper (query_hash, query_text, results, nb_results)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (query_hash) DO UPDATE SET
                results = EXCLUDED.results,
                nb_results = EXCLUDED.nb_results,
                date_requete = NOW(),
                expire_at = NOW() + INTERVAL '30 days'
        """, (query_hash, query, json.dumps(results, ensure_ascii=False), len(results)))
    except Exception:
        pass

    return results


def _strip_html(html: str) -> str:
    html = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.IGNORECASE)
    html = re.sub(r"<style[\s\S]*?</style>", " ", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text).strip()


def fetch_page_text(url: str, timeout_s: int = 20) -> Tuple[int, str, str]:
    """Fetch a page and return (status, final_url, text). No ScrapingBee."""
    url = (url or "").strip()
    if not url:
        return 0, "", ""
    headers = {"User-Agent": "Mozilla/5.0 (ConfidensIA-FINESS/1.0)"}
    try:
        r = requests.get(url, headers=headers, timeout=timeout_s, allow_redirects=True)
        status = r.status_code
        final_url = str(getattr(r, "url", url))
        if status != 200:
            return status, final_url, ""
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "pdf" in ctype or final_url.lower().endswith(".pdf"):
            return status, final_url, ""
        text = r.text or ""
        if "<html" in text.lower() or "</" in text:
            text = _strip_html(text)
        return status, final_url, text
    except Exception:
        return 0, url, ""


# -- Cloud Run logging helpers ------------------------------------------------
_RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

def log_structured(severity: str, message: str, **kwargs) -> None:
    """Log structured JSON for Cloud Logging (also readable locally)."""
    log_entry = {
        "severity": severity,
        "message": message,
        "run_id": _RUN_ID,
        "timestamp": datetime.now().isoformat(),
        **kwargs
    }
    print(json.dumps(log_entry, ensure_ascii=False))
    sys.stdout.flush()

def log_checkpoint(phase: str, index: int, total: int, entity_id: str = "") -> None:
    """Log progress checkpoint every 10 entities for Cloud Run monitoring."""
    if index % 10 == 0 or index == total:
        log_structured(
            "INFO",
            f"{phase} checkpoint: {index}/{total}",
            phase=phase,
            progress_pct=round(100 * index / total, 1) if total > 0 else 0,
            entity_id=entity_id
        )

# -- Module-level LLM provider (set by main) ----------------------------------
_llm_provider: str = "mistral"  # "mistral" or "gemini"


def _clean_secret(value: str) -> str:
    """Clean secrets from BOM, null bytes, and whitespace."""
    if not value:
        return ""
    # Remove BOM (\ufeff), null bytes, and other problematic characters
    cleaned = value.replace('\ufeff', '').replace('\x00', '').strip()
    return cleaned


def _call_gemini(api_key: str, model: str, prompt: str, max_output_tokens: int, config: dict) -> str:
    """Low-level Gemini REST API call with retry."""
    model_name = (model or "").strip() or str(config["model"])
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": float(config["temperature"]),
            "maxOutputTokens": int(max_output_tokens),
        },
    }
    max_retries = int(config["max_retries"])
    backoff_base = float(config["retry_backoff"])
    backoff_429 = float(config.get("retry_backoff_429", 15.0))

    for attempt in range(max_retries):
        try:
            resp = requests.post(endpoint, json=payload, timeout=int(config["timeout_s"]))

            if resp.status_code == 429:
                wait = backoff_429 * (attempt + 1) + random.uniform(0, 5)
                print(f"      [GEMINI] 429 rate limit, retry {attempt+1}/{max_retries} in {wait:.0f}s")
                time.sleep(wait)
                continue

            if resp.status_code in {500, 502, 503, 504}:
                wait = backoff_base * (2 ** attempt) + random.uniform(0, 2)
                print(f"      [GEMINI] {resp.status_code} server error, retry {attempt+1}/{max_retries} in {wait:.1f}s")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                print(f"      [GEMINI] {resp.status_code} — {resp.text[:200]}")
                return ""

            raw = resp.json() or {}
            return (
                raw.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
                .strip()
            )
        except requests.exceptions.Timeout:
            wait = backoff_base * (2 ** attempt) + random.uniform(0, 2)
            print(f"      [GEMINI] timeout, retry {attempt+1}/{max_retries} in {wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            wait = backoff_base * (attempt + 1)
            print(f"      [GEMINI] error: {e}, retry {attempt+1}/{max_retries} in {wait:.1f}s")
            time.sleep(wait)
    print(f"      [GEMINI] ✗ {max_retries} retries exhausted")
    return ""


def _call_mistral(api_key: str, model: str, prompt: str, max_output_tokens: int, config: dict) -> str:
    """Low-level Mistral/Ministral chat completions API call with retry."""
    model_name = (model or "").strip() or str(config["model"])
    endpoint = "https://api.mistral.ai/v1/chat/completions"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Bearer {api_key}",
    }
    
    # Clean prompt: remove BOM and other problematic characters
    cleaned_prompt = prompt.replace('\ufeff', '').replace('\x00', '')
    
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": cleaned_prompt}],
        "temperature": float(config["temperature"]),
        "max_tokens": int(max_output_tokens),
    }
    max_retries = int(config["max_retries"])
    backoff_base = float(config["retry_backoff"])
    backoff_429 = float(config.get("retry_backoff_429", 10.0))

    for attempt in range(max_retries):
        try:
            # Manually serialize to UTF-8 to avoid latin-1 encoding issues
            payload_json = json.dumps(payload, ensure_ascii=False).encode('utf-8')
            resp = requests.post(endpoint, headers=headers, data=payload_json,
                                 timeout=int(config["timeout_s"]))

            if resp.status_code == 429:
                wait = backoff_429 * (attempt + 1) + random.uniform(0, 5)
                print(f"      [MISTRAL] 429 rate limit, retry {attempt+1}/{max_retries} in {wait:.0f}s")
                time.sleep(wait)
                continue

            if resp.status_code in {500, 502, 503, 504}:
                wait = backoff_base * (2 ** attempt) + random.uniform(0, 2)
                print(f"      [MISTRAL] {resp.status_code} server error, retry {attempt+1}/{max_retries} in {wait:.1f}s")
                time.sleep(wait)
                continue

            if resp.status_code != 200:
                print(f"      [MISTRAL] {resp.status_code} — {resp.text[:200]}")
                return ""

            raw = resp.json() or {}
            choices = raw.get("choices", [])
            if choices:
                return (choices[0].get("message", {}).get("content", "") or "").strip()
            return ""

        except requests.exceptions.Timeout:
            wait = backoff_base * (2 ** attempt) + random.uniform(0, 2)
            print(f"      [MISTRAL] timeout, retry {attempt+1}/{max_retries} in {wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            wait = backoff_base * (attempt + 1)
            print(f"      [MISTRAL] error: {e}, retry {attempt+1}/{max_retries} in {wait:.1f}s")
            time.sleep(wait)
    print(f"      [MISTRAL] ✗ {max_retries} retries exhausted")
    return ""


def llm_generate_text(*, api_key: str, model: str, prompt: str, max_output_tokens: int = 1200) -> str:
    """Call the configured LLM provider and return text.

    Provider is determined by the module-level _llm_provider variable
    (set in main via LLM_PROVIDER env var, default 'mistral').
    """
    key = (api_key or "").strip()
    if not key:
        return ""
    if _llm_provider == "gemini":
        return _call_gemini(key, model, prompt, max_output_tokens, GEMINI_CONFIG)
    else:
        return _call_mistral(key, model, prompt, max_output_tokens, MISTRAL_CONFIG)


def llm_generate_json(*, api_key: str, model: str, prompt: str, max_output_tokens: int = 1200) -> Dict[str, Any]:
    """Call the configured LLM and parse the response as JSON."""
    text = llm_generate_text(api_key=api_key, model=model, prompt=prompt, max_output_tokens=max_output_tokens)
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


# =============================================================================
# Geocoding (Nominatim + BAN fallback)
# =============================================================================

_geocoder = None


def _get_geocoder():
    global _geocoder
    if _geocoder is None and HAS_GEOPY:
        _geocoder = Nominatim(user_agent="ConfidensIA-FINESS-Enrichment/1.0")
    return _geocoder


_NO_GEO: Dict[str, Any] = {"latitude": None, "longitude": None, "geocode_precision": "non_trouvé"}


def geocode_finess_address(adresse_complete: str, code_postal: str, commune: str) -> Dict[str, Any]:
    """Geocode a FINESS address via Nominatim then BAN fallback.

    Robust against SSL/connection errors — never crashes the pipeline.
    """
    parts = [p for p in [adresse_complete, code_postal, commune, "France"] if p and p.strip()]
    full_address = ", ".join(parts)

    # Skip Nominatim if local SSL bypass is enabled (it also has SSL issues)
    skip_ssl = os.getenv("GEOCODING_SKIP_SSL_VERIFY", "").strip() == "1"
    geocoder = _get_geocoder()
    if geocoder and not skip_ssl:
        try:
            time.sleep(1.1)  # Rate limit Nominatim: 1 req/s
            location = geocoder.geocode(full_address, timeout=10)
            if location:
                has_num = bool(re.search(r"\d+", (adresse_complete or "").split(",")[0]))
                precision = "rooftop" if has_num else "locality"
                return {
                    "latitude": round(location.latitude, 6),
                    "longitude": round(location.longitude, 6),
                    "geocode_precision": precision,
                }
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"   [GEO] Erreur Nominatim: {e}")

    # Fallback: API Adresse data.gouv.fr (BAN)
    return _geocode_via_ban(full_address)


def _geocode_via_ban(query: str) -> Dict[str, Any]:
    """Fallback geocoding via API Adresse du gouvernement français (BAN).

    Robust against SSL/connection/timeout errors.
    """
    skip_ssl = os.getenv("GEOCODING_SKIP_SSL_VERIFY", "").strip() == "1"
    try:
        resp = requests.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": query, "limit": 1},
            timeout=15,
            verify=not skip_ssl,  # ⚠️ local debug only
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("features"):
                coords = data["features"][0]["geometry"]["coordinates"]
                score = data["features"][0]["properties"].get("score", 0)
                precision = "rooftop" if score > 0.7 else "street" if score > 0.5 else "locality"
                return {
                    "latitude": round(coords[1], 6),
                    "longitude": round(coords[0], 6),
                    "geocode_precision": precision,
                }
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f"   [GEO] Erreur BAN: {e}")
    return dict(_NO_GEO)


def geocode_all_entities(cur, departements: List[str], 
                         gest_ids: Optional[set] = None,
                         etab_ids: Optional[set] = None) -> Tuple[int, int]:
    """Geocode all gestionnaires + etablissements without coordinates.

    If gest_ids/etab_ids are provided, only geocode those entities (for testing with --limit).
    
    Returns (nb_gestionnaires_geocoded, nb_etablissements_geocoded).
    """
    dept_clause = "AND departement_code = ANY(%s)" if departements != ["all"] else ""
    params: tuple = (departements,) if departements != ["all"] else ()

    # 1. Gestionnaires
    if gest_ids:
        cur.execute(f"""
            SELECT id_gestionnaire, adresse_complete, code_postal, commune
            FROM finess_gestionnaire
            WHERE latitude IS NULL AND id_gestionnaire = ANY(%s) {dept_clause}
        """, (list(gest_ids),) + params)
    else:
        cur.execute(f"""
            SELECT id_gestionnaire, adresse_complete, code_postal, commune
            FROM finess_gestionnaire
            WHERE latitude IS NULL {dept_clause}
        """, params)
    gest_rows = cur.fetchall()
    nb_g = 0
    nb_g_err = 0
    for row in gest_rows:
        try:
            geo = geocode_finess_address(row["adresse_complete"] or "", row["code_postal"] or "", row["commune"] or "")
            if geo["latitude"]:
                cur.execute("""
                    UPDATE finess_gestionnaire SET
                        latitude = %s, longitude = %s, geocode_precision = %s
                    WHERE id_gestionnaire = %s
                """, (geo["latitude"], geo["longitude"], geo["geocode_precision"], row["id_gestionnaire"]))
                nb_g += 1
        except KeyboardInterrupt:
            raise
        except Exception as e:
            nb_g_err += 1
            if nb_g_err <= 3:
                print(f"   [GEO] Erreur gestionnaire {row['id_gestionnaire']}: {e}")

    # 2. Établissements
    if etab_ids:
        cur.execute(f"""
            SELECT id_finess, adresse_complete, code_postal, commune
            FROM finess_etablissement
            WHERE latitude IS NULL AND id_finess = ANY(%s) {dept_clause}
        """, (list(etab_ids),) + params)
    else:
        cur.execute(f"""
            SELECT id_finess, adresse_complete, code_postal, commune
            FROM finess_etablissement
            WHERE latitude IS NULL {dept_clause}
        """, params)
    etab_rows = cur.fetchall()
    nb_e = 0
    nb_e_err = 0
    for row in etab_rows:
        try:
            geo = geocode_finess_address(row["adresse_complete"] or "", row["code_postal"] or "", row["commune"] or "")
            if geo["latitude"]:
                cur.execute("""
                    UPDATE finess_etablissement SET
                        latitude = %s, longitude = %s, geocode_precision = %s
                    WHERE id_finess = %s
                """, (geo["latitude"], geo["longitude"], geo["geocode_precision"], row["id_finess"]))
                nb_e += 1
        except KeyboardInterrupt:
            raise
        except Exception as e:
            nb_e_err += 1
            if nb_e_err <= 3:
                print(f"   [GEO] Erreur établissement {row['id_finess']}: {e}")

    print(f"[GEO] {nb_g}/{len(gest_rows)} gestionnaires géocodés"
          f"{f' ({nb_g_err} erreurs)' if nb_g_err else ''}"
          f", {nb_e}/{len(etab_rows)} établissements géocodés"
          f"{f' ({nb_e_err} erreurs)' if nb_e_err else ''}")
    return nb_g, nb_e


# =============================================================================
# Step 1 — Business rules (deterministic, no API)
# =============================================================================

def apply_all_business_rules(cur, departements: List[str]) -> None:
    """Apply all deterministic enrichments (catégorie, secteur, financeur, tarification).

    NOTE: The établissement UPDATEs are safe for parallel execution because they
    filter by departement_code and different Cloud Run executions process different
    departments.  The gestionnaire UPDATE is done row-by-row with advisory locks
    to prevent deadlocks when a gestionnaire spans multiple departments.
    """
    dept_clause = "AND departement_code = ANY(%s)" if departements != ["all"] else ""
    params: tuple = (departements,) if departements != ["all"] else ()

    # Catégorie normalisée
    for lib_finess, cat_norm in CATEGORIE_NORMALISEE.items():
        cur.execute(f"""
            UPDATE finess_etablissement SET categorie_normalisee = %s
            WHERE categorie_libelle = %s AND categorie_normalisee IS NULL {dept_clause}
        """, (cat_norm, lib_finess) + params)

    # Secteur d'activité par établissement
    for cat_norm, secteur in SECTEUR_PAR_CATEGORIE.items():
        cur.execute(f"""
            UPDATE finess_etablissement SET secteur_activite = %s
            WHERE categorie_normalisee = %s AND secteur_activite IS NULL {dept_clause}
        """, (secteur, cat_norm) + params)

    # Financeur par établissement
    for cat_norm, fin in FINANCEUR_PAR_CATEGORIE.items():
        cur.execute(f"""
            UPDATE finess_etablissement SET
                financeur_principal = %s,
                financeur_secondaire = %s
            WHERE categorie_normalisee = %s
              AND financeur_principal IS NULL {dept_clause}
        """, (fin.get("principal"), fin.get("secondaire"), cat_norm) + params)

    # Tarification par établissement
    for cat_norm, tarif in TARIFICATION_PAR_CATEGORIE.items():
        cur.execute(f"""
            UPDATE finess_etablissement SET type_tarification = %s
            WHERE categorie_normalisee = %s AND type_tarification IS NULL {dept_clause}
        """, (tarif, cat_norm) + params)

    # Secteur principal du gestionnaire — ROW-BY-ROW to avoid deadlocks
    # First: compute the dominant sector per gestionnaire (read-only CTE)
    cur.execute(f"""
        WITH secteurs_par_gest AS (
            SELECT id_gestionnaire, secteur_activite,
                   COUNT(*) AS nb,
                   COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY id_gestionnaire) AS pct
            FROM finess_etablissement
            WHERE secteur_activite IS NOT NULL {dept_clause}
            GROUP BY id_gestionnaire, secteur_activite
        )
        SELECT id_gestionnaire,
               CASE
                   WHEN COUNT(*) FILTER (WHERE pct >= 30) >= 2 THEN 'Multi-secteurs'
                   ELSE (SELECT secteur_activite FROM secteurs_par_gest s2
                         WHERE s2.id_gestionnaire = secteurs_par_gest.id_gestionnaire
                         ORDER BY nb DESC LIMIT 1)
               END AS secteur_principal
        FROM secteurs_par_gest
        GROUP BY id_gestionnaire
    """, params)
    gest_secteurs = cur.fetchall()

    # Then: update each gestionnaire individually (no cross-row locking)
    nb_updated = 0
    for row in gest_secteurs:
        cur.execute("""
            UPDATE finess_gestionnaire SET secteur_activite_principal = %s
            WHERE id_gestionnaire = %s AND secteur_activite_principal IS NULL
        """, (row["secteur_principal"], row["id_gestionnaire"]))
        nb_updated += cur.rowcount

    print(f"[RULES] Règles métier appliquées (catégorie, secteur, financeur, tarification) — {nb_updated} gestionnaires mis à jour")


# =============================================================================
# Step 3 — Serper enrichment
# =============================================================================

def build_serper_queries(etab: Dict[str, Any]) -> List[str]:
    """Build Serper queries for an establishment."""
    nom = etab.get("raison_sociale", "")
    commune = etab.get("commune", "")
    cat = etab.get("categorie_normalisee", "")
    dept = etab.get("departement_nom", "")

    queries = [
        f'"{nom}" {commune}',
        f'"{nom}" public accueilli missions {cat}',
        f'directeur "{nom}" {commune}',
        f'"{nom}" actualité recrutement projet',
        f'site:linkedin.com/in "{nom}" directeur',
        f'"{nom}" contact email @',
        f'"{nom}" CPOM autorisation ARS {dept}',
    ]

    # Extra queries by category
    if cat in ("EHPAD", "USLD", "RA"):
        queries.extend([q.format(nom=nom) for q in EXTRA_QUERIES_EHPAD])
    elif cat in ("IME", "IMPRO", "SESSAD", "ITEP", "IEM", "CAMSP"):
        queries.extend([q.format(nom=nom) for q in EXTRA_QUERIES_HANDICAP_ENFANT])
    elif cat in ("MECS", "FDE", "AEMO"):
        queries.extend([q.format(nom=nom) for q in EXTRA_QUERIES_PROTECTION_ENFANCE])

    return queries


def build_serper_queries_gestionnaire(gest: Dict[str, Any]) -> List[str]:
    """Build Serper queries for a gestionnaire (EJ)."""
    nom = gest.get("raison_sociale", "")
    return [
        f'"{nom}" association fondation site officiel',
        f'"{nom}" organigramme direction équipe',
        f'site:linkedin.com/company "{nom}"',
        f'"{nom}" NEXEM FEHAP UNIOPSS URIOPSS membre adhérent',
        f'"{nom}" rapport activité comptes annuels',
    ]


def extraire_site_officiel(results: List[Dict[str, Any]], nom_etab: str) -> Optional[str]:
    """Extract the official website from Serper results."""
    for r in results:
        url = r.get("link", "")
        domain = urlparse(url).netloc.lower()
        if any(excl in domain for excl in SITE_EXCLUSIONS):
            continue
        title = (r.get("title", "") + " " + r.get("snippet", "")).lower()
        nom_lower = nom_etab.lower()
        if any(word in title for word in nom_lower.split() if len(word) > 3):
            return url

    for r in results:
        url = r.get("link", "")
        domain = urlparse(url).netloc.lower()
        if not any(excl in domain for excl in SITE_EXCLUSIONS):
            return url
    return None


def enrich_via_serper(etab: Dict[str, Any], api_key: str, cur) -> Dict[str, Any]:
    """Run Serper queries for an establishment and build context dict."""
    queries = build_serper_queries(etab)
    all_results: List[Dict[str, Any]] = []
    serper_count = 0

    for q in queries:
        results = get_or_search_serper(q, api_key, cur)
        all_results.extend(results)
        serper_count += 1

    # Identify official site
    site = extraire_site_officiel(all_results, etab.get("raison_sociale", ""))

    # Build combined text from snippets
    combined = "\n".join([
        f"- {r.get('title', '')} : {r.get('snippet', '')} ({r.get('link', '')})"
        for r in all_results[:30]
    ])

    # Extract emails from snippets
    emails = set()
    for r in all_results:
        text = r.get("snippet", "") + " " + r.get("title", "")
        found = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
        emails.update(found)

    return {
        "combined_text": combined,
        "serper_results": all_results,
        "site_web": site,
        "emails_trouves": list(emails),
        "serper_count": serper_count,
        "page_equipe_text": "",
        "page_contact_text": "",
        "pages_scrapped": [],
    }


# =============================================================================
# Step 4 — Scraping
# =============================================================================

def scrape_etablissement_pages(site_url: str) -> Dict[str, Any]:
    """Scrape key pages from an establishment website (requests only, no ScrapingBee)."""
    combined_text = ""
    page_equipe_text = ""
    page_contact_text = ""
    pages_ok: List[str] = []
    emails_found: set[str] = set()

    base_url = site_url.rstrip("/")

    for suffix in PAGES_CIBLES:
        url = base_url + suffix
        status, final_url, text = fetch_page_text(url)

        if status == 200 and text and len(text) > 100:
            combined_text += f"\n--- PAGE: {final_url} ---\n{text[:15000]}\n"
            pages_ok.append(final_url)

            if any(kw in suffix for kw in ["/equipe", "/direction", "/organigramme"]):
                page_equipe_text += text[:10000]
            if "/contact" in suffix:
                page_contact_text += text[:5000]

            found = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
            emails_found.update(found)

        time.sleep(0.3)  # Politesse

    return {
        "combined_text": combined_text[:50000],
        "page_equipe_text": page_equipe_text,
        "page_contact_text": page_contact_text,
        "pages_scrapped": pages_ok,
        "emails_trouves": list(emails_found),
    }


# =============================================================================
# Step 5 — LLM qualification
# =============================================================================

def qualify_with_llm(etab: Dict[str, Any], context: Dict[str, Any],
                     llm_key: str, llm_model: str) -> Dict[str, Any]:
    """Qualify establishment using LLM."""
    prompt = PROMPT_QUALIFICATION_PUBLIC.format(
        raison_sociale=etab.get("raison_sociale", ""),
        categorie_libelle=etab.get("categorie_libelle", ""),
        categorie_normalisee=etab.get("categorie_normalisee", ""),
        departement_nom=etab.get("departement_nom", ""),
        commune=etab.get("commune", ""),
        texte_pages_web=context.get("combined_text", "")[:30000],
    )
    return llm_generate_json(api_key=llm_key, model=llm_model, prompt=prompt)


def apply_qualification(etab_id: str, qual: Dict[str, Any], site_web: Optional[str], cur, dry_run: bool) -> None:
    """Apply LLM qualification results to finess_etablissement."""
    if dry_run or not qual:
        return

    cur.execute("""
        UPDATE finess_etablissement SET
            type_public = COALESCE(%s, type_public),
            type_public_synonymes = COALESCE(%s, type_public_synonymes),
            specificites_public = COALESCE(%s, specificites_public),
            pathologies_specifiques = COALESCE(%s, pathologies_specifiques),
            age_min = COALESCE(%s, age_min),
            age_max = COALESCE(%s, age_max),
            tranches_age = COALESCE(%s, tranches_age),
            type_accueil = COALESCE(%s, type_accueil),
            periode_ouverture = COALESCE(%s, periode_ouverture),
            ouverture_365 = COALESCE(%s, ouverture_365),
            site_web = COALESCE(%s, site_web),
            email = COALESCE(%s, email)
        WHERE id_finess = %s
    """, (
        qual.get("type_public"),
        qual.get("type_public_synonymes"),
        qual.get("specificites_public"),
        qual.get("pathologies_specifiques"),
        qual.get("age_min"),
        qual.get("age_max"),
        qual.get("tranche_age_label"),
        qual.get("type_accueil"),
        qual.get("periode_ouverture"),
        qual.get("ouverture_365"),
        qual.get("site_web_officiel") or site_web,
        qual.get("email_contact"),
        etab_id,
    ))


# =============================================================================
# Step 6 — Dirigeants + DAF
# =============================================================================

def extract_dirigeants_from_text(
    gest: Dict[str, Any],
    text: str,
    llm_key: str,
    llm_model: str,
    source_type: str = "site_officiel",
) -> List[Dict[str, Any]]:
    """Use LLM to extract dirigeants from a text block."""
    if not text.strip():
        return []
    prompt = PROMPT_EXTRACTION_DIRIGEANTS.format(
        raison_sociale=gest.get("raison_sociale", ""),
        commune=gest.get("commune", ""),
        departement_nom=gest.get("departement_nom", ""),
        texte_combine=text[:20000],
    )
    result = llm_generate_json(api_key=llm_key, model=llm_model, prompt=prompt)
    dirigeants = result.get("dirigeants") or []
    for d in dirigeants:
        d["source_type"] = source_type
    return dirigeants


def deduplicate_dirigeants(dirigeants: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deduplicate dirigeants by nom (case-insensitive)."""
    seen: dict[str, Dict[str, Any]] = {}
    for d in dirigeants:
        key = (d.get("nom") or "").strip().upper()
        if not key:
            continue
        if key not in seen:
            seen[key] = d
        else:
            # Keep higher confidence
            conf_order = {"haute": 3, "moyenne": 2, "basse": 1}
            if conf_order.get(d.get("confiance", ""), 0) > conf_order.get(seen[key].get("confiance", ""), 0):
                seen[key] = d
    return list(seen.values())


def enrich_dirigeants(
    gest: Dict[str, Any],
    context: Dict[str, Any],
    llm_key: str,
    llm_model: str,
    serper_key: str,
    cur,
) -> List[Dict[str, Any]]:
    """Full dirigeants pipeline: site officiel + LinkedIn + Serper générique."""
    all_dirs: List[Dict[str, Any]] = []

    # Source 1: site officiel (page équipe)
    if context.get("page_equipe_text"):
        dirs = extract_dirigeants_from_text(gest, context["page_equipe_text"],
                                            llm_key, llm_model, "site_officiel")
        all_dirs.extend(dirs)

    # Source 2: LinkedIn via Serper
    if serper_key:
        for q in [
            f'site:linkedin.com/in directeur "{gest.get("raison_sociale", "")}"',
            f'site:linkedin.com/in président "{gest.get("raison_sociale", "")}"',
        ]:
            results = get_or_search_serper(q, serper_key, cur)
            li_text = "\n".join([
                f"- {r.get('title', '')} — {r.get('snippet', '')} ({r.get('link', '')})"
                for r in results[:5]
            ])
            if li_text.strip():
                dirs = extract_dirigeants_from_text(gest, li_text,
                                                    llm_key, llm_model, "linkedin_serper")
                all_dirs.extend(dirs)
    
    # Source 3: Serper générique (web large — presse, communiqués, annuaires)
    if serper_key:
        raison = gest.get("raison_sociale", "")
        commune = gest.get("commune", "")
        for q in [
            f'"{raison}" "directeur général"',
            f'"{raison}" "président"',
            f'"directeur" "{raison}" {commune}',
        ]:
            results = get_or_search_serper(q, serper_key, cur)
            web_text = "\n".join([
                f"- {r.get('title', '')} — {r.get('snippet', '')} ({r.get('link', '')})"
                for r in results[:5]
            ])
            if web_text.strip():
                dirs = extract_dirigeants_from_text(gest, web_text,
                                                    llm_key, llm_model, "web_serper")
                all_dirs.extend(dirs)

    return deduplicate_dirigeants(all_dirs)


def identify_daf(
    gest: Dict[str, Any],
    dirigeants: List[Dict[str, Any]],
    serper_key: str,
    llm_key: str,
    llm_model: str,
    cur,
) -> Optional[Dict[str, Any]]:
    """Identify the DAF specifically. Search in existing dirigeants then via Serper."""
    # 1. Check already identified dirigeants
    for d in dirigeants:
        fn = (d.get("fonction_normalisee") or "").strip()
        fb = (d.get("fonction_brute") or "").strip()
        if fn in ("DAF", "Directeur Administratif et Financier") or \
           any(s.lower() in fb.lower() for s in DAF_SYNONYMES):
            return d

    # 2. Dedicated Serper search
    if not serper_key:
        return None

    for q in [
        f'"directeur administratif et financier" "{gest.get("raison_sociale", "")}"',
        f'"DAF" "{gest.get("raison_sociale", "")}" site:linkedin.com/in',
        f'"responsable administratif et financier" "{gest.get("raison_sociale", "")}"',
    ]:
        results = get_or_search_serper(q, serper_key, cur)
        if results:
            daf_text = "\n".join([
                f"- {r.get('title', '')} — {r.get('snippet', '')} ({r.get('link', '')})"
                for r in results[:5]
            ])
            dirs = extract_dirigeants_from_text(gest, daf_text,
                                                llm_key, llm_model, "serper_daf_cible")
            for d in dirs:
                fn = (d.get("fonction_normalisee") or "").strip()
                fb = (d.get("fonction_brute") or "").strip()
                if fn in ("DAF", "Directeur Administratif et Financier") or \
                   any(s.lower() in fb.lower() for s in DAF_SYNONYMES):
                    return d
    return None


def store_dirigeants(gest_id: str, dirigeants: List[Dict[str, Any]], cur, dry_run: bool) -> int:
    """Insert dirigeants into finess_dirigeant. Returns count."""
    if dry_run:
        return len(dirigeants)
    count = 0
    for d in dirigeants:
        cur.execute("""
            INSERT INTO finess_dirigeant (
                id_gestionnaire, civilite, nom, prenom,
                fonction_brute, fonction_normalisee,
                source_url, source_type, confiance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            gest_id,
            d.get("civilite"),
            d.get("nom"),
            d.get("prenom"),
            d.get("fonction_brute"),
            d.get("fonction_normalisee"),
            d.get("source_url"),
            d.get("source_type"),
            d.get("confiance", "moyenne"),
        ))
        count += 1
    return count


def store_daf_on_gestionnaire(gest_id: str, daf: Dict[str, Any], cur, dry_run: bool) -> None:
    """Store DAF info directly on finess_gestionnaire."""
    if dry_run:
        return
    cur.execute("""
        UPDATE finess_gestionnaire SET
            daf_nom = %s,
            daf_prenom = %s,
            daf_email = %s,
            daf_linkedin_url = %s,
            daf_source = %s,
            daf_confiance = %s
        WHERE id_gestionnaire = %s
    """, (
        daf.get("nom"),
        daf.get("prenom"),
        daf.get("email_reconstitue"),
        daf.get("linkedin_url"),
        daf.get("source_url"),
        daf.get("confiance", "moyenne"),
        gest_id,
    ))


# =============================================================================
# Step 7 — Email reconstruction
# =============================================================================

def _is_person_email(email: str) -> bool:
    """Check if an email is likely a person's (not generic)."""
    generic = {"contact", "info", "accueil", "direction", "secretariat",
               "admin", "rh", "communication", "compta", "comptabilite",
               "standard", "reception", "candidature", "recrutement",
               "noreply", "no-reply"}
    local = email.split("@")[0].lower().replace(".", "").replace("-", "").replace("_", "")
    return local not in generic and len(local) > 2


def detect_mail_pattern(emails: List[str], domaine: str) -> Dict[str, Any]:
    """Detect the email pattern from collected emails."""
    domain_emails = [e for e in emails if e.lower().endswith(f"@{domaine.lower()}")]
    person_emails = [e for e in domain_emails if _is_person_email(e)]

    if not person_emails:
        return {"domaine": domaine, "structure": None, "confiance": "basse"}

    patterns: List[str] = []
    for email in person_emails:
        local = email.split("@")[0]
        if "." in local:
            parts = local.split(".")
            if len(parts) == 2:
                patterns.append("p.nom" if len(parts[0]) == 1 else "prenom.nom")
            else:
                patterns.append("prenom.nom.ext")
        elif "-" in local:
            patterns.append("prenom-nom")
        elif "_" in local:
            patterns.append("prenom_nom")
        else:
            patterns.append("autre")

    if patterns:
        most_common = Counter(patterns).most_common(1)[0]
        return {
            "domaine": domaine,
            "structure": most_common[0],
            "confiance": "haute" if most_common[1] >= 2 else "moyenne",
            "exemples": person_emails[:3],
        }
    return {"domaine": domaine, "structure": None, "confiance": "basse"}


def _normalise_for_email(text: str) -> str:
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9]", "", text)
    return text


def reconstituer_email(prenom: str, nom: str, structure: str, domaine: str) -> Optional[str]:
    """Reconstruct an email from pattern."""
    if not prenom or not nom or not structure or not domaine:
        return None
    p = _normalise_for_email(prenom)
    n = _normalise_for_email(nom)
    templates = {
        "prenom.nom": f"{p}.{n}@{domaine}",
        "p.nom": f"{p[0]}.{n}@{domaine}",
        "prenom": f"{p}@{domaine}",
        "nom.prenom": f"{n}.{p}@{domaine}",
        "prenom-nom": f"{p}-{n}@{domaine}",
        "prenom_nom": f"{p}_{n}@{domaine}",
    }
    return templates.get(structure)


def _extract_domain_from_site(site_web: Optional[str]) -> Optional[str]:
    """Extract clean domain from a site URL, filtering portals/annuaires."""
    if not site_web:
        return None
    netloc = urlparse(site_web).netloc.lower()
    # Strip www. prefix
    if netloc.startswith("www."):
        netloc = netloc[4:]
    # Skip sites matching SITE_EXCLUSIONS
    if any(excl in netloc for excl in SITE_EXCLUSIONS):
        return None
    if not netloc or "." not in netloc:
        return None
    return netloc


def _extract_domain_from_emails(emails: List[str]) -> Optional[str]:
    """Extract the most common non-generic domain from a list of emails."""
    generic_domains = {"gmail.com", "yahoo.fr", "yahoo.com", "hotmail.com",
                       "hotmail.fr", "outlook.com", "outlook.fr", "orange.fr",
                       "wanadoo.fr", "free.fr", "sfr.fr", "laposte.net",
                       "live.fr", "live.com"}
    domains: Dict[str, int] = {}
    for e in emails:
        d = e.split("@")[-1].lower()
        if d and "." in d and d not in generic_domains:
            domains[d] = domains.get(d, 0) + 1
    if domains:
        return max(domains, key=domains.get)  # type: ignore[arg-type]
    return None


def search_domain_emails(
    domaine: str,
    raison_sociale: str,
    serper_key: str,
    cur,
) -> List[str]:
    """Search Serper for real emails at a given domain.

    Queries like '"@domain.tld"' often surface real person emails
    in LinkedIn profiles, annuaires, and web pages.
    """
    queries = [
        f'"@{domaine}"',
        f'"@{domaine}" "{raison_sociale}"',
    ]
    found_emails: set[str] = set()
    for q in queries:
        results = get_or_search_serper(q, serper_key, cur)
        for r in results:
            text = (r.get("snippet", "") + " " + r.get("title", "")
                    + " " + r.get("link", ""))
            matches = re.findall(
                r"[a-zA-Z0-9._%+-]+@" + re.escape(domaine),
                text, re.IGNORECASE,
            )
            found_emails.update(m.lower() for m in matches)
    return list(found_emails)


def qualify_email_pattern_llm(
    emails: List[str],
    domaine: str,
    llm_key: str,
    llm_model: str,
) -> Dict[str, Any]:
    """Use LLM to qualify the email naming pattern from examples.

    Returns {"structure": "prenom.nom"|"p.nom"|..., "confiance": "haute"|"moyenne"|"basse"}.
    """
    person_emails = [e for e in emails if _is_person_email(e)]
    if not person_emails:
        return {"structure": None, "confiance": "basse"}

    examples_str = "\n".join(f"  - {e}" for e in person_emails[:10])
    prompt = f"""Analyse ces adresses email trouvées au domaine @{domaine} :
{examples_str}

Identifie le modèle de construction des emails personnels (pas les adresses génériques comme contact@, info@, etc.).

Réponds UNIQUEMENT en JSON :
{{
  "structure": "<le pattern parmi : prenom.nom | p.nom | nom.prenom | prenom-nom | prenom_nom | prenom | autre>",
  "confiance": "<haute si >= 2 exemples concordants, moyenne si 1 exemple clair, basse sinon>",
  "explication": "<courte justification>"
}}"""

    try:
        result = llm_generate_json(
            api_key=llm_key, model=llm_model,
            prompt=prompt, max_output_tokens=300,
        )
        structure = result.get("structure", "").strip().lower()
        valid = {"prenom.nom", "p.nom", "nom.prenom", "prenom-nom",
                 "prenom_nom", "prenom", "autre"}
        if structure not in valid:
            structure = None
        return {
            "structure": structure,
            "confiance": result.get("confiance", "moyenne"),
            "explication": result.get("explication", ""),
        }
    except Exception as e:
        print(f"      [EMAIL-LLM] Erreur: {e}")
        return {"structure": None, "confiance": "basse"}


def reconstruct_emails_for_gestionnaire(
    gest_id: str,
    emails_trouves: List[str],
    site_web: Optional[str],
    raison_sociale: str,
    serper_key: Optional[str],
    llm_key: Optional[str],
    llm_model: str,
    cur,
    dry_run: bool,
) -> int:
    """Detect mail pattern and reconstruct emails for all dirigeants.

    Strategy (robust):
    1. Extract domain from site_web (preferred) or from found emails.
    2. Serper search '"@domain.tld"' to discover real person emails at that domain.
    3. Merge discovered emails with previously found ones.
    4. Use LLM to qualify the naming pattern from real examples.
    5. Fallback to 'prenom.nom' (French convention) if nothing found.
    6. Apply pattern to all dirigeants who don't have an email yet.
    """
    # ── Step 1: resolve domain ──
    domaine = _extract_domain_from_site(site_web)
    if not domaine:
        domaine = _extract_domain_from_emails(emails_trouves)
    if not domaine:
        return 0

    # ── Step 2: Serper "@domain" discovery ──
    domain_emails = list(emails_trouves)  # start with what we have
    if serper_key:
        discovered = search_domain_emails(domaine, raison_sociale, serper_key, cur)
        domain_emails = list(set(domain_emails + discovered))
        if discovered:
            print(f"      [EMAIL-SERPER] {len(discovered)} emails trouvés via '@{domaine}'")

    # ── Step 3: detect pattern (rule-based first) ──
    pattern = detect_mail_pattern(domain_emails, domaine)
    structure = pattern.get("structure")
    confiance = pattern.get("confiance", "basse")

    # ── Step 4: LLM qualification if ambiguous or no pattern ──
    person_emails_at_domain = [e for e in domain_emails
                               if e.lower().endswith(f"@{domaine.lower()}")
                               and _is_person_email(e)]
    if person_emails_at_domain and llm_key:
        llm_result = qualify_email_pattern_llm(
            person_emails_at_domain, domaine, llm_key, llm_model)
        llm_structure = llm_result.get("structure")
        if llm_structure and llm_structure != "autre":
            # LLM overrides rule-based if it found something concrete
            if not structure or confiance == "basse":
                structure = llm_structure
                confiance = llm_result.get("confiance", "moyenne")
            print(f"      [EMAIL-LLM] pattern={llm_structure} "
                  f"confiance={llm_result.get('confiance')} "
                  f"({llm_result.get('explication', '')})")

    # ── Step 5: fallback to prenom.nom ──
    if not structure or structure == "autre":
        structure = "prenom.nom"
        confiance = "basse"  # inferred, not verified

    # ── Store pattern on gestionnaire ──
    if not dry_run:
        cur.execute("""
            UPDATE finess_gestionnaire SET
                domaine_mail = %s,
                structure_mail = %s
            WHERE id_gestionnaire = %s
        """, (domaine, structure, gest_id))

    # ── Step 6: reconstruct for each dirigeant ──
    cur.execute("""
        SELECT id, prenom, nom
        FROM finess_dirigeant
        WHERE id_gestionnaire = %s AND email_reconstitue IS NULL
    """, (gest_id,))
    dirigeants = cur.fetchall()

    count = 0
    for d in dirigeants:
        email = reconstituer_email(d["prenom"] or "", d["nom"] or "", structure, domaine)
        if email and not dry_run:
            cur.execute("""
                UPDATE finess_dirigeant SET
                    email_reconstitue = %s,
                    email_organisation = %s
                WHERE id = %s
            """, (email, f"@{domaine}", d["id"]))
            count += 1
        elif email:
            count += 1

    return count


# =============================================================================
# Step 8 — Signaux de tension
# =============================================================================

def enrich_signaux_gestionnaire(
    gest: Dict[str, Any],
    context: Dict[str, Any],
    llm_key: str,
    llm_model: str,
    serper_key: str,
    cur,
) -> Dict[str, Any]:
    """Search and qualify tension signals at the gestionnaire level."""
    actu_text = context.get("combined_text", "")

    # Dedicated queries for the gestionnaire
    if serper_key:
        for q in [
            f'"{gest.get("raison_sociale", "")}" actualité {gest.get("commune", "")}',
            f'"{gest.get("raison_sociale", "")}" recrutement emploi',
            f'"{gest.get("raison_sociale", "")}" projet transformation extension',
        ]:
            results = get_or_search_serper(q, serper_key, cur)
            actu_text += "\n".join([
                f"- {r.get('title', '')} : {r.get('snippet', '')} ({r.get('link', '')})"
                for r in results[:5]
            ]) + "\n"

    if not actu_text.strip():
        return {"signaux": [], "signal_tension": False, "signal_tension_detail": None}

    prompt = PROMPT_SIGNAUX_TENSION.format(
        raison_sociale=gest.get("raison_sociale", ""),
        categorie_normalisee=gest.get("dominante_type", "") or gest.get("secteur_activite_principal", ""),
        commune=gest.get("commune", ""),
        departement_nom=gest.get("departement_nom", ""),
        texte_actualites=actu_text[:15000],
    )
    return llm_generate_json(api_key=llm_key, model=llm_model, prompt=prompt) or {
        "signaux": [], "signal_tension": False, "signal_tension_detail": None
    }


def update_signaux_gestionnaire(gest_id: str, signaux_result: Dict[str, Any], cur, dry_run: bool) -> None:
    """Write signal results to the gestionnaire record."""
    if dry_run:
        return
    cur.execute("""
        UPDATE finess_gestionnaire SET
            signaux_recents = %s,
            signal_tension = %s,
            signal_tension_detail = %s
        WHERE id_gestionnaire = %s
    """, (
        json.dumps(signaux_result.get("signaux", []), ensure_ascii=False),
        bool(signaux_result.get("signal_tension")),
        signaux_result.get("signal_tension_detail"),
        gest_id,
    ))


# =============================================================================
# Step 9 — Réseau fédéral (gestionnaire level)
# =============================================================================

def enrich_reseau_federal(
    gest: Dict[str, Any],
    context: Dict[str, Any],
    llm_key: str,
    llm_model: str,
    cur,
    dry_run: bool,
) -> Optional[str]:
    """Identify the federal network of a gestionnaire."""
    text_web = context.get("combined_text", "")
    if not text_web.strip():
        return None

    prompt = PROMPT_RESEAU_FEDERAL.format(
        raison_sociale=gest.get("raison_sociale", ""),
        forme_juridique_libelle=gest.get("forme_juridique_libelle", ""),
        texte_web=text_web[:10000],
    )
    result = llm_generate_json(api_key=llm_key, model=llm_model, prompt=prompt)
    reseau = result.get("reseau_federal")

    if reseau and not dry_run:
        cur.execute("""
            UPDATE finess_gestionnaire SET reseau_federal = %s
            WHERE id_gestionnaire = %s AND reseau_federal IS NULL
        """, (reseau, gest["id_gestionnaire"]))

    return reseau


# =============================================================================
# Logging & status helpers
# =============================================================================

def log_enrichissement(cur, id_finess: str, entite_type: str, etape: str,
                       statut: str, details: Dict[str, Any],
                       serper_reqs: int = 0, gemini_tokens: int = 0, duree_ms: int = 0) -> None:
    """Write an enrichment log entry."""
    try:
        cur.execute("""
            INSERT INTO finess_enrichissement_log
            (id_finess, entite_type, etape, statut, details, serper_requetes, gemini_tokens, duree_ms)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_finess, entite_type, etape, statut,
              json.dumps(details, ensure_ascii=False, default=str), serper_reqs, gemini_tokens, duree_ms))
    except Exception:
        pass


def mark_enriched(etab_id: str, cur) -> None:
    cur.execute("""
        UPDATE finess_etablissement SET
            enrichissement_statut = 'enrichi',
            date_enrichissement = NOW()
        WHERE id_finess = %s
    """, (etab_id,))


def mark_in_progress(etab_id: str, cur) -> None:
    cur.execute("""
        UPDATE finess_etablissement SET enrichissement_statut = 'en_cours'
        WHERE id_finess = %s
    """, (etab_id,))


def mark_error(etab_id: str, error_msg: str, cur) -> None:
    cur.execute("""
        UPDATE finess_etablissement SET
            enrichissement_statut = 'erreur',
            enrichissement_log = jsonb_build_object('error', %s, 'date', NOW()::text)
        WHERE id_finess = %s
    """, (error_msg, etab_id))


# =============================================================================
# Data loading
# =============================================================================

def load_etabs_to_enrich(cur, departements: List[str], limit: int) -> List[Dict[str, Any]]:
    """Load establishments to enrich from Supabase (excludes already enriched + SAA)."""
    dept_clause = "AND departement_code = ANY(%s)" if departements != ["all"] else ""
    limit_clause = f"LIMIT {limit}" if limit > 0 else ""
    params: tuple = (departements,) if departements != ["all"] else ()

    cur.execute(f"""
        SELECT * FROM finess_etablissement
        WHERE enrichissement_statut IN ('brut', 'en_cours', 'erreur')
          AND categorie_normalisee IS NOT NULL
          AND categorie_normalisee != 'SAA'
        {dept_clause}
        ORDER BY
            CASE enrichissement_statut
                WHEN 'en_cours' THEN 1
                WHEN 'erreur' THEN 2
                WHEN 'brut' THEN 3
            END,
            id_finess
        {limit_clause}
    """, params)
    return cur.fetchall()


def load_gestionnaires(cur, departements: List[str]) -> Dict[str, Dict[str, Any]]:
    """Load gestionnaires keyed by id_gestionnaire (excludes gestionnaires with only SAA).
    
    Strategy: simple SELECT + Python-side filtering to avoid Supabase statement_timeout.
    1. Load all gestionnaires for the requested départements
    2. Query which id_gestionnaire have at least one non-SAA établissement
    3. Filter in Python (instant)
    """
    dept_clause = "AND departement_code = ANY(%s)" if departements != ["all"] else ""
    params: tuple = (departements,) if departements != ["all"] else ()

    # Step 1: load all gestionnaires (simple, fast query)
    cur.execute(f"""
        SELECT * FROM finess_gestionnaire
        WHERE 1=1 {dept_clause}
    """, params)
    all_gest = {row["id_gestionnaire"]: dict(row) for row in cur.fetchall()}

    # Step 2: find gestionnaire IDs that have at least one non-SAA établissement
    cur.execute(f"""
        SELECT DISTINCT id_gestionnaire 
        FROM finess_etablissement
        WHERE categorie_normalisee IS NOT NULL
          AND categorie_normalisee != 'SAA'
          {dept_clause}
    """, params)
    non_saa_gest_ids = {row["id_gestionnaire"] for row in cur.fetchall()}

    # Step 3: filter in Python
    filtered = {gid: g for gid, g in all_gest.items() if gid in non_saa_gest_ids}
    excluded = len(all_gest) - len(filtered)
    if excluded > 0:
        print(f"[SAA] Gestionnaires: {len(all_gest)} total, {excluded} exclus (SAA-only), {len(filtered)} retenus")
    return filtered


# =============================================================================
# Reports
# =============================================================================

def generate_reports(out_dir: str, departements: List[str], db: DatabaseManager) -> None:
    """Generate CSV/JSON reports for completed enrichments."""
    if pd_ is None:
        print("[REPORT] pandas non installé — rapports CSV ignorés")
        return

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    dept_label = "_".join(departements) if departements != ["all"] else "all"

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Établissements
            cur.execute("""
                SELECT * FROM finess_etablissement
                WHERE departement_code = ANY(%s) OR %s
                ORDER BY departement_code, raison_sociale
            """, (departements, departements == ["all"]))
            rows = cur.fetchall()
            if rows:
                df = pd_.DataFrame(rows)
                path_etab = out / f"finess_{dept_label}_etablissements.csv"
                df.to_csv(path_etab, index=False, encoding="utf-8")
                print(f"[REPORT] {path_etab} ({len(df)} lignes)")

            # Gestionnaires
            cur.execute("""
                SELECT * FROM finess_gestionnaire
                WHERE departement_code = ANY(%s) OR %s
                ORDER BY departement_code, raison_sociale
            """, (departements, departements == ["all"]))
            rows = cur.fetchall()
            if rows:
                df = pd_.DataFrame(rows)
                path_gest = out / f"finess_{dept_label}_gestionnaires.csv"
                df.to_csv(path_gest, index=False, encoding="utf-8")
                print(f"[REPORT] {path_gest} ({len(df)} lignes)")

            # Dirigeants
            cur.execute("""
                SELECT fd.*, fg.raison_sociale AS gestionnaire_nom
                FROM finess_dirigeant fd
                JOIN finess_gestionnaire fg ON fd.id_gestionnaire = fg.id_gestionnaire
                WHERE fg.departement_code = ANY(%s) OR %s
                ORDER BY fg.raison_sociale, fd.nom
            """, (departements, departements == ["all"]))
            rows = cur.fetchall()
            if rows:
                df = pd_.DataFrame(rows)
                path_dir = out / f"finess_{dept_label}_dirigeants.csv"
                df.to_csv(path_dir, index=False, encoding="utf-8")
                print(f"[REPORT] {path_dir} ({len(df)} lignes)")

            # Stats
            cur.execute("""
                SELECT
                    COUNT(*) AS total_etabs,
                    SUM(CASE WHEN enrichissement_statut = 'enrichi' THEN 1 ELSE 0 END) AS enrichis,
                    SUM(CASE WHEN enrichissement_statut = 'erreur' THEN 1 ELSE 0 END) AS erreurs,
                    SUM(CASE WHEN site_web IS NOT NULL THEN 1 ELSE 0 END) AS avec_site_web,
                    SUM(CASE WHEN type_public IS NOT NULL THEN 1 ELSE 0 END) AS avec_type_public,
                    SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) AS geolocalises,
                    SUM(CASE WHEN ouverture_365 IS NOT NULL THEN 1 ELSE 0 END) AS avec_ouverture
                FROM finess_etablissement
                WHERE departement_code = ANY(%s) OR %s
            """, (departements, departements == ["all"]))
            stats = dict(cur.fetchone()) if cur.rowcount else {}

            path_stats = out / f"finess_{dept_label}_stats.json"
            with open(path_stats, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2, ensure_ascii=False, default=str)
            print(f"[REPORT] {path_stats}")


# =============================================================================
# Email notification
# =============================================================================

def send_completion_email(
    departements: List[str],
    gest_enriched: int,
    gest_total: int,
    etabs_qualified: int,
    elapsed_minutes: float
) -> None:
    """Send completion email via Elasticmail API v2."""
    api_key = _clean_secret(os.getenv("ELASTICMAIL_API_KEY", ""))
    recipient = os.getenv("NOTIFICATION_EMAIL", "patrick.danto@confidensia.fr").strip()
    sender = os.getenv("SENDER_EMAIL", "patrick.danto@bmse.fr").strip()
    
    if not api_key:
        print("⚠️ ELASTICMAIL_API_KEY not set, skipping email notification")
        return
    
    dept_label = ",".join(departements) if departements != ["all"] else "ALL"
    
    subject = f"✅ FINESS Enrichment Complete: Dept {dept_label}"
    body = f"""FINESS Enrichment Pipeline Completed Successfully

Run ID: {_RUN_ID}
Départements: {dept_label}
Duration: {elapsed_minutes:.1f} minutes

Results:
- Gestionnaires enriched: {gest_enriched}/{gest_total}
- Établissements qualified: {etabs_qualified}

Check Cloud Run logs for details:
https://console.cloud.google.com/run/jobs/details/europe-west1/finess-enrich-{dept_label.lower()}

---
Automated notification from FINESS enrichment pipeline
"""
    
    payload = {
        'apikey': api_key,
        'from': sender,
        'fromName': 'FINESS Pipeline',
        'to': recipient,
        'subject': subject,
        'bodyText': body,
    }
    
    try:
        import requests
        response = requests.post(
            "https://api.elasticemail.com/v2/email/send",
            data=payload,
            timeout=30,
            verify=False
        )
        if response.status_code == 200:
            result = response.json()
            if result.get('success'):
                print(f"✅ Completion email sent to {recipient}")
                log_structured("INFO", "Completion email sent", recipient=recipient)
            else:
                print(f"⚠️ Email API error: {result.get('error', 'Unknown')}")
        else:
            print(f"⚠️ Email send failed: HTTP {response.status_code}")
    except Exception as e:
        print(f"⚠️ Failed to send email: {str(e)}")


# =============================================================================
# Main pipeline
# =============================================================================

def main() -> None:
    global _llm_provider
    
    t_start_pipeline = time.time()

    args = parse_args()
    db = DatabaseManager()

    # Resolve LLM provider (mistral or gemini)
    _llm_provider = os.getenv("LLM_PROVIDER", "mistral").strip().lower()
    serper_key = _clean_secret(os.getenv("SERPER_API_KEY", ""))

    # Suppress SSL warnings when verification is disabled (local debug only)
    if os.getenv("GEOCODING_SKIP_SSL_VERIFY", "").strip() == "1":
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if _llm_provider == "gemini":
        llm_key = _clean_secret(os.getenv("GEMINI_API_KEY", ""))
        llm_model = os.getenv("GEMINI_MODEL", str(GEMINI_CONFIG["model"])).strip()
    else:
        llm_key = _clean_secret(os.getenv("MISTRAL_API_KEY", ""))
        llm_model = os.getenv("MISTRAL_MODEL", str(MISTRAL_CONFIG["model"])).strip()

    print("=" * 70)
    print("PIPELINE ENRICHISSEMENT FINESS — Architecture 2 passes")
    print(f"  Run ID : {_RUN_ID}")
    print(f"  Départements : {args.departements}")
    print(f"  Limit : {args.limit}")
    print(f"  Dry-run : {args.dry_run}")
    print(f"  LLM Provider : {_llm_provider.upper()}")
    print(f"  LLM Model : {llm_model}")
    print(f"  LLM API Key : {'SET' if llm_key else 'NOT SET'}")
    print(f"  SERPER_API_KEY : {'SET' if serper_key else 'NOT SET'}")
    print(f"  Skip serper : {args.skip_serper}")
    print(f"  Skip LLM : {args.skip_llm}")
    print(f"  Skip geocode : {args.skip_geocode}")
    print("=" * 70)
    sys.stdout.flush()
    
    log_structured("INFO", "Pipeline started", 
                   departements=args.departements,
                   limit=args.limit,
                   llm_provider=_llm_provider,
                   llm_model=llm_model)

    # Support both nargs='+' list (from Cloud Run --args space-separated)
    # and old comma-separated single string (--departements 31,32,33)
    if len(args.departements) == 1:
        raw = args.departements[0]
        departements = raw.split(",") if raw != "all" else ["all"]
    else:
        departements = args.departements

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:

            # ── Disable Supabase statement_timeout for this session ──
            # Supabase sets statement_timeout=2min which kills DDL and large queries.
            # We reset it to 15 minutes for the enrichment pipeline.
            cur.execute("SET statement_timeout = '900s'")
            # Also set lock_timeout to fail fast on locks instead of waiting forever
            cur.execute("SET lock_timeout = '30s'")
            conn.commit()

            # ── Migration: add columns if missing ──
            # DDL (ALTER TABLE) needs higher lock_timeout because parallel
            # executions contend for AccessExclusiveLock on the same tables.
            # We retry with exponential backoff to handle contention.
            for _ddl_attempt in range(10):
                try:
                    cur.execute("SET lock_timeout = '60s'")
                    cur.execute("""
                        ALTER TABLE finess_gestionnaire
                        ADD COLUMN IF NOT EXISTS signaux_recents JSONB
                    """)
                    cur.execute("""
                        ALTER TABLE finess_etablissement
                        ADD COLUMN IF NOT EXISTS specificites_public TEXT
                    """)
                    conn.commit()
                    print(f"[MIGRATION] DDL OK (attempt {_ddl_attempt + 1})")
                    break
                except RETRIABLE_PG_ERRORS as e:
                    conn.rollback()
                    wait = min(2 ** _ddl_attempt + random.uniform(0, 1), 120)
                    print(f"[MIGRATION] DDL lock contention (attempt {_ddl_attempt + 1}/10): {e.__class__.__name__} — retry in {wait:.1f}s")
                    time.sleep(wait)
            else:
                print("[MIGRATION] ⚠ DDL failed after 10 attempts — continuing anyway (columns may already exist)")

            # Restore lock_timeout for the rest of the pipeline
            cur.execute("SET lock_timeout = '30s'")
            conn.commit()

            # ── ÉTAPE 1 : Business rules (batch) ──
            # Wrapped in retry_on_deadlock because parallel Cloud Run executions
            # may contend on gestionnaire rows (multi-department gestionnaires).
            t0_rules = time.time()
            if not args.dry_run:
                ok = retry_on_deadlock(
                    lambda cur2: apply_all_business_rules(cur2, departements),
                    conn,
                    max_retries=5,
                    base_delay=5.0,
                    label="apply_all_business_rules",
                )
                if not ok:
                    print("[WARN] Business rules partially failed (deadlock), continuing...")
            print(f"[STEP 1] Règles métier — {time.time() - t0_rules:.1f}s")

            # ================================================================
            # PASSE 1 : GESTIONNAIRES
            #   Serper gestionnaire, scraping site, dirigeants, DAF,
            #   emails, signaux, réseau fédéral
            # ================================================================
            gestionnaires = load_gestionnaires(cur, departements)

            # Pre-populate site_web cache from DB (for already-enriched gestionnaires)
            gest_site_web: Dict[str, Optional[str]] = {
                gid: g.get("site_web") for gid, g in gestionnaires.items()
                if g.get("site_web")
            }

            # Pre-load the ET we'll qualify in Pass 2 (needed to scope Pass 1)
            etabs = load_etabs_to_enrich(cur, departements, args.limit)

            # Only process gestionnaires needed by Pass 2 ET (and not yet enriched)
            needed_gest_ids = {e.get("id_gestionnaire") for e in etabs
                               if e.get("id_gestionnaire")} if args.limit else None
            
            # ── ÉTAPE 2 : Geocoding (batch) ──
            # Now that we know which entities will be enriched, geocode only those if --limit
            if not args.skip_geocode and not args.dry_run:
                t0_geo = time.time()
                if args.limit:
                    etab_ids = {e["id_finess"] for e in etabs}
                    geocode_all_entities(cur, departements, gest_ids=needed_gest_ids, etab_ids=etab_ids)
                else:
                    geocode_all_entities(cur, departements)
                conn.commit()
                print(f"[STEP 2] Géocodage — {time.time() - t0_geo:.1f}s")
            elif args.skip_geocode:
                print("[STEP 2] Géocodage — SKIP (--skip-geocode)")

            gest_list = [
                g for g in gestionnaires.values()
                if g.get("enrichissement_statut") != "enrichi"
                and (needed_gest_ids is None or g["id_gestionnaire"] in needed_gest_ids)
            ]
            print(f"\n{'='*70}")
            print(f"PASSE 1 — GESTIONNAIRES ({len(gest_list)} à enrichir"
                  f"{f' (scoped by --limit {args.limit})' if args.limit else ''}"
                  f" / {len(gestionnaires)} total)")
            print(f"{'='*70}")

            gest_enriched_count = 0
            gest_combined_text: Dict[str, str] = {}  # cached for Pass 2

            for i, gest in enumerate(gest_list):
                gest_id = gest["id_gestionnaire"]
                log_checkpoint("PASSE_1_GESTIONNAIRES", i+1, len(gest_list), gest_id)
                print(f"\n[G {i+1}/{len(gest_list)}] {gest.get('raison_sociale', '?')} "
                      f"({gest.get('secteur_activite_principal', '?')}) — {gest.get('commune', '?')}")
                t0 = time.time()

                try:
                    context: Dict[str, Any] = {
                        "combined_text": "",
                        "serper_results": [],
                        "emails_trouves": [],
                        "page_equipe_text": "",
                        "page_contact_text": "",
                        "pages_scrapped": [],
                        "site_web": None,
                        "serper_count": 0,
                    }

                    # ── Serper gestionnaire (5 queries) ──
                    if not args.skip_serper and serper_key:
                        gest_queries = build_serper_queries_gestionnaire(gest)
                        all_results: List[Dict[str, Any]] = []
                        for q in gest_queries:
                            results = get_or_search_serper(q, serper_key, cur)
                            all_results.extend(results)
                            context["serper_count"] += 1

                        context["serper_results"] = all_results
                        context["site_web"] = extraire_site_officiel(
                            all_results, gest.get("raison_sociale", ""))

                        context["combined_text"] = "\n".join([
                            f"- {r.get('title', '')} : {r.get('snippet', '')} ({r.get('link', '')})"
                            for r in all_results[:30]
                        ])

                        # Extract emails from snippets
                        for r in all_results:
                            text = r.get("snippet", "") + " " + r.get("title", "")
                            found = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
                            context["emails_trouves"].extend(found)
                        context["emails_trouves"] = list(set(context["emails_trouves"]))

                        # Store site_web on gestionnaire
                        if context["site_web"] and not args.dry_run:
                            cur.execute("""
                                UPDATE finess_gestionnaire SET site_web = %s
                                WHERE id_gestionnaire = %s AND site_web IS NULL
                            """, (context["site_web"], gest_id))

                        print(f"   [SERPER] {context['serper_count']} queries, "
                              f"site={context.get('site_web', '—')}")

                    gest_site_web[gest_id] = context.get("site_web")

                    # Cache combined_text for Pass 2 (before scraping enriches it)
                    # We'll store the full text AFTER scraping

                    # ── Scraping site gestionnaire ──
                    site_web = context.get("site_web") or gest.get("site_web")
                    if site_web and not args.skip_serper:
                        scrape_result = scrape_etablissement_pages(site_web)
                        context["combined_text"] += "\n" + scrape_result.get("combined_text", "")
                        context["page_equipe_text"] = scrape_result.get("page_equipe_text", "")
                        context["page_contact_text"] = scrape_result.get("page_contact_text", "")
                        context["pages_scrapped"] = scrape_result.get("pages_scrapped", [])
                        context["emails_trouves"] = list(set(
                            context["emails_trouves"] + scrape_result.get("emails_trouves", [])
                        ))
                        print(f"   [SCRAPE] {len(context['pages_scrapped'])} pages OK, "
                              f"{len(scrape_result.get('emails_trouves', []))} emails")

                    # ── Dirigeants + DAF ──
                    if not args.skip_llm and llm_key:
                        dirigeants = enrich_dirigeants(
                            gest, context, llm_key, llm_model, serper_key, cur)
                        nb_dir = store_dirigeants(gest_id, dirigeants, cur, args.dry_run)
                        print(f"   [DIR] {nb_dir} dirigeants identifiés")

                        daf = identify_daf(gest, dirigeants, serper_key,
                                           llm_key, llm_model, cur)
                        if daf:
                            store_daf_on_gestionnaire(gest_id, daf, cur, args.dry_run)
                            print(f"   [DAF] {daf.get('prenom', '')} {daf.get('nom', '')}")

                    # ── Emails ──
                    all_emails = list(set(context.get("emails_trouves", [])))
                    nb_emails = reconstruct_emails_for_gestionnaire(
                        gest_id, all_emails,
                        site_web,
                        gest.get("raison_sociale", ""),
                        serper_key if not args.skip_serper else None,
                        llm_key if not args.skip_llm else None,
                        llm_model,
                        cur, args.dry_run,
                    )
                    if nb_emails:
                        print(f"   [EMAIL] {nb_emails} emails reconstitués")

                    # ── Signaux de tension ──
                    if not args.skip_serper and serper_key and not args.skip_llm and llm_key:
                        sig = enrich_signaux_gestionnaire(
                            gest, context, llm_key, llm_model, serper_key, cur)
                        update_signaux_gestionnaire(gest_id, sig, cur, args.dry_run)
                        if sig.get("signal_tension"):
                            print(f"   [SIGNAL] ⚠ {sig.get('signal_tension_detail', '')}")

                    # ── Réseau fédéral ──
                    reseau = enrich_reseau_federal(gest, context, llm_key, llm_model, cur, args.dry_run)
                    if reseau:
                        print(f"   [RESEAU] {reseau}")

                    # Mark gestionnaire enriched
                    if not args.dry_run:
                        cur.execute("""
                            UPDATE finess_gestionnaire SET
                                enrichissement_statut = 'enrichi',
                                date_enrichissement = NOW()
                            WHERE id_gestionnaire = %s
                        """, (gest_id,))

                    # Cache combined_text for Pass 2 ET qualification
                    gest_combined_text[gest_id] = context.get("combined_text", "")

                    conn.commit()
                    gest_enriched_count += 1
                    elapsed = time.time() - t0
                    log_enrichissement(cur, gest_id, "gestionnaire", "complet", "succes",
                                       {"elapsed_s": elapsed}, context.get("serper_count", 0))
                    conn.commit()
                    print(f"   ✅ Gestionnaire enrichi en {elapsed:.1f}s")

                except RETRIABLE_PG_ERRORS as e:
                    conn.rollback()
                    # Restore session settings after rollback
                    cur.execute("SET statement_timeout = '900s'")
                    cur.execute("SET lock_timeout = '30s'")
                    conn.commit()
                    print(f"   ⚠ Deadlock/lock gestionnaire {gest_id}: {type(e).__name__}, retrying once...")
                    time.sleep(random.uniform(2, 8))
                    # Single retry for the commit of already-gathered data
                    try:
                        if not args.dry_run:
                            cur.execute("""
                                UPDATE finess_gestionnaire SET
                                    enrichissement_statut = 'enrichi',
                                    date_enrichissement = NOW()
                                WHERE id_gestionnaire = %s
                            """, (gest_id,))
                        conn.commit()
                        gest_enriched_count += 1
                        print(f"   ✅ Gestionnaire enrichi (après retry)")
                    except Exception as e2:
                        conn.rollback()
                        print(f"   ❌ Erreur gestionnaire (retry échoué): {e2}")

                except Exception as e:
                    conn.rollback()
                    # Restore session settings after rollback
                    try:
                        cur.execute("SET statement_timeout = '900s'")
                        cur.execute("SET lock_timeout = '30s'")
                        conn.commit()
                    except Exception:
                        pass
                    print(f"   ❌ Erreur gestionnaire: {e}")

                time.sleep(0.3)

            # ================================================================
            # PASSE 2 : ÉTABLISSEMENTS
            #   Serper ET-spécifique (catégorie), LLM qualification,
            #   propagation site_web du gestionnaire
            # ================================================================
            print(f"\n{'='*70}")
            print(f"PASSE 2 — ÉTABLISSEMENTS ({len(etabs)})")
            print(f"{'='*70}")

            for i, etab in enumerate(etabs):
                etab_id = etab["id_finess"]
                gest_id = etab.get("id_gestionnaire")
                log_checkpoint("PASSE_2_ETABLISSEMENTS", i+1, len(etabs), etab_id)
                print(f"\n[E {i+1}/{len(etabs)}] {etab['raison_sociale']} "
                      f"({etab.get('categorie_normalisee', '?')}) — {etab.get('commune', '?')}")
                t0 = time.time()

                try:
                    if not args.dry_run:
                        mark_in_progress(etab_id, cur)

                    # Propagate site_web from gestionnaire if ET doesn't have one
                    inherited_site = gest_site_web.get(gest_id or "")
                    if inherited_site and not etab.get("site_web") and not args.dry_run:
                        cur.execute("""
                            UPDATE finess_etablissement SET site_web = %s
                            WHERE id_finess = %s AND site_web IS NULL
                        """, (inherited_site, etab_id))

                    # Start with gestionnaire context if available
                    gest_text = gest_combined_text.get(gest_id or "", "")

                    context: Dict[str, Any] = {
                        "combined_text": gest_text,
                        "serper_results": [],
                        "emails_trouves": [],
                        "site_web": etab.get("site_web") or inherited_site,
                        "serper_count": 0,
                    }

                    # ── Serper ET-spécifique (catégorie uniquement) ──
                    if not args.skip_serper and serper_key:
                        et_queries = _build_et_specific_queries(etab)
                        if et_queries:
                            for q in et_queries:
                                results = get_or_search_serper(q, serper_key, cur)
                                context["combined_text"] += "\n".join([
                                    f"- {r.get('title', '')} : {r.get('snippet', '')} ({r.get('link', '')})"
                                    for r in results[:5]
                                ]) + "\n"
                                context["serper_count"] += 1
                            print(f"   [SERPER] {context['serper_count']} queries ET-spécifiques")

                    # ── Scraping ciblé (si ET a son propre site, différent du gestionnaire) ──
                    et_site = etab.get("site_web")
                    if et_site and et_site != inherited_site and not args.skip_serper:
                        scrape_result = scrape_etablissement_pages(et_site)
                        context["combined_text"] += "\n" + scrape_result.get("combined_text", "")
                        print(f"   [SCRAPE] {len(scrape_result.get('pages_scrapped', []))} pages")

                    # ── LLM qualification ──
                    if not args.skip_llm and llm_key and context.get("combined_text", "").strip():
                        qual = qualify_with_llm(etab, context, llm_key, llm_model)
                        apply_qualification(etab_id, qual, context.get("site_web"), cur, args.dry_run)
                        specs = qual.get('specificites_public', '')
                        print(f"   [LLM] type_public={qual.get('type_public', '?')}, "
                              f"ouverture_365={qual.get('ouverture_365', '?')}")
                        if specs:
                            print(f"   [LLM] spécificités: {specs}")

                    # Mark complete
                    if not args.dry_run:
                        mark_enriched(etab_id, cur)
                    conn.commit()

                    elapsed = time.time() - t0
                    log_enrichissement(cur, etab_id, "etablissement", "qualification", "succes",
                                       {"elapsed_s": elapsed}, context.get("serper_count", 0))
                    conn.commit()
                    print(f"   ✅ Qualifié en {elapsed:.1f}s")

                except RETRIABLE_PG_ERRORS as e:
                    conn.rollback()
                    try:
                        cur.execute("SET statement_timeout = '900s'")
                        cur.execute("SET lock_timeout = '30s'")
                        conn.commit()
                    except Exception:
                        pass
                    print(f"   ⚠ Lock/deadlock ET {etab_id}: {type(e).__name__}, skipping")
                    if not args.dry_run:
                        try:
                            mark_error(etab_id, f"lock:{type(e).__name__}", cur)
                            conn.commit()
                        except Exception:
                            conn.rollback()

                except Exception as e:
                    conn.rollback()
                    try:
                        cur.execute("SET statement_timeout = '900s'")
                        cur.execute("SET lock_timeout = '30s'")
                        conn.commit()
                    except Exception:
                        pass
                    if not args.dry_run:
                        mark_error(etab_id, str(e)[:500], cur)
                        conn.commit()
                    print(f"   ❌ Erreur: {e}")

                time.sleep(0.3)

    # Generate reports
    if args.out_dir:
        generate_reports(args.out_dir, departements, db)

    elapsed_minutes = (time.time() - t_start_pipeline) / 60
    
    print("\n" + "=" * 70)
    print("ENRICHISSEMENT TERMINÉ")
    print(f"  Passe 1 : {gest_enriched_count}/{len(gest_list)} gestionnaires enrichis")
    print(f"  Passe 2 : {len(etabs)} établissements qualifiés")
    print(f"  Duration : {elapsed_minutes:.1f} minutes")
    print("=" * 70)
    sys.stdout.flush()
    
    log_structured("INFO", "Pipeline completed successfully",
                   gest_enriched=gest_enriched_count,
                   gest_total=len(gest_list),
                   etabs_qualified=len(etabs),
                   elapsed_minutes=round(elapsed_minutes, 1))
    
    # Send completion email notification
    send_completion_email(
        departements=departements,
        gest_enriched=gest_enriched_count,
        gest_total=len(gest_list),
        etabs_qualified=len(etabs),
        elapsed_minutes=elapsed_minutes
    )


def _build_et_specific_queries(etab: Dict[str, Any]) -> List[str]:
    """Build ET-specific Serper queries (category extras + qualification only).

    Much lighter than the old build_serper_queries() since gestionnaire-level
    queries (site, dirigeants, LinkedIn) are already done in Pass 1.
    """
    nom = etab.get("raison_sociale", "")
    commune = etab.get("commune", "")
    cat = etab.get("categorie_normalisee", "")

    # Base: just the ET name query for qualification context
    queries = [
        f'"{nom}" {commune} public accueilli missions',
    ]

    # Extra queries by category (those that add ET-specific value)
    if cat in ("EHPAD", "USLD", "RA", "EHPA"):
        queries.extend([q.format(nom=nom) for q in EXTRA_QUERIES_EHPAD])
    elif cat in ("IME", "SESSAD", "ITEP", "IEM", "CAMSP", "CMPP", "EEAP"):
        queries.extend([q.format(nom=nom) for q in EXTRA_QUERIES_HANDICAP_ENFANT])
    elif cat in ("MECS", "FDE", "AEMO", "LVA"):
        queries.extend([q.format(nom=nom) for q in EXTRA_QUERIES_PROTECTION_ENFANCE])

    return queries


# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrichissement FINESS par département (lit/écrit directement dans Supabase)"
    )
    parser.add_argument(
        "--departements", nargs="+", default=["65"],
        help="Code(s) département, séparés par espace ou virgule, ou 'all' (défaut: 65)",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Nombre max d'établissements par département (0 = pas de limite)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Ne pas écrire en base, uniquement afficher et générer des rapports",
    )
    parser.add_argument(
        "--out-dir", default="outputs/finess",
        help="Dossier de sortie pour les rapports CSV/JSON",
    )
    parser.add_argument(
        "--skip-serper", action="store_true",
        help="Ne pas faire les requêtes Serper",
    )
    parser.add_argument(
        "--skip-llm", action="store_true",
        help="Ne pas appeler le LLM (règles métier + géocodage uniquement)",
    )
    parser.add_argument(
        "--skip-geocode", action="store_true",
        help="Ne pas géocoder les adresses",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
