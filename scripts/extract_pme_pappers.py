"""
extract_pme_pappers.py
----------------------
Extrait les PME d'Ile-de-France (50-500 salaries) depuis l'API Pappers.

Prerequis:
- Variable .env: PAPPERS_API_KEY=<votre_cle>

Usage:
  python scripts/extract_pme_pappers.py \
      --output data/pme_idf_50_500_pappers.csv \
      --max-pages 200 \
      --per-page 100 \
      --log-level INFO
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger("extract_pme_pappers")

PAPPERS_SEARCH_URL = "https://api.pappers.fr/v2/recherche"
IDF_DEPTS = ["75", "77", "78", "91", "92", "93", "94", "95"]

OUTPUT_COLUMNS = [
    "siren",
    "siret_siege",
    "nom_entreprise",
    "personne_morale",
    "entreprise_cessee",
    "statut_rcs",
    "forme_juridique",
    "categorie_juridique",
    "code_naf",
    "libelle_code_naf",
    "domaine_activite",
    "tranche_effectif",
    "effectif_min",
    "effectif_max",
    "annee_effectif",
    "date_creation",
    "ville",
    "code_postal",
    "code_commune",
    "adresse_ligne_1",
    "adresse_ligne_2",
    "departement",
]


def load_env(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def fetch_search(params: Dict[str, Any], retries: int = 4) -> Optional[Dict[str, Any]]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(PAPPERS_SEARCH_URL, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = min(30, 2 * attempt)
                log.warning("Rate limit Pappers, attente %ss", wait)
                time.sleep(wait)
                continue
            if resp.status_code in (401, 403):
                raise SystemExit(f"Cle Pappers invalide (HTTP {resp.status_code})")
            log.warning("HTTP %s sur Pappers (tentative %s/%s)", resp.status_code, attempt, retries)
            time.sleep(2 * attempt)
        except requests.RequestException as exc:
            log.warning("Erreur reseau Pappers: %s (tentative %s/%s)", exc, attempt, retries)
            time.sleep(2 * attempt)
    return None


def map_result_to_row(item: Dict[str, Any], dept: str) -> Dict[str, Any]:
    siege = item.get("siege") or {}
    return {
        "siren": item.get("siren", ""),
        "siret_siege": siege.get("siret", ""),
        "nom_entreprise": item.get("nom_entreprise", ""),
        "personne_morale": bool(item.get("personne_morale", False)),
        "entreprise_cessee": item.get("entreprise_cessee", ""),
        "statut_rcs": item.get("statut_rcs", ""),
        "forme_juridique": item.get("forme_juridique", ""),
        "categorie_juridique": item.get("categorie_juridique", ""),
        "code_naf": item.get("code_naf", ""),
        "libelle_code_naf": item.get("libelle_code_naf", ""),
        "domaine_activite": item.get("domaine_activite", ""),
        "tranche_effectif": item.get("tranche_effectif", ""),
        "effectif_min": item.get("effectif_min", ""),
        "effectif_max": item.get("effectif_max", ""),
        "annee_effectif": item.get("annee_effectif", ""),
        "date_creation": item.get("date_creation", ""),
        "ville": siege.get("ville", ""),
        "code_postal": siege.get("code_postal", ""),
        "code_commune": siege.get("code_commune", ""),
        "adresse_ligne_1": siege.get("adresse_ligne_1", ""),
        "adresse_ligne_2": siege.get("adresse_ligne_2", ""),
        "departement": dept,
    }


def keep_item(item: Dict[str, Any], keep_missing_effectif: bool) -> bool:
    # Entreprises actives uniquement
    if item.get("entreprise_cessee") in (1, "1", True):
        return False

    # Societes (personne morale) uniquement pour un pipeline B2B PME
    if not item.get("personne_morale", False):
        return False

    e_min = as_int(item.get("effectif_min"))
    e_max = as_int(item.get("effectif_max"))

    # Si effectifs indisponibles, garder optionnellement.
    if e_min is None and e_max is None:
        return keep_missing_effectif

    # Garde les entreprises dont l'intervalle chevauche [50, 500]
    if e_max is not None and e_max < 50:
        return False
    if e_min is not None and e_min > 500:
        return False
    return True


def extract_dept(
    dept: str,
    api_key: str,
    writer: csv.DictWriter,
    seen_siren: set[str],
    max_pages: int,
    per_page: int,
    keep_missing_effectif: bool,
    stats: Dict[str, int],
) -> None:
    page = 1

    log.info("Dept %s: debut", dept)

    while page <= max_pages:
        params: Dict[str, Any] = {
            "api_token": api_key,
            "q": "",
            "departement": dept,
            "effectif_min": 50,
            "effectif_max": 500,
            "par_page": per_page,
            "page": page,
        }
        data = fetch_search(params)
        if not data:
            log.error("Dept %s page %s: reponse vide", dept, page)
            break

        resultats: List[Dict[str, Any]] = data.get("resultats", [])
        total = as_int(data.get("total")) or 0

        if page == 1:
            log.info("Dept %s: total annonce=%s", dept, total)

        if not resultats:
            break

        new_rows = 0
        for item in resultats:
            siren = str(item.get("siren", "")).strip()
            if not siren or siren in seen_siren:
                continue
            if not keep_item(item, keep_missing_effectif=keep_missing_effectif):
                continue

            seen_siren.add(siren)
            writer.writerow(map_result_to_row(item, dept))
            new_rows += 1
            stats["written"] += 1

        stats["fetched"] += len(resultats)
        log.info(
            "Dept %s page %s: fetched=%s retained=%s total_written=%s",
            dept,
            page,
            len(resultats),
            new_rows,
            stats["written"],
        )

        # Pagination simple: stop si la page courante couvre deja le total annonce
        if page * per_page >= total:
            break

        page += 1
        time.sleep(0.12)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extraction PME IDF (50-500) via Pappers")
    parser.add_argument("--output", default="data/pme_idf_50_500_pappers.csv")
    parser.add_argument("--api-key-env", default="PAPPERS_API_KEY")
    parser.add_argument("--depts", nargs="+", default=IDF_DEPTS)
    parser.add_argument("--max-pages", type=int, default=200)
    parser.add_argument("--per-page", type=int, default=100)
    parser.add_argument("--keep-missing-effectif", action="store_true", default=False)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    load_env(Path(".env"))
    api_key = (os.environ.get(args.api_key_env) or "").strip()
    if not api_key:
        raise SystemExit(f"Cle manquante: definir {args.api_key_env} dans .env")

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    stats: Dict[str, int] = {"fetched": 0, "written": 0}
    seen_siren: set[str] = set()

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()

        for dept in args.depts:
            extract_dept(
                dept=dept,
                api_key=api_key,
                writer=writer,
                seen_siren=seen_siren,
                max_pages=args.max_pages,
                per_page=args.per_page,
                keep_missing_effectif=args.keep_missing_effectif,
                stats=stats,
            )
            f.flush()

    log.info("Termine: fetched=%s, written=%s, output=%s", stats["fetched"], stats["written"], out_path)


if __name__ == "__main__":
    main()
