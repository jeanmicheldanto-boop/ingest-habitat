"""
extract_pme_sirene.py
---------------------
Extrait depuis l'API SIRENE (INSEE) les établissements PME d'Ile-de-France
ayant entre 50 et 500 salariés.

Tranches d'effectifs SIRENE:
  21 = 50-99 sal.   22 = 100-199 sal.
  31 = 200-249 sal. 32 = 250-499 sal.

Departements IDF: 75, 77, 78, 91, 92, 93, 94, 95

Usage:
  python scripts/extract_pme_sirene.py \
      --output data/pme_idf_50_500.csv \
      --token-env SIRENE_TOKEN \
      [--max-pages 500] \
      [--log-level INFO]

Token d'acces:
  Obtenir un token OAuth2 sur https://portail-api.insee.fr
  puis le placer dans le .env : SIRENE_TOKEN=Bearer xxx
  OU passer directement SIRENE_TOKEN=xxx (le "Bearer " est ajout automatiquement)
"""

import argparse
import csv
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

log = logging.getLogger("extract_pme_sirene")

# ---------- constantes --------------------------------------------------

IDF_DEPTS = ["75", "77", "78", "91", "92", "93", "94", "95"]

# Tranches 50-499 sal.
TRANCHES_50_500 = ["21", "22", "31", "32"]

SIRENE_BASE_CANDIDATES = [
    "https://api.insee.fr/api-sirene/3.11",
    "https://api.insee.fr/api-sirene/3",
    "https://api.insee.fr/entreprises/sirene/V3.11",
    "https://api.insee.fr/entreprises/sirene/V3",
]

# Champs a extraire
CHAMPS_ETABLISSEMENT = [
    "siret",
    "siren",
    "denominationUniteLegale",
    "sigleUniteLegale",
    "categorieJuridiqueUniteLegale",
    "activitePrincipaleUniteLegale",
    "trancheEffectifsUniteLegale",
    "trancheEffectifsEtablissement",
    "anneeEffectifsEtablissement",
    "etatAdministratifUniteLegale",
    "etatAdministratifEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "codeCommuneEtablissement",
    "libelleCommuneEtrangerEtablissement",
    "codePaysEtrangerEtablissement",
    "typeVoieEtablissement",
    "libelleVoieEtablissement",
    "numeroVoieEtablissement",
    "complementAdresseEtablissement",
    "activitePrincipaleEtablissement",
    "denominationUsuelle1UniteLegale",
    "denominationUsuelle2UniteLegale",
    "denominationUsuelle3UniteLegale",
    "nomUniteLegale",
    "nomUsageUniteLegale",
    "prenomUsuelUniteLegale",
    "categorieEntreprise",
    "etablissementSiege",
    "geo_adresse",
    "geo_l4",
    "geo_l5",
    "libelleCommuneEtablissement",
]

OUTPUT_COLS = [
    "siret",
    "siren",
    "raison_sociale",
    "sigle",
    "categorie_juridique",
    "naf_unite_legale",
    "naf_etablissement",
    "tranche_effectifs_etablissement",
    "tranche_effectifs_unite_legale",
    "annee_effectifs",
    "etat_admin_ul",
    "etat_admin_etab",
    "siege",
    "adresse",
    "code_postal",
    "commune",
    "code_commune",
    "categorie_entreprise",
]


# ---------- helpers --------------------------------------------------

def load_env(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def get_token(env_var: str) -> str:
    raw = (os.environ.get(env_var) or "").strip()
    if not raw:
        raise SystemExit(
            f"Token manquant. Definir {env_var}=<token> dans le .env ou l'environnement.\n"
            "Obtenir un token sur https://portail-api.insee.fr"
        )
    if not raw.lower().startswith("bearer "):
        raw = f"Bearer {raw}"
    return raw


def get_api_key(env_var: str) -> str:
    return (os.environ.get(env_var) or "").strip()


def sirene_get(url: str, params: Dict, headers: Dict, retries: int = 4) -> Optional[Dict]:
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                log.warning("Rate-limited, attente %ss", wait)
                time.sleep(wait)
                continue
            if resp.status_code in (401, 403):
                raise SystemExit(f"Token invalide ou expire (HTTP {resp.status_code})")
            log.warning("HTTP %s pour %s (tentative %s/%s)", resp.status_code, url, attempt, retries)
            time.sleep(3 * attempt)
        except requests.RequestException as exc:
            log.warning("Erreur reseau: %s (tentative %s/%s)", exc, attempt, retries)
            time.sleep(3 * attempt)
    return None


def resolve_sirene_base(headers: Dict, forced_base: str = "") -> str:
    """Trouve dynamiquement un endpoint SIRENE valide (200) pour /siret."""
    candidates = [forced_base.strip()] if forced_base.strip() else SIRENE_BASE_CANDIDATES
    probe_params = {
        "q": "codeDepartementEtablissement:75 AND trancheEffectifsEtablissement:21",
        "nombre": 1,
        "curseur": "*",
    }

    for base in candidates:
        url = f"{base}/siret"
        try:
            resp = requests.get(url, params=probe_params, headers=headers, timeout=20)
            if resp.status_code == 200:
                log.info("Endpoint SIRENE actif: %s", base)
                return base
            if resp.status_code in (401, 403):
                raise SystemExit(f"Authentification invalide pour {base} (HTTP {resp.status_code})")
            log.warning("Endpoint KO %s (HTTP %s)", base, resp.status_code)
        except requests.RequestException as exc:
            log.warning("Erreur reseau sur endpoint %s: %s", base, exc)

    raise SystemExit(
        "Aucun endpoint SIRENE valide.\n"
        "Essayez --base-url https://api.insee.fr/api-sirene/3.11"
    )


def flatten_etablissement(etab: Dict[str, Any], ul: Dict[str, Any]) -> Dict[str, str]:
    """Produit une ligne CSV aplatie a partir d'un etablissement + son UL."""
    # Raison sociale
    raison = (
        ul.get("denominationUniteLegale")
        or ul.get("denominationUsuelle1UniteLegale")
        or f"{ul.get('prenomUsuelUniteLegale','')} {ul.get('nomUsageUniteLegale') or ul.get('nomUniteLegale','')}".strip()
    )
    # Adresse
    adresse_parts = [
        etab.get("numeroVoieEtablissement", ""),
        etab.get("typeVoieEtablissement", ""),
        etab.get("libelleVoieEtablissement", ""),
        etab.get("complementAdresseEtablissement", ""),
    ]
    adresse = " ".join(p for p in adresse_parts if p).strip()

    return {
        "siret": etab.get("siret", ""),
        "siren": etab.get("siren", ""),
        "raison_sociale": raison or "",
        "sigle": ul.get("sigleUniteLegale", ""),
        "categorie_juridique": ul.get("categorieJuridiqueUniteLegale", ""),
        "naf_unite_legale": ul.get("activitePrincipaleUniteLegale", ""),
        "naf_etablissement": etab.get("activitePrincipaleEtablissement", ""),
        "tranche_effectifs_etablissement": etab.get("trancheEffectifsEtablissement", ""),
        "tranche_effectifs_unite_legale": ul.get("trancheEffectifsUniteLegale", ""),
        "annee_effectifs": etab.get("anneeEffectifsEtablissement", ""),
        "etat_admin_ul": ul.get("etatAdministratifUniteLegale", ""),
        "etat_admin_etab": etab.get("etatAdministratifEtablissement", ""),
        "siege": "oui" if etab.get("etablissementSiege") else "non",
        "adresse": adresse,
        "code_postal": etab.get("codePostalEtablissement", ""),
        "commune": etab.get("libelleCommuneEtablissement", ""),
        "code_commune": etab.get("codeCommuneEtablissement", ""),
        "categorie_entreprise": ul.get("categorieEntreprise", ""),
    }


# ---------- extraction --------------------------------------------------

def extract_for_dept_tranche(
    sirene_base: str,
    dept: str,
    tranche: str,
    headers: Dict,
    out_writer: csv.DictWriter,
    seen_siret: set,
    max_pages: int,
    stats: Dict,
) -> None:
    """Pagine sur /siret pour un departement + une tranche d'effectifs."""
    url = f"{sirene_base}/siret"
    cursor = "*"
    page = 0

    q = (
        f"codeDepartementEtablissement:{dept} "
        f"AND trancheEffectifsEtablissement:{tranche} "
        f"AND etatAdministratifEtablissement:A "
        f"AND etatAdministratifUniteLegale:A"
    )

    log.info("Dept=%s tranche=%s → debut extraction", dept, tranche)

    while page < max_pages:
        params = {
            "q": q,
            "nombre": 1000,
            "curseur": cursor,
            "champs": ",".join([
                "siret", "siren", "etablissementSiege",
                "trancheEffectifsEtablissement", "anneeEffectifsEtablissement",
                "etatAdministratifEtablissement",
                "codePostalEtablissement", "libelleCommuneEtablissement", "codeCommuneEtablissement",
                "typeVoieEtablissement", "libelleVoieEtablissement", "numeroVoieEtablissement",
                "complementAdresseEtablissement",
                "activitePrincipaleEtablissement",
                # UL inline
                "denominationUniteLegale", "denominationUsuelle1UniteLegale",
                "sigleUniteLegale", "categorieJuridiqueUniteLegale",
                "activitePrincipaleUniteLegale", "trancheEffectifsUniteLegale",
                "etatAdministratifUniteLegale", "categorieEntreprise",
                "nomUniteLegale", "nomUsageUniteLegale", "prenomUsuelUniteLegale",
            ]),
        }
        data = sirene_get(url, params, headers)
        if not data:
            log.error("Reponse vide dept=%s tranche=%s page=%s, arret", dept, tranche, page)
            break

        header = data.get("header", {})
        total = int(header.get("total", 0))
        next_cursor = header.get("curseurSuivant")
        etabs = data.get("etablissements", [])

        if page == 0:
            log.info("  Total attendu: %d etablissements", total)

        new = 0
        for etab in etabs:
            siret = etab.get("siret", "")
            if siret in seen_siret:
                continue
            seen_siret.add(siret)
            ul = etab.get("uniteLegale", {})
            row = flatten_etablissement(etab, ul)
            out_writer.writerow(row)
            new += 1
            stats["total"] += 1

        log.info(
            "  Page %d: %d resultats, %d nouveaux (total=%d)",
            page + 1, len(etabs), new, stats["total"],
        )

        if not next_cursor or next_cursor == cursor or not etabs:
            break
        cursor = next_cursor
        page += 1
        # Respecter le rate-limit SIRENE (~500 req/min pour abonnes, soyons prudents)
        time.sleep(0.15)


# ---------- main --------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Extrait les PME IDF 50-500 sal. depuis l'API SIRENE")
    ap.add_argument("--output", default="data/pme_idf_50_500.csv")
    ap.add_argument("--token-env", default="SIRENE_TOKEN", help="Nom de la variable d'env contenant le token Bearer")
    ap.add_argument("--api-key-env", default="SIRENE_API_KEY", help="Nom de la variable d'env contenant la cle API INSEE")
    ap.add_argument("--depts", nargs="+", default=IDF_DEPTS, help="Codes departements (defaut: IDF)")
    ap.add_argument("--tranches", nargs="+", default=TRANCHES_50_500, help="Tranches effectifs SIRENE (defaut: 21 22 31 32)")
    ap.add_argument("--max-pages", type=int, default=500, help="Limite de pages par (dept, tranche)")
    ap.add_argument("--base-url", default="", help="Force un endpoint SIRENE (ex: https://api.insee.fr/api-sirene/3.11)")
    ap.add_argument("--siege-seulement", action="store_true", default=False,
                    help="Ne garder que les etablissements sieges")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    load_env(Path(".env"))
    api_key = get_api_key(args.api_key_env)
    token = ""
    try:
        token = get_token(args.token_env)
    except SystemExit:
        token = ""

    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = token
        log.info("Auth SIRENE: Bearer token via %s", args.token_env)
    elif api_key:
        headers["X-Gravitee-Api-Key"] = api_key
        log.info("Auth SIRENE: API key via %s", args.api_key_env)
    else:
        raise SystemExit(
            "Aucune authentification SIRENE configuree.\n"
            f"Definir {args.token_env}=<token> ou {args.api_key_env}=<api_key> dans .env"
        )

    sirene_base = resolve_sirene_base(headers, forced_base=args.base_url)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    stats = {"total": 0}
    seen_siret: set = set()

    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_COLS)
        writer.writeheader()

        for dept in args.depts:
            for tranche in args.tranches:
                extract_for_dept_tranche(
                    sirene_base=sirene_base,
                    dept=dept,
                    tranche=tranche,
                    headers=headers,
                    out_writer=writer,
                    seen_siret=seen_siret,
                    max_pages=args.max_pages,
                    stats=stats,
                )
                f.flush()

    log.info("Termine. %d etablissements ecrits dans %s", stats["total"], out_path)

    # Bref resume
    rows = list(csv.DictReader(out_path.open(encoding="utf-8")))
    by_dept: Dict[str, int] = {}
    by_tranche: Dict[str, int] = {}
    for r in rows:
        cp = r.get("code_postal", "")[:2]
        by_dept[cp] = by_dept.get(cp, 0) + 1
        t = r.get("tranche_effectifs_etablissement", "?")
        by_tranche[t] = by_tranche.get(t, 0) + 1
    log.info("Repartition par dept: %s", dict(sorted(by_dept.items())))
    log.info("Repartition par tranche: %s", by_tranche)


if __name__ == "__main__":
    main()
