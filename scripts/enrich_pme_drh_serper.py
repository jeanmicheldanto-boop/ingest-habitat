from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import requests

# Ensure workspace root is importable when running this script directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.enrich_communes_idf_contacts import (
    Client,
    build_email,
    clean_domain,
    detect_pattern,
    extract_emails,
    load_dotenv_if_present,
)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
log = logging.getLogger("pme_drh_serper")

EXTRA_OUTPUT_COLUMNS = [
    "domain",
    "drh_prenom",
    "drh_nom",
    "drh_poste",
    "drh_confidence",
    "drh_source_url",
    "drh_reason",
    "drh_email_public",
    "drh_email_reconstitue",
    "drh_email_confidence",
]


def read_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(f)]


def hunter_domain(company: str, api_key: str) -> Optional[str]:
    if not api_key or not company:
        return None
    try:
        r = requests.get(
            "https://api.hunter.io/v2/domain-search",
            params={"company": company, "api_key": api_key},
            timeout=20,
        )
        if r.status_code != 200:
            return None
        data = r.json().get("data", {})
        return clean_domain(data.get("domain", ""))
    except Exception:
        return None


def serper_domain(client: Client, company: str, city: str) -> Optional[str]:
    queries = [
        f'"{company}" "{city}" site officiel',
        f'"{company}" "{city}" entreprise',
    ]
    for q in queries:
        items = client.serper_search(q, num=5, label="domain_discovery_pme")
        for it in items:
            d = clean_domain(it.get("link", ""))
            if d:
                return d
    return None


def extract_drh_from_snippets(
    client: Client,
    company: str,
    city: str,
    domain: str,
    snippets: List[Dict[str, Any]],
) -> Dict[str, str]:
    if not snippets:
        return {"prenom": "", "nom": "", "poste": "", "confidence": "basse", "source_url": "", "reason": "no_snippets", "email_public": ""}

    lines = []
    for it in snippets[:10]:
        lines.append(f"- {it.get('title','')} | {it.get('snippet','')} | {it.get('link','')}")
    txt = "\n".join(lines)[:7000]

    prompt = (
        f"Tu identifies le DRH (ou responsable RH equivalent) d'une entreprise privee.\n"
        f"Entreprise: {company}\n"
        f"Ville: {city}\n"
        f"Domaine web estime: {domain or 'inconnu'}\n"
        "Regles strictes:\n"
        "- Ignore les profils anciens/ex/retraites\n"
        "- Ignore les resultats qui parlent de mairies/collectivites sans lien entreprise\n"
        "- Si aucun nom fiable, renvoie prenom et nom vides\n"
        "Extraits:\n"
        f"{txt}\n\n"
        "Reponds UNIQUEMENT en JSON:\n"
        "{\n"
        '  "prenom":"",\n'
        '  "nom":"",\n'
        '  "poste":"",\n'
        '  "confidence":"haute|moyenne|basse",\n'
        '  "source_url":"",\n'
        '  "reason":"",\n'
        '  "email_public":""\n'
        "}"
    )
    out = client.llm_json(prompt, max_tokens=350)
    return {
        "prenom": (out.get("prenom") or "").strip(),
        "nom": (out.get("nom") or "").strip(),
        "poste": (out.get("poste") or "").strip(),
        "confidence": (out.get("confidence") or "basse").strip().lower(),
        "source_url": (out.get("source_url") or "").strip(),
        "reason": (out.get("reason") or "").strip(),
        "email_public": (out.get("email_public") or "").strip().lower(),
    }


def collect_domain_emails(client: Client, domain: str) -> List[str]:
    if not domain:
        return []
    found: set[str] = set()
    for page in ["", "/contact", "/about", "/equipe", "/team", "/mentions-legales", "/nous-contacter"]:
        html = client.fetch_url(f"https://{domain}{page}")
        if html:
            for e in extract_emails(html, domain=domain):
                found.add(e)
    return sorted(found)


def build_drh_queries(company: str, city: str, domain: str) -> List[str]:
    queries: List[str] = []
    if domain:
        queries.append(f'site:{domain} ("directeur des ressources humaines" OR DRH OR "responsable RH" OR "ressources humaines")')
        queries.append(f'site:{domain} (pdf OR organigramme OR equipe) (DRH OR "ressources humaines")')
    queries.append(f'"{company}" "{city}" ("directeur des ressources humaines" OR DRH OR "responsable RH")')
    return queries


def process_row(client: Client, row: Dict[str, str], hunter_key: str, use_hunter: bool = True) -> Dict[str, str]:
    company = row.get("nom_entreprise", "")
    city = row.get("ville", "")

    domain = ""
    if use_hunter:
        domain = hunter_domain(company, hunter_key) or ""
    if not domain:
        domain = serper_domain(client, company, city) or ""

    snippets: List[Dict[str, Any]] = []
    for idx, q in enumerate(build_drh_queries(company, city, domain), start=1):
        snippets.extend(client.serper_search(q, num=5, label=f"drh_query_{idx}"))
        if len(snippets) >= 8:
            break

    drh = extract_drh_from_snippets(client, company, city, domain, snippets)

    email_rebuilt = ""
    email_conf = ""
    if not drh.get("email_public") and domain and drh.get("prenom") and drh.get("nom"):
        domain_emails = collect_domain_emails(client, domain)
        person_emails = [e for e in domain_emails if "@" in e and not e.split("@")[0] in {"contact", "info", "hello", "rh", "recrutement"}]
        pattern, conf = detect_pattern(person_emails)
        if pattern:
            email_rebuilt = build_email(drh["prenom"], drh["nom"], pattern, domain) or ""
            email_conf = conf
        if not email_rebuilt:
            email_rebuilt = build_email(drh["prenom"], drh["nom"], "prenom.nom", domain) or ""
            email_conf = "basse"

    out = dict(row)
    out.update(
        {
            "domain": domain,
            "drh_prenom": drh.get("prenom", ""),
            "drh_nom": drh.get("nom", ""),
            "drh_poste": drh.get("poste", ""),
            "drh_confidence": drh.get("confidence", "basse"),
            "drh_source_url": drh.get("source_url", ""),
            "drh_reason": drh.get("reason", ""),
            "drh_email_public": drh.get("email_public", ""),
            "drh_email_reconstitue": email_rebuilt,
            "drh_email_confidence": email_conf,
        }
    )
    return out


def read_processed_sirens(path: Path) -> Set[str]:
    if not path.exists():
        return set()
    seen: Set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            siren = (row.get("siren") or "").strip()
            if siren:
                seen.add(siren)
    return seen


def append_rows(path: Path, rows: List[Dict[str, str]], fieldnames: List[str], write_header: bool) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = "a" if path.exists() else "w"
    with path.open(mode, encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header and mode == "w":
            w.writeheader()
        w.writerows(rows)


def safe_flush_cache(client: Client) -> None:
    try:
        client.flush_cache()
    except OSError as exc:
        # Cache persistence should never block data production.
        log.warning("Flush cache ignore (OSError): %s", exc)


def main() -> None:
    ap = argparse.ArgumentParser(description="Pilotage DRH PME via Serper + Hunter")
    ap.add_argument("--input", default="data/pme_idf_50_500_pappers_sample.csv")
    ap.add_argument("--output", default="data/pme_idf_50_500_drh_serper_pilot.csv")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--cache-dir", default="data/cache_pme_drh_serper")
    ap.add_argument("--progress-every", type=int, default=10)
    ap.add_argument("--flush-every", type=int, default=10)
    ap.add_argument("--disable-hunter", action="store_true", default=False)
    ap.add_argument("--resume", action="store_true", default=True)
    args = ap.parse_args()

    load_dotenv_if_present(Path(".env"))

    serper_key = os.getenv("SERPER_API_KEY", "")
    hunter_key = os.getenv("HUNTER_API_KEY", "")
    llm_provider = os.getenv("LLM_PROVIDER", "gemini")
    llm_key = os.getenv("GEMINI_API_KEY", "") if llm_provider == "gemini" else os.getenv("MISTRAL_API_KEY", "")
    llm_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash") if llm_provider == "gemini" else os.getenv("MISTRAL_MODEL", "mistral-small-latest")

    if not serper_key:
        raise SystemExit("SERPER_API_KEY manquante dans .env")
    if not hunter_key:
        log.warning("HUNTER_API_KEY manquante: fallback domaine uniquement via Serper")
    if not llm_key:
        raise SystemExit("Cle LLM manquante (GEMINI_API_KEY ou MISTRAL_API_KEY)")

    client = Client(serper_key=serper_key, provider=llm_provider, llm_key=llm_key, llm_model=llm_model, cache_dir=Path(args.cache_dir))

    rows = read_rows(Path(args.input))
    if args.limit > 0:
        rows = rows[: args.limit]

    output_path = Path(args.output)
    processed_sirens: Set[str] = set()
    if args.resume and output_path.exists():
        processed_sirens = read_processed_sirens(output_path)
        if processed_sirens:
            log.info("Resume actif: %s lignes deja presentes dans %s", len(processed_sirens), output_path)

    input_fields = list(rows[0].keys()) if rows else []
    output_fields = input_fields + [c for c in EXTRA_OUTPUT_COLUMNS if c not in input_fields]

    out_rows: List[Dict[str, str]] = []
    write_header = not output_path.exists()
    for i, r in enumerate(rows, start=1):
        siren = (r.get("siren") or "").strip()
        if processed_sirens and siren in processed_sirens:
            continue

        out_rows.append(process_row(client, r, hunter_key=hunter_key, use_hunter=not args.disable_hunter))
        if len(out_rows) >= max(1, args.flush_every):
            append_rows(output_path, out_rows, output_fields, write_header=write_header)
            write_header = False
            out_rows.clear()
            safe_flush_cache(client)

        if i % max(1, args.progress_every) == 0:
            log.info("Progression %s/%s", i, len(rows))

    append_rows(output_path, out_rows, output_fields, write_header=write_header)
    safe_flush_cache(client)

    stats = client.serper_stats()
    final_count = len(read_processed_sirens(output_path))
    log.info("Termine: rows_total_output=%s output=%s", final_count, args.output)
    log.info("Serper stats: network_calls=%s cache_hits=%s organic_items=%s", stats.get("network_calls"), stats.get("cache_hits"), stats.get("organic_items"))


if __name__ == "__main__":
    main()
