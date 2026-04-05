"""Analyse et validation des sites web d'établissements.

Objectif:
- Extraire les `site_web` depuis la base (hors Résidence autonomie / Résidence services seniors / MARPA)
- Marquer les URLs manifestement non officielles (annuaires, réseaux sociaux, PDF, immobilier, etc.)
- (Optionnel) Vérifier via ScrapingBee + Gemini si un site est bien officiel (établissement, gestionnaire, commune, gouv)

Usage:
  python scripts/analyze_suspicious_urls.py --department 76
  python scripts/analyze_suspicious_urls.py --limit 200 --verify-llm --verify-mode ambiguous
    python scripts/analyze_suspicious_urls.py --limit 500 --exclude-review outputs/url_review_auto_20260130_223825.csv
    python scripts/analyze_suspicious_urls.py --limit 50000 --safe-sous-categories ra_rss

Clés attendues (env):
- SCRAPINGBEE_API_KEY (optionnel)
- GEMINI_API_KEY (optionnel)  # Google AI Studio key

Ce script est READ-ONLY (aucune écriture DB).
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# Permet d'exécuter le script depuis le dossier `scripts/`.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from database import DatabaseManager


def _load_excluded_etablissement_ids(path: str) -> set[str]:
    """Charge des IDs d'établissements à exclure.

    - CSV: doit contenir une colonne `etablissement_id`.
    - TXT: 1 id par ligne.
    """

    p = (path or "").strip()
    if not p:
        return set()
    if not os.path.exists(p):
        raise FileNotFoundError(p)

    ids: set[str] = set()
    if p.lower().endswith(".csv"):
        with open(p, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "etablissement_id" not in reader.fieldnames:
                raise ValueError(f"CSV sans colonne 'etablissement_id': {p}")
            for row in reader:
                v = (row.get("etablissement_id") or "").strip()
                if v:
                    ids.add(v)
    else:
        with open(p, "r", encoding="utf-8") as f:
            for line in f:
                v = line.strip()
                if v:
                    ids.add(v)
    return ids


SAFE_SOUS_CATEGORIES_RA_RSS = {
    "Résidence autonomie",
    "Résidence services seniors",
    # variantes historiques
    "residence_autonomie",
    "residence_services_seniors",
}

SAFE_SOUS_CATEGORIES_MARPA = {
    "MARPA",
    # variantes historiques
    "marpa",
}


def build_safe_sous_categories(mode: str) -> set[str]:
    """Construit l'ensemble des sous-catégories à exclure.

    Modes:
    - ra_rss_marpa (défaut historique)
    - ra_rss
    - none
    """

    m = (mode or "").strip().lower()
    if m in {"", "ra_rss_marpa", "default", "all"}:
        return set(SAFE_SOUS_CATEGORIES_RA_RSS) | set(SAFE_SOUS_CATEGORIES_MARPA)
    if m in {"ra_rss", "ra/rss"}:
        return set(SAFE_SOUS_CATEGORIES_RA_RSS)
    if m in {"none", "no", "0"}:
        return set()
    raise ValueError(f"Mode safe-sous-categories inconnu: {mode}")


# Domaines/plateformes à exclure comme "site officiel".
# (fusion de quality_check_readonly.py + patterns rencontrés dans mvp/scrapers/source_classifier.py)
EXCLUDED_DOMAINS = {
    # annuaires seniors
    "essentiel-autonomie.com",
    "papyhappy.fr",
    "papyhappy.com",
    "retraite.com",
    "annuairessehpad.com",
    "capgeris.com",
    "sanitaire-social.com",
    "logement-seniors.com",
    "capresidencesseniors.com",
    "france-maison-de-retraite.org",
    "lesmaisonsderetraite.fr",
    "conseildependance.fr",
    "tarif-senior.com",
    "cohabiting-seniors.com",
    "capretraite.fr",
    "trouver-maison-de-retraite.fr",
    "annuaire-retraite.com",
    # réseaux sociaux
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "tiktok.com",
    # maps / annuaires génériques
    "pagesjaunes.fr",
    "mappy.com",
    # plateformes rdv
    "doctolib.fr",
    # immobilier
    "seloger.com",
    "logic-immo.com",
    "avendrealouer.fr",
    "explorimmoneuf.com",
    "immoneuf.com",
    # divers bruit
    "villesetvillagesouilfaitbonvivre.com",
}


# Hébergeurs/lecteurs de documents (souvent pas un site officiel)
DOCUMENT_HOST_DOMAINS = {
    "calameo.com",
    "issuu.com",
    "docplayer.fr",
    "drive.google.com",
    "dropbox.com",
    "onedrive.live.com",
    "scribd.com",
}


# Domaines plutôt "sûrs" (gestionnaires/acteurs reconnus) – issus de la spec.
# NB: on ne les "whitelist" pas aveuglément; on les utilise pour réduire la suspicion.
TRUSTED_SOURCE_HINTS = {
    "pour-les-personnes-agees.gouv.fr",
    "agesetvie.com",
    "monsenior.fr",
    "ensemble2generations",
    "ensemble-differents",
    "udaf",
    "habitat-humanisme",
    "unapei",
    "adapei",
    "ensemble2generations",
    "coeur-de-vie",
    "residences-commetoit",
    "senioriales",
    "soliha",
    "cettefamille",
    "vivre-devenir",
    "cosima",
    "associationbatir",
    "vitalliance",
}


# Allowlist explicite (demandée): si le domaine contient l'un de ces tokens,
# on considère l'URL comme valide (OK) et on ne lance pas de vérification LLM.
# NB: on matche en *substring* sur le hostname (ex: udaf79.fr -> "udaf").
ALLOWLIST_DOMAIN_TOKENS = {
    "agesetvie",
    "cettefamille",
    "monsenior",
    "udaf",
    "habitat-humanisme",
    "domani",
    # historiques / déjà demandés
    "ensemble2generations",
    "ensemble-differents",
}


def is_allowlisted_domain(domain: str) -> bool:
    d = (domain or "").lower()
    if not d:
        return False
    return any(tok in d for tok in ALLOWLIST_DOMAIN_TOKENS)


@dataclass
class UrlCheck:
    etablissement_id: str
    nom: str
    departement: str
    commune: str
    gestionnaire: str
    site_web: str
    source: str
    sous_categories: List[str]
    domain: str
    category: str
    reasons: List[str]
    recommended_decision: str = ""  # KEEP|DROP|REPLACE
    llm_verdict: str = ""
    llm_confidence: float = 0.0
    llm_reason: str = ""
    llm_is_about_establishment: bool = False
    llm_is_senior_housing: bool = False
    suggested_official_url: str = ""
    http_status: int = 0
    final_url: str = ""


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def normalize_url(raw: str) -> str:
    if raw is None:
        return ""

    url = raw.strip().strip("\"").strip("'")

    # Si plusieurs URL collées, on prend la première "plausible".
    for sep in ["\n", " ", ";", ",", "|"]:
        if sep in url:
            parts = [p.strip() for p in url.split(sep) if p.strip()]
            if parts:
                url = parts[0]
            break

    if not url:
        return ""

    # Ajouter scheme si manquant
    if not re.match(r"^https?://", url, flags=re.IGNORECASE):
        url = "https://" + url

    return url


def url_domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def is_safe_sous_categorie(sous_categories: Iterable[str], safe_set: set[str]) -> bool:
    for sc in sous_categories:
        if not sc:
            continue
        if sc in safe_set:
            return True
    return False


def classify_url(url: str, domain: str, source: str) -> Tuple[str, List[str]]:
    reasons: List[str] = []

    lowered = (url or "").lower()

    if not domain:
        return "invalid", ["Domaine vide / URL invalide"]

    # Allowlist stricte: considérée valide par règle métier (pas de vérification LLM).
    if is_allowlisted_domain(domain):
        return "allowlisted", ["Domaine allowlist (règle métier)"]

    # Exclusions fortes par domaine
    if domain in EXCLUDED_DOMAINS:
        return "excluded_domain", [f"Domaine exclu ({domain})"]

    # Document / PDF
    if domain in DOCUMENT_HOST_DOMAINS:
        return "document_host", [f"Hébergeur de document ({domain})"]

    if lowered.endswith(".pdf") or "/pdf" in lowered:
        reasons.append("URL semble pointer vers un PDF")

    # Réseaux sociaux
    if domain in {"facebook.com", "instagram.com", "linkedin.com", "x.com", "twitter.com", "youtube.com", "tiktok.com"}:
        return "social", [f"Réseau social ({domain})"]

    # Maps / annuaires
    if domain in {"pagesjaunes.fr", "mappy.com"} or "google.com/maps" in lowered:
        return "directory_map", ["Annuaire / carte (PagesJaunes/Mappy/Google Maps)"]

    # Immobilier
    if domain in {"seloger.com", "logic-immo.com", "avendrealouer.fr", "explorimmoneuf.com", "immoneuf.com"}:
        return "real_estate", ["Site immobilier (hors sujet)"]

    # Gating par keywords d'URL
    if any(k in lowered for k in ["/annuaire", "comparateur", "devis", "gratuit", "sans-engagement"]):
        reasons.append("Keywords annuaire/comparateur")

    # Réduction suspicion si source semble fiable
    if any(hint in (source or "") for hint in TRUSTED_SOURCE_HINTS):
        reasons.append("Source initiale considérée plutôt fiable")

    if reasons:
        return "ambiguous", reasons

    return "likely_ok", []


def fetch_page_text(url: str, scrapingbee_api_key: str, timeout_s: int = 25) -> Tuple[int, str, str]:
    """Retourne (status_code, final_url, extracted_text)."""

    if scrapingbee_api_key:
        params = {
            "api_key": scrapingbee_api_key,
            "url": url,
            "render_js": "false",
            "block_resources": "true",
            "timeout": str(timeout_s * 1000),
        }
        r = requests.get("https://app.scrapingbee.com/api/v1/", params=params, timeout=timeout_s)
        status = r.status_code
        final = url
        html = r.text if status == 200 else ""
    else:
        r = requests.get(url, timeout=timeout_s, headers={"User-Agent": "Mozilla/5.0"}, allow_redirects=True)
        status = r.status_code
        final = r.url
        html = r.text if status == 200 else ""

    if not html:
        return status, final, ""

    soup = BeautifulSoup(html, "html.parser")

    # Nettoyage basique
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = (soup.title.get_text(strip=True) if soup.title else "").strip()
    h1 = (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "").strip()

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    # Tronquer
    text = text[:3000]
    header = " | ".join([x for x in [title, h1] if x])
    if header:
        text = header + "\n" + text

    return status, final, text


def gemini_classify_site(
    *,
    api_key: str,
    model: str,
    establishment_name: str,
    commune: str,
    gestionnaire: str,
    url: str,
    page_text: str,
) -> Tuple[str, float, str, str, bool, bool]:
    """Retourne (verdict, confidence, reason, suggested_official_url, is_about_establishment, is_senior_housing)."""

    model_name = (model or "").strip()
    if model_name.startswith("models/"):
        model_name = model_name[len("models/") :]
    if not model_name:
        model_name = "gemini-1.5-flash"

    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"

    system_rules = (
        "Tu es un auditeur de sites web. Objectif: déterminer si l'URL fournie est un site officiel "
        "(établissement, gestionnaire, commune/CCAS, ou site gouvernemental) pour l'établissement donné. "
        "Tu dois être strict: les annuaires, comparateurs, réseaux sociaux, plateformes de RDV, pages de carte, "
        "hébergeurs de PDF/flipbook, et sites immobiliers sont NON officiels. "
        "Les sites de commune peuvent être valides si la page parle de l'établissement/du service."
    )

    prompt = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": (
                            f"{system_rules}\n\n"
                            f"ETABLISSEMENT: {establishment_name}\n"
                            f"COMMUNE: {commune}\n"
                            f"GESTIONNAIRE (si connu): {gestionnaire}\n"
                            f"URL: {url}\n\n"
                            "EXTRAIT PAGE (tronqué):\n"
                            f"{page_text}\n\n"
                            "Rends uniquement un JSON strict avec ces champs:\n"
                            "{\n"
                            "  \"verdict\": \"OFFICIEL_ETABLISSEMENT\"|\"OFFICIEL_GESTIONNAIRE\"|\"OFFICIEL_COMMUNE\"|\"OFFICIEL_GOUV\"|\"ANNUAIRE\"|\"PLATEFORME\"|\"PDF\"|\"HORS_SUJET\"|\"INCONNU\",\n"
                            "  \"confidence\": 0.0-1.0,\n"
                            "  \"reason\": \"...\",\n"
                            "  \"is_about_establishment\": true/false,\n"
                            "  \"is_senior_housing\": true/false,\n"
                            "  \"suggested_official_url\": \"\"\n"
                            "}\n"
                        )
                    }
                ],
            }
        ],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 250},
    }

    last_error: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    for attempt in range(1, 4):
        try:
            resp = requests.post(endpoint, json=prompt, timeout=40)

            # Erreurs transitoires : on réessaie (quota, incident, gateway)
            if resp.status_code in {429, 500, 502, 503, 504}:
                last_error = f"Gemini error {resp.status_code}: {resp.text[:200]}"
                if attempt < 3:
                    time.sleep(2 ** (attempt - 1))
                    continue
                raise RuntimeError(last_error)

            if resp.status_code != 200:
                raise RuntimeError(f"Gemini error {resp.status_code}: {resp.text[:200]}")

            data = resp.json()
            break
        except (requests.Timeout, requests.ConnectionError) as e:
            last_error = f"Gemini network error: {e}"
            if attempt < 3:
                time.sleep(2 ** (attempt - 1))
                continue
            raise RuntimeError(last_error)

    if not data:
        raise RuntimeError(last_error or "Gemini error: unknown failure")
    text = (
        data.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [{}])[0]
        .get("text", "")
        .strip()
    )

    # Extraire JSON
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise ValueError(f"Gemini output non-JSON: {text[:200]}")

    obj = json.loads(text[start : end + 1])
    verdict = str(obj.get("verdict", "INCONNU"))
    confidence = float(obj.get("confidence", 0.0) or 0.0)
    reason = str(obj.get("reason", "") or "")
    suggested = str(obj.get("suggested_official_url", "") or "")
    is_about = bool(obj.get("is_about_establishment", False))
    is_senior = bool(obj.get("is_senior_housing", False))

    return verdict, confidence, reason, suggested, is_about, is_senior


def write_report_md(path: str, rows: List[UrlCheck]) -> None:
    from collections import Counter

    by_category = Counter(r.category for r in rows)
    by_domain = Counter(r.domain for r in rows)

    lines: List[str] = []
    lines.append(f"# Audit URLs sites web ({len(rows)} lignes)\n")
    lines.append(f"Généré le {datetime.now().isoformat(timespec='seconds')}\n")

    lines.append("## Répartition par catégorie\n")
    for cat, n in by_category.most_common():
        lines.append(f"- **{cat}**: {n}")

    lines.append("\n## Top domaines\n")
    for dom, n in by_domain.most_common(30):
        lines.append(f"- {dom}: {n}")

    lines.append("\n## Exemples (max 60)\n")
    for r in rows[:60]:
        reasons = "; ".join(r.reasons) if r.reasons else ""
        llm = f" | LLM={r.llm_verdict} ({r.llm_confidence:.2f})" if r.llm_verdict else ""
        lines.append(f"- {r.departement} — **{r.nom}** ({r.commune}) — {r.site_web} — `{r.category}` {reasons}{llm}")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def write_csv(path: str, rows: List[UrlCheck]) -> None:
    fieldnames = [
        "etablissement_id",
        "nom",
        "departement",
        "commune",
        "gestionnaire",
        "site_web",
        "domain",
        "category",
        "reasons",
        "recommended_decision",
        "llm_verdict",
        "llm_confidence",
        "llm_reason",
        "llm_is_about_establishment",
        "llm_is_senior_housing",
        "suggested_official_url",
        "http_status",
        "final_url",
        "source",
        "sous_categories",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "etablissement_id": r.etablissement_id,
                    "nom": r.nom,
                    "departement": r.departement,
                    "commune": r.commune,
                    "gestionnaire": r.gestionnaire,
                    "site_web": r.site_web,
                    "domain": r.domain,
                    "category": r.category,
                    "reasons": "; ".join(r.reasons),
                    "recommended_decision": r.recommended_decision,
                    "llm_verdict": r.llm_verdict,
                    "llm_confidence": f"{r.llm_confidence:.3f}" if r.llm_confidence else "",
                    "llm_reason": r.llm_reason,
                    "llm_is_about_establishment": str(bool(r.llm_is_about_establishment)),
                    "llm_is_senior_housing": str(bool(r.llm_is_senior_housing)),
                    "suggested_official_url": r.suggested_official_url,
                    "http_status": r.http_status,
                    "final_url": r.final_url,
                    "source": r.source,
                    "sous_categories": ", ".join([x for x in r.sous_categories if x]),
                }
            )


def write_review_csv(path: str, rows: List[UrlCheck]) -> None:
    """CSV compatible avec scripts/url_propositions_workflow.py import-decisions."""

    fieldnames = [
        "etablissement_id",
        "nom",
        "departement",
        "commune",
        "sous_categories",
        "current_site_web",
        "source",
        "decision",
        "new_site_web",
        "note",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            note_parts: List[str] = []
            if r.category:
                note_parts.append(f"category={r.category}")
            if r.reasons:
                note_parts.append("reasons=" + "; ".join(r.reasons))
            if r.llm_verdict:
                note_parts.append(
                    f"llm={r.llm_verdict} conf={r.llm_confidence:.2f} about={r.llm_is_about_establishment} senior={r.llm_is_senior_housing}"
                )
            if r.llm_reason:
                note_parts.append("llm_reason=" + r.llm_reason[:300])

            decision = r.recommended_decision or ""
            new_site = ""
            if decision == "REPLACE" and r.suggested_official_url:
                new_site = r.suggested_official_url

            w.writerow(
                {
                    "etablissement_id": r.etablissement_id,
                    "nom": r.nom,
                    "departement": r.departement,
                    "commune": r.commune,
                    "sous_categories": ", ".join([x for x in r.sous_categories if x]),
                    "current_site_web": r.site_web,
                    "source": r.source,
                    "decision": decision,
                    "new_site_web": new_site,
                    "note": " | ".join(note_parts),
                }
            )


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv(override=False)
    except Exception:
        # Optionnel: si python-dotenv n'est pas dispo, on continue avec l'environnement.
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument("--department", "--departement", dest="department", default="", help="Filtre departement exact (ex: '76' ou 'Seine-Maritime (76)')")
    parser.add_argument("--limit", type=int, default=500, help="Limite de lignes")
    parser.add_argument(
        "--safe-sous-categories",
        default="ra_rss_marpa",
        choices=["ra_rss_marpa", "ra_rss", "none"],
        help="Sous-catégories à exclure du périmètre (défaut: ra_rss_marpa).",
    )
    parser.add_argument(
        "--exclude-review",
        default="",
        help="Optionnel: CSV (ex: outputs/url_review_auto_*.csv) contenant `etablissement_id` à exclure du tirage",
    )
    parser.add_argument(
        "--exclude-ids",
        default="",
        help="Optionnel: fichier TXT (1 etablissement_id par ligne) à exclure du tirage",
    )
    parser.add_argument("--verify-llm", action="store_true", help="Active scraping + Gemini")
    parser.add_argument("--verify-mode", choices=["ambiguous", "all"], default="ambiguous")
    parser.add_argument(
        "--gemini-model",
        default="",
        help="Nom du modèle Gemini (ex: gemini-2.0-flash, gemini-1.5-flash). Peut aussi venir de GEMINI_MODEL.",
    )
    parser.add_argument("--export-review-csv", action="store_true", help="Génère un CSV de revue compatible avec import-decisions")
    args = parser.parse_args()

    scrapingbee_key = os.getenv("SCRAPINGBEE_API_KEY", "").strip()
    # L'utilisateur souhaite pouvoir réutiliser la même clé Google que Places.
    gemini_key = (os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_MAPS_API_KEY", "").strip())
    gemini_model = (args.gemini_model or os.getenv("GEMINI_MODEL", "") or "gemini-2.0-flash").strip()

    db = DatabaseManager()

    safe_set = build_safe_sous_categories(args.safe_sous_categories)

    where_dept = ""
    params: List[Any] = []
    if args.department:
        where_dept = " AND e.departement = %s "
        params.append(args.department)

    excluded_ids: set[str] = set()
    try:
        if args.exclude_review:
            excluded_ids |= _load_excluded_etablissement_ids(args.exclude_review)
        if args.exclude_ids:
            excluded_ids |= _load_excluded_etablissement_ids(args.exclude_ids)
    except Exception as e:
        print(f"WARN: impossible de charger les exclusions: {e}")
        excluded_ids = set()

    where_exclude = ""
    if excluded_ids:
        where_exclude = " AND NOT (e.id = ANY(%s::uuid[])) "
        params.append(sorted(excluded_ids))

    where_safe = ""
    if safe_set:
        # On paramètre la liste pour éviter de reconstruire du SQL avec des valeurs.
        where_safe = """
      AND NOT EXISTS (
        SELECT 1 FROM etablissement_sous_categorie esc2
        JOIN sous_categories sc2 ON sc2.id = esc2.sous_categorie_id
        WHERE esc2.etablissement_id = e.id
          AND sc2.libelle = ANY(%s)
      )
        """
        params.append(sorted(safe_set))

    sql = f"""
    SELECT DISTINCT e.id, e.nom, e.departement, e.commune, e.site_web, e.source, COALESCE(e.gestionnaire,'') as gestionnaire,
           array_agg(DISTINCT sc.libelle) FILTER (WHERE sc.libelle IS NOT NULL) as sous_categories
    FROM etablissements e
    LEFT JOIN etablissement_sous_categorie esc ON esc.etablissement_id = e.id
    LEFT JOIN sous_categories sc ON sc.id = esc.sous_categorie_id
    WHERE e.is_test = false
      AND e.site_web IS NOT NULL
      AND trim(e.site_web) != ''
      {where_dept}
    {where_exclude}
    {where_safe}
    GROUP BY e.id, e.nom, e.departement, e.commune, e.site_web, e.source, e.gestionnaire
    ORDER BY e.departement, e.nom
    LIMIT {int(args.limit)};
    """

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            fetched = cur.fetchall()

    results: List[UrlCheck] = []

    for etab_id, nom, dept, commune, site_web_raw, source, gestionnaire, sous_cats in fetched:
        sous_cats_list = [x for x in (sous_cats or []) if x]
        if is_safe_sous_categorie(sous_cats_list, safe_set):
            # Double sécurité: on ignore ici aussi
            continue

        site_web = normalize_url(site_web_raw or "")
        domain = url_domain(site_web)
        category, reasons = classify_url(site_web, domain, source or "")

        # Décision automatique initiale
        if category == "allowlisted":
            # Whitelist explicite: on considère OK sans vérification.
            recommended = "KEEP"
        elif category in {"excluded_domain", "document_host", "real_estate", "social", "directory_map", "invalid"}:
            recommended = "DROP"
        else:
            recommended = ""

        row = UrlCheck(
            etablissement_id=str(etab_id),
            nom=str(nom or ""),
            departement=str(dept or ""),
            commune=str(commune or ""),
            gestionnaire=str(gestionnaire or ""),
            site_web=site_web,
            source=str(source or ""),
            sous_categories=sous_cats_list,
            domain=domain,
            category=category,
            reasons=reasons,
            recommended_decision=recommended,
        )

        # On vérifie via scraping+LLM les catégories non-triviales, pour confirmer que c'est un site valide
        # ET que ça parle bien de l'habitat senior correspondant.
        should_verify = (
            args.verify_llm
            and bool(gemini_key)
            and category in {"ambiguous", "likely_ok"}
            and (args.verify_mode == "all" or category == "ambiguous")
        )

        if should_verify:
            try:
                status, final_url, text = fetch_page_text(site_web, scrapingbee_key)
            except Exception as e:
                row.llm_verdict = "ERROR"
                row.reasons.append(f"Scrape error: {e}")
                status, final_url, text = 0, "", ""

            row.http_status = int(status or 0)
            row.final_url = final_url
            if text:
                try:
                    verdict, conf, llm_reason, suggested, is_about, is_senior = gemini_classify_site(
                        api_key=gemini_key,
                        model=gemini_model,
                        establishment_name=row.nom,
                        commune=row.commune,
                        gestionnaire=row.gestionnaire,
                        url=final_url,
                        page_text=text,
                    )
                    row.llm_verdict = verdict
                    row.llm_confidence = conf
                    row.llm_reason = llm_reason
                    row.llm_is_about_establishment = bool(is_about)
                    row.llm_is_senior_housing = bool(is_senior)
                    row.suggested_official_url = suggested

                    # Décision automatique après LLM
                    if verdict in {"OFFICIEL_ETABLISSEMENT", "OFFICIEL_GESTIONNAIRE", "OFFICIEL_COMMUNE", "OFFICIEL_GOUV"}:
                        if row.llm_is_about_establishment and row.llm_is_senior_housing:
                            row.recommended_decision = "KEEP"
                        else:
                            row.recommended_decision = "DROP"
                            row.reasons.append("LLM: site ne semble pas concerner cet habitat senior")
                    elif verdict in {"ANNUAIRE", "PLATEFORME", "PDF", "HORS_SUJET"}:
                        row.recommended_decision = "DROP"
                    else:
                        # INCONNU -> laisse vide (revue possible)
                        row.recommended_decision = row.recommended_decision or ""
                except Exception as e:
                    row.llm_verdict = "ERROR"
                    row.reasons.append(f"LLM error: {e}")
            else:
                row.reasons.append("Scraping: page vide ou inaccessible")

        results.append(row)

    os.makedirs("outputs", exist_ok=True)
    tag = _now_tag()
    out_md = os.path.join("outputs", f"url_audit_{tag}.md")
    out_csv = os.path.join("outputs", f"url_audit_{tag}.csv")
    out_review = os.path.join("outputs", f"url_review_auto_{tag}.csv")

    write_report_md(out_md, results)
    write_csv(out_csv, results)
    if args.export_review_csv:
        write_review_csv(out_review, results)

    print(f"OK: {len(results)} lignes")
    print(f"- rapport: {out_md}")
    print(f"- csv:    {out_csv}")
    if args.export_review_csv:
        print(f"- review: {out_review}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
