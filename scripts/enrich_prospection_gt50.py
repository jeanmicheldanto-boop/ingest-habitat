from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


# Override empty variables inherited from terminal sessions (e.g. SERPER_API_KEY='')
load_dotenv(override=True)


GENERIC_LOCALPARTS = {
    "contact",
    "info",
    "accueil",
    "secretariat",
    "secretariat",
    "direction",
    "siege",
    "siege",
    "communication",
    "presse",
    "recrutement",
    "rh",
    "ressourceshumaines",
    "dpo",
    "rgpd",
    "webmaster",
    "support",
    "administration",
}

EXCLUDED_DOMAINS = {
    "essentiel-autonomie.com",
    "papyhappy.com",
    "pour-les-personnes-agees.gouv.fr",
    "retraiteplus.fr",
    "logement-seniors.com",
    "capresidencesseniors.com",
    "capgeris.com",
    "france-maison-de-retraite.org",
    "lesmaisonsderetraite.fr",
    "ascellianceresidence.fr",
    "ascelliance-retraite.fr",
    "conseildependance.fr",
    "tarif-senior.com",
    "pagesjaunes.fr",
    "mappy.com",

    # Company directories / registries (useful for verification, but not official websites)
    "annuaire-entreprises.data.gouv.fr",
    "pappers.fr",
    "societe.com",
    "manageo.fr",
    "verif.com",
    "infogreffe.fr",
    "bilansgratuits.fr",
    "entreprises.lefigaro.fr",

    # Other directories / job boards / institutional directories
    "carrefoursemploi.org",
    "etablissements.fhf.fr",
}


EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


POSTCODE_RE = re.compile(r"\b(\d{5})\b")


GENERIC_NAME_STOPWORDS = {
    "association",
    "federation",
    "fédération",
    "societe",
    "société",
    "anonyme",
    "economique",
    "économique",
    "mixte",
    "gestion",
    "services",
    "service",
    "et",
    "des",
    "de",
    "du",
    "la",
    "le",
    "les",
    "pr",
    "pour",
    "adultes",
    "jeunes",
    "handicapes",
    "handicapés",
}


def _domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def is_generic_email(email: str) -> bool:
    email = (email or "").strip().lower()
    if not email or "@" not in email:
        return False
    local, domain = email.split("@", 1)

    # Reject obvious aggregator domains
    if any(domain.endswith(d) for d in EXCLUDED_DOMAINS):
        return False

    # Accept a curated list of generic inboxes
    local_clean = local.replace("-", "").replace("_", "").replace(".", "")
    if local_clean in {p.replace("-", "").replace("_", "").replace(".", "") for p in GENERIC_LOCALPARTS}:
        return True

    # Also accept common variants like contact.<something>
    for prefix in GENERIC_LOCALPARTS:
        if local.startswith(prefix + ".") or local.startswith(prefix + "-") or local.startswith(prefix + "_"):
            return True

    return False


def is_name_too_generic(name: str) -> bool:
    """Heuristic: returns True when the name has no distinctive tokens."""
    s = (name or "").strip().lower()
    if not s:
        return True

    # Known very generic patterns
    if "societe anonyme d'economie mixte" in s or "société anonyme d'économie mixte" in s:
        return True

    tokens = [t for t in re.split(r"\W+", s) if len(t) >= 4]
    distinctive = [t for t in tokens if t not in GENERIC_NAME_STOPWORDS]
    return len(distinctive) == 0


def serper_search(query: str, num: int = 5) -> list[dict[str, Any]]:
    api_key = os.getenv("SERPER_API_KEY", "")
    if not api_key:
        raise RuntimeError("SERPER_API_KEY missing in environment")

    resp = requests.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
        json={"q": query, "gl": "fr", "hl": "fr", "num": num},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("organic", []) or []


def serper_search_multi(queries: list[str], num: int = 5, sleep_s: float = 0.2) -> list[dict[str, Any]]:
    all_results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for q in queries:
        organic = serper_search(q, num=num)
        for item in organic:
            link = (item.get("link") or "").strip()
            if not link or link in seen:
                continue
            seen.add(link)
            item["_query"] = q
            all_results.append(item)
        if sleep_s:
            time.sleep(sleep_s)
    return all_results


def fetch_html(url: str, timeout: int = 15) -> tuple[str, str]:
    """Return (final_url, html)"""
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    return r.url, r.text


def extract_emails_from_html(html: str) -> set[str]:
    return {e.lower() for e in EMAIL_RE.findall(html or "")}


def extract_postcode_city(address: str) -> tuple[str, str]:
    """Best-effort extraction from FINESS EJ address string."""
    if not address:
        return "", ""
    m = POSTCODE_RE.search(address)
    if not m:
        return "", ""
    cp = m.group(1)

    # naive: take the token right after postcode
    tail = address[m.end() :].strip()
    # remove separators
    tail = tail.lstrip(", ")
    city = tail.split(",")[0].strip()
    city = city.split()[0:3]
    return cp, " ".join(city).strip()


def find_contact_legal_from_homepage(homepage_url: str, html: str) -> tuple[str, str]:
    """Try to discover contact/legal pages via anchors."""
    contact_url = ""
    legal_url = ""

    try:
        soup = BeautifulSoup(html or "", "html.parser")
        anchors = soup.find_all("a")
        for a in anchors:
            href = (a.get("href") or "").strip()
            text = (a.get_text(" ") or "").strip().lower()
            if not href:
                continue
            low = href.lower()
            if not contact_url and ("contact" in low or "contact" in text or "nous contacter" in text):
                contact_url = requests.compat.urljoin(homepage_url, href)
            if not legal_url and (
                "mentions" in low
                or "legal" in low
                or "rgpd" in low
                or "confidential" in low
                or "mentions" in text
            ):
                legal_url = requests.compat.urljoin(homepage_url, href)
            if contact_url and legal_url:
                break
    except Exception:
        return "", ""

    return contact_url, legal_url


def _domain_rank_key(domain: str) -> tuple[int, int, str]:
    # Prefer fewer labels (e.g. croix-rouge.fr over paris.croix-rouge.fr) when tied.
    labels = [p for p in domain.split(".") if p]
    return (len(labels), -len(domain), domain)


def score_domains(
    organic: list[dict[str, Any]],
    gestionnaire_nom: str,
    postcode: str,
    city: str,
) -> dict[str, int]:
    """Assign a simple score per domain from Serper results.

    We combine: frequency across results + keyword URLs + snippet geo hits.
    """
    scores: dict[str, int] = {}
    name_tokens = [t for t in re.split(r"\W+", (gestionnaire_nom or "").lower()) if len(t) >= 4]

    for item in organic:
        url = (item.get("link") or "").strip()
        if not url:
            continue
        d = _domain(url)
        if not d or any(d.endswith(bad) for bad in EXCLUDED_DOMAINS):
            continue

        low_url = url.lower()
        title = (item.get("title") or "").lower()
        snippet = (item.get("snippet") or "").lower()

        s = scores.get(d, 0)
        s += 10  # base vote

        # Prefer legal/contact pages (strong signals)
        if any(k in low_url for k in ["mentions-legales", "mentions_legales", "legal", "rgpd"]):
            s += 12
        if any(k in low_url for k in ["contact", "nous-contacter", "contactez"]):
            s += 10

        # Geo disambiguation
        if postcode and postcode in snippet:
            s += 8
        if city and city.lower() in snippet:
            s += 6

        # Light name match (tokens appearing in title/snippet)
        hits = 0
        for t in name_tokens[:6]:
            if t in title or t in snippet:
                hits += 1
        s += min(10, hits * 2)

        scores[d] = s

    return scores


def domain_query_votes(organic: list[dict[str, Any]]) -> dict[str, set[str]]:
    votes: dict[str, set[str]] = {}
    for item in organic:
        url = (item.get("link") or "").strip()
        if not url:
            continue
        d = _domain(url)
        if not d or any(d.endswith(bad) for bad in EXCLUDED_DOMAINS):
            continue
        q = (item.get("_query") or "").strip()
        if not q:
            continue
        votes.setdefault(d, set()).add(q)
    return votes


def pick_best_domain(
    domain_scores: dict[str, int],
    query_votes: dict[str, set[str]],
    require_vote_count: int = 1,
) -> str:
    if not domain_scores:
        return ""

    if require_vote_count > 1:
        filtered = {d: s for d, s in domain_scores.items() if len(query_votes.get(d, set())) >= require_vote_count}
        if filtered:
            domain_scores = filtered

    # primary: score desc, secondary: fewer labels, then lexicographic
    return sorted(domain_scores.items(), key=lambda kv: (-kv[1],) + _domain_rank_key(kv[0]))[0][0]


def pick_urls_for_domain(organic: list[dict[str, Any]], domain: str) -> dict[str, str]:
    """Pick best candidate URLs restricted to a chosen domain."""
    website = ""
    contact = ""
    legal = ""

    def url_priority(u: str) -> tuple[int, int]:
        low = u.lower()
        p = 0
        if any(k in low for k in ["mentions-legales", "mentions_legales", "legal", "rgpd"]):
            p += 20
        if any(k in low for k in ["contact", "nous-contacter", "contactez"]):
            p += 15
        # prefer shorter paths for website
        return (p, -len(low))

    candidates: list[str] = []
    for item in organic:
        url = (item.get("link") or "").strip()
        if not url:
            continue
        if _domain(url) != domain:
            continue
        candidates.append(url)

    if candidates:
        # website: shortest "rootish" URL
        website = sorted(candidates, key=lambda u: (urlparse(u).path.count("/"), len(u)))[0]

        # If the best "website" is actually a legal/contact page, normalize to homepage
        low_web = website.lower()
        if any(k in low_web for k in ["mentions-legales", "mentions_legales", "/contact", "nous-contacter", "contactez"]):
            scheme = urlparse(website).scheme or "https"
            website = f"{scheme}://{domain}/"

        # contact/legal: best matches
        scored = sorted(candidates, key=lambda u: url_priority(u), reverse=True)
        for u in scored:
            low = u.lower()
            if not legal and any(k in low for k in ["mentions-legales", "mentions_legales", "legal", "rgpd", "confidentialite"]):
                legal = u
            if not contact and any(k in low for k in ["contact", "nous-contacter", "contactez"]):
                contact = u
            if contact and legal:
                break

    return {"website_url": website, "contact_url": contact, "legal_url": legal}


def search_director_info(gestionnaire_nom: str, website_domain: str, sleep_s: float = 0.15) -> dict[str, Any]:
    """Search for director/CEO information via targeted Serper queries."""
    if not website_domain:
        return {"candidate_urls": [], "serper_snippets": []}

    # Query 1: Governance/team pages on official site
    query_gov = f"site:{website_domain} (directeur OR directrice OR président OR présidente OR gouvernance OR équipe)"
    
    # Query 2: Press releases / news
    query_press = f"{gestionnaire_nom} (directeur général OR directrice générale OR président OR présidente)"
    
    # Query 3: Annual reports
    query_reports = f"site:{website_domain} rapport annuel"

    queries = [query_gov, query_press, query_reports]
    
    try:
        results = serper_search_multi(queries, num=5, sleep_s=sleep_s)
    except Exception:
        return {"candidate_urls": [], "serper_snippets": []}

    # Score and prioritize URLs
    candidate_urls: list[tuple[str, str, int]] = []
    seen: set[str] = set()
    
    for item in results:
        url = (item.get("link") or "").strip()
        if not url or url in seen:
            continue
        
        d = _domain(url)
        if not d or d != website_domain:  # Only official site URLs
            continue
            
        seen.add(url)
        low = url.lower()
        title = (item.get("title") or "").lower()
        snippet = (item.get("snippet") or "").lower()
        
        score = 0
        url_type = "page"
        
        # High-value pages
        if any(k in low for k in ["gouvernance", "equipe", "direction", "qui-sommes-nous", "organisation", "presentation"]):
            score = 15
            url_type = "governance"
        elif any(k in low for k in ["rapport-annuel", "rapport_annuel", "publications"]) or url.endswith(".pdf"):
            score = 12
            url_type = "report"
        elif any(k in low for k in ["presse", "actualites", "communiques", "news"]):
            score = 10
            url_type = "press"
        elif any(k in snippet for k in ["directeur", "directrice", "président", "présidente"]):
            score = 8
            url_type = "mention"
        else:
            score = 3
        
        if score >= 8:  # Only keep relevant pages
            candidate_urls.append((url, url_type, score))
    
    # Sort by score desc
    candidate_urls.sort(key=lambda x: -x[2])
    
    return {
        "candidate_urls": candidate_urls[:5],  # Keep top 5
        "serper_snippets": [item for item in results if _domain(item.get("link", "")) == website_domain][:10]
    }


def extract_director_with_groq(
    gestionnaire_nom: str,
    html_content: str,
    snippets: list[dict[str, Any]],
    model: str
) -> dict[str, Any]:
    """Extract director/CEO name from HTML content and snippets using Groq LLM."""
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return {"director_name": "", "director_title": "", "confidence": 0}

    # Limit HTML to avoid token overflow
    html_sample = (html_content or "")[:15000]
    
    # Build concise snippets summary
    snippets_text = "\n".join([
        f"- {item.get('title', '')}: {item.get('snippet', '')}"
        for item in snippets[:8]
    ])

    prompt = f"""Tu es un assistant d'extraction d'information publique.

CONTEXTE:
Organisation: {gestionnaire_nom}
Source: site web officiel et résultats de recherche publics

MISSION:
Extraire le nom du Directeur Général / Directrice Générale (ou Président/e si structure associative).

RÈGLES STRICTES:
1. N'extraire QUE si l'information est EXPLICITEMENT mentionnée
2. Privilégier la mention la plus récente ou officielle
3. Donner le nom complet (Prénom NOM)
4. Donner le titre exact (ex: "Directeur Général", "Présidente")
5. Si aucune info claire: retourner des champs vides

SNIPPETS SERPER:
{snippets_text}

EXTRAIT HTML:
{html_sample}

RÉPONSE (JSON strict uniquement):
{{
  "director_name": "Prénom NOM" ou "",
  "director_title": "Directeur Général" ou "",
  "source_confidence": 0-100
}}"""

    try:
        resp = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "max_tokens": 300,
            },
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]

        # Parse JSON
        content = content.strip()
        if content.startswith("```"):
            content = content.strip("`")
            content = re.sub(r"^\s*json\s*", "", content, flags=re.IGNORECASE).strip()
        
        # Try direct parse
        try:
            result = json.loads(content)
        except Exception:
            # Try to extract JSON object
            m = re.search(r"\{.*\}", content, flags=re.DOTALL)
            if not m:
                return {"director_name": "", "director_title": "", "confidence": 0}
            result = json.loads(m.group(0))
        
        return {
            "director_name": (result.get("director_name") or "").strip(),
            "director_title": (result.get("director_title") or "").strip(),
            "confidence": int(result.get("source_confidence", 0))
        }
    
    except Exception:
        return {"director_name": "", "director_title": "", "confidence": 0}


def llm_normalize_with_groq(payload: dict[str, Any], model: str) -> dict[str, Any]:
    """Uses Groq OpenAI-compatible endpoint to normalize organization info (NOT personal info)."""
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return {}

    prompt = f"""Tu es un assistant de normalisation pour une base de contacts d'organisation.

MISSION:
A partir des résultats de recherche web, extrais et normalise les informations de l'ORGANISATION.

CE QUE TU DOIS DONNER (informations sur l'ORGANISATION):
- official_name: nom public courant de l'organisation (ex: "APF France Handicap", "Croix-Rouge française")
- acronym: sigle/acronyme officiel si évident (ex: "APF", "ADMR"), sinon ""
- website_url: URL du site officiel principal
- domain: nom de domaine principal (ex: "apf-francehandicap.org")
- generic_emails: liste d'emails GÉNÉRIQUES de l'organisation (contact@, info@, secretariat@, direction@)

CE QUE TU NE DOIS PAS DONNER (informations PERSONNELLES):
- Noms de personnes physiques (directeurs, présidents, etc.)
- Emails nominatifs (prenom.nom@...)
- Téléphones personnels

DONNEES:
{json.dumps(payload, ensure_ascii=False)}

RÉPONSE (JSON strict uniquement):"""

    resp = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
            "max_tokens": 400,
        },
        timeout=30,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]

    # Best-effort JSON parse
    content = content.strip()
    if content.startswith("```"):
        content = content.strip("`")
        # Remove a leading language tag like "json\n"
        content = re.sub(r"^\s*json\s*", "", content, flags=re.IGNORECASE).strip()
    try:
        return json.loads(content)
    except Exception:
        # Try to extract the first JSON object from a chatty response
        m = re.search(r"\{.*\}", content, flags=re.DOTALL)
        if not m:
            return {}
        candidate = m.group(0).strip()
        try:
            return json.loads(candidate)
        except Exception:
            return {}


@dataclass
class EnrichmentResult:
    website_url: str = ""
    domain: str = ""
    contact_url: str = ""
    legal_url: str = ""
    generic_emails: list[str] | None = None
    primary_email: str = ""
    official_name: str = ""
    acronym: str = ""
    director_name: str = ""
    director_title: str = ""
    director_confidence: int = 0
    sources: str = ""
    query: str = ""
    confidence: int = 0


def enrich_one(
    gestionnaire_nom: str,
    gestionnaire_adresse: str,
    finess_ej: str,
    nb_essms: int,
    groq_model: str,
    sleep_s: float,
) -> EnrichmentResult:
    cp, city = extract_postcode_city(gestionnaire_adresse)
    geo = " ".join([p for p in [cp, city] if p]).strip()
    query_site = f"{gestionnaire_nom} {geo} site officiel".strip()
    query_legal = f"{gestionnaire_nom} {geo} mentions légales".strip()
    query_contact = f"{gestionnaire_nom} {geo} contact email".strip()
    queries = [query_site, query_legal, query_contact]
    query = " | ".join(queries)

    organic = serper_search_multi(queries, num=6, sleep_s=0.15)

    domain_scores = score_domains(organic, gestionnaire_nom=gestionnaire_nom, postcode=cp, city=city)
    votes = domain_query_votes(organic)

    require_votes = 2 if is_name_too_generic(gestionnaire_nom) else 1
    best_domain = pick_best_domain(domain_scores, votes, require_vote_count=require_votes)

    urls = pick_urls_for_domain(organic, best_domain) if best_domain else {"website_url": "", "contact_url": "", "legal_url": ""}

    sources = [item.get("link", "") for item in organic if item.get("link")]
    sources = [u for u in sources if u]

    emails: set[str] = set()
    fetched: list[dict[str, Any]] = []

    for key in ["contact_url", "legal_url", "website_url"]:
        url = urls.get(key) or ""
        if not url:
            continue
        try:
            final_url, html = fetch_html(url)
            fetched.append({"url": final_url, "title": "", "snippet": ""})
            emails |= extract_emails_from_html(html)

            # If we fetched the homepage (or best website), try to discover links
            if key == "website_url":
                discovered_contact, discovered_legal = find_contact_legal_from_homepage(final_url, html)
                if not urls.get("contact_url") and discovered_contact:
                    urls["contact_url"] = discovered_contact
                if not urls.get("legal_url") and discovered_legal:
                    urls["legal_url"] = discovered_legal
        except Exception:
            continue
        finally:
            if sleep_s:
                time.sleep(sleep_s)

    generic_emails = sorted([e for e in emails if is_generic_email(e)])
    primary_email = generic_emails[0] if generic_emails else ""

    domain = _domain(urls.get("website_url", ""))

    llm_payload = {
        "finess_ej": finess_ej,
        "gestionnaire_nom_finess": gestionnaire_nom,
        "gestionnaire_adresse_finess": gestionnaire_adresse,
        "nb_essms": nb_essms,
        "serper_queries": queries,
        "serper_results": [
            {
                "query": item.get("_query", ""),
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
            }
            for item in organic
        ],
        "domain_scores": domain_scores,
        "domain_query_votes": {d: len(v) for d, v in votes.items()},
        "picked_domain": best_domain,
        "picked_urls": urls,
        "found_generic_emails": generic_emails,
    }

    normalized = llm_normalize_with_groq(llm_payload, model=groq_model) if groq_model else {}

    official_name = (normalized.get("official_name") or "").strip()
    acronym = (normalized.get("acronym") or "").strip()
    website_url = (normalized.get("website_url") or urls.get("website_url") or "").strip()
    domain_norm = (normalized.get("domain") or "").strip() or _domain(website_url)

    generic_emails_norm = normalized.get("generic_emails")
    if isinstance(generic_emails_norm, list) and generic_emails_norm:
        generic_emails = [e.lower().strip() for e in generic_emails_norm if is_generic_email(e)]
        primary_email = generic_emails[0] if generic_emails else primary_email

    # NEW: Search for director information if we have a domain and LLM is enabled
    director_info = {"director_name": "", "director_title": "", "confidence": 0}
    
    if best_domain and groq_model and website_url:  # Only if LLM enabled and we found a site
        try:
            search_results = search_director_info(gestionnaire_nom, best_domain, sleep_s=0.15)
            
            if search_results["candidate_urls"]:
                # Fetch the best candidate page
                best_url = search_results["candidate_urls"][0][0]
                try:
                    _, html = fetch_html(best_url)
                    director_info = extract_director_with_groq(
                        gestionnaire_nom,
                        html,
                        search_results["serper_snippets"],
                        model=groq_model
                    )
                    if sleep_s:
                        time.sleep(sleep_s)
                except Exception:
                    pass
        except Exception:
            pass

    # Basic confidence heuristic
    confidence = 0
    if urls.get("website_url"):
        confidence += 35
    if urls.get("contact_url") or urls.get("legal_url"):
        confidence += 20
    if primary_email:
        confidence += 25
    if domain_norm:
        confidence += 10
    if best_domain and domain_norm and best_domain == domain_norm:
        confidence += 10

    return EnrichmentResult(
        website_url=website_url,
        domain=domain_norm or domain,
        contact_url=(urls.get("contact_url") or "").strip(),
        legal_url=(urls.get("legal_url") or "").strip(),
        generic_emails=generic_emails,
        primary_email=primary_email,
        official_name=official_name,
        acronym=acronym,
        director_name=director_info["director_name"],
        director_title=director_info["director_title"],
        director_confidence=director_info["confidence"],
        sources=";".join(sources[:6]),
        query=query,
        confidence=min(100, confidence),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich prospecting info for FINESS EJ with nb_essms > 50.")
    parser.add_argument("--in", dest="in_path", default="outputs/finess_gestionnaires_essms_gt10.xlsx")
    parser.add_argument("--out", dest="out_path", default="outputs/prospection_gt50.xlsx")
    parser.add_argument("--min", dest="min_essms", type=int, default=50)
    parser.add_argument("--sleep", dest="sleep_s", type=float, default=0.6)
    parser.add_argument("--groq-model", dest="groq_model", default=os.getenv("GROQ_MODEL", "llama-3.1-8b-instant"))
    parser.add_argument("--no-llm", action="store_true", help="Disable LLM normalization")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of rows (0=no limit)")

    args = parser.parse_args()

    df = pd.read_excel(args.in_path)
    df = df[df["nb_essms"] > args.min_essms].copy()
    if args.limit:
        df = df.head(args.limit).copy()

    groq_model = "" if args.no_llm else args.groq_model

    out_rows: list[dict[str, Any]] = []
    for i, row in enumerate(df.itertuples(index=False), start=1):
        gestionnaire_nom = getattr(row, "gestionnaire_nom")
        gestionnaire_adresse = getattr(row, "gestionnaire_adresse", "")
        finess_ej = getattr(row, "finess_ej")
        nb_essms = int(getattr(row, "nb_essms"))

        print(f"[{i}/{len(df)}] Enriching {finess_ej} - {gestionnaire_nom[:60]}...")

        try:
            res = enrich_one(
                gestionnaire_nom,
                gestionnaire_adresse,
                finess_ej,
                nb_essms,
                groq_model=groq_model,
                sleep_s=args.sleep_s,
            )
        except Exception as e:
            res = EnrichmentResult(sources=f"error:{e}")

        out = row._asdict()  # existing columns
        out.update(
            {
                "nom_public": res.official_name,
                "acronyme": res.acronym,
                "site_web": res.website_url,
                "domaine": res.domain,
                "email_contact": res.primary_email,
                "emails_generiques": ";".join(res.generic_emails or []),
                "url_contact": res.contact_url,
                "url_mentions_legales": res.legal_url,
                "dirigeant_nom": res.director_name,
                "dirigeant_titre": res.director_title,
                "dirigeant_confidence": res.director_confidence,
                "sources_web": res.sources,
                "query_web": res.query,
                "confidence": res.confidence,
            }
        )
        out_rows.append(out)

    out_df = pd.DataFrame(out_rows)
    out_df.to_excel(args.out_path, index=False)
    print(f"Wrote {len(out_df)} rows to {args.out_path}")


if __name__ == "__main__":
    main()
