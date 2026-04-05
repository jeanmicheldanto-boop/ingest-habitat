from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import random
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
log = logging.getLogger("idf_contacts")


BAD_DOMAINS = {
    "facebook.com", "linkedin.com", "twitter.com", "x.com", "youtube.com", "instagram.com",
    "wikipedia.org", "pagesjaunes.fr", "annuaire-mairie.fr", "communes.com", "cartesfrance.fr",
    "data.gouv.fr", "service-public.fr", "gouv.fr", "francebleu.fr", "leparisien.fr", "actu.fr",
    "adresses-mairies.fr", "collectivite.fr", "wordpress.com", "wixsite.com", "blogspot.com",
    "villesavivre.fr",
    "pappers.fr", "pappers.com",
}

BAD_DOMAIN_PATTERNS = {
    "annuaire", "collectivite", "adresses-mairies", "mairieinfo", "wixsite", "wordpress",
    "blogspot", "facebook", "linkedin", "wikipedia", "pagesjaunes",
    "pappers",
}

DIRECTORY_PAGE_PATTERNS = {
    "annuaire des mairies", "toutes les villes", "code postal", "meteo", "immobilier", "hotel",
    "restaurants", "population des villes", "villes de france", "mairie de france",
}

PLACEHOLDER_NAME_TOKENS = {
    "drh", "dgs", "mairie", "ville", "direction", "service", "services", "ressources", "humaines",
    "general", "generale", "administration", "cabinet",
}

NAME_TITLE_TOKENS = {
    "monsieur", "madame", "mme", "mr", "m", "inconnu", "unknown",
    "directeur", "directrice", "service", "services", "general", "generale",
}

ROLE_QUERIES = {
    "drh": [
        'site:{domain} ("directeur des ressources humaines" OR "DRH" OR "responsable ressources humaines") "{commune}"',
        'site:{domain} ("procès-verbal" OR "compte rendu" OR "organigramme") ("DRH" OR "ressources humaines") "{commune}"',
        '"{commune}" "{dept}" ("directeur des ressources humaines" OR "DRH" OR "responsable ressources humaines") 2024 OR 2025',
    ],
    "dgs": [
        'site:{domain} ("directeur general des services" OR "DGS" OR "direction generale des services") "{commune}"',
        'site:{domain} ("procès-verbal" OR "compte rendu" OR "organigramme") ("DGS" OR "directeur général") "{commune}"',
        '"{commune}" "{dept}" ("directeur general des services" OR "DGS") 2024 OR 2025',
    ],
}

VALID_PATTERNS = {"prenom.nom", "p.nom", "nom.prenom", "prenom-nom", "prenom_nom"}
GENERIC_LOCALS = {
    "contact", "info", "accueil", "direction", "secretariat", "admin", "rh", "mairie",
    "communication", "compta", "comptabilite", "standard", "reception", "recrutement",
    "candidature", "cabinet", "courrier", "no-reply", "noreply",
}
RH_GENERIC_EMAIL_HINTS = {
    "rh", "drh", "recrutement", "candidature", "emploi", "mobilite", "carriere",
    "ressourceshumaines", "ressourcehumaine", "ressources.humaines", "ressources_humaines",
}
STATIC_EMAIL_PAGES = [
    "", "/contact", "/annuaire", "/organigramme", "/mairie", "/recrutement", "/emploi",
    "/offres-d-emploi", "/nous-rejoindre", "/ressources-humaines",
]
MAX_STATIC_EMAIL_PAGES = 6


@dataclass
class RoleResult:
    role: str
    prenom: str = ""
    nom: str = ""
    poste: str = ""
    confidence: str = "basse"
    source_url: str = ""
    email_public: str = ""
    email_reconstitue: str = ""
    email_confidence: str = "basse"
    reason: str = ""


def normalize_text(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]", "", s)


def clean_domain(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        netloc = urlparse(url).netloc.lower().strip()
    except Exception:
        return None
    if netloc.startswith("www."):
        netloc = netloc[4:]
    if not netloc or "." not in netloc:
        return None
    for bad in BAD_DOMAINS:
        if netloc == bad or netloc.endswith("." + bad) or bad in netloc:
            return None
    if any(p in netloc for p in BAD_DOMAIN_PATTERNS):
        return None
    return netloc


def is_domain_suspicious(domain: str) -> bool:
    d = (domain or "").strip().lower()
    if not d:
        return True
    if any(p in d for p in BAD_DOMAIN_PATTERNS):
        return True
    if any(d == bad or d.endswith("." + bad) or bad in d for bad in BAD_DOMAINS):
        return True
    return False


def looks_like_person_name(prenom: str, nom: str) -> bool:
    raw_full = f"{prenom} {nom}".strip().lower()
    raw_full = unicodedata.normalize("NFD", raw_full)
    raw_full = "".join(c for c in raw_full if unicodedata.category(c) != "Mn")
    if any(marker in raw_full for marker in ["monsieur", "madame", "inconnu", "unknown"]):
        return False

    tokens = [normalize_name_token(t) for t in re.split(r"[\s\-']+", raw_full) if t.strip()]
    if tokens and sum(1 for t in tokens if t in NAME_TITLE_TOKENS) >= 2:
        return False

    p = normalize_name_token(prenom)
    n = normalize_name_token(nom)
    if len(p) < 2 or len(n) < 2:
        return False
    if p in PLACEHOLDER_NAME_TOKENS or n in PLACEHOLDER_NAME_TOKENS:
        return False
    return True


def normalize_name_token(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z]", "", s)


def parse_json_safe(text: str) -> Dict[str, Any]:
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    cleaned = re.sub(r"^```json\s*|\s*```$", "", text.strip(), flags=re.IGNORECASE)
    try:
        return json.loads(cleaned)
    except Exception:
        return {}


def backoff_sleep(attempt: int, base: float = 1.5) -> None:
    time.sleep(base * (2 ** max(0, attempt - 1)) + random.uniform(0, 0.8))


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = re.sub(r"[^0-9]", "", s)
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def load_dotenv_if_present(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key or key in os.environ:
                continue
            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]
            os.environ[key] = value
    except Exception:
        # Non-blocking: env file parsing should never stop the pipeline.
        return


class JsonCache:
    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, Any] = {}
        if self.path.exists():
            try:
                # URL cache can become very large; skip eager load if oversized.
                if self.path.name == "url_cache.json" and self.path.stat().st_size > 8 * 1024 * 1024:
                    log.warning("Cache URL volumineux (%s), chargement differe", self.path)
                    self.data = {}
                    return
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}

    def get(self, key: str) -> Any:
        return self.data.get(key)

    def has(self, key: str) -> bool:
        return key in self.data

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")


class Client:
    def __init__(self, serper_key: str, provider: str, llm_key: str, llm_model: str, cache_dir: Path):
        self.serper_key = (serper_key or "").strip()
        self.provider = (provider or "gemini").strip().lower()
        self.llm_key = (llm_key or "").strip()
        self.llm_model = (llm_model or "").strip()
        self.serper_cache = JsonCache(cache_dir / "serper_cache.json")
        self.url_cache = JsonCache(cache_dir / "url_cache.json")
        self.population_cache = JsonCache(cache_dir / "population_cache.json")
        self.serper_network_calls = 0
        self.serper_cache_hits = 0
        self.serper_result_items = 0
        self.serper_profiles: Dict[str, Dict[str, Any]] = {}

    def _cache_key(self, prefix: str, value: str) -> str:
        digest = hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()
        return f"{prefix}:{digest}"

    def _record_serper_profile(
        self,
        label: str,
        query: str,
        result_count: int,
        from_cache: bool,
        network_call: bool,
    ) -> None:
        key = label or "unlabeled"
        profile = self.serper_profiles.setdefault(
            key,
            {
                "calls": 0,
                "network_calls": 0,
                "cache_hits": 0,
                "result_items": 0,
                "non_empty_calls": 0,
                "queries": {},
            },
        )
        profile["calls"] += 1
        profile["result_items"] += int(result_count)
        if network_call:
            profile["network_calls"] += 1
        if from_cache:
            profile["cache_hits"] += 1
        if result_count > 0:
            profile["non_empty_calls"] += 1
        queries = profile["queries"]
        item = queries.setdefault(
            query,
            {"calls": 0, "network_calls": 0, "cache_hits": 0, "result_items": 0, "non_empty_calls": 0},
        )
        item["calls"] += 1
        item["result_items"] += int(result_count)
        if network_call:
            item["network_calls"] += 1
        if from_cache:
            item["cache_hits"] += 1
        if result_count > 0:
            item["non_empty_calls"] += 1

    def serper_search(self, query: str, num: int = 8, label: str = "search") -> List[Dict[str, Any]]:
        key = self._cache_key("serper", f"{query}|{num}")
        cached = self.serper_cache.get(key)
        if isinstance(cached, list):
            self.serper_cache_hits += 1
            self.serper_result_items += len(cached)
            self._record_serper_profile(label, query, len(cached), from_cache=True, network_call=False)
            return cached

        if not self.serper_key:
            return []

        for attempt in range(1, 6):
            try:
                self.serper_network_calls += 1
                resp = requests.post(
                    "https://google.serper.dev/search",
                    headers={"X-API-KEY": self.serper_key, "Content-Type": "application/json"},
                    json={"q": query, "num": int(num)},
                    timeout=30,
                )
                if resp.status_code == 200:
                    items = [x for x in (resp.json() or {}).get("organic", []) if isinstance(x, dict)]
                    self.serper_result_items += len(items)
                    self._record_serper_profile(label, query, len(items), from_cache=False, network_call=True)
                    self.serper_cache.set(key, items)
                    return items
                if resp.status_code in {429, 500, 502, 503, 504}:
                    backoff_sleep(attempt)
                    continue
                break
            except Exception:
                backoff_sleep(attempt)

        self._record_serper_profile(label, query, 0, from_cache=False, network_call=False)
        self.serper_cache.set(key, [])
        return []

    def fetch_url(self, url: str) -> str:
        key = self._cache_key("url", url)
        cached = self.url_cache.get(key)
        if isinstance(cached, str):
            return cached

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        }
        # URL crawling is the main latency hotspot: keep it bounded.
        for attempt in range(1, 3):
            try:
                resp = requests.get(url, timeout=8, headers=headers)
                if resp.status_code == 200:
                    text = resp.text or ""
                    self.url_cache.set(key, text[:350000])
                    return text
                if resp.status_code in {429, 500, 502, 503, 504}:
                    backoff_sleep(attempt)
                    continue
                break
            except Exception:
                backoff_sleep(attempt)

        self.url_cache.set(key, "")
        return ""

    def llm_json(self, prompt: str, max_tokens: int = 600) -> Dict[str, Any]:
        if not self.llm_key:
            return {}
        txt = self.llm_text(prompt, max_tokens=max_tokens)
        return parse_json_safe(txt)

    def get_commune_population(self, code_insee: str) -> Optional[int]:
        code = (code_insee or "").strip()
        if not code:
            return None

        cache_key = self._cache_key("population", code)
        if self.population_cache.has(cache_key):
            cached = self.population_cache.get(cache_key)
            if isinstance(cached, int):
                return cached
            return None

        url = f"https://geo.api.gouv.fr/communes/{code}?fields=nom,population&format=json&geometry=centre"
        for attempt in range(1, 5):
            try:
                resp = requests.get(url, timeout=20)
                if resp.status_code == 200:
                    body = resp.json() or {}
                    pop = body.get("population")
                    if isinstance(pop, int):
                        self.population_cache.set(cache_key, pop)
                        return pop
                    self.population_cache.set(cache_key, None)
                    return None
                if resp.status_code in {429, 500, 502, 503, 504}:
                    backoff_sleep(attempt)
                    continue
                break
            except Exception:
                backoff_sleep(attempt)

        self.population_cache.set(cache_key, None)
        return None

    def llm_text(self, prompt: str, max_tokens: int = 600) -> str:
        if not self.llm_key:
            return ""
        if self.provider == "mistral":
            return self._call_mistral(prompt, max_tokens)
        return self._call_gemini(prompt, max_tokens)

    def _call_gemini(self, prompt: str, max_tokens: int) -> str:
        model = self.llm_model or os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
        endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.llm_key}"
        payload = {
            "contents": [{"role": "user", "parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": int(max_tokens)},
        }
        for attempt in range(1, 6):
            try:
                resp = requests.post(endpoint, json=payload, timeout=60)
                if resp.status_code == 200:
                    body = resp.json() or {}
                    return (
                        body.get("candidates", [{}])[0]
                        .get("content", {})
                        .get("parts", [{}])[0]
                        .get("text", "")
                        .strip()
                    )
                if resp.status_code in {429, 500, 502, 503, 504}:
                    backoff_sleep(attempt)
                    continue
                break
            except Exception:
                backoff_sleep(attempt)
        return ""

    def _call_mistral(self, prompt: str, max_tokens: int) -> str:
        model = self.llm_model or os.getenv("MISTRAL_MODEL", "mistral-small-latest")
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": int(max_tokens),
        }
        headers = {"Authorization": f"Bearer {self.llm_key}", "Content-Type": "application/json"}
        for attempt in range(1, 6):
            try:
                resp = requests.post(
                    "https://api.mistral.ai/v1/chat/completions",
                    data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                    headers=headers,
                    timeout=60,
                )
                if resp.status_code == 200:
                    choices = (resp.json() or {}).get("choices", [])
                    if choices:
                        return (choices[0].get("message", {}).get("content", "") or "").strip()
                    return ""
                if resp.status_code in {429, 500, 502, 503, 504}:
                    backoff_sleep(attempt)
                    continue
                break
            except Exception:
                backoff_sleep(attempt)
        return ""

    def flush_cache(self) -> None:
        self.serper_cache.flush()
        self.url_cache.flush()
        self.population_cache.flush()

    def serper_stats(self) -> Dict[str, int]:
        return {
            "network_calls": self.serper_network_calls,
            "cache_hits": self.serper_cache_hits,
            "organic_items": self.serper_result_items,
        }

    def serper_profile_rows(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for label, data in sorted(
            self.serper_profiles.items(),
            key=lambda item: (item[1].get("network_calls", 0), item[1].get("calls", 0)),
            reverse=True,
        ):
            rows.append(
                {
                    "label": label,
                    "calls": data["calls"],
                    "network_calls": data["network_calls"],
                    "cache_hits": data["cache_hits"],
                    "result_items": data["result_items"],
                    "non_empty_calls": data["non_empty_calls"],
                    "queries": [
                        {"query": q, **stats}
                        for q, stats in sorted(
                            data["queries"].items(),
                            key=lambda item: (item[1].get("network_calls", 0), item[1].get("result_items", 0)),
                            reverse=True,
                        )
                    ],
                }
            )
        return rows


def extract_emails(text: str, domain: Optional[str] = None) -> List[str]:
    if not text:
        return []
    pattern = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    emails = [e.lower() for e in re.findall(pattern, text, flags=re.IGNORECASE)]
    if domain:
        domain = domain.lower()
        emails = [e for e in emails if e.endswith("@" + domain)]
    return sorted(set(emails))


def is_person_email(email: str) -> bool:
    local = email.split("@")[0].lower()
    token = re.sub(r"[^a-z]", "", local)
    return token not in GENERIC_LOCALS and len(token) > 2


def dedupe_search_results(results: List[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
    unique: List[Dict[str, Any]] = []
    seen = set()
    for item in results:
        if not isinstance(item, dict):
            continue
        key = (
            (item.get("link") or "").strip().lower(),
            (item.get("title") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
        if len(unique) >= limit:
            break
    return unique


def is_rh_generic_email(email: str) -> bool:
    local = (email.split("@")[0] if "@" in email else "").strip().lower()
    if not local:
        return False
    tokens = [t for t in re.split(r"[^a-z0-9]+", local) if t]
    compact = re.sub(r"[^a-z]", "", local)
    if any(t in {"rh", "drh", "recrutement", "candidature", "emploi", "mobilite", "carriere"} for t in tokens):
        return True
    return any(hint in compact for hint in [
        "ressourceshumaines", "ressourcehumaine", "recrutement", "candidature", "mobilite", "carriere"
    ])


def score_rh_generic_email(email: str) -> int:
    local = (email.split("@")[0] if "@" in email else "").strip().lower()
    compact = re.sub(r"[^a-z]", "", local)
    score = 0
    if local in {"rh", "drh", "recrutement", "candidature", "emploi"}:
        score += 8
    if "drh" in compact:
        score += 6
    if "rh" in re.split(r"[^a-z0-9]+", local):
        score += 4
    if "ressourceshumaines" in compact or "ressourcehumaine" in compact:
        score += 5
    if "recrutement" in compact or "candidature" in compact:
        score += 4
    if "emploi" in compact or "mobilite" in compact or "carriere" in compact:
        score += 3
    return score


def detect_pattern(person_emails: List[str]) -> Tuple[Optional[str], str]:
    if not person_emails:
        return None, "basse"
    patterns: List[str] = []
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
    if not patterns:
        return None, "basse"
    counts: Dict[str, int] = {}
    for p in patterns:
        counts[p] = counts.get(p, 0) + 1
    best = sorted(counts.items(), key=lambda x: x[1], reverse=True)[0]
    conf = "haute" if best[1] >= 3 else "moyenne"
    return best[0], conf


def qualify_pattern_with_llm(
    client: Client,
    domain: str,
    person_emails: List[str],
    heuristic_pattern: Optional[str],
    heuristic_confidence: str,
) -> Tuple[Optional[str], str]:
    if not client.llm_key or not domain or not person_emails:
        return heuristic_pattern, heuristic_confidence

    local_parts: List[str] = []
    d_norm = domain.strip().lower()
    for email in person_emails[:20]:
        if "@" not in email:
            continue
        local, dom = email.split("@", 1)
        if dom.strip().lower() != d_norm:
            continue
        local = local.strip().lower()
        if local:
            local_parts.append(local)

    if not local_parts:
        return heuristic_pattern, heuristic_confidence

    prompt = (
        f"Tu dois identifier la structure dominante des emails personnels pour le domaine @{domain}.\n"
        "Locales observes (partie avant @):\n"
        + "\n".join([f"- {x}" for x in local_parts])
        + "\n\n"
        "Reponds UNIQUEMENT en JSON:\n"
        '{"structure":"prenom.nom|p.nom|nom.prenom|prenom-nom|prenom_nom|inconnu",'
        '"confidence":"haute|moyenne|basse","reason":"..."}'
    )
    res = client.llm_json(prompt, max_tokens=220)
    llm_pattern = (res.get("structure") or "").strip().lower()
    llm_conf = (res.get("confidence") or "").strip().lower()
    if llm_pattern in VALID_PATTERNS and llm_conf in {"haute", "moyenne", "basse"}:
        return llm_pattern, llm_conf
    return heuristic_pattern, heuristic_confidence


def build_email(prenom: str, nom: str, pattern: str, domain: str) -> Optional[str]:
    p = normalize_name_token(prenom)
    n = normalize_name_token(nom)
    if not p or not n or not domain:
        return None
    templates = {
        "prenom.nom": f"{p}.{n}@{domain}",
        "p.nom": f"{p[0]}.{n}@{domain}",
        "nom.prenom": f"{n}.{p}@{domain}",
        "prenom-nom": f"{p}-{n}@{domain}",
        "prenom_nom": f"{p}_{n}@{domain}",
    }
    return templates.get(pattern)


def pick_domain_with_heuristics(commune: str, dept: str, results: List[Dict[str, Any]]) -> List[Tuple[str, int, str]]:
    c_norm = normalize_text(commune)
    d_norm = normalize_text(dept)
    candidates: Dict[str, Tuple[int, str]] = {}
    for r in results:
        if not isinstance(r, dict):
            continue
        url = r.get("link", "")
        title = (r.get("title", "") or "")
        snippet = (r.get("snippet", "") or "")
        domain = clean_domain(url)
        if not domain:
            continue
        score = 0
        reason = []
        dn = normalize_text(domain)
        t = normalize_text(title + " " + snippet)
        if c_norm and c_norm in dn:
            score += 5
            reason.append("commune_in_domain")
        if c_norm and c_norm in t:
            score += 3
            reason.append("commune_in_snippet")
        if "mairie" in dn or "ville" in dn:
            score += 2
            reason.append("mairie_or_ville_domain")
        if d_norm and d_norm[:6] and d_norm[:6] in dn:
            score += 1
            reason.append("dept_hint")
        if domain.endswith(".fr"):
            score += 1
            reason.append("fr_tld")
        old = candidates.get(domain)
        if not old or score > old[0]:
            candidates[domain] = (score, ",".join(reason))
    ranked = sorted([(d, v[0], v[1]) for d, v in candidates.items()], key=lambda x: x[1], reverse=True)
    return ranked[:8]


def validate_domain_candidate(client: Client, commune: str, dept: str, domain: str) -> Tuple[int, str]:
    if not domain or is_domain_suspicious(domain):
        return -6, "suspicious"

    commune_n = normalize_text(commune)
    dept_n = normalize_text(dept)
    score = 0
    reasons: List[str] = []

    query = f'site:{domain} mairie "{commune}" "{dept}"'
    results = client.serper_search(query, num=4, label="domain_validate")
    if not results:
        return -2, "no_site_signals"

    for r in results[:3]:
        if not isinstance(r, dict):
            continue
        txt = normalize_text(f"{r.get('title', '')} {r.get('snippet', '')}")
        if "mairie" in txt or "ville" in txt or "municipal" in txt:
            score += 2
            reasons.append("municipal_term")
        if commune_n and commune_n in txt:
            score += 2
            reasons.append("commune_in_snippet")
        if dept_n and dept_n[:6] and dept_n[:6] in txt:
            score += 1
            reasons.append("dept_in_snippet")

    if domain.endswith(".fr"):
        score += 1
        reasons.append("fr_tld")

    # Homepage check to reject directory-like websites.
    home = client.fetch_url(f"https://{domain}")
    home_l = (home or "").lower()
    if home_l:
        if any(p in home_l for p in DIRECTORY_PAGE_PATTERNS):
            score -= 6
            reasons.append("homepage_directory_pattern")
        if ("mairie" in home_l or "ville" in home_l) and normalize_text(commune) in normalize_text(home_l[:30000]):
            score += 2
            reasons.append("homepage_commune_signal")

    return score, ",".join(sorted(set(reasons)))


def pick_best_domain(client: Client, commune: str, dept: str, base_ranked: List[Tuple[str, int, str]]) -> Tuple[str, str, List[Tuple[str, int, str]]]:
    if not base_ranked:
        return "", "basse", []

    rescored: List[Tuple[str, int, str]] = []
    validate_limit = 2
    if len(base_ranked) == 1:
        validate_limit = 1
    elif base_ranked[0][1] - base_ranked[1][1] >= 3:
        validate_limit = 1

    for domain, base_score, base_reason in base_ranked[:validate_limit]:
        bonus, bonus_reason = validate_domain_candidate(client, commune, dept, domain)
        total = base_score + bonus
        reason = ",".join([x for x in [base_reason, bonus_reason] if x])
        rescored.append((domain, total, reason))

    rescored.sort(key=lambda x: x[1], reverse=True)
    best = rescored[0]
    conf = "haute" if best[1] >= 10 else ("moyenne" if best[1] >= 6 else "basse")
    return best[0], conf, rescored


def llm_pick_domain(client: Client, commune: str, dept: str, ranked_domains: List[Tuple[str, int, str]]) -> Tuple[str, str]:
    if not ranked_domains:
        return "", "basse"
    if is_domain_suspicious(ranked_domains[0][0]):
        return "", "basse"
    if not client.llm_key:
        best = ranked_domains[0]
        conf = "haute" if best[1] >= 10 else ("moyenne" if best[1] >= 6 else "basse")
        return best[0], conf
    options = "\n".join([f"- {d} | score={s} | {r}" for d, s, r in ranked_domains])
    prompt = (
        "Choisis le domaine OFFICIEL de la mairie.\n"
        f"Commune: {commune}\n"
        f"Departement: {dept}\n"
        f"Candidats:\n{options}\n\n"
        "Reponds UNIQUEMENT en JSON:\n"
        "{\"domain\":\"...\",\"confidence\":\"haute|moyenne|basse\",\"reason\":\"...\"}"
    )
    result = client.llm_json(prompt, max_tokens=220)
    dom = (result.get("domain") or "").strip().lower()
    conf = (result.get("confidence") or "").strip().lower()
    if dom and any(dom == x[0] for x in ranked_domains) and conf in {"haute", "moyenne", "basse"}:
        if is_domain_suspicious(dom):
            return "", "basse"
        return dom, conf
    best = ranked_domains[0]
    conf = "haute" if best[1] >= 10 else ("moyenne" if best[1] >= 6 else "basse")
    return best[0], conf


def extract_role_from_snippets(
    client: Client,
    role: str,
    commune: str,
    dept: str,
    domain: str,
    snippets: List[Dict[str, Any]],
) -> RoleResult:
    role_label = "DRH" if role == "drh" else "DGS"
    snippets = dedupe_search_results(snippets, limit=8)
    if not snippets:
        return RoleResult(role=role, reason="no_snippets")

    lines = []
    for item in snippets:
        lines.append(f"- {item.get('title', '')} | {item.get('snippet', '')} | {item.get('link', '')}")
    snippets_text = "\n".join(lines)[:6000]
    prompt = (
        f"Tu dois identifier le {role_label} ACTUEL de la mairie de {commune} ({dept}).\n"
        f"Domaine officiel estime: {domain or 'inconnu'}\n"
        "REGLES STRICTES:\n"
        "- Ignore toute personne décrite comme 'en retraite', 'retraité', 'ex-', 'ancien', 'précédent', 'predecesseur'.\n"
        "- Ignore toute information dont la date est antérieure à 2023.\n"
        "- Ne retourne PAS d'email générique (rh@, drh@, contact@, accueil@, direction@, mairie@).\n"
        "Extraits:\n"
        f"{snippets_text}\n\n"
        "Reponds UNIQUEMENT en JSON:\n"
        "{\n"
        "  \"prenom\":\"...\",\n"
        "  \"nom\":\"...\",\n"
        "  \"poste\":\"...\",\n"
        "  \"email_public\":\"\",\n"
        "  \"confidence\":\"haute|moyenne|basse\",\n"
        "  \"source_url\":\"\",\n"
        "  \"reason\":\"...\"\n"
        "}\n"
        "Si aucun nom fiable et ACTUEL, renvoie prenom et nom vides."
    )
    result = client.llm_json(prompt, max_tokens=350)
    rr = RoleResult(role=role)
    rr.prenom = (result.get("prenom") or "").strip()
    rr.nom = (result.get("nom") or "").strip()
    rr.poste = (result.get("poste") or "").strip()
    rr.email_public = (result.get("email_public") or "").strip().lower()
    rr.confidence = (result.get("confidence") or "basse").strip().lower()
    rr.source_url = (result.get("source_url") or "").strip()
    rr.reason = (result.get("reason") or "").strip()

    if rr.confidence not in {"haute", "moyenne", "basse"}:
        rr.confidence = "basse"
    if not looks_like_person_name(rr.prenom, rr.nom):
        rr.prenom = ""
        rr.nom = ""
        rr.confidence = "basse"
    if rr.email_public and "@" not in rr.email_public:
        rr.email_public = ""
    if rr.email_public:
        local = rr.email_public.split("@")[0].lower().replace(".", "").replace("-", "").replace("_", "")
        if local in {g.replace(".", "").replace("-", "").replace("_", "") for g in GENERIC_LOCALS}:
            rr.email_public = ""
    return rr


def gather_domain_emails(client: Client, commune: str, domain: str, use_serper: bool = False) -> List[str]:
    if not domain:
        return []
    found: set[str] = set()

    for page in STATIC_EMAIL_PAGES[:MAX_STATIC_EMAIL_PAGES]:
        base = f"https://{domain}{page}"
        html = client.fetch_url(base)
        if not html:
            continue
        for email in extract_emails(html, domain=domain):
            found.add(email)
        if len(found) >= 12:
            break

    if use_serper and len(found) < 3:
        # Bounded Serper strategy: one compact query, then optional fallback only if still weak.
        queries = [
            f'site:{domain} "@{domain}" (contact OR organigramme OR annuaire OR recrutement OR "{commune}")',
            f'"@{domain}" "{commune}" (mairie OR "ville de")',
        ]
        for q in queries:
            results = client.serper_search(q, num=4, label="email_domain")
            for r in results:
                if not isinstance(r, dict):
                    continue
                text = f"{r.get('title', '')} {r.get('snippet', '')} {r.get('link', '')}"
                for email in extract_emails(text, domain=domain):
                    found.add(email)
            if len(found) >= 6:
                break
            if q == queries[0] and len(found) >= 2:
                break

    return sorted(found)


def find_generic_rh_email(
    client: Client,
    commune: str,
    domain: str,
    domain_emails: List[str],
    seed_urls: List[str],
    allow_serper_search: bool = False,
) -> Tuple[str, str, str, str]:
    if not domain:
        return "", "basse", "", "no_domain"

    generic_candidates = sorted(
        {email for email in domain_emails if is_rh_generic_email(email)},
        key=score_rh_generic_email,
        reverse=True,
    )
    if generic_candidates:
        best = generic_candidates[0]
        confidence = "haute" if score_rh_generic_email(best) >= 8 else "moyenne"
        return best, confidence, f"https://{domain}", "domain_pages"

    best_email = ""
    best_score = -1
    best_url = ""

    for link in seed_urls:
        link = (link or "").strip()
        if not link or link.lower().endswith((".pdf", ".doc", ".docx")):
            continue
        html = client.fetch_url(link)
        if not html:
            continue
        for email in extract_emails(html, domain=domain):
            if not is_rh_generic_email(email):
                continue
            score = score_rh_generic_email(email) + 2
            if score > best_score:
                best_email = email
                best_score = score
                best_url = link

    if allow_serper_search:
        queries = [
            f'site:{domain} ("recrutement" OR "offre d\'emploi" OR "fiche de poste" OR "ressources humaines" OR "DRH" OR "service RH") "@{domain}"',
            f'"{commune}" mairie ("recrutement" OR "offre d\'emploi" OR "fiche de poste" OR "ressources humaines" OR "DRH") "@{domain}"',
        ]
        for q in queries:
            results = client.serper_search(q, num=4, label="email_generic_rh")
            for item in dedupe_search_results(results, limit=6):
                text = f"{item.get('title', '')} {item.get('snippet', '')}"
                for email in extract_emails(text, domain=domain):
                    if not is_rh_generic_email(email):
                        continue
                    score = score_rh_generic_email(email)
                    if score > best_score:
                        best_email = email
                        best_score = score
                        best_url = (item.get("link") or "").strip()

                link = (item.get("link") or "").strip()
                if not link or not clean_domain(link) or link.lower().endswith((".pdf", ".doc", ".docx")):
                    continue
                html = client.fetch_url(link)
                if not html:
                    continue
                for email in extract_emails(html, domain=domain):
                    if not is_rh_generic_email(email):
                        continue
                    score = score_rh_generic_email(email) + 1
                    if score > best_score:
                        best_email = email
                        best_score = score
                        best_url = link
            if best_score >= 8:
                break

    if not best_email:
        return "", "basse", "", "no_generic_rh_email"
    confidence = "haute" if best_score >= 8 else "moyenne"
    return best_email, confidence, best_url, "job_pages"


def process_commune(
    client: Client,
    commune_row: Dict[str, str],
    sleep_s: float = 0.0,
    generic_rh_serper: bool = False,
) -> Dict[str, Any]:
    commune = (commune_row.get("commune") or "").strip()
    dept = (commune_row.get("departement") or "").strip()
    dept_code = (commune_row.get("code_departement") or "").strip()
    insee = (commune_row.get("code_insee") or "").strip()

    log.info("[%s] Debut traitement commune=%s (%s)", insee or "NA", commune, dept)

    if sleep_s > 0:
        time.sleep(sleep_s)

    domain_results: List[Dict[str, Any]] = []
    for dq in [
        f'"{commune}" "{dept}" (mairie OR "ville de" OR "site officiel")',
        f'"mairie {commune}" "{dept}"',
    ]:
        domain_results.extend(client.serper_search(dq, num=5, label="domain_discovery"))
        if len(dedupe_search_results(domain_results, limit=8)) >= 5:
            break
    ranked_domains = pick_domain_with_heuristics(commune, dept, domain_results)
    best_domain, best_confidence, rescored = pick_best_domain(client, commune, dept, ranked_domains)
    llm_domain, llm_confidence = llm_pick_domain(client, commune, dept, rescored if rescored else ranked_domains)
    domain = llm_domain or best_domain
    domain_confidence = llm_confidence if llm_domain else best_confidence
    log.info("[%s] Domaine=%s (conf=%s, candidats=%d)", insee or "NA", domain or "", domain_confidence, len(ranked_domains))

    role_outputs: Dict[str, RoleResult] = {}
    for role in ["drh", "dgs"]:
        snippets: List[Dict[str, Any]] = []
        templates = ROLE_QUERIES[role]
        site_template = templates[0]
        pv_template = templates[1]
        web_template = templates[2]

        if domain:
            site_query = site_template.format(commune=commune, dept=dept, domain=domain)
            snippets.extend(client.serper_search(site_query, num=5, label=f"role_{role}_site"))

        # Fallback 1: PV/organigramme query on same domain when site query finds no person name
        first_result = extract_role_from_snippets(client, role, commune, dept, domain, snippets)
        if not first_result.nom and domain:
            pv_query = pv_template.format(commune=commune, dept=dept, domain=domain)
            pv_snippets = client.serper_search(pv_query, num=5, label=f"role_{role}_pv")
            snippets.extend(pv_snippets)

        # Fallback 2: open-web query if still no person found after PV
        second_result = extract_role_from_snippets(client, role, commune, dept, domain, snippets)
        if not second_result.nom:
            web_query = web_template.format(commune=commune, dept=dept, domain=domain or "")
            web_snippets = client.serper_search(web_query, num=5, label=f"role_{role}_web")
            snippets.extend(web_snippets)

        role_outputs[role] = extract_role_from_snippets(client, role, commune, dept, domain, snippets)
        log.info(
            "[%s] Role=%s snippets=%d candidat=%s %s conf=%s",
            insee or "NA",
            role.upper(),
            len(dedupe_search_results(snippets, limit=8)),
            role_outputs[role].prenom,
            role_outputs[role].nom,
            role_outputs[role].confidence,
        )

    needs_person_email_pattern = any(
        role_outputs[role].prenom and role_outputs[role].nom and not role_outputs[role].email_public
        for role in ["drh", "dgs"]
    )
    domain_emails = gather_domain_emails(client, commune, domain, use_serper=needs_person_email_pattern)
    drh_generic_email, drh_generic_confidence, drh_generic_source_url, drh_generic_reason = find_generic_rh_email(
        client,
        commune,
        domain,
        domain_emails,
        [role_outputs["drh"].source_url, role_outputs["dgs"].source_url],
        allow_serper_search=generic_rh_serper,
    )
    person_emails = [e for e in domain_emails if is_person_email(e)]
    pattern, pattern_confidence = detect_pattern(person_emails)
    log.info(
        "[%s] Emails domaine=%d personnels=%d generic_rh=%s pattern=%s conf=%s",
        insee or "NA",
        len(domain_emails),
        len(person_emails),
        drh_generic_email,
        pattern or "",
        pattern_confidence,
    )

    pattern, pattern_confidence = qualify_pattern_with_llm(
        client,
        domain,
        person_emails,
        pattern,
        pattern_confidence,
    )

    for role in ["drh", "dgs"]:
        role_result = role_outputs[role]
        if role_result.email_public:
            role_result.email_confidence = "haute"
            continue
        if domain and pattern and role_result.prenom and role_result.nom:
            rebuilt = build_email(role_result.prenom, role_result.nom, pattern, domain)
            if rebuilt:
                role_result.email_reconstitue = rebuilt
                role_result.email_confidence = pattern_confidence
        elif domain and role_result.prenom and role_result.nom:
            rebuilt = build_email(role_result.prenom, role_result.nom, "prenom.nom", domain)
            if rebuilt:
                role_result.email_reconstitue = rebuilt
                role_result.email_confidence = "basse"
                if not pattern:
                    pattern = "prenom.nom"
                    pattern_confidence = "basse"

        if (
            not role_result.email_reconstitue
            and not role_result.email_public
            and domain
            and role_result.prenom
            and role_result.nom
        ):
            rebuilt_default = build_email(role_result.prenom, role_result.nom, "prenom.nom", domain)
            if rebuilt_default:
                role_result.email_reconstitue = rebuilt_default
                role_result.email_confidence = role_result.email_confidence or "basse"
                if not pattern:
                    pattern = "prenom.nom"
                    pattern_confidence = "basse"

    log.info(
        "[%s] Fin commune DRH=%s %s DGS=%s %s",
        insee or "NA",
        role_outputs["drh"].prenom,
        role_outputs["drh"].nom,
        role_outputs["dgs"].prenom,
        role_outputs["dgs"].nom,
    )

    return {
        "code_insee": insee,
        "commune": commune,
        "departement": dept,
        "code_departement": dept_code,
        "domain": domain,
        "domain_confidence": domain_confidence,
        "email_pattern": pattern or "",
        "email_pattern_confidence": pattern_confidence,
        "emails_domain_samples": " | ".join(domain_emails[:8]),
        "drh_prenom": role_outputs["drh"].prenom,
        "drh_nom": role_outputs["drh"].nom,
        "drh_poste": role_outputs["drh"].poste,
        "drh_confidence": role_outputs["drh"].confidence,
        "drh_source_url": role_outputs["drh"].source_url,
        "drh_email_public": role_outputs["drh"].email_public,
        "drh_email_generic": drh_generic_email,
        "drh_email_generic_confidence": drh_generic_confidence,
        "drh_email_generic_source_url": drh_generic_source_url,
        "drh_email_generic_reason": drh_generic_reason,
        "drh_email_reconstitue": role_outputs["drh"].email_reconstitue,
        "drh_email_confidence": role_outputs["drh"].email_confidence,
        "drh_reason": role_outputs["drh"].reason,
        "dgs_prenom": role_outputs["dgs"].prenom,
        "dgs_nom": role_outputs["dgs"].nom,
        "dgs_poste": role_outputs["dgs"].poste,
        "dgs_confidence": role_outputs["dgs"].confidence,
        "dgs_source_url": role_outputs["dgs"].source_url,
        "dgs_email_public": role_outputs["dgs"].email_public,
        "dgs_email_reconstitue": role_outputs["dgs"].email_reconstitue,
        "dgs_email_confidence": role_outputs["dgs"].email_confidence,
        "dgs_reason": role_outputs["dgs"].reason,
    }


def read_communes(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not (row.get("commune") and row.get("code_insee")):
                continue
            rows.append({k: (v or "").strip() for k, v in row.items()})

    # Dedup by INSEE code, keep first row for stability.
    seen = set()
    dedup: List[Dict[str, str]] = []
    for row in rows:
        code = row.get("code_insee", "")
        if code in seen:
            continue
        seen.add(code)
        dedup.append(row)
    return dedup


def write_output(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def get_llm_settings() -> Tuple[str, str, str]:
    provider = os.getenv("LLM_PROVIDER", "gemini").strip().lower()
    if provider == "mistral":
        return provider, os.getenv("MISTRAL_API_KEY", ""), os.getenv("MISTRAL_MODEL", "mistral-small-latest")
    return provider, os.getenv("GEMINI_API_KEY", ""), os.getenv("GEMINI_MODEL", "gemini-2.0-flash")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trouve DRH + DGS des communes IDF et reconstitue les emails via domaine officiel."
    )
    parser.add_argument(
        "--input",
        default="data/communes_idf.csv",
        help="CSV input avec code_insee, commune, departement, code_departement",
    )
    parser.add_argument(
        "--output",
        default="data/communes_idf_contacts_drh_dgs.csv",
        help="CSV de sortie",
    )
    parser.add_argument("--limit", type=int, default=0, help="Nombre max de communes (0 = toutes)")
    parser.add_argument("--offset", type=int, default=0, help="Offset de depart")
    parser.add_argument("--sleep", type=float, default=0.0, help="Pause en secondes entre communes")
    parser.add_argument("--flush-every", type=int, default=20, help="Frequence de flush cache")
    parser.add_argument("--progress-every", type=int, default=1, help="Log progression toutes les N communes traitees")
    parser.add_argument(
        "--cache-dir",
        default="data/cache_communes_idf_contacts",
        help="Repertoire de cache JSON pour Serper/HTML/population",
    )
    parser.add_argument(
        "--serper-profile-output",
        default="",
        help="Chemin JSON optionnel pour exporter le profil detaille des requetes Serper",
    )
    parser.add_argument(
        "--enable-generic-rh-serper",
        action="store_true",
        help="Active la recherche Serper dediee aux mails generiques RH (desactive par defaut).",
    )
    parser.add_argument(
        "--min-population",
        type=int,
        default=2000,
        help="Exclure les communes avec population strictement inferieure a ce seuil (default: 2000)",
    )
    parser.add_argument(
        "--no-population-filter",
        action="store_true",
        help="Desactiver le filtre population",
    )
    parser.add_argument("--log-level", default="INFO", help="DEBUG|INFO|WARNING|ERROR")
    return parser.parse_args()


def main() -> int:
    load_dotenv_if_present(Path(".env"))
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, str(args.log_level).upper(), logging.INFO))

    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.exists():
        log.error("Fichier introuvable: %s", input_path)
        return 1

    provider, llm_key, llm_model = get_llm_settings()
    serper_key = os.getenv("SERPER_API_KEY", "")

    if not serper_key:
        log.warning("SERPER_API_KEY absente: la recherche sera tres limitee.")
    if not llm_key:
        log.warning("Cle LLM absente: extraction des roles et qualification plus faibles.")
    if args.enable_generic_rh_serper:
        log.info("Mode couteux active: recherche Serper des mails generiques RH.")

    cache_dir = Path(args.cache_dir)
    client = Client(serper_key, provider, llm_key, llm_model, cache_dir)

    communes = read_communes(input_path)
    if args.offset:
        communes = communes[args.offset:]
    if args.limit and args.limit > 0:
        communes = communes[:args.limit]

    log.info("Communes a traiter: %d", len(communes))
    rows: List[Dict[str, Any]] = []
    skipped_small = 0
    no_population = 0
    processed = 0

    min_population = 0 if args.no_population_filter else max(0, int(args.min_population))
    if min_population > 0:
        log.info("Filtre population actif: communes avec pop < %d exclues", min_population)

    for idx, commune in enumerate(communes, start=1):
        try:
            started = time.time()
            insee = (commune.get("code_insee") or "").strip()
            population = parse_int(commune.get("population"))
            if population is None:
                population = client.get_commune_population(insee)
            if min_population > 0 and population is not None and population < min_population:
                skipped_small += 1
                if skipped_small % 25 == 0:
                    log.info("Communes exclues (<%d habitants): %d", min_population, skipped_small)
                continue
            if min_population > 0 and population is None:
                no_population += 1

            row = process_commune(
                client,
                commune,
                sleep_s=max(0.0, args.sleep),
                generic_rh_serper=bool(args.enable_generic_rh_serper),
            )
            row["population"] = population if population is not None else ""
            rows.append(row)
            processed += 1
            if processed % max(1, args.progress_every) == 0:
                elapsed = time.time() - started
                log.info(
                    "Progression %d/%d | dernier=%s (%s) DRH=%s %s DGS=%s %s | %.1fs",
                    processed,
                    len(communes),
                    row["commune"],
                    row["domain"],
                    row["drh_prenom"],
                    row["drh_nom"],
                    row["dgs_prenom"],
                    row["dgs_nom"],
                    elapsed,
                )
            if idx % max(1, args.flush_every) == 0:
                client.flush_cache()
        except KeyboardInterrupt:
            log.warning("Interruption utilisateur, ecriture partielle.")
            break
        except Exception as exc:
            log.warning("Erreur commune=%s: %s", commune.get("commune"), exc)
            rows.append(
                {
                    "code_insee": commune.get("code_insee", ""),
                    "commune": commune.get("commune", ""),
                    "departement": commune.get("departement", ""),
                    "code_departement": commune.get("code_departement", ""),
                    "population": "",
                    "domain": "",
                    "domain_confidence": "basse",
                    "email_pattern": "",
                    "email_pattern_confidence": "basse",
                    "emails_domain_samples": "",
                    "drh_prenom": "",
                    "drh_nom": "",
                    "drh_poste": "",
                    "drh_confidence": "basse",
                    "drh_source_url": "",
                    "drh_email_public": "",
                    "drh_email_generic": "",
                    "drh_email_generic_confidence": "basse",
                    "drh_email_generic_source_url": "",
                    "drh_email_generic_reason": "",
                    "drh_email_reconstitue": "",
                    "drh_email_confidence": "basse",
                    "drh_reason": f"error:{exc}",
                    "dgs_prenom": "",
                    "dgs_nom": "",
                    "dgs_poste": "",
                    "dgs_confidence": "basse",
                    "dgs_source_url": "",
                    "dgs_email_public": "",
                    "dgs_email_reconstitue": "",
                    "dgs_email_confidence": "basse",
                    "dgs_reason": f"error:{exc}",
                }
            )

    write_output(output_path, rows)
    client.flush_cache()
    log.info(
        "Termine. Sortie: %s (%d lignes, exclues_petite_commune=%d, pop_inconnue=%d)",
        output_path,
        len(rows),
        skipped_small,
        no_population,
    )
    stats = client.serper_stats()
    log.info(
        "Serper stats: requetes_reseau=%d cache_hits=%d snippets=%d",
        stats["network_calls"],
        stats["cache_hits"],
        stats["organic_items"],
    )
    for row in client.serper_profile_rows():
        log.info(
            "Serper profile %s: appels=%d reseau=%d cache=%d non_vides=%d items=%d",
            row["label"],
            row["calls"],
            row["network_calls"],
            row["cache_hits"],
            row["non_empty_calls"],
            row["result_items"],
        )
    if args.serper_profile_output:
        profile_path = Path(args.serper_profile_output)
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        profile_path.write_text(
            json.dumps(client.serper_profile_rows(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
