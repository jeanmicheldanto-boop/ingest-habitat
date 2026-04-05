"""Passe B V2: qualification Serper + LLM ciblee pour signaux tension gestionnaires.

Objectif:
- Cibler les gestionnaires prioritaires (grands, keywords_v1, signaux V1 sensibles)
- Interroger Serper via cache SQL finess_cache_serper
- Qualifier les 4 axes (financier, rh, qualite, juridique) via LLM
- Ecrire les colonnes V2 detail/sources/confiance avec methode=serper_passe_b

Usage:
    python scripts/signaux_v2_passe_b.py --batch-size 100 --batch-offset 0
    python scripts/signaux_v2_passe_b.py --dept 75,69 --batch-size 30
    python scripts/signaux_v2_passe_b.py --dry-run --skip-serper-llm --batch-size 10

Env attendues:
    SERPER_API_KEY
    LLM_PROVIDER=gemini|mistral
    GEMINI_API_KEY / MISTRAL_API_KEY
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2.extras
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from database import DatabaseManager
from enrich_finess_config import GEMINI_CONFIG, MISTRAL_CONFIG

NOM_PUBLIC_CONNU = {
    "MEDICA": "Medica",
    "COLISEE": "Colisee France",
    "DOMUSVI": "DomusVi",
    "KORIAN": "Korian",
    "ORPEA": "Orpea",
    "EMEIS": "Emeis",
    "APF": "APF France Handicap",
    "VYV": "VYV3",
    "FONDATION ANAIS": "Fondation Anais",
    "LADAPT": "LADAPT",
    "FRANCE HORIZON": "France Horizon",
    "COALLIA": "Coallia",
    "SOS VILLAGES": "SOS Villages d'Enfants",
}

NEGATIVE_PATTERNS = {
    "financier": [
        r"\bdeficit\b", r"\btresorerie\b", r"\bplan social\b", r"\bpse\b", r"licenci",
        r"redressement judiciaire", r"liquidation judiciaire", r"procedure collective", r"cessation",
        r"menace de fermeture", r"fermeture",
    ],
    "juridique": [
        r"redressement judiciaire", r"liquidation judiciaire", r"procedure collective",
        r"administrateur judiciaire", r"mise sous administration provisoire",
    ],
    "qualite": [
        r"injonction ars", r"mise en demeure", r"fermeture administrative", r"maltraitance",
        r"incident grave",
    ],
    "rh": [
        r"\bgreve\b", r"grevistes", r"piquet", r"plan de departs", r"fermeture faute de personnel",
        r"conflit social", r"\bplan social\b", r"\bpse\b", r"licenciement collectif",
    ],
}

POSITIVE_OR_NEUTRAL_FINANCIAL_PATTERNS = [
    r"\bexcedent\b",
    r"augmentation de la tresorerie",
    r"tresorerie positive",
    r"retour a l'equilibre",
    r"amelioration de la tresorerie",
    r"croissance",
    r"deficit de la generosite du public",
    r"dans les comptes combines",
    r"sans plus de precision",
    r"d'apres la federation hospitaliere",
    r"ehpad publics",
    r"sauvetage",
    r"evite grace a",
]

WEAK_RH_PATTERNS = [
    r"difficultes de recrutement",
    r"recrutement",
    r"tension sur le recrutement",
    r"manque de personnel",
    r"inquietudes",
    r"conditions de travail",
    r"tensions syndicales",
]

NEGATED_RH_PATTERNS = [
    r"absence de plan de sauvegarde de l'emploi",
    r"absence de pse",
]

WEAK_QUALITE_PATTERNS = [
    r"publication d'un rapport d'inspection",
    r"rapport d'inspection",
    r"fermeture de services en raison des contaminations",
    r"cessation d'activite",
]

WEAK_JURIDIQUE_PATTERNS = [
    r"litige juridique",
    r"cour d'appel",
    r"tribunal administratif",
    r"prud'hom",
    r"contentieux",
    r"recours",
    r"potentiellement lie",
    r"risque potentiel",
    r"liquidation potentielle",
    r"en lien avec",
    r"d'une societe",
    r"par vyv3 bourgogne",
    r"fedosad",
    r"protocole signe avec",
    r"dans le cadre de la liquidation judiciaire de la sas",
    r"liquidation judiciaire de la sas",
]

JURIDIQUE_NEGATION_PATTERNS = [
    r"ne fait pas l'objet",
    r"sans liquidation",
    r"sans redressement",
    r"evite grace a",
    r"redressement judiciaire evite",
    r"potentiellement",
    r"risque potentiel",
    r"en lien avec",
    r"sauvetage",
    r"par vyv3 bourgogne",
]


def _extract_json_object(text: str) -> Optional[str]:
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return text[start:end + 1]
    return None


def _short(text: str, limit: int = 380) -> str:
    clean = (text or "").strip().replace("\n", " ")
    return clean[:limit]


def normalize_text(text: str) -> str:
    raw = (text or "").lower()
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", raw)
        if not unicodedata.combining(ch)
    )


def _clean_secret(value: str) -> str:
    return (value or "").replace("\ufeff", "").replace("\x00", "").strip()


def _call_gemini(api_key: str, model: str, prompt: str, max_tokens: int = 1000) -> str:
    model_name = (model or "").strip() or str(GEMINI_CONFIG["model"])
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": max_tokens,
            "responseMimeType": "application/json",
        },
    }
    for attempt in range(5):
        try:
            resp = requests.post(endpoint, json=payload, timeout=70)
            if resp.status_code == 429:
                time.sleep(8 * (attempt + 1) + random.uniform(0, 2))
                continue
            if resp.status_code in {500, 502, 503, 504}:
                time.sleep(2 ** attempt + random.uniform(0, 1))
                continue
            if resp.status_code != 200:
                return ""
            data = resp.json() or {}
            candidates = data.get("candidates") or []
            if not candidates:
                return ""
            parts = (candidates[0].get("content") or {}).get("parts") or []
            texts = [p.get("text", "") for p in parts if isinstance(p, dict) and p.get("text")]
            return "\n".join(texts).strip()
        except Exception:
            time.sleep(2 ** attempt)
    return ""


def _call_mistral(api_key: str, model: str, prompt: str, max_tokens: int = 1000) -> str:
    model_name = (model or "").strip() or str(MISTRAL_CONFIG["model"])
    headers = {"Content-Type": "application/json; charset=utf-8", "Authorization": f"Bearer {api_key}"}
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": max_tokens,
    }
    for attempt in range(5):
        try:
            resp = requests.post(
                "https://api.mistral.ai/v1/chat/completions",
                headers=headers,
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                timeout=70,
            )
            if resp.status_code == 429:
                time.sleep(8 * (attempt + 1) + random.uniform(0, 2))
                continue
            if resp.status_code in {500, 502, 503, 504}:
                time.sleep(2 ** attempt + random.uniform(0, 1))
                continue
            if resp.status_code != 200:
                return ""
            choices = (resp.json() or {}).get("choices", [])
            if not choices:
                return ""
            return (choices[0].get("message", {}).get("content") or "").strip()
        except Exception:
            time.sleep(2 ** attempt)
    return ""


def llm_json(provider: str, api_key: str, model: str, prompt: str, max_tokens: int = 1000) -> Dict[str, Any]:
    key = _clean_secret(api_key)
    if not key:
        return {}
    raw = _call_gemini(key, model, prompt, max_tokens) if provider == "gemini" else _call_mistral(key, model, prompt, max_tokens)
    if not raw:
        # Fallback provider: utile si Gemini retourne vide sur certains prompts
        if provider == "gemini":
            m_key = _clean_secret(os.getenv("MISTRAL_API_KEY", ""))
            if m_key:
                raw = _call_mistral(m_key, os.getenv("MISTRAL_MODEL", str(MISTRAL_CONFIG["model"])), prompt, max_tokens)
        else:
            g_key = _clean_secret(os.getenv("GEMINI_API_KEY", ""))
            if g_key:
                raw = _call_gemini(g_key, os.getenv("GEMINI_MODEL", str(GEMINI_CONFIG["model"])), prompt, max_tokens)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        cleaned = re.sub(r"^```json\\s*|\\s*```$", "", raw, flags=re.IGNORECASE).strip()
        try:
            return json.loads(cleaned)
        except Exception:
            extracted = _extract_json_object(cleaned)
            if extracted:
                try:
                    return json.loads(extracted)
                except Exception:
                    return {}
            return {}


def serper_search(query: str, api_key: str, num: int = 8) -> List[Dict[str, Any]]:
    key = _clean_secret(api_key)
    if not key:
        return []
    try:
        resp = requests.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": key, "Content-Type": "application/json"},
            json={"q": query, "num": int(num)},
            timeout=25,
        )
        if resp.status_code != 200:
            return []
        data = resp.json() or {}
        return [x for x in (data.get("organic") or []) if isinstance(x, dict)]
    except Exception:
        return []


def get_or_search_serper(cur: psycopg2.extras.RealDictCursor, query: str, api_key: str, num: int = 8) -> List[Dict[str, Any]]:
    query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
    cur.execute(
        "SELECT results FROM finess_cache_serper WHERE query_hash = %s AND expire_at > NOW()",
        (query_hash,),
    )
    row = cur.fetchone()
    if row:
        try:
            cached = row["results"]
            if isinstance(cached, str):
                return json.loads(cached)
            return cached or []
        except Exception:
            return []

    results = serper_search(query, api_key=api_key, num=num)
    try:
        cur.execute(
            """
            INSERT INTO finess_cache_serper (query_hash, query_text, results, nb_results)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (query_hash) DO UPDATE SET
                results = EXCLUDED.results,
                nb_results = EXCLUDED.nb_results,
                date_requete = NOW(),
                expire_at = NOW() + INTERVAL '30 days'
            """,
            (query_hash, query, json.dumps(results, ensure_ascii=False), len(results)),
        )
        # Commit immediately so cache survives a later rollback (LLM error etc.)
        cur.connection.commit()
    except Exception:
        pass
    return results


def choose_public_name(raison_sociale: Optional[str], sigle: Optional[str]) -> str:
    rs = (raison_sociale or "").strip()
    sg = (sigle or "").strip()
    hay = f"{rs} {sg}".upper()
    for key, public_name in NOM_PUBLIC_CONNU.items():
        if key in hay:
            return public_name
    if len(sg) >= 3:
        return sg
    return rs


def has_sensitive_v1_type(signaux_recents: Any) -> bool:
    if not isinstance(signaux_recents, list):
        return False
    for elem in signaux_recents:
        if not isinstance(elem, dict):
            continue
        t = (elem.get("type") or "").strip().lower()
        if t in {"fermeture", "conflit", "inspection"}:
            return True
    return False


def summarize_v1(signaux_recents: Any) -> str:
    if not isinstance(signaux_recents, list):
        return ""
    lines: List[str] = []
    for elem in signaux_recents[:12]:
        if not isinstance(elem, dict):
            continue
        stype = (elem.get("type") or "").strip()
        resume = (elem.get("resume") or "").strip()
        impact = (elem.get("impact") or "").strip()
        if stype or resume or impact:
            lines.append(f"- type={stype} impact={impact} resume={resume}".strip())
    return "\n".join(lines)


def build_queries(name: str, include_rh_query: bool) -> List[str]:
    q = [
        (
            f'"{name}" (deficit OR tresorerie OR "plan social" OR PSE OR licenciements '
            f'OR "redressement judiciaire" OR "procedure collective" OR liquidation OR fermeture)'
        ),
        (
            f'"{name}" ("injonction ARS" OR "mise en demeure" OR "fermeture administrative" '
            f'OR "incident grave" OR maltraitance OR inspection)'
        ),
    ]
    if include_rh_query:
        q.append(f'"{name}" (greve OR grevistes OR piquet OR "plan de departs" OR "plan social" OR "fermeture faute de personnel")')
    return q


def build_queries_passe_a(name: str) -> List[str]:
    return [
        f'"{name}" (deficit OR tresorerie OR "plan social" OR PSE)',
        f'"{name}" (liquidation OR "redressement judiciaire" OR "procedure collective" OR fermeture)',
    ]


def collect_snippets(cur: psycopg2.extras.RealDictCursor, serper_key: str, queries: List[str], max_per_query: int) -> List[Dict[str, str]]:
    uniq: Dict[str, Dict[str, str]] = {}
    for q in queries:
        rows = get_or_search_serper(cur, q, serper_key, num=max_per_query)
        for r in rows:
            link = (r.get("link") or "").strip()
            title = (r.get("title") or "").strip()
            snippet = (r.get("snippet") or "").strip()
            key = link or hashlib.sha256((title + "|" + snippet).encode("utf-8")).hexdigest()
            if key in uniq:
                continue
            uniq[key] = {
                "query": q,
                "title": title,
                "snippet": snippet,
                "url": link,
                "source": (r.get("source") or "").strip(),
            }
    return list(uniq.values())


def build_prompt(row: Dict[str, Any], nom_req: str, v1_resume: str, snippets: List[Dict[str, str]]) -> str:
    snip_lines = []
    for s in snippets[:12]:
        snip_lines.append(
            f"- title={_short(s.get('title',''))} | snippet={_short(s.get('snippet',''))} | url={s.get('url','')}"
        )
    snippets_txt = "\n".join(snip_lines) if snip_lines else "(aucun snippet)"

    return f"""
Tu es un analyste du secteur medico-social francais.

Gestionnaire: {nom_req}
Raison sociale base: {row.get('raison_sociale') or ''}
Secteur: {row.get('secteur_activite_principal') or ''}
Nb etablissements: {row.get('nb_etablissements') or 0}
Departement: {row.get('departement_nom') or row.get('departement_code') or ''}

Signaux V1 resumes:
{v1_resume or '(aucun)'}

Nouveaux extraits de recherche:
{snippets_txt}

Mission: ne retenir QUE les difficultes individuelles reelles de ce gestionnaire.
Exclure bruit sectoriel, mobilisation de branche, CPOM, extension, recrutement normal.

Reponds en JSON strict:
{{
  "signal_financier": true|false,
  "signal_financier_detail": "<texte court ou null>",
  "signal_rh": true|false,
  "signal_rh_detail": "<texte court ou null>",
  "signal_qualite": true|false,
  "signal_qualite_detail": "<texte court ou null>",
  "signal_juridique": true|false,
  "signal_juridique_detail": "<texte court ou null>",
  "sources": ["<url1>", "<url2>"],
  "confiance": "haute|moyenne|basse",
  "periode": "<YYYY ou YYYY-YYYY ou null>"
}}

Si aucun signal individuel: mettre tous les booleens a false et details=null.
""".strip()


def _alias_profile(row: Dict[str, Any], nom_req: str) -> Dict[str, List[str]]:
    raw = [
        str(row.get("raison_sociale") or ""),
        str(row.get("sigle") or ""),
        str(nom_req or ""),
    ]
    normalized: List[str] = []
    for val in raw:
        v = normalize_text(val)
        v = re.sub(r"[^a-z0-9\s]", " ", v)
        v = re.sub(r"\s+", " ", v).strip()
        if len(v) >= 3:
            normalized.append(v)

    # Add mapped public names when present in base labels.
    hay = " ".join(normalized)
    for key, public_name in NOM_PUBLIC_CONNU.items():
        k = normalize_text(key)
        if k in hay:
            v = normalize_text(public_name)
            v = re.sub(r"[^a-z0-9\s]", " ", v)
            v = re.sub(r"\s+", " ", v).strip()
            if len(v) >= 3:
                normalized.append(v)

    normalized = list(dict.fromkeys(normalized))

    strong_aliases: List[str] = []
    weak_tokens: List[str] = []
    for alias in normalized:
        tokens = [t for t in alias.split() if t]
        if len(alias) >= 10 or len(tokens) >= 2:
            strong_aliases.append(alias)
        for tok in tokens:
            if len(tok) >= 3 and len(tok) <= 5:
                weak_tokens.append(tok)

    strong_aliases = list(dict.fromkeys(strong_aliases))
    weak_tokens = list(dict.fromkeys(weak_tokens))[:20]
    return {"strong_aliases": strong_aliases, "weak_tokens": weak_tokens}


def _passes_alias_guard(snippet: Dict[str, str], profile: Dict[str, List[str]]) -> bool:
    text = normalize_text(((snippet.get("title") or "") + " " + (snippet.get("snippet") or "")))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return False

    strong_aliases = profile.get("strong_aliases") or []
    weak_tokens = profile.get("weak_tokens") or []

    has_strong = any(alias in text for alias in strong_aliases if alias)
    local_marker = bool(re.search(r"\b(ehpad|ime|mas|foyer|residence|site|etablissement|centre)\b", text))
    has_weak = any(re.search(rf"\b{re.escape(tok)}\b", text) for tok in weak_tokens if tok)

    if has_strong:
        return True
    if local_marker and has_weak:
        return True
    return False


def build_scope_prompt(row: Dict[str, Any], nom_req: str, snippets: List[Dict[str, str]]) -> str:
    snip_lines: List[str] = []
    for idx, s in enumerate(snippets[:14], start=1):
        snip_lines.append(
            f"- idx={idx} | title={_short(s.get('title',''), 180)} | snippet={_short(s.get('snippet',''), 220)} | url={s.get('url','')}"
        )
    snippets_txt = "\n".join(snip_lines) if snip_lines else "(aucun snippet)"
    profile = _alias_profile(row, nom_req)
    strong_aliases = profile.get("strong_aliases") or []
    weak_tokens = profile.get("weak_tokens") or []

    return f"""
Tu fais uniquement un controle d'imputabilite de snippets.

Gestionnaire cible:
- nom_requete: {nom_req}
- raison_sociale_base: {row.get('raison_sociale') or ''}
- sigle: {row.get('sigle') or ''}
- departement: {row.get('departement_nom') or row.get('departement_code') or ''}

Alias valides (ne pas faire d'egalite stricte de chaine):
- strong_aliases: {strong_aliases}
- weak_tokens (ambigus): {weak_tokens}

Snippets a classer:
{snippets_txt}

Pour chaque idx, classe la portee:
- gestionnaire_exact
- etablissement_local
- entite_du_groupe
- secteur_general
- hors_perimetre

Regles critiques:
- "gestionnaire_exact" si le snippet correspond clairement au gestionnaire cible via strong_aliases
    ou une formulation equivalente non ambiguë.
- "etablissement_local" seulement si un site local est mentionne ET rattache explicitement au
    gestionnaire cible.
- Si un nom de groupe ou sigle court ambigu est present sans ancrage fort, classer
    "entite_du_groupe" (pas keep).

Regle keep=true uniquement pour:
- gestionnaire_exact
- etablissement_local (si le snippet semble bien rattache au gestionnaire cible)

Reponds en JSON strict:
{{
  "items": [
    {{"idx": 1, "scope": "gestionnaire_exact|etablissement_local|entite_du_groupe|secteur_general|hors_perimetre", "keep": true|false, "reason": "texte court"}}
  ]
}}
""".strip()


def apply_scope_filter_llm(
    provider: str,
    llm_key: str,
    llm_model: str,
    row: Dict[str, Any],
    nom_req: str,
    snippets: List[Dict[str, str]],
) -> Dict[str, Any]:
    if not snippets:
        return {"kept": [], "dropped": 0, "used": False}

    prompt = build_scope_prompt(row, nom_req, snippets)
    scope_obj = llm_json(provider, llm_key, llm_model, prompt, max_tokens=500)
    items = scope_obj.get("items") if isinstance(scope_obj, dict) else None
    if not isinstance(items, list) or not items:
        return {"kept": snippets, "dropped": 0, "used": False}

    keep_map: Dict[int, bool] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        idx = it.get("idx")
        keep = as_bool(it.get("keep"))
        if isinstance(idx, int) and idx >= 1:
            keep_map[idx] = keep

    if not keep_map:
        return {"kept": snippets, "dropped": 0, "used": False}

    kept: List[Dict[str, str]] = []
    for idx, s in enumerate(snippets[:14], start=1):
        if keep_map.get(idx, False):
            kept.append(s)
    if len(snippets) > 14:
        kept.extend(snippets[14:])

    # Deterministic safety guard: prevent weak alias matches from being treated as exact imputability.
    profile = _alias_profile(row, nom_req)
    llm_kept = kept
    kept = [s for s in kept if _passes_alias_guard(s, profile)]
    if llm_kept and not kept:
        # Do not drop all snippets solely because deterministic guard is too strict.
        kept = llm_kept

    # Fallback safety: never drop everything on parsing/classification uncertainty.
    if not kept:
        return {"kept": snippets, "dropped": 0, "used": False}

    return {"kept": kept, "dropped": max(0, len(snippets) - len(kept)), "used": True}


def as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "oui"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def normalize_result(result: Dict[str, Any], snippets: List[Dict[str, str]]) -> Dict[str, Any]:
    if isinstance(result, list):
        first_obj = next((x for x in result if isinstance(x, dict)), {})
        result = first_obj
    if not isinstance(result, dict):
        result = {}

    sources = result.get("sources") if isinstance(result.get("sources"), list) else []
    clean_sources: List[str] = []
    for s in sources:
        if isinstance(s, str) and s.strip():
            clean_sources.append(s.strip())
    if not clean_sources:
        # fallback with snippet urls
        clean_sources = [s["url"] for s in snippets if s.get("url")][:4]

    confiance = (result.get("confiance") or "basse").strip().lower() if isinstance(result.get("confiance"), str) else "basse"
    if confiance not in {"haute", "moyenne", "basse"}:
        confiance = "basse"

    return {
        "signal_financier": as_bool(result.get("signal_financier")),
        "signal_financier_detail": (result.get("signal_financier_detail") if isinstance(result.get("signal_financier_detail"), str) else None),
        "signal_rh": as_bool(result.get("signal_rh")),
        "signal_rh_detail": (result.get("signal_rh_detail") if isinstance(result.get("signal_rh_detail"), str) else None),
        "signal_qualite": as_bool(result.get("signal_qualite")),
        "signal_qualite_detail": (result.get("signal_qualite_detail") if isinstance(result.get("signal_qualite_detail"), str) else None),
        "signal_juridique": as_bool(result.get("signal_juridique")),
        "signal_juridique_detail": (result.get("signal_juridique_detail") if isinstance(result.get("signal_juridique_detail"), str) else None),
        "sources": clean_sources,
        "confiance": confiance,
        "review_required": False,
    }


def has_negative_snippet(snippets: List[Dict[str, str]]) -> bool:
    corpus = " ".join(
        ((s.get("title") or "") + " " + (s.get("snippet") or "")).lower()
        for s in snippets
    )
    for plist in NEGATIVE_PATTERNS.values():
        for pat in plist:
            if re.search(pat, corpus):
                return True
    return False


def infer_from_snippets(snippets: List[Dict[str, str]]) -> Dict[str, Any]:
    corpus = " ".join(
        ((s.get("title") or "") + " " + (s.get("snippet") or "")).lower()
        for s in snippets
    )
    is_fin = any(re.search(p, corpus) for p in NEGATIVE_PATTERNS["financier"])
    is_rh = any(re.search(p, corpus) for p in NEGATIVE_PATTERNS["rh"])
    is_qual = any(re.search(p, corpus) for p in NEGATIVE_PATTERNS["qualite"])
    is_jur = any(re.search(p, corpus) for p in NEGATIVE_PATTERNS["juridique"])

    return {
        "signal_financier": is_fin,
        "signal_financier_detail": "Detection keyword fallback sur snippets Serper" if is_fin else None,
        "signal_rh": is_rh,
        "signal_rh_detail": "Detection keyword fallback sur snippets Serper" if is_rh else None,
        "signal_qualite": is_qual,
        "signal_qualite_detail": "Detection keyword fallback sur snippets Serper" if is_qual else None,
        "signal_juridique": is_jur,
        "signal_juridique_detail": "Detection keyword fallback sur snippets Serper" if is_jur else None,
        "sources": [s["url"] for s in snippets if s.get("url")][:5],
        "confiance": "basse",
    }


def build_evidence_corpus(snippets: List[Dict[str, str]], norm: Dict[str, Any]) -> Dict[str, str]:
    snippet_corpus = normalize_text(" ".join(
        ((s.get("title") or "") + " " + (s.get("snippet") or "")).lower()
        for s in snippets
    ))
    detail_corpus = normalize_text(" ".join(
        str(norm.get(key) or "").lower()
        for key in [
            "signal_financier_detail",
            "signal_rh_detail",
            "signal_qualite_detail",
            "signal_juridique_detail",
        ]
    ))
    return {
        "snippet": snippet_corpus,
        "detail": detail_corpus,
        "combined": (snippet_corpus + " " + detail_corpus).strip(),
    }


def count_matches(patterns: List[str], text: str) -> int:
    return sum(1 for pat in patterns if re.search(pat, text))


def has_any(patterns: List[str], text: str) -> bool:
    return count_matches(patterns, text) > 0


def evidence_score(axis: str, corpora: Dict[str, str]) -> int:
    patterns = NEGATIVE_PATTERNS[axis]
    return count_matches(patterns, corpora["snippet"]) + count_matches(patterns, corpora["detail"])


def distinct_url_count(norm: Dict[str, Any], snippets: List[Dict[str, str]]) -> int:
    urls: set[str] = set()
    for src in norm.get("sources") or []:
        if isinstance(src, str) and src.strip():
            urls.add(src.strip().lower())
    for s in snippets:
        u = (s.get("url") or "").strip().lower()
        if u:
            urls.add(u)
    return len(urls)


def tighten_result(row: Dict[str, Any], norm: Dict[str, Any], snippets: List[Dict[str, str]]) -> Dict[str, Any]:
    corpora = build_evidence_corpus(snippets, norm)
    fin_score = evidence_score("financier", corpora)
    rh_score = evidence_score("rh", corpora)
    qual_score = evidence_score("qualite", corpora)
    jur_score = evidence_score("juridique", corpora)

    snippet_corpus = corpora["snippet"]
    combined_corpus = corpora["combined"]

    # Piste 4: pre-filtre financier asymetrique (deterministe)
    if norm["signal_financier"]:
        has_positive_or_ambiguous_fin = has_any(POSITIVE_OR_NEUTRAL_FINANCIAL_PATTERNS, combined_corpus)
        if fin_score == 0 or has_positive_or_ambiguous_fin:
            norm["signal_financier"] = False
            norm["signal_financier_detail"] = None

    if norm["signal_rh"]:
        weak_only = has_any(WEAK_RH_PATTERNS, combined_corpus) and rh_score == 0
        negated_rh = has_any(NEGATED_RH_PATTERNS, combined_corpus)
        if weak_only or negated_rh or rh_score == 0:
            norm["signal_rh"] = False
            norm["signal_rh_detail"] = None

    if norm["signal_qualite"]:
        weak_quality = has_any(WEAK_QUALITE_PATTERNS, combined_corpus) and qual_score == 0
        if weak_quality or qual_score == 0:
            norm["signal_qualite"] = False
            norm["signal_qualite_detail"] = None

    # Piste 3: pre-filtre juridique strict (deterministe)
    if norm["signal_juridique"]:
        has_hard_legal = has_any(NEGATIVE_PATTERNS["juridique"], snippet_corpus)
        weak_legal = has_any(WEAK_JURIDIQUE_PATTERNS, snippet_corpus)
        negated_legal = has_any(JURIDIQUE_NEGATION_PATTERNS, combined_corpus)
        if jur_score == 0 or negated_legal or (weak_legal and not has_hard_legal):
            norm["signal_juridique"] = False
            norm["signal_juridique_detail"] = None

    active_axes = {
        "financier": int(bool(norm["signal_financier"])),
        "rh": int(bool(norm["signal_rh"])),
        "qualite": int(bool(norm["signal_qualite"])),
        "juridique": int(bool(norm["signal_juridique"])),
    }
    axis_scores = {
        "financier": fin_score,
        "rh": rh_score,
        "qualite": qual_score,
        "juridique": jur_score,
    }

    active_count = sum(active_axes.values())

    # Piste 5: post-filtre multi-axes (deterministe)
    # Si 3+ axes mais moins de 3 URLs distinctes, on garde les axes proposes
    # mais on degrade en basse confiance et on force la revue QA.
    if active_count >= 3 and distinct_url_count(norm, snippets) < 3:
        norm["confiance"] = "basse"
        norm["review_required"] = True

    if not any([
        norm["signal_financier"],
        norm["signal_rh"],
        norm["signal_qualite"],
        norm["signal_juridique"],
    ]):
        norm["confiance"] = "basse"

    return norm


def update_row(cur: psycopg2.extras.RealDictCursor, gid: str, norm: Dict[str, Any]) -> None:
    cur.execute(
        """
        UPDATE finess_gestionnaire
        SET
            signal_financier = %s,
            signal_financier_detail = %s,
            signal_financier_sources = %s,
            signal_rh = %s,
            signal_rh_detail = %s,
            signal_qualite = %s,
            signal_qualite_detail = %s,
            signal_qualite_sources = %s,
            signal_juridique = %s,
            signal_juridique_detail = %s,
            signal_v2_confiance = %s,
            signal_v2_date = NOW(),
            signal_v2_methode = 'serper_passe_b'
        WHERE id_gestionnaire = %s
        """,
        (
            norm["signal_financier"],
            norm["signal_financier_detail"],
            norm["sources"],
            norm["signal_rh"],
            norm["signal_rh_detail"],
            norm["signal_qualite"],
            norm["signal_qualite_detail"],
            norm["sources"],
            norm["signal_juridique"],
            norm["signal_juridique_detail"],
            norm["confiance"],
            gid,
        ),
    )


def mark_passe_a(cur: psycopg2.extras.RealDictCursor, gid: str) -> None:
    cur.execute(
        """
        UPDATE finess_gestionnaire
        SET
            signal_v2_methode = 'serper_passe_a',
            signal_v2_confiance = COALESCE(signal_v2_confiance, 'basse'),
            signal_v2_date = NOW()
        WHERE id_gestionnaire = %s
        """,
        (gid,),
    )


def fetch_candidates_b(
    cur: psycopg2.extras.RealDictCursor,
    batch_offset: int,
    batch_size: int,
    dept_list: Optional[List[str]],
    force_rerun: bool,
) -> List[Dict[str, Any]]:
    dept_filter_sql = ""
    params: List[Any] = []

    if dept_list:
        dept_filter_sql = " AND g.departement_code = ANY(%s)"
        params.append(dept_list)

    rerun_filter = "" if force_rerun else "AND COALESCE(g.signal_v2_methode, '') <> 'serper_passe_b'"

    sql = f"""
    WITH base AS (
        SELECT
            g.id_gestionnaire,
            g.raison_sociale,
            g.sigle,
            g.departement_code,
            g.departement_nom,
            g.secteur_activite_principal,
            g.nb_etablissements,
            g.signal_v2_methode,
            g.signal_rh,
            g.signaux_recents,
            CASE
                WHEN COALESCE(g.nb_etablissements, 0) > 50 THEN 0
                WHEN COALESCE(g.nb_etablissements, 0) > 20 THEN 1
                WHEN g.signal_v2_methode = 'keywords_v1' THEN 2
                WHEN EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(COALESCE(g.signaux_recents, '[]'::jsonb)) e
                    WHERE lower(COALESCE(e->>'type','')) IN ('fermeture','conflit','inspection')
                ) THEN 3
                ELSE 9
            END AS priority_rank
        FROM finess_gestionnaire g
        WHERE (
            COALESCE(g.nb_etablissements, 0) > 50
            OR g.signal_v2_methode = 'keywords_v1'
            OR EXISTS (
                SELECT 1
                FROM jsonb_array_elements(COALESCE(g.signaux_recents, '[]'::jsonb)) e
                WHERE lower(COALESCE(e->>'type','')) IN ('fermeture','conflit','inspection')
            )
        )
        {rerun_filter}
        {dept_filter_sql}
    )
    SELECT *
    FROM base
    WHERE priority_rank < 9
    ORDER BY priority_rank ASC, COALESCE(nb_etablissements, 0) DESC, id_gestionnaire ASC
    OFFSET %s LIMIT %s
    """

    params.extend([batch_offset, batch_size])
    cur.execute(sql, tuple(params))
    return [dict(r) for r in cur.fetchall()]


def fetch_candidates_a(
    cur: psycopg2.extras.RealDictCursor,
    batch_offset: int,
    batch_size: int,
    dept_list: Optional[List[str]],
) -> List[Dict[str, Any]]:
    dept_filter_sql = ""
    params: List[Any] = []

    if dept_list:
        dept_filter_sql = " AND g.departement_code = ANY(%s)"
        params.append(dept_list)

    sql = f"""
    SELECT
        g.id_gestionnaire,
        g.raison_sociale,
        g.sigle,
        g.departement_code,
        g.departement_nom,
        g.secteur_activite_principal,
        g.nb_etablissements,
        g.signal_v2_methode,
        g.signal_rh,
        g.signaux_recents
    FROM finess_gestionnaire g
    WHERE COALESCE(g.signal_v2_methode, '') NOT IN ('serper_passe_a', 'serper_passe_b')
      AND (
          COALESCE(g.nb_etablissements, 0) > 10
          OR COALESCE(g.nb_etablissements, 0) = 1
      )
      AND COALESCE(g.signal_v2_methode, '') IN ('', 'keywords_v1_excluded')
      {dept_filter_sql}
    ORDER BY COALESCE(g.nb_etablissements, 0) DESC, g.id_gestionnaire ASC
    OFFSET %s LIMIT %s
    """

    params.extend([batch_offset, batch_size])
    cur.execute(sql, tuple(params))
    return [dict(r) for r in cur.fetchall()]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Passe B V2 (Serper + LLM) pour signaux gestionnaires")
    p.add_argument("--batch-offset", type=int, default=0)
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--dept", default="", help="Liste de departements, ex: 75,69")
    p.add_argument("--max-serper-results", type=int, default=8)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--force-rerun", action="store_true", help="Requalifie meme si deja serper_passe_b")
    p.add_argument("--skip-serper-llm", action="store_true", help="Smoke test sans appels API")
    p.add_argument("--run-passe-a", action="store_true", help="Active la passe A avant qualification B")
    p.add_argument("--scope-filter-llm", action="store_true", help="Applique un pre-filtre LLM d'imputabilite snippets")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    provider = os.getenv("LLM_PROVIDER", "gemini").strip().lower()
    serper_key = _clean_secret(os.getenv("SERPER_API_KEY", ""))
    if provider == "gemini":
        llm_key = _clean_secret(os.getenv("GEMINI_API_KEY", ""))
        llm_model = os.getenv("GEMINI_MODEL", str(GEMINI_CONFIG["model"]))
    else:
        llm_key = _clean_secret(os.getenv("MISTRAL_API_KEY", ""))
        llm_model = os.getenv("MISTRAL_MODEL", str(MISTRAL_CONFIG["model"]))

    if not args.skip_serper_llm:
        if not serper_key:
            raise RuntimeError("SERPER_API_KEY manquante")
        if not llm_key:
            raise RuntimeError(f"Cle LLM manquante pour provider={provider}")

    dept_list = [x.strip() for x in args.dept.split(",") if x.strip()] if args.dept else None

    print("=" * 72)
    print("PASSE V2 - SERPER + LLM")
    print(f"batch={args.batch_offset}+{args.batch_size} dry_run={args.dry_run} skip_api={args.skip_serper_llm}")
    print(f"provider={provider} llm_model={llm_model}")
    print(f"run_passe_a={args.run_passe_a}")
    print(f"scope_filter_llm={args.scope_filter_llm}")
    if dept_list:
        print(f"departements={','.join(dept_list)}")
    print("=" * 72)

    db = DatabaseManager()
    stats = {
        "processed": 0,
        "updated": 0,
        "passe_a_only": 0,
        "passe_a_to_b": 0,
        "no_snippets": 0,
        "llm_empty": 0,
        "fallback_keyword": 0,
        "errors": 0,
    }

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cands = (
                fetch_candidates_a(
                    cur,
                    batch_offset=args.batch_offset,
                    batch_size=args.batch_size,
                    dept_list=dept_list,
                )
                if args.run_passe_a
                else fetch_candidates_b(
                    cur,
                    batch_offset=args.batch_offset,
                    batch_size=args.batch_size,
                    dept_list=dept_list,
                    force_rerun=args.force_rerun,
                )
            )

            print(f"Candidats: {len(cands)}")

            for i, row in enumerate(cands, start=1):
                gid = row["id_gestionnaire"]
                name = choose_public_name(row.get("raison_sociale"), row.get("sigle"))
                include_rh = bool(row.get("signal_rh")) or has_sensitive_v1_type(row.get("signaux_recents"))
                queries = build_queries(name, include_rh_query=include_rh)

                print(f"[{i}/{len(cands)}] {gid} | {name} | nb_et={row.get('nb_etablissements')}")

                # Guard against concurrent worker having already processed this row
                if not args.force_rerun and not args.skip_serper_llm:
                    cur.execute(
                        "SELECT signal_v2_methode FROM finess_gestionnaire WHERE id_gestionnaire = %s",
                        (gid,),
                    )
                    live = cur.fetchone()
                    if live and (live["signal_v2_methode"] or "") == "serper_passe_b":
                        print("  -> déjà traité par un autre worker, skip")
                        continue

                stats["processed"] += 1

                try:
                    if args.skip_serper_llm:
                        snippets = []
                        result = {
                            "signal_financier": False,
                            "signal_financier_detail": None,
                            "signal_rh": False,
                            "signal_rh_detail": None,
                            "signal_qualite": False,
                            "signal_qualite_detail": None,
                            "signal_juridique": False,
                            "signal_juridique_detail": None,
                            "sources": [],
                            "confiance": "basse",
                        }
                    else:
                        snippets_a: List[Dict[str, str]] = []
                        if args.run_passe_a:
                            snippets_a = collect_snippets(cur, serper_key, build_queries_passe_a(name), args.max_serper_results)
                            if not snippets_a:
                                if not args.dry_run:
                                    mark_passe_a(cur, gid)
                                    conn.commit()
                                stats["passe_a_only"] += 1
                                print("  -> passe A: aucun snippet, marque serper_passe_a")
                                continue
                            if not has_negative_snippet(snippets_a):
                                if not args.dry_run:
                                    mark_passe_a(cur, gid)
                                    conn.commit()
                                stats["passe_a_only"] += 1
                                print("  -> passe A: pas de mots-cles negatifs, marque serper_passe_a")
                                continue
                            stats["passe_a_to_b"] += 1
                            print("  -> passe A: indice negatif detecte, qualification B")

                        snippets = collect_snippets(cur, serper_key, queries, args.max_serper_results)
                        if snippets_a:
                            snippets = snippets_a + snippets

                        # dedup urls/title/snippet
                        dedup: Dict[str, Dict[str, str]] = {}
                        for s in snippets:
                            key = (s.get("url") or "").strip() or hashlib.sha256(
                                ((s.get("title") or "") + "|" + (s.get("snippet") or "")).encode("utf-8")
                            ).hexdigest()
                            dedup[key] = s
                        snippets = list(dedup.values())

                        if args.scope_filter_llm and not args.skip_serper_llm:
                            scope_filtered = apply_scope_filter_llm(
                                provider=provider,
                                llm_key=llm_key,
                                llm_model=llm_model,
                                row=row,
                                nom_req=name,
                                snippets=snippets,
                            )
                            snippets = scope_filtered["kept"]
                            if scope_filtered.get("used"):
                                print(f"  -> scope filter: dropped={scope_filtered['dropped']} kept={len(snippets)}")

                        if not snippets:
                            stats["no_snippets"] += 1
                            print("  -> aucun snippet")
                            continue

                        v1_resume = summarize_v1(row.get("signaux_recents"))
                        prompt = build_prompt(row, name, v1_resume, snippets)
                        result = llm_json(provider, llm_key, llm_model, prompt, max_tokens=900)
                        if not result:
                            stats["llm_empty"] += 1
                            fallback = infer_from_snippets(snippets)
                            if any(
                                [
                                    fallback["signal_financier"],
                                    fallback["signal_rh"],
                                    fallback["signal_qualite"],
                                    fallback["signal_juridique"],
                                ]
                            ):
                                result = fallback
                                stats["fallback_keyword"] += 1
                                print("  -> LLM vide, fallback keyword active")
                            else:
                                if args.run_passe_a:
                                    if not args.dry_run:
                                        mark_passe_a(cur, gid)
                                        conn.commit()
                                    stats["passe_a_only"] += 1
                                    print("  -> LLM vide sans indice, conserve serper_passe_a")
                                else:
                                    print("  -> reponse LLM vide")
                                continue

                    norm = normalize_result(result, snippets)
                    norm = tighten_result(row, norm, snippets)

                    print(
                        "  -> axes",
                        f"fin={int(norm['signal_financier'])}",
                        f"rh={int(norm['signal_rh'])}",
                        f"qual={int(norm['signal_qualite'])}",
                        f"jur={int(norm['signal_juridique'])}",
                        f"conf={norm['confiance']}",
                        f"review={int(bool(norm.get('review_required')))}",
                    )

                    if not args.dry_run:
                        update_row(cur, gid, norm)
                        conn.commit()
                        stats["updated"] += 1
                    else:
                        stats["updated"] += 1

                except Exception as exc:
                    conn.rollback()
                    stats["errors"] += 1
                    print(f"  -> erreur: {exc}")

    print("=" * 72)
    print("STATUT:", stats)
    print("=" * 72)


if __name__ == "__main__":
    main()
