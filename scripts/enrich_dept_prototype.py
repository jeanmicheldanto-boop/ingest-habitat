"""Prototype d'enrichissement (2 départements) — mode test.

Objectif
- Exécuter les règles de la doc sprint:
  - Tarifications/fourchette prix: tentative d'enrichissement systématique (refresh)
  - Services: enrichir uniquement si 0 ou 1 service en base
  - Logements: enrichissement systématique en test + rapport de comparaison vs base
  - Description: si score qualité insuffisant, générer 300–400 mots (ton bienveillant)
  - AVP/public cible: focus HI/HIG via recherche web (incl. PDF/délibérations quand trouvables)

Modes
- --dry-run (par défaut): ne modifie rien en base, écrit uniquement des rapports.
- --dry-run: mode lecture seule, n'écrit rien en base
- Par défaut (sans --dry-run): crée des entrées dans `propositions`/`proposition_items`

Pré-requis (env)
- SERPER_API_KEY (recommandé)
- SCRAPINGBEE_API_KEY (optionnel, améliore le scraping)
- GEMINI_API_KEY (recommandé) + GEMINI_MODEL (optionnel)

Ce script est volontairement "safe":
- il tolère l'absence de clés (il saute les étapes LLM)
- il n'applique jamais directement des UPDATE en base
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Permet d'importer les utilitaires dans `scripts/` sans en faire un package.
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from database import DatabaseManager

try:
    import enrich_quality_scorer
except Exception:  # pragma: no cover
    enrich_quality_scorer = None


SERVICE_KEYS = [
    "activites_organisees",
    "personnel_de_nuit",
    "commerces_a_pied",
    "medecin_intervenant",
    "espace_partage",
    "conciergerie",
]


def _service_key_to_db_libelle(service_key: str) -> str:
    service_key = (service_key or "").strip()
    mapping = {
        "activites_organisees": "activités organisées",
        "personnel_de_nuit": "personnel de nuit",
        "commerces_a_pied": "commerces à pied",
        "medecin_intervenant": "médecin intervenant",
        "espace_partage": "espace_partage",
        "conciergerie": "conciergerie",
    }
    return mapping.get(service_key, service_key)


def resolve_service_id(cur, service_key: str) -> Optional[str]:
    """Résout un `service_key` (pipeline) en `services.id` (DB).

    On évite de créer de nouveaux services: on s'appuie sur la table existante.
    """

    service_key = (service_key or "").strip()
    if not service_key:
        return None

    db_name = _service_key_to_db_libelle(service_key)

    cur.execute(
        """
        SELECT id::text
        FROM services
        WHERE libelle = %s
           OR LOWER(REPLACE(libelle, ' ', '_')) = LOWER(%s)
           OR LOWER(libelle) ILIKE LOWER(%s)
        LIMIT 1;
        """,
        (db_name, service_key.replace("_", " "), f"%{service_key.replace('_', ' ')}%"),
    )
    r = cur.fetchone()
    return str(r[0]) if r and r[0] else None


def get_can_publish_diagnosis(*, cur, etablissement_id: str) -> Dict[str, Any]:
    """Retourne un diagnostic aligné avec `public.can_publish(id)`.

    Objectif: décider tôt (avant enrichissement) des champs à compléter pour maximiser la publishability.
    """

    cur.execute(
        """
        SELECT
          public.can_publish(e.id) as can_publish,
          (COALESCE(NULLIF(trim(e.nom),''), NULL) IS NULL) as missing_nom,
          (COALESCE(NULLIF(trim(e.adresse_l1),''), NULLIF(trim(e.adresse_l2),''), NULL) IS NULL) as missing_address,
          (COALESCE(NULLIF(trim(e.commune),''), NULL) IS NULL) as missing_commune,
          (COALESCE(NULLIF(trim(e.code_postal),''), NULL) IS NULL) as missing_code_postal,
          (e.geom IS NULL) as missing_geom,
          (COALESCE(NULLIF(trim(e.gestionnaire),''), NULL) IS NULL) as missing_gestionnaire,
          (NOT (e.habitat_type IS NOT NULL OR EXISTS (SELECT 1 FROM public.etablissement_sous_categorie esc WHERE esc.etablissement_id = e.id))) as missing_typage,
          (NOT (
              e.email IS NULL
              OR COALESCE(NULLIF(trim(e.email),''), NULL) IS NULL
              OR e.email ~* '^[A-Za-z0-9._%%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
          )) as invalid_email
        FROM public.etablissements e
        WHERE e.id = %s;
        """,
        (etablissement_id,),
    )
    r = cur.fetchone()
    if not r:
        return {"can_publish": None}

    (
        can_publish,
        missing_nom,
        missing_address,
        missing_commune,
        missing_code_postal,
        missing_geom,
        missing_gestionnaire,
        missing_typage,
        invalid_email,
    ) = r

    return {
        "can_publish": bool(can_publish),
        "missing_nom": bool(missing_nom),
        "missing_address": bool(missing_address),
        "missing_commune": bool(missing_commune),
        "missing_code_postal": bool(missing_code_postal),
        "missing_geom": bool(missing_geom),
        "missing_gestionnaire": bool(missing_gestionnaire),
        "missing_typage": bool(missing_typage),
        "invalid_email": bool(invalid_email),
    }


def _looks_like_email(x: str) -> bool:
    x = (x or "").strip()
    if not x or "@" not in x:
        return False
    return bool(re.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", x))


def api_adresse_geocode(*, q: str, limit: int = 1, timeout_s: int = 12) -> Optional[Dict[str, Any]]:
    """Géocodage/validation via api-adresse.data.gouv.fr (France)."""

    q = (q or "").strip()
    if not q:
        return None

    try:
        r = requests.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": q, "limit": int(limit)},
            timeout=timeout_s,
        )
        if r.status_code != 200:
            return None
        data = r.json() or {}
        feats = data.get("features") or []
        if not feats:
            return None
        f = feats[0] or {}
        props = f.get("properties") or {}
        geom = f.get("geometry") or {}
        coords = geom.get("coordinates") or []
        if not isinstance(coords, list) or len(coords) < 2:
            return None

        lon = coords[0]
        lat = coords[1]
        return {
            "score": float(props.get("score") or 0.0),
            "label": props.get("label") or "",
            "postcode": props.get("postcode") or "",
            "city": props.get("city") or "",
            "type": props.get("type") or "unknown",
            "lat": float(lat),
            "lon": float(lon),
        }
    except Exception:
        return None


def _map_api_adresse_type_to_precision(t: str) -> str:
    t = (t or "").strip().lower()
    if t in {"housenumber", "poi"}:
        return "rooftop"
    if t in {"street"}:
        return "street"
    if t in {"municipality"}:
        return "locality"
    return "unknown"


def extract_publish_fields(*, etab: Dict[str, Any], ctx: Dict[str, Any], gemini_key: str, gemini_model: str) -> Dict[str, Any]:
    """Extrait les champs utiles à `can_publish` (adresse/gestionnaire/email) en réutilisant le même contexte."""

    prompt = f"""
Tu es un assistant d'extraction de données à partir de pages web.

Contexte:
Nom: {etab.get('nom','')}
Commune (base): {etab.get('commune','')}
Département (base): {etab.get('departement','')}
Site web (base): {etab.get('site_web','')}

Extraits de pages (texte):
{(ctx or {}).get('combined_text','')[:22000]}

Tâche: extraire uniquement si c'est explicitement présent.

Rends STRICTEMENT du JSON avec ces clés:
{{
  "adresse_l1": <string|null>,
  "code_postal": <string|null>,
  "commune": <string|null>,
  "gestionnaire": <string|null>,
  "email": <string|null>,
  "confidence": <number 0..1>,
  "evidence_urls": <array of strings>
}}

Règles:
- Pas d'invention. Si absent: null.
- "gestionnaire" = nom d'organisme/structure (pas une personne).
- Pour l'email, ne renvoyer que si valide.
""".strip()

    raw = gemini_generate_text(api_key=gemini_key, model=gemini_model, prompt=prompt, max_output_tokens=650)
    m = re.search(r"\{[\s\S]*\}", raw or "")
    txt = m.group(0) if m else (raw or "")
    try:
        data = json.loads(txt)
    except Exception:
        return {}

    out: Dict[str, Any] = {}
    for k in ["adresse_l1", "code_postal", "commune", "gestionnaire", "email"]:
        v = data.get(k)
        if isinstance(v, str):
            v = v.strip()
            if not v:
                v = None
        out[k] = v

    conf = data.get("confidence")
    try:
        out["confidence"] = float(conf)
    except Exception:
        out["confidence"] = 0.0

    ev = data.get("evidence_urls")
    if isinstance(ev, list):
        out["evidence_urls"] = [str(x).strip() for x in ev if str(x).strip()]
    else:
        out["evidence_urls"] = []

    # Safety: ensure valid email
    if out.get("email") and not _looks_like_email(str(out.get("email"))):
        out["email"] = None

    return out


def _derive_adresse_l1_from_api_adresse_label(label: str) -> Optional[str]:
    label = (label or "").strip()
    if not label:
        return None

    # api-adresse label often looks like: "10 Rue X 49000 Angers".
    # Keep it conservative: strip the trailing "<CP> <Ville>" when it matches.
    m = re.match(r"^(.*?)(?:,)?\s+\d{5}\s+[^,]+$", label)
    addr = (m.group(1) if m else label).strip()
    return addr or None


def build_publishability_update(
    *,
    etab: Dict[str, Any],
    publish_diag: Dict[str, Any],
    publish_fixes: Dict[str, Any],
    address_validation: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Construit un dict `to_update` (sans écrire en DB).

    But: maximiser `can_publish` dès le run initial, en combinant extraction (LLM)
    + validation/geo (api-adresse) avec des garde-fous.
    """

    to_update: Dict[str, Any] = {}
    if not publish_diag or publish_diag.get("can_publish") is not False:
        return to_update

    llm_conf = 0.0
    try:
        llm_conf = float((publish_fixes or {}).get("confidence") or 0.0)
    except Exception:
        llm_conf = 0.0
    llm_ok = llm_conf >= 0.6

    geo_score = 0.0
    geo_type = ""
    if address_validation:
        try:
            geo_score = float(address_validation.get("score") or 0.0)
        except Exception:
            geo_score = 0.0
        geo_type = str(address_validation.get("type") or "").strip().lower()

    geo_ok_strong = geo_score >= 0.75 and geo_type in {"housenumber", "street", "poi"}
    geo_ok_city = geo_score >= 0.90 and geo_type in {"municipality"}

    # Adresse
    if publish_diag.get("missing_address"):
        if llm_ok and (publish_fixes or {}).get("adresse_l1"):
            to_update["adresse_l1"] = str(publish_fixes.get("adresse_l1"))
        elif geo_ok_strong:
            addr = _derive_adresse_l1_from_api_adresse_label(str(address_validation.get("label") or ""))
            if addr:
                to_update["adresse_l1"] = addr

    # Code postal
    if publish_diag.get("missing_code_postal"):
        cp = None
        if llm_ok and (publish_fixes or {}).get("code_postal"):
            cp = str(publish_fixes.get("code_postal"))
        elif address_validation and (geo_ok_strong or geo_ok_city) and address_validation.get("postcode"):
            cp = str(address_validation.get("postcode"))
        if cp and re.match(r"^\d{5}$", cp.strip()):
            to_update["code_postal"] = cp.strip()

    # Commune
    if publish_diag.get("missing_commune"):
        com = None
        if llm_ok and (publish_fixes or {}).get("commune"):
            com = str(publish_fixes.get("commune"))
        elif address_validation and (geo_ok_strong or geo_ok_city) and address_validation.get("city"):
            com = str(address_validation.get("city"))
        if com and com.strip():
            to_update["commune"] = com.strip()

    # Gestionnaire
    if publish_diag.get("missing_gestionnaire") and llm_ok and (publish_fixes or {}).get("gestionnaire"):
        to_update["gestionnaire"] = str(publish_fixes.get("gestionnaire"))

    # Email: seulement si invalide et qu'on a un email valide trouvé
    if publish_diag.get("invalid_email") and llm_ok and (publish_fixes or {}).get("email"):
        to_update["email"] = str(publish_fixes.get("email"))

    # Géocodage: si geom manquant et api-adresse a une réponse suffisamment fiable
    if publish_diag.get("missing_geom") and address_validation and geo_score >= 0.60:
        try:
            to_update["geom"] = {"lat": float(address_validation["lat"]), "lon": float(address_validation["lon"])}
            to_update["geocode_precision"] = _map_api_adresse_type_to_precision(geo_type)
        except Exception:
            pass

    # Ne pas réécrire avec une valeur identique (utile en dry-run aussi)
    for k in list(to_update.keys()):
        if k in {"geom", "geocode_precision"}:
            continue
        oldv = etab.get(k)
        if oldv is not None and str(oldv).strip() == str(to_update.get(k)).strip():
            to_update.pop(k, None)

    return to_update


def _pick_diverse_by_sous_categories(etabs: List[Dict[str, Any]], *, limit: int) -> List[Dict[str, Any]]:
    """Sélectionne un sous-ensemble d'établissements qui maximise la diversité des sous-catégories.

    Heuristique (gloutonne): on privilégie les établissements qui ajoutent des sous-catégories
    non encore couvertes, en favorisant les sous-catégories rares.
    """

    if limit <= 0 or limit >= len(etabs):
        return list(etabs)

    cats_by_id: Dict[str, set[str]] = {}
    freq: Dict[str, int] = {}

    for e in etabs:
        eid = str(e.get("id") or "")
        cats = set([str(x) for x in (e.get("_sous_categories_hint") or []) if str(x).strip()])
        cats_by_id[eid] = cats
        for c in cats:
            freq[c] = freq.get(c, 0) + 1

    def rarity_score(cats: set[str]) -> float:
        score = 0.0
        for c in cats:
            f = float(freq.get(c) or 1)
            score += 1.0 / f
        return score

    selected: List[Dict[str, Any]] = []
    covered: set[str] = set()
    remaining = list(etabs)

    while remaining and len(selected) < limit:
        best_idx = 0
        best_gain = -1
        best_score = -1.0

        for idx, e in enumerate(remaining):
            cats = cats_by_id.get(str(e.get("id") or ""), set())
            new = cats - covered
            gain = len(new)
            score = rarity_score(new) if new else rarity_score(cats)
            if gain > best_gain or (gain == best_gain and score > best_score):
                best_gain = gain
                best_score = score
                best_idx = idx

        chosen = remaining.pop(best_idx)
        selected.append(chosen)
        covered |= cats_by_id.get(str(chosen.get("id") or ""), set())

    return selected


def compute_fourchette_prix_from_monthly(*, prix_min: Optional[float], prix_max: Optional[float], loyer_base: Optional[float], charges: Optional[float]) -> Optional[str]:
    """Calcule la `fourchette_prix` (enum DB) à partir d'un montant mensuel.

    Convention demandée:
    - `euro`      : < 750 € / mois
    - `deux_euros`: 750 à 1500 € / mois
    - `trois_euros`: > 1500 € / mois

    Règle de prudence:
    - si l'intervalle (min/max) traverse plusieurs bandes, on retourne None.
    """

    def band(x: float) -> str:
        if x < 750:
            return "euro"
        if x <= 1500:
            return "deux_euros"
        return "trois_euros"

    # Montant "référence" si on a loyer+charges (souvent plus fiable)
    ref: Optional[float] = None
    if isinstance(loyer_base, (int, float)) and isinstance(charges, (int, float)):
        ref = float(loyer_base) + float(charges)
    elif isinstance(prix_min, (int, float)):
        ref = float(prix_min)

    if isinstance(prix_min, (int, float)) and isinstance(prix_max, (int, float)) and prix_min > 0 and prix_max > 0:
        bmin = band(float(prix_min))
        bmax = band(float(prix_max))
        return bmin if bmin == bmax else None

    if ref is None or ref <= 0:
        return None
    return band(float(ref))


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _jsonb(val: Any) -> str:
    return json.dumps(val, ensure_ascii=False)


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
        organic = data.get("organic") or []
        return [x for x in organic if isinstance(x, dict)]
    except Exception:
        return []


def _strip_html_to_text(html: str) -> str:
    # ultra-simple: on retire scripts/styles et on compact.
    html = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.IGNORECASE)
    html = re.sub(r"<style[\s\S]*?</style>", " ", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def fetch_page_text(url: str, *, scrapingbee_api_key: str = "", timeout_s: int = 35) -> Tuple[int, str, str]:
    """Récupère une page en texte. Utilise ScrapingBee si configuré."""

    url = (url or "").strip()
    if not url:
        return 0, "", ""

    params = None
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        if scrapingbee_api_key.strip():
            params = {
                "api_key": scrapingbee_api_key.strip(),
                "url": url,
                "render_js": "false",
                "block_resources": "true",
                "timeout": "30000",
            }
            r = requests.get("https://app.scrapingbee.com/api/v1/", params=params, timeout=timeout_s)
            final_url = url
        else:
            r = requests.get(url, headers=headers, timeout=timeout_s, allow_redirects=True)
            final_url = str(getattr(r, "url", url))

        status = int(getattr(r, "status_code", 0) or 0)
        if status != 200:
            return status, final_url, ""

        ctype = (r.headers.get("Content-Type") or "").lower()
        # PDF: on ne tente pas l'extraction ici (on garde vide)
        if "pdf" in ctype or final_url.lower().endswith(".pdf"):
            return status, final_url, ""

        text = r.text or ""
        if "<html" in text.lower() or "</" in text:
            text = _strip_html_to_text(text)
        return status, final_url, text

    except Exception:
        return 0, url, ""


def gemini_generate_text(*, api_key: str, model: str, prompt: str, max_output_tokens: int = 800) -> str:
    """Appel Gemini (texte)."""

    key = (api_key or "").strip()
    if not key:
        return ""

    model_name = (model or "").strip() or "gemini-2.0-flash"
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={key}"

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.25, "maxOutputTokens": int(max_output_tokens)},
    }

    last_err = ""
    for attempt in range(3):
        try:
            resp = requests.post(endpoint, json=payload, timeout=60)
            if resp.status_code in {429, 500, 502, 503, 504}:
                time.sleep(1.2 * (attempt + 1))
                last_err = f"Gemini transient {resp.status_code}: {resp.text[:200]}"
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"Gemini error {resp.status_code}: {resp.text[:200]}")

            raw = resp.json() or {}
            text = (
                raw.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
                .strip()
            )
            return text
        except Exception as e:
            last_err = str(e)
            time.sleep(0.8 * (attempt + 1))

    return ""


def gemini_generate_json(*, api_key: str, model: str, prompt: str, max_output_tokens: int = 900) -> Dict[str, Any]:
    """Appel Gemini (JSON)."""

    text = gemini_generate_text(api_key=api_key, model=model, prompt=prompt, max_output_tokens=max_output_tokens)
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


def normalize_label(label: str) -> str:
    s = (label or "").strip().lower()
    s = re.sub(r"[\.,;:/()\[\]{}]", " ", s)
    s = s.replace("œ", "oe")
    # retirer accents (simple)
    accents = {
        "à": "a",
        "â": "a",
        "ä": "a",
        "ç": "c",
        "é": "e",
        "è": "e",
        "ê": "e",
        "ë": "e",
        "î": "i",
        "ï": "i",
        "ô": "o",
        "ö": "o",
        "ù": "u",
        "û": "u",
        "ü": "u",
    }
    for k, v in accents.items():
        s = s.replace(k, v)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def map_logement_synonyms(norm: str) -> str:
    s = norm
    s = s.replace("pieces", "pieces")
    # t1/f1/studio
    if re.search(r"\b(t1|f1|studio)\b", s):
        return "studio"
    if re.search(r"\b(t2|f2)\b", s):
        return "2 pieces"
    if re.search(r"\b(t3|f3)\b", s):
        return "3 pieces"
    if re.search(r"\b(t4|f4)\b", s):
        return "4 pieces"
    if "chambre" in s and "colocation" in s:
        return "chambre colocation"
    return s


def _safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def compare_surfaces(a: Optional[float], b: Optional[float], tol: float = 2.0) -> bool:
    if a is None or b is None:
        return True  # on ne pénalise pas si un côté est manquant
    return abs(float(a) - float(b)) <= tol


def _count_words(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text or ""))


def _count_chars(text: str) -> int:
    return len((text or "").strip())


def _is_ctx_sparse(ctx: Dict[str, Any]) -> bool:
    """Heuristique: si on n'a presque pas de contenu source exploitable, on autorise une description plus courte.

    On ne se base pas sur la taille du JSON sérialisé (trop bruitée), mais sur la longueur
    du texte extrait des pages.
    """

    pages: List[Dict[str, Any]] = []
    try:
        if isinstance((ctx or {}).get("pages"), list):
            pages = (ctx or {}).get("pages", [])
        elif isinstance((ctx or {}).get("context_general"), dict):
            pages = (ctx or {}).get("context_general", {}).get("pages", [])
    except Exception:
        pages = []

    if not pages:
        return True

    total_text = 0
    for p in pages:
        if not isinstance(p, dict):
            continue
        total_text += len((p.get("text") or "").strip())
        if total_text >= 1500:
            break

    return total_text < 900


def match_logements(
    base_rows: List[Dict[str, Any]], extracted_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Compare logements base vs extraction.

    Retourne un dict compact: match_status + appariements + divergences.
    """

    # Cas non comparable: aucune donnée en base.
    # On ne veut pas compter ça comme un "mismatch" mais comme un signal "base_empty".
    if not base_rows:
        extra_ext: List[Dict[str, Any]] = []
        for r in extracted_rows:
            lib = map_logement_synonyms(normalize_label(str(r.get("libelle") or "")))
            e = {**r, "_norm": lib}
            if not e.get("_norm") or e.get("_norm") in {"appartements", "logements", "hebergements"}:
                extra_ext.append({"status": "ambiguous", **e})
            else:
                extra_ext.append({"status": "mismatch", **e})

        status = "base_empty"
        if extra_ext and all(x.get("status") == "ambiguous" for x in extra_ext):
            status = "ambiguous"

        return {
            "match_status": status,
            "paired": [],
            "base_only": [],
            "extracted_only": extra_ext,
        }

    base_norm = []
    for r in base_rows:
        lib = map_logement_synonyms(normalize_label(str(r.get("libelle") or "")))
        base_norm.append({**r, "_norm": lib})

    ext_norm = []
    for r in extracted_rows:
        lib = map_logement_synonyms(normalize_label(str(r.get("libelle") or "")))
        ext_norm.append({**r, "_norm": lib})

    used_base: set[int] = set()
    pairs: List[Dict[str, Any]] = []
    extra_ext: List[Dict[str, Any]] = []

    for e in ext_norm:
        if not e.get("_norm") or e.get("_norm") in {"appartements", "logements", "hebergements"}:
            extra_ext.append({"status": "ambiguous", **e})
            continue

        j = None
        for idx, b in enumerate(base_norm):
            if idx in used_base:
                continue
            if b.get("_norm") == e.get("_norm"):
                j = idx
                break

        if j is None:
            extra_ext.append({"status": "mismatch", **e})
            continue

        used_base.add(j)
        b = base_norm[j]

        # exact vs partial
        exact = True
        fields_div: List[str] = []

        for k in ["surface_min", "surface_max"]:
            if not compare_surfaces(_safe_float(b.get(k)), _safe_float(e.get(k))):
                exact = False
                fields_div.append(k)

        for k in ["meuble", "pmr", "domotique", "plain_pied"]:
            bv = b.get(k)
            ev = e.get(k)
            if bv is not None and ev is not None and bool(bv) != bool(ev):
                exact = False
                fields_div.append(k)

        if b.get("nb_unites") is not None and e.get("nb_unites") is not None:
            if int(b.get("nb_unites")) != int(e.get("nb_unites")):
                exact = False
                fields_div.append("nb_unites")

        pairs.append(
            {
                "base_norm": b.get("_norm"),
                "extracted_norm": e.get("_norm"),
                "match": "exact" if exact else "partial",
                "divergent_fields": fields_div,
                "base": b,
                "extracted": e,
            }
        )

    base_left = [b for idx, b in enumerate(base_norm) if idx not in used_base]

    status = "exact"
    if any(p["match"] == "partial" for p in pairs) or base_left or extra_ext:
        status = "partial"
    if any(x.get("status") == "mismatch" for x in extra_ext):
        status = "mismatch"

    return {
        "match_status": status,
        "paired": pairs,
        "base_only": base_left,
        "extracted_only": extra_ext,
    }


def _norm_for_match(s: str) -> str:
    s = normalize_label(s or "").lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _avp_evidence_looks_specific(*, etab: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
    """Heuristique: considère l'évidence "spécifique" si titres/snippets/URLs mentionnent l'établissement ou la commune."""

    name = _norm_for_match(str(etab.get("nom") or ""))
    commune = _norm_for_match(str(etab.get("commune") or ""))
    if not name and not commune:
        return False

    haystacks: List[str] = []
    for o in (ctx.get("organic") or []):
        if isinstance(o, dict):
            haystacks.append(_norm_for_match(str(o.get("title") or "")))
            haystacks.append(_norm_for_match(str(o.get("snippet") or "")))
            haystacks.append(_norm_for_match(str(o.get("url") or "")))
    for p in (ctx.get("pages") or []):
        if isinstance(p, dict):
            haystacks.append(_norm_for_match(str(p.get("title") or "")))
            haystacks.append(_norm_for_match(str(p.get("snippet") or "")))
            haystacks.append(_norm_for_match(str(p.get("url") or "")))

    # Pour éviter les faux positifs sur un mot très courant, on demande 2+ tokens pour le nom.
    name_tokens = [t for t in name.split() if len(t) >= 3]
    name_phrase = " ".join(name_tokens[:6]).strip()

    for h in haystacks:
        if commune and commune in h:
            return True
        if name_phrase and len(name_tokens) >= 2 and name_phrase in h:
            return True
        if name_tokens and sum(1 for t in name_tokens if t in h) >= 2:
            return True
    return False


def build_context(*, etab: Dict[str, Any], organic: List[Dict[str, Any]], scrapingbee_key: str, max_pages: int = 3) -> Dict[str, Any]:
    """Construit un contexte texte (snippets + pages) pour LLM."""

    pages: List[Dict[str, Any]] = []
    for item in organic[: max_pages]:
        url = (item.get("link") or "").strip()
        if not url:
            continue
        status, final_url, text = fetch_page_text(url, scrapingbee_api_key=scrapingbee_key)
        pages.append(
            {
                "url": final_url or url,
                "status": status,
                "title": (item.get("title") or "").strip(),
                "snippet": (item.get("snippet") or "").strip(),
                "text": (text or "")[:8000],
                "is_pdf": bool((final_url or url).lower().endswith(".pdf")),
            }
        )

    # Texte compact "lisible" (SERP + extraits pages), réutilisable pour des extractions ciblées.
    parts: List[str] = []
    for o in organic[:10]:
        if not isinstance(o, dict):
            continue
        title = (o.get("title") or "").strip()
        snippet = (o.get("snippet") or "").strip()
        link = (o.get("link") or "").strip()
        if title or snippet:
            parts.append(f"[SERP] {title}\n{snippet}\n{link}".strip())
    for p in pages:
        if not isinstance(p, dict):
            continue
        title = (p.get("title") or "").strip()
        snippet = (p.get("snippet") or "").strip()
        url = (p.get("url") or "").strip()
        text = (p.get("text") or "").strip()
        chunk = "\n".join([x for x in [f"[PAGE] {title}", snippet, url, text] if x]).strip()
        if chunk:
            parts.append(chunk)
    combined_text = "\n\n---\n\n".join(parts).strip()

    return {
        "query_context": {
            "nom": etab.get("nom") or "",
            "commune": etab.get("commune") or "",
            "departement": etab.get("departement") or "",
        },
        "combined_text": combined_text,
        "organic": [
            {
                "url": (o.get("link") or "").strip(),
                "title": (o.get("title") or "").strip(),
                "snippet": (o.get("snippet") or "").strip(),
            }
            for o in organic[:10]
        ],
        "pages": pages,
    }


def extract_tarifications(*, etab: Dict[str, Any], ctx: Dict[str, Any], gemini_key: str, gemini_model: str) -> Dict[str, Any]:
    prompt = f"""
Tu extrais des informations de tarification (loyer/prix) pour un établissement d'habitat seniors.

Établissement: {etab.get('nom','')} — {etab.get('commune','')} ({etab.get('departement','')})

Sources (snippets/pages):
{json.dumps(ctx, ensure_ascii=False)[:18000]}

Consignes:
- Ne JAMAIS inventer. Si aucune information fiable, renvoie des champs null.
- Les montants sont en euros (€). On cherche idéalement un coût **mensuel**.
- Vigilance "services": l'objectif est d'estimer le coût **obligatoire** (loyer + charges), et d'**exclure la restauration** si elle est optionnelle et séparée.
    - Si la restauration est explicitement obligatoire et incluse, tu peux l'inclure.
    - Si tu ne sais pas, reste prudent et indique `pricing_scope="inconnu"`.
- Fourchette attendue (par mois):
    - `euro` si < 750
    - `deux_euros` si 750 à 1500
    - `trois_euros` si > 1500

Réponds UNIQUEMENT en JSON:
{{
  "prix_min": <number|null>,
  "prix_max": <number|null>,
  "loyer_base": <number|null>,
  "charges": <number|null>,
  "periode": "mois"|"semaine"|"jour"|"inconnue"|null,
  "fourchette_prix": "euro"|"deux_euros"|"trois_euros"|null,
    "pricing_scope": "hors_restauration"|"inclut_restauration"|"inconnu"|null,
  "evidence_urls": ["..."],
  "confidence": <0-1>
}}
""".strip()

    out = gemini_generate_json(api_key=gemini_key, model=gemini_model, prompt=prompt, max_output_tokens=700)
    if not out:
        return {}

    # normalisation légère
    for k in ["prix_min", "prix_max", "loyer_base", "charges", "confidence"]:
        if k in out:
            out[k] = _safe_float(out.get(k))

    fp = out.get("fourchette_prix")
    if fp not in {None, "euro", "deux_euros", "trois_euros"}:
        out["fourchette_prix"] = None

    ps = out.get("pricing_scope")
    if ps not in {None, "hors_restauration", "inclut_restauration", "inconnu"}:
        out["pricing_scope"] = None

    # Si Gemini n'a pas donné la fourchette, on la dérive (prudent).
    if out.get("fourchette_prix") is None:
        out["fourchette_prix"] = compute_fourchette_prix_from_monthly(
            prix_min=out.get("prix_min"),
            prix_max=out.get("prix_max"),
            loyer_base=out.get("loyer_base"),
            charges=out.get("charges"),
        )

    return out


def extract_services(*, etab: Dict[str, Any], ctx: Dict[str, Any], gemini_key: str, gemini_model: str) -> Dict[str, Any]:
    prompt = f"""
Tu dois déterminer les services présents pour un établissement, à partir des sources.

Établissement: {etab.get('nom','')} — {etab.get('commune','')} ({etab.get('departement','')})

Liste des services possibles (choisir uniquement parmi ces clés):
{SERVICE_KEYS}

Sources:
{json.dumps(ctx, ensure_ascii=False)[:18000]}

Règles:
- Ne choisis un service que s'il est explicitement mentionné ou très fortement implicite.
- Si tu n'as pas assez d'info, renvoie une liste vide.

Réponds UNIQUEMENT en JSON:
{{
  "services": ["<service_key>", "..."],
  "evidence_urls": ["..."],
  "confidence": <0-1>
}}
""".strip()

    out = gemini_generate_json(api_key=gemini_key, model=gemini_model, prompt=prompt, max_output_tokens=500)
    if not out:
        return {}

    sv = out.get("services")
    if not isinstance(sv, list):
        sv = []
    sv = [str(x).strip() for x in sv if str(x).strip() in set(SERVICE_KEYS)]
    out["services"] = sorted(set(sv))
    out["confidence"] = _safe_float(out.get("confidence"))

    return out


def extract_logements(*, etab: Dict[str, Any], ctx: Dict[str, Any], gemini_key: str, gemini_model: str) -> List[Dict[str, Any]]:
    prompt = f"""
Tu extrais les types de logements proposés par l'établissement (si disponible).

Établissement: {etab.get('nom','')} — {etab.get('commune','')} ({etab.get('departement','')})

Sources:
{json.dumps(ctx, ensure_ascii=False)[:18000]}

Réponds UNIQUEMENT en JSON:
{{
  "logements_types": [
    {{
      "libelle": "...",
      "surface_min": <number|null>,
      "surface_max": <number|null>,
      "meuble": <true|false|null>,
      "pmr": <true|false|null>,
      "domotique": <true|false|null>,
      "plain_pied": <true|false|null>,
      "nb_unites": <integer|null>
    }}
  ],
  "evidence_urls": ["..."],
  "confidence": <0-1>
}}
""".strip()

    out = gemini_generate_json(api_key=gemini_key, model=gemini_model, prompt=prompt, max_output_tokens=900)
    rows = out.get("logements_types") if isinstance(out, dict) else None
    if not isinstance(rows, list):
        return []

    cleaned: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        lib = (r.get("libelle") or "").strip()
        if not lib:
            continue
        cleaned.append(
            {
                "libelle": lib,
                "surface_min": _safe_float(r.get("surface_min")),
                "surface_max": _safe_float(r.get("surface_max")),
                "meuble": None if r.get("meuble") is None else bool(r.get("meuble")),
                "pmr": None if r.get("pmr") is None else bool(r.get("pmr")),
                "domotique": None if r.get("domotique") is None else bool(r.get("domotique")),
                "plain_pied": None if r.get("plain_pied") is None else bool(r.get("plain_pied")),
                "nb_unites": None if r.get("nb_unites") is None else int(r.get("nb_unites")),
            }
        )

    return cleaned


def _normalize_for_checks(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _description_fails_guardrails(*, desc: str, etab: Dict[str, Any], sous_categories: Optional[List[str]] = None) -> Optional[str]:
    """Retourne une raison (string) si la description doit être rejetée."""

    d = (desc or "").strip()
    if not d:
        return "empty"

    nd = _normalize_for_checks(d)

    # 1) Pas de voix "gestionnaire" / 1ère personne
    if re.search(r"\b(notre|nos|nous|bienvenue|découvrez|rejoignez[- ]nous)\b", nd, flags=re.IGNORECASE):
        return "first_person_or_welcome"

    # 2) Rejeter les textes type annuaire/CTA (parasites)
    bad_fragments = [
        "capacité : 0",
        "0 appartement",
        "0 disponibles",
        "service gratuit",
        "planifier une visite",
        "déposer un dossier",
        "infos : avis",
        "description complète",
    ]
    if any(x in nd for x in bad_fragments):
        return "listing_or_cta_garbage"
    if any(sym in d for sym in ["⚡", "✅", "⭐", "→"]):
        return "emoji_or_ui_symbols"

    # 3) Éviter les classifications fausses (béguinage)
    sc = " ".join([(x or "") for x in (sous_categories or [])]).lower()
    name = (etab.get("nom") or "").lower()
    is_beguinage = ("béguin" in name) or ("beguin" in name) or ("béguin" in sc) or ("beguin" in sc)
    if is_beguinage and ("résidence autonomie" in nd or "residence autonomie" in nd):
        return "beguinage_misclassified"

    # 4) Pas de nombres invraisemblables / placeholders
    if re.search(r"\b(capacit[ée]\s*de\s*0|\b0\s*(places|logements|appartements))\b", nd):
        return "zero_capacity"

    return None


def generate_description(
    *,
    etab: Dict[str, Any],
    ctx: Dict[str, Any],
    gemini_key: str,
    gemini_model: str,
    sous_categories: Optional[List[str]] = None,
) -> str:
    # Cible: 1 paragraphe court, non commercial.
    target_min_chars = 400
    target_max_chars = 700

    # Si on manque d'infos exploitables dans les sources, on préfère une description plus courte
    # plutôt que d'ajouter du texte générique.
    sparse = _is_ctx_sparse(ctx)
    short_min_chars = 220
    short_max_chars = 450

    draft = ""
    ctx_blob = json.dumps(ctx, ensure_ascii=False)[:18000]

    sc_hint = ", ".join([x for x in (sous_categories or []) if (x or "").strip()])
    habitat_type = etab.get("habitat_type")

    for attempt in range(1, 5):
        if not draft:
            prompt = f"""
Rédige une description en français pour un établissement d'habitat seniors.

Objectif longueur:
- entre {target_min_chars} et {target_max_chars} caractères (espaces compris)
- MAIS si les sources ne contiennent pas assez d'informations spécifiques, fais plus court (≈ {short_min_chars}–{short_max_chars} caractères) plutôt que de généraliser.

Ton: bienveillant, institutionnel neutre, factuel (pas commercial).
Format: un SEUL paragraphe (4–5 lignes environ), phrases plutôt courtes, pas de liste.

Contraintes:
- N'invente rien. Si une info n'est pas présente dans les sources, ne l'affirme pas.
- Pas de superlatifs/comparatifs ("le meilleur", etc.).
- Ne pas inventer de tarifs.
- Évite les formulations vagues et génériques (ex: "cadre de vie exceptionnel") si les sources ne le justifient pas.
- Ne parle PAS à la 1ère personne (interdit: "notre", "nos", "nous", "bienvenue", "rejoignez-nous").
- Ne recopie pas des blocs type annuaire/CTA (ex: "Planifier une visite", "Déposer un dossier", "Service gratuit", "Infos: avis").
- N'affirme pas de capacité/chiffres si ce n'est pas explicitement mentionné dans les sources.
- Si l'établissement est un "béguinage", ne le décris pas comme une "résidence autonomie".

Établissement:
- Nom: {etab.get('nom','')}
- Commune: {etab.get('commune','')}
- Département: {etab.get('departement','')}
- Gestionnaire: {etab.get('gestionnaire','')}
- Site web (si connu): {etab.get('site_web','')}
- Habitat_type (base): {habitat_type}
- Sous-catégories (base): {sc_hint}

Sources:
{ctx_blob}

Sortie attendue:
- 1ère ligne: "LEN: <nombre>" (nombre de caractères, approximation OK)
- puis la description finale.
""".strip()
        else:
            cc_prev = _count_chars(draft)
            within_main = target_min_chars <= cc_prev <= target_max_chars
            within_short = short_min_chars <= cc_prev <= short_max_chars
            if within_main or (sparse and within_short):
                return draft

            if cc_prev < target_min_chars:
                action = "allonger légèrement en ajoutant uniquement des éléments factuels présents dans les sources"
            else:
                action = "raccourcir"

            prompt = f"""
Tu dois {action} la description ci-dessous.

Règles:
- Un seul paragraphe. Pas de listes.
- Ne rajoute AUCUN fait non présent dans les sources.
- Si tu manques d'informations spécifiques, raccourcis plutôt que d'ajouter du texte générique.

Objectif longueur:
- idéalement {target_min_chars}–{target_max_chars} caractères,
- ou {short_min_chars}–{short_max_chars} si vraiment peu d'informations.

Sources:
{ctx_blob}

Description actuelle:
{draft}

Sortie attendue:
- 1ère ligne: "LEN: <nombre>"
- puis la description finale.
""".strip()

        text = gemini_generate_text(api_key=gemini_key, model=gemini_model, prompt=prompt, max_output_tokens=600)
        text = (text or "").strip()
        text = re.sub(r"^\s*\"|\"\s*$", "", text).strip()
        # Retire le préfixe LEN si présent
        text = re.sub(r"^\s*LEN\s*[:=]\s*\d+\s*\r?\n+", "", text, flags=re.IGNORECASE).strip()

        draft = text

        # Garde-fous: si ça ressemble à du "parasite" ou à une classification erronée, on préfère ne pas réécrire.
        reason = _description_fails_guardrails(desc=draft, etab=etab, sous_categories=sous_categories)
        if reason:
            return ""

        cc = _count_chars(draft)
        if target_min_chars <= cc <= target_max_chars:
            return draft
        if sparse and short_min_chars <= cc <= short_max_chars:
            return draft

    return draft


def _is_hi_hig(sous_categories: List[str]) -> bool:
    s = " ".join([x.lower() for x in (sous_categories or [])])
    return ("inclusif" in s) or ("interg" in s) or ("intergén" in s)


def extract_avp_public(*, etab: Dict[str, Any], ctx: Dict[str, Any], gemini_key: str, gemini_model: str) -> Dict[str, Any]:
    """Extraction AVP/public_cible (strict) pour HI/HIG."""

    prompt = f"""
Tu dois déterminer si l'établissement est lié à l'AVP (Aide à la Vie Partagée) et quel public est accueilli.

Établissement: {etab.get('nom','')} — {etab.get('commune','')} ({etab.get('departement','')})

Sources:
{json.dumps(ctx, ensure_ascii=False)[:18000]}

Règles:
- "avp_eligible" uniquement si les sources mentionnent explicitement AVP / "aide à la vie partagée" (ou équivalent) OU un document institutionnel (CD/ARS) lié au projet.
- Sinon: "a_verifier".
- Ne jamais inventer des dates.

Réponds UNIQUEMENT en JSON:
{{
  "eligibilite_statut": "avp_eligible"|"a_verifier"|"non_eligible"|null,
  "public_cible_suggere": ["personnes_agees"|"personnes_handicapees"|"mixtes"|"alzheimer_accessible"],
  "avp_infos": {{
    "statut": "intention"|"en_projet"|"ouvert"|null,
    "date_intention": "YYYY-MM-DD"|null,
    "date_en_projet": "YYYY-MM-DD"|null,
    "date_ouverture": "YYYY-MM-DD"|null,
    "public_accueilli": "..."|null
  }},
  "evidence_urls": ["..."],
  "confidence": <0-1>
}}
""".strip()

    out = gemini_generate_json(api_key=gemini_key, model=gemini_model, prompt=prompt, max_output_tokens=800)
    if not out:
        return {}

    def _normalize_public_cible_list(pcs_in: Any) -> List[str]:
        if not isinstance(pcs_in, list):
            pcs_list: List[str] = []
        else:
            pcs_list = [str(x).strip() for x in pcs_in if str(x).strip()]

        allowed = {"personnes_agees", "personnes_handicapees", "mixtes", "alzheimer_accessible"}
        pcs_list = [p for p in pcs_list if p in allowed]

        # Règle métier (user):
        # - PA+PH => on stocke les deux (pas "mixtes").
        has_pa = "personnes_agees" in pcs_list
        has_ph = "personnes_handicapees" in pcs_list
        if (has_pa or has_ph) and "mixtes" in pcs_list:
            pcs_list = [p for p in pcs_list if p != "mixtes"]

        # "mixtes" = autres publics (jeunes/étudiants/publics en difficulté/intergénérationnel),
        # uniquement si c'est explicitement dans les sources.
        if "mixtes" in pcs_list and not (has_pa or has_ph):
            hay = " ".join(
                [
                    str(((ctx or {}).get("combined_text") or "")),
                    str(json.dumps((ctx or {}).get("pages") or [], ensure_ascii=False)),
                    str(json.dumps((ctx or {}).get("organic") or [], ensure_ascii=False)),
                    str(((out.get("avp_infos") or {}).get("public_accueilli") or "")),
                ]
            ).lower()
            other_public_markers = [
                "jeune",
                "jeunes",
                "étudiant",
                "etudiant",
                "étudiants",
                "etudiants",
                "insertion",
                "précar",
                "precar",
                "publics en difficulté",
                "public en difficulté",
                "intergénération",
                "intergeneration",
                "familles",
                "parents",
                "adultes",
                "travailleurs",
            ]
            if not any(m in hay for m in other_public_markers):
                pcs_list = [p for p in pcs_list if p != "mixtes"]

        # Alzheimer: seulement si mention explicite "alzheimer" (user)
        if "alzheimer_accessible" in pcs_list:
            hay = " ".join(
                [
                    str(((ctx or {}).get("combined_text") or "")),
                    str(((out.get("avp_infos") or {}).get("public_accueilli") or "")),
                ]
            ).lower()
            if not re.search(r"\balzheimer\b", hay):
                pcs_list = [p for p in pcs_list if p != "alzheimer_accessible"]

        # Ordre stable (lisible)
        order = {"personnes_agees": 1, "personnes_handicapees": 2, "mixtes": 3, "alzheimer_accessible": 4}
        pcs_list = sorted(set(pcs_list), key=lambda x: order.get(x, 99))
        return pcs_list

    out["public_cible_suggere"] = _normalize_public_cible_list(out.get("public_cible_suggere"))

    st = out.get("eligibilite_statut")
    if st not in {None, "avp_eligible", "a_verifier", "non_eligible"}:
        out["eligibilite_statut"] = None

    out["confidence"] = _safe_float(out.get("confidence"))

    return out


def load_etablissements(
    db: DatabaseManager,
    departement: str,
    limit: Optional[int],
    *,
    sample_diverse: bool = False,
    sample_pool: int = 500,
    use_random_order: bool = False,
) -> List[Dict[str, Any]]:
    """
    Charge les établissements à enrichir.
    
    Si use_random_order=True, utilise ORDER BY RANDOM() pour distribuer
    les établissements de façon aléatoire entre les jobs parallèles.
    """
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            dep = (departement or "").strip()
            dep_like = f"%({dep})%" if dep else "%"
            sql = """
            SELECT
                e.id::text,
                COALESCE(e.nom,''),
                COALESCE(e.commune,''),
                COALESCE(e.code_postal,''),
                COALESCE(e.adresse_l1,''),
                COALESCE(e.adresse_l2,''),
                COALESCE(e.site_web,''),
                COALESCE(e.gestionnaire,''),
                COALESCE(e.presentation,''),
                COALESCE(e.public_cible,''),
                COALESCE(e.eligibilite_statut::text,''),
                COALESCE(e.habitat_type::text,''),
                COALESCE(e.statut_editorial::text,''),
                COALESCE(e.departement,''),
                COALESCE(array_agg(sc.libelle) FILTER (WHERE sc.libelle IS NOT NULL), ARRAY[]::text[]) AS sous_categories_hint
            FROM etablissements e
            LEFT JOIN etablissement_sous_categorie esc ON esc.etablissement_id = e.id
            LEFT JOIN sous_categories sc ON sc.id = esc.sous_categorie_id
            WHERE e.is_test=false AND (
                e.departement = %s
                OR e.departement ILIKE %s
            )
            GROUP BY e.id, e.nom, e.commune, e.code_postal, e.adresse_l1, e.adresse_l2,
                     e.site_web, e.gestionnaire, e.presentation, e.public_cible,
                     e.eligibilite_statut, e.habitat_type, e.statut_editorial, e.departement
            """
            
            # Ordre aléatoire pour distribuer entre jobs parallèles
            if use_random_order:
                sql += " ORDER BY RANDOM()"
            else:
                sql += " ORDER BY e.nom"
            
            params: List[Any] = [dep, dep_like]
            # Si on échantillonne "diverse", on récupère un pool plus grand puis on choisit côté Python.
            if sample_diverse and limit is not None:
                pool = max(int(limit) * 10, int(sample_pool))
                sql += " LIMIT %s"
                params.append(int(pool))
            elif limit is not None:
                sql += " LIMIT %s"
                params.append(int(limit))
            
            cur.execute(sql, params)
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "nom": r[1],
                "commune": r[2],
                "code_postal": r[3],
                "adresse_l1": r[4],
                "adresse_l2": r[5],
                "site_web": r[6],
                "gestionnaire": r[7],
                "presentation": r[8],
                "public_cible": r[9],
                "eligibilite_statut": r[10],
                "habitat_type": r[11],
                "statut_editorial": r[12],
                "departement": r[13],
                "_sous_categories_hint": list(r[14] or []),
            }
        )

    if sample_diverse and limit is not None:
        out = _pick_diverse_by_sous_categories(out, limit=int(limit))

    return out


def load_sous_categories(cur, etab_id: str) -> List[str]:
    cur.execute(
        """
        SELECT sc.libelle
        FROM etablissement_sous_categorie esc
        JOIN sous_categories sc ON sc.id = esc.sous_categorie_id
        WHERE esc.etablissement_id = %s
        ORDER BY sc.libelle;
        """,
        (etab_id,),
    )
    return [str(r[0]) for r in cur.fetchall() if r and r[0]]


def load_services(cur, etab_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT s.id::text, COALESCE(s.libelle,'')
        FROM etablissement_service es
        JOIN services s ON s.id = es.service_id
        WHERE es.etablissement_id = %s
        ORDER BY s.libelle;
        """,
        (etab_id,),
    )
    return [{"id": r[0], "libelle": r[1]} for r in cur.fetchall()]


def load_tarifications(cur, etab_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id::text, fourchette_prix::text, prix_min, prix_max, loyer_base, charges,
               COALESCE(periode,''), COALESCE(source,''), date_observation
        FROM tarifications
        WHERE etablissement_id = %s
        ORDER BY date_observation DESC NULLS LAST, id DESC;
        """,
        (etab_id,),
    )
    out: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        out.append(
            {
                "id": r[0],
                "fourchette_prix": r[1] if r[1] else None,
                "prix_min": _safe_float(r[2]),
                "prix_max": _safe_float(r[3]),
                "loyer_base": _safe_float(r[4]),
                "charges": _safe_float(r[5]),
                "periode": r[6] or None,
                "source": r[7] or None,
                "date_observation": str(r[8]) if r[8] else None,
            }
        )
    return out


def load_logements(cur, etab_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id::text, COALESCE(libelle,''), surface_min, surface_max,
               meuble, pmr, domotique, plain_pied, nb_unites
        FROM logements_types
        WHERE etablissement_id = %s
        ORDER BY libelle;
        """,
        (etab_id,),
    )
    out: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        out.append(
            {
                "id": r[0],
                "libelle": r[1],
                "surface_min": _safe_float(r[2]),
                "surface_max": _safe_float(r[3]),
                "meuble": None if r[4] is None else bool(r[4]),
                "pmr": None if r[5] is None else bool(r[5]),
                "domotique": None if r[6] is None else bool(r[6]),
                "plain_pied": None if r[7] is None else bool(r[7]),
                "nb_unites": None if r[8] is None else int(r[8]),
            }
        )
    return out


def create_proposition(
    *,
    cur,
    etablissement_id: str,
    type_cible: str,
    action: str,
    source: str,
    payload: Dict[str, Any],
    review_note: str,
    cible_id: Optional[str] = None,
) -> str:
    cur.execute(
        """
        INSERT INTO propositions (etablissement_id, cible_id, type_cible, action, statut, source, payload, review_note)
        VALUES (%s, %s, %s, %s, 'en_attente', %s, %s::jsonb, %s)
        RETURNING id;
        """,
        (etablissement_id, cible_id, type_cible, action, source, _jsonb(payload), review_note),
    )
    return str(cur.fetchone()[0])


def add_item(*, cur, proposition_id: str, table_name: str, column_name: str, old_value: Any, new_value: Any) -> None:
    cur.execute(
        """
        INSERT INTO proposition_items (proposition_id, table_name, column_name, old_value, new_value)
        VALUES (%s, %s, %s, %s::jsonb, %s::jsonb);
        """,
        (proposition_id, table_name, column_name, _jsonb(old_value), _jsonb(new_value)),
    )


def main(argv: Optional[Iterable[str]] = None) -> int:
    import sys
    
    # Force immediate output flush
    def log(msg):
        print(msg, flush=True)
        sys.stdout.flush()
        sys.stderr.write(msg + "\n")
        sys.stderr.flush()
    
    p = argparse.ArgumentParser()
    p.add_argument("--departements", default="45,76", help="Liste de départements (codes), ex: 45,76")
    p.add_argument("--limit", type=int, default=10, help="Limite par département (test) ; 0 = pas de limite")
    p.add_argument(
        "--sample-diverse-sous-categories",
        action="store_true",
        help="Si --limit>0, tente de maximiser la diversité des sous-catégories dans l'échantillon.",
    )
    p.add_argument(
        "--sample-pool",
        type=int,
        default=500,
        help="Taille du pool candidat avant sélection diverse (par département).",
    )
    p.add_argument("--dry-run", action="store_true", help="N'écrit rien en base (mode lecture seule)")
    p.add_argument("--random-order", action="store_true", help="Utilise ORDER BY RANDOM() pour distribuer entre jobs parallèles")
    p.add_argument("--out-dir", default="outputs", help="Dossier de sortie")
    p.add_argument("--gemini-model", default=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"))
    p.add_argument("--min-desc-score", type=float, default=60.0)
    p.add_argument("--sleep", type=float, default=0.0)
    args = p.parse_args(list(argv) if argv is not None else None)

    # Comportement par défaut: écrire les propositions (sauf si --dry-run)
    args.write_propositions = not args.dry_run

    serper_key = os.getenv("SERPER_API_KEY", "").strip()
    scrapingbee_key = os.getenv("SCRAPINGBEE_API_KEY", "").strip()
    gemini_key = (os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_MAPS_API_KEY", "").strip())

    print("="*80)
    print("[START] Script enrich_dept_prototype.py demarrage")
    print("="*80)
    
    # Debug: afficher si les clés API sont configurées
    print(f"[API] GEMINI_API_KEY: {'SET (len=' + str(len(gemini_key)) + ')' if gemini_key else 'NOT SET'}")
    print(f"[API] SERPER_API_KEY: {'SET (len=' + str(len(serper_key)) + ')' if serper_key else 'NOT SET'}")
    print(f"[API] SCRAPINGBEE_API_KEY: {'SET (len=' + str(len(scrapingbee_key)) + ')' if scrapingbee_key else 'NOT SET'}")
    
    deps = [d.strip() for d in str(args.departements).split(",") if d.strip()]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[CONFIG] Departements: {deps}")
    print(f"[CONFIG] Limit: {args.limit}")
    print(f"[CONFIG] Random order: {args.random_order}")
    print(f"[CONFIG] Dry run: {args.dry_run}")
    print(f"[CONFIG] Write propositions: {args.write_propositions}")
    print(f"[CONFIG] DB_HOST: {os.getenv('DB_HOST', 'NOT SET')}")
    print(f"[CONFIG] Out dir: {out_dir}")

    tag = _now_tag()
    jsonl_path = out_dir / f"enrich_proto_{tag}.jsonl"
    logements_csv = out_dir / f"enrich_proto_logements_compare_{tag}.csv"
    props_list_path = out_dir / f"enrich_proto_propositions_{tag}.txt"

    print("[DB] Initialisation DatabaseManager...")
    db = DatabaseManager()
    print("[DB] DatabaseManager cree avec succes")
    
    # Test de connexion DB
    print("[DB] Test de connexion...")
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                print(f"[DB] Connexion OK - Test query result: {result}")
    except Exception as e:
        print(f"[DB] ERREUR DE CONNEXION: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # Préparer CSV logement compare (utf-8 BOM pour Excel)
    with open(logements_csv, "w", newline="", encoding="utf-8-sig") as fcsv:
        w = csv.DictWriter(
            fcsv,
            fieldnames=[
                "departement",
                "etablissement_id",
                "nom",
                "commune",
                "match_status",
                "n_base",
                "n_extracted",
                "n_pairs",
                "n_mismatch",
                "n_ambiguous",
            ],
        )
        w.writeheader()

        processed = 0
        created_props = 0
        created_prop_ids: List[str] = []

        print(f"[OUTPUT] Fichier JSONL: {jsonl_path}")
        with open(jsonl_path, "w", encoding="utf-8") as fout:
            for dep in deps:
                print(f"[DEPT] Traitement departement: {dep}")
                limit = None if int(args.limit) == 0 else int(args.limit)
                print(f"[DEPT] Chargement etablissements (limit={limit})...")
                etabs = load_etablissements(
                    db,
                    dep,
                    limit,
                    sample_diverse=bool(args.sample_diverse_sous_categories),
                    sample_pool=int(args.sample_pool),
                    use_random_order=bool(args.random_order),
                )
                print(f"[DEPT] Departement {dep}: {len(etabs)} etablissements charges (limit={args.limit})")

                print(f"[DEPT] Debut traitement des {len(etabs)} etablissements...")
                with db.get_connection() as conn:
                    with conn.cursor() as cur:
                        for idx, etab in enumerate(etabs, 1):
                            processed += 1
                            print(f"[ETAB {idx}/{len(etabs)}] Traitement etablissement...")

                            etab_id = etab["id"]
                            print(f"[ETAB {idx}/{len(etabs)}] ID: {etab_id}, Nom: {etab.get('nom', 'N/A')[:50]}")
                            sous_cats = load_sous_categories(cur, etab_id)
                            services = load_services(cur, etab_id)
                            tarifs = load_tarifications(cur, etab_id)
                            logements_base = load_logements(cur, etab_id)

                            # Score description existante
                            score_info = {"total": None, "needs_rewrite": None}
                            if enrich_quality_scorer is not None:
                                try:
                                    score_info = enrich_quality_scorer.score_description_quality(
                                        etab.get("presentation") or "",
                                        etab,
                                        gemini_api_key=gemini_key,
                                        gemini_model=str(args.gemini_model),
                                    )
                                except Exception:
                                    score_info = {"total": None, "needs_rewrite": None}

                            # Recherche web "générale" (on réutilise pour description/logements/services/tarifs)
                            query_base = f"\"{etab.get('nom','')}\" {etab.get('commune','')}"
                            organic = serper_search(query_base, num=8, api_key=serper_key)
                            ctx = build_context(etab=etab, organic=organic, scrapingbee_key=scrapingbee_key, max_pages=3)

                            # Pré-check publication (can_publish)
                            publish_diag = get_can_publish_diagnosis(cur=cur, etablissement_id=etab_id)
                            publish_fixes: Dict[str, Any] = {}
                            address_validation: Optional[Dict[str, Any]] = None

                            if gemini_key and publish_diag and publish_diag.get("can_publish") is False:
                                # Mutualisation: on réutilise le même ctx pour extraire les champs manquants
                                publish_fixes = extract_publish_fields(
                                    etab=etab,
                                    ctx=ctx,
                                    gemini_key=gemini_key,
                                    gemini_model=str(args.gemini_model),
                                )

                                # Validation + géocodage via api-adresse si on a une adresse candidate
                                addr_q_parts = []
                                if publish_fixes.get("adresse_l1"):
                                    addr_q_parts.append(str(publish_fixes.get("adresse_l1")))
                                if publish_fixes.get("code_postal"):
                                    addr_q_parts.append(str(publish_fixes.get("code_postal")))
                                if publish_fixes.get("commune"):
                                    addr_q_parts.append(str(publish_fixes.get("commune")))
                                if not addr_q_parts:
                                    # fallback: base commune + nom
                                    if etab.get("nom"):
                                        addr_q_parts.append(str(etab.get("nom")))
                                    if etab.get("commune"):
                                        addr_q_parts.append(str(etab.get("commune")))

                                address_validation = api_adresse_geocode(q=" ".join(addr_q_parts))

                            # Tarifications (tentative systématique)
                            tarifs_new = {}
                            if gemini_key:
                                tarifs_new = extract_tarifications(etab=etab, ctx=ctx, gemini_key=gemini_key, gemini_model=str(args.gemini_model))
                                print(f"[EXTRACT] Tarifs: confidence={tarifs_new.get('confidence', 'N/A')}", flush=True)

                            # Services (seulement si 0 ou 1 service)
                            services_new = {}
                            if len(services) <= 1 and gemini_key:
                                services_new = extract_services(etab=etab, ctx=ctx, gemini_key=gemini_key, gemini_model=str(args.gemini_model))
                                print(f"[EXTRACT] Services: {len(services_new.get('services', []))} found, confidence={services_new.get('confidence', 'N/A')}", flush=True)

                            # Logements (systématique en test)
                            logements_new: List[Dict[str, Any]] = []
                            if gemini_key:
                                logements_new = extract_logements(etab=etab, ctx=ctx, gemini_key=gemini_key, gemini_model=str(args.gemini_model))
                                print(f"[EXTRACT] Logements: {len(logements_new)} found", flush=True)

                            comp = match_logements(logements_base, logements_new)

                            w.writerow(
                                {
                                    "departement": dep,
                                    "etablissement_id": etab_id,
                                    "nom": etab.get("nom") or "",
                                    "commune": etab.get("commune") or "",
                                    "match_status": comp.get("match_status"),
                                    "n_base": len(logements_base),
                                    "n_extracted": len(logements_new),
                                    "n_pairs": len(comp.get("paired") or []),
                                    "n_mismatch": sum(1 for x in (comp.get("extracted_only") or []) if x.get("status") == "mismatch"),
                                    "n_ambiguous": sum(1 for x in (comp.get("extracted_only") or []) if x.get("status") == "ambiguous"),
                                }
                            )
                            fcsv.flush()

                            # Description (si score insuffisant)
                            new_desc = ""
                            needs_rewrite = bool(score_info.get("needs_rewrite"))
                            if gemini_key and (needs_rewrite or (score_info.get("total") is not None and float(score_info.get("total")) < float(args.min_desc_score))):
                                new_desc = generate_description(
                                    etab=etab,
                                    ctx=ctx,
                                    gemini_key=gemini_key,
                                    gemini_model=str(args.gemini_model),
                                    sous_categories=sous_cats,
                                )

                            new_desc_clean = (new_desc or "").strip()
                            existing_desc_clean = (etab.get("presentation") or "").strip()
                            desc_text = new_desc_clean or existing_desc_clean

                            # AVP/public cible (HI/HIG)
                            avp_out = {}
                            if gemini_key and _is_hi_hig(sous_cats):
                                query_avp = f"\"{etab.get('nom','')}\" {etab.get('commune','')} (\"aide à la vie partagée\" OR AVP OR \"habitat inclusif\" OR PVSP) filetype:pdf"
                                organic2 = serper_search(query_avp, num=8, api_key=serper_key)
                                ctx2 = build_context(etab=etab, organic=organic2, scrapingbee_key=scrapingbee_key, max_pages=3)
                                avp_out = extract_avp_public(etab=etab, ctx=ctx2, gemini_key=gemini_key, gemini_model=str(args.gemini_model))

                                # Durcissement: si "avp_eligible" mais évidence trop générique (pas spécifique à l'établissement), on downgrade.
                                if (avp_out or {}).get("eligibilite_statut") == "avp_eligible" and not _avp_evidence_looks_specific(etab=etab, ctx=ctx2):
                                    avp_out["eligibilite_statut"] = "a_verifier"
                                    avp_out["confidence"] = min(float(avp_out.get("confidence") or 0.6), 0.6)
                                    avp_out["validation_note"] = "downgraded: evidence not specific to establishment"
                            else:
                                ctx2 = {}

                            etab_public = {k: v for k, v in etab.items() if not str(k).startswith("_")}

                            publish_to_update_preview = build_publishability_update(
                                etab=etab,
                                publish_diag=publish_diag,
                                publish_fixes=publish_fixes,
                                address_validation=address_validation,
                            )

                            record = {
                                "etablissement": {
                                    **etab_public,
                                    "sous_categories": sous_cats,
                                    "services_base": services,
                                    "tarifications_base": tarifs,
                                    "logements_base": logements_base,
                                },
                                "scoring": score_info,
                                "extraction": {
                                    "context_general": ctx,
                                    "publishability": {
                                        "precheck": publish_diag,
                                        "extracted": publish_fixes,
                                        "address_validation": address_validation,
                                        "to_update_preview": publish_to_update_preview,
                                    },
                                    "tarifications": tarifs_new,
                                    "services": services_new,
                                    "logements": logements_new,
                                    "logements_compare": comp,
                                    "description": {"generated": bool(new_desc_clean), "text": desc_text},
                                    "avp": {"context": ctx2, "result": avp_out},
                                },
                                "meta": {"ts": datetime.now().isoformat()},
                            }

                            fout.write(json.dumps(record, ensure_ascii=False) + "\n")

                            # Optionnel: écrire des propositions (sans apply)
                            if args.write_propositions:
                                # Publishability fixes (adresse/gestionnaire/email/geom) — prioritaire
                                if publish_diag and publish_diag.get("can_publish") is False:
                                    to_update = publish_to_update_preview

                                    # Créer une proposition unique etablissement/update si on a au moins 1 champ utile
                                    if to_update:
                                        prop_id = create_proposition(
                                            cur=cur,
                                            etablissement_id=etab_id,
                                            type_cible="etablissement",
                                            action="update",
                                            source="enrich_proto",
                                            payload={
                                                "reason": "publishability_precheck_fix",
                                                "precheck": publish_diag,
                                                "extracted": publish_fixes,
                                                "address_validation": address_validation,
                                            },
                                            review_note="Pré-check can_publish: complétion champs requis (prototype)",
                                            cible_id=None,
                                        )
                                        for col, newv in to_update.items():
                                            oldv = etab.get(col)
                                            add_item(cur=cur, proposition_id=prop_id, table_name="etablissements", column_name=col, old_value=oldv, new_value=newv)
                                        created_props += 1
                                        created_prop_ids.append(prop_id)

                                # On ne crée des propositions que si on a quelque chose de concret.
                                # Tarifs
                                if tarifs_new and (tarifs_new.get("confidence") or 0) >= 0.55:
                                    existing = tarifs[0] if tarifs else None
                                    action = "update" if existing else "create"
                                    cible_id = existing.get("id") if existing else None
                                    prop_id = create_proposition(
                                        cur=cur,
                                        etablissement_id=etab_id,
                                        type_cible="tarifications",
                                        action=action,
                                        source="enrich_proto",
                                        payload={
                                            "extracted": tarifs_new,
                                            "evidence": tarifs_new.get("evidence_urls") or [],
                                            "pricing_scope": tarifs_new.get("pricing_scope"),
                                        },
                                        review_note="Tarifs: extraction automatique (prototype)",
                                        cible_id=cible_id,
                                    )
                                    for col in ["prix_min", "prix_max", "loyer_base", "charges", "periode", "fourchette_prix"]:
                                        newv = tarifs_new.get(col)
                                        if newv is None:
                                            continue
                                        oldv = existing.get(col) if existing else None
                                        add_item(cur=cur, proposition_id=prop_id, table_name="tarifications", column_name=col, old_value=oldv, new_value=newv)
                                    created_props += 1
                                    created_prop_ids.append(prop_id)

                                # Services (add only)
                                if services_new and (services_new.get("confidence") or 0) >= 0.6:
                                    sv_keys = services_new.get("services") or []
                                    if sv_keys:
                                        # uniquement si <=1 service en base (déjà respecté)
                                        resolved: List[Tuple[str, str]] = []  # (service_key, service_id)
                                        unresolved: List[str] = []
                                        for sk in sv_keys:
                                            sid = resolve_service_id(cur, str(sk))
                                            if sid:
                                                resolved.append((str(sk), sid))
                                            else:
                                                unresolved.append(str(sk))

                                        # Rien d'applicable en base (pas de correspondance dans services)
                                        # -> on évite de créer une proposition inutilisable.
                                        if not resolved:
                                            continue

                                        prop_id = create_proposition(
                                            cur=cur,
                                            etablissement_id=etab_id,
                                            type_cible="etablissement_service",
                                            action="create",
                                            source="enrich_proto",
                                            payload={
                                                "services": sv_keys,
                                                "resolved": [{"service_key": sk, "service_id": sid} for sk, sid in resolved],
                                                "unresolved": unresolved,
                                                "evidence": services_new.get("evidence_urls") or [],
                                            },
                                            review_note="Services: ajout liaisons (prototype, add-only)",
                                            cible_id=None,
                                        )
                                        # items: on stocke directement le service_id (applicable immédiatement)
                                        for sk, sid in resolved:
                                            add_item(
                                                cur=cur,
                                                proposition_id=prop_id,
                                                table_name="etablissement_service",
                                                column_name="service_id",
                                                old_value=None,
                                                new_value=sid,
                                            )
                                        created_props += 1
                                        created_prop_ids.append(prop_id)

                                # Logements (create only: add missing)
                                if logements_new:
                                    prop_id = create_proposition(
                                        cur=cur,
                                        etablissement_id=etab_id,
                                        type_cible="logements_types",
                                        action="create",
                                        source="enrich_proto",
                                        payload={"logements_types": logements_new, "compare": comp},
                                        review_note="Logements: extraction automatique (prototype, add-only)",
                                        cible_id=None,
                                    )
                                    add_item(cur=cur, proposition_id=prop_id, table_name="logements_types", column_name="logements_types", old_value=None, new_value=logements_new)
                                    created_props += 1
                                    created_prop_ids.append(prop_id)

                                # Description
                                if new_desc_clean and new_desc_clean != existing_desc_clean:
                                    prop_id = create_proposition(
                                        cur=cur,
                                        etablissement_id=etab_id,
                                        type_cible="etablissement",
                                        action="update",
                                        source="enrich_proto",
                                        payload={"reason": "rewrite_description", "score": score_info},
                                        review_note="Description: réécriture automatique (prototype)",
                                        cible_id=None,
                                    )
                                    add_item(cur=cur, proposition_id=prop_id, table_name="etablissements", column_name="presentation", old_value=etab.get("presentation") or "", new_value=new_desc_clean)
                                    created_props += 1
                                    created_prop_ids.append(prop_id)

                                # AVP/public cible (HI/HIG)
                                if avp_out and (avp_out.get("confidence") or 0) >= 0.6:
                                    # eligibilite_statut
                                    if avp_out.get("eligibilite_statut"):
                                        prop_id = create_proposition(
                                            cur=cur,
                                            etablissement_id=etab_id,
                                            type_cible="etablissement",
                                            action="update",
                                            source="enrich_proto",
                                            payload={"avp": avp_out, "evidence": avp_out.get("evidence_urls") or []},
                                            review_note="AVP/public: suggestion automatique (prototype)",
                                            cible_id=None,
                                        )
                                        add_item(
                                            cur=cur,
                                            proposition_id=prop_id,
                                            table_name="etablissements",
                                            column_name="eligibilite_statut",
                                            old_value=etab.get("eligibilite_statut") or None,
                                            new_value=avp_out.get("eligibilite_statut"),
                                        )
                                        # public_cible suggéré (texte normalisé) — on stocke une liste, l'apply décidera du format
                                        if avp_out.get("public_cible_suggere"):
                                            add_item(
                                                cur=cur,
                                                proposition_id=prop_id,
                                                table_name="etablissements",
                                                column_name="public_cible",
                                                old_value=etab.get("public_cible") or "",
                                                new_value=",".join(avp_out.get("public_cible_suggere") or []),
                                            )
                                        created_props += 1
                                        created_prop_ids.append(prop_id)

                                conn.commit()

                            if args.sleep:
                                time.sleep(float(args.sleep))

    print("="*80)
    print("[SUCCESS] Script termine avec succes")
    print("="*80)
    print(f"[RESULTS] Outputs: {jsonl_path}")
    print(f"[RESULTS] Logements report: {logements_csv}")
    print(f"[RESULTS] Etablissements traites: {processed}")
    if args.write_propositions:
        print(f"[RESULTS] Propositions creees: {created_props}")
        if created_prop_ids:
            with open(props_list_path, "w", encoding="utf-8") as f:
                for pid in created_prop_ids:
                    f.write(str(pid) + "\n")
            print(f"- propositions_list: {props_list_path}")

    return 0


if __name__ == "__main__":
    import sys
    import traceback
    
    # Force output flushing
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    
    print("="*80, flush=True)
    print("[SCRIPT START] enrich_dept_prototype.py DEBUT EXECUTION", flush=True)
    print("="*80, flush=True)
    
    try:
        exit_code = main()
        print("="*80, flush=True)
        print(f"[SCRIPT END] Exit code: {exit_code}", flush=True)
        print("="*80, flush=True)
        sys.exit(exit_code)
    except Exception as e:
        print("="*80, flush=True)
        print(f"[SCRIPT ERROR] Exception non geree: {e}", flush=True)
        print("="*80, flush=True)
        traceback.print_exc(file=sys.stdout)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
