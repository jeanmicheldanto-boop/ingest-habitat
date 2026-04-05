"""Fix blockers preventing `statut_editorial='publie'`.

This script targets establishments that fail `public.can_publish(id)` by fetching missing
fundamentals (address / geocoding / gestionnaire) using:
- Serper search (SERPER_API_KEY)
- Scraping (SCRAPINGBEE_API_KEY optional)
- Gemini extraction (GEMINI_API_KEY)
- api-adresse.data.gouv.fr to validate + get coordinates

It creates `propositions` + `proposition_items` for the missing fields.

Typical flow (sample):
  python scripts/url_propositions_workflow.py diagnose-publish-from-list --input outputs/url_proposition_ids_*.txt
  python scripts/publish_fix_from_diagnosis.py --diagnosis outputs/publish_diagnosis_*.csv --limit 3
  python scripts/url_propositions_workflow.py approve-from-list --input outputs/publish_fix_proposition_ids_*.txt
  python scripts/url_propositions_workflow.py apply --mode approved
  python scripts/url_propositions_workflow.py republish-from-list --input outputs/publish_fix_proposition_ids_*.txt

Safety: only fills fields when they are missing (per diagnosis) and requires a minimum confidence.
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
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# Allow running from scripts/
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from database import DatabaseManager


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _jsonb(val: Any) -> str:
    return json.dumps(val, ensure_ascii=False)


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower().replace("www.", "")
    except Exception:
        return ""


EXCLUDED_DOMAIN_HINTS = {
    # annuaires/comparateurs
    "papyhappy.fr",
    "pour-les-personnes-agees.gouv.fr",  # can be a source but rarely has full address details
    "coeur-de-vie.fr",
    "mazette.fr",
    "sahanest.fr",
    "essentiel-autonomie.com",
    "villesetvillagesouilfaitbonvivre.com",
    "doctolib.fr",
}


def serper_search(query: str, num: int = 8) -> List[Dict[str, Any]]:
    api_key = os.getenv("SERPER_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("SERPER_API_KEY missing in environment")

    resp = requests.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
        json={"q": query, "num": int(num)},
        timeout=40,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("organic", []) or []


def fetch_page_text(url: str, scrapingbee_api_key: str, timeout_s: int = 25) -> Tuple[int, str, str]:
    """Return (status_code, final_url, extracted_text)."""

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
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = (soup.title.get_text(strip=True) if soup.title else "").strip()
    h1 = (soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "").strip()

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    text = text[:3500]

    header = " | ".join([x for x in [title, h1] if x])
    if header:
        text = header + "\n" + text

    return status, final, text


def gemini_extract_fundamentals(
    *,
    api_key: str,
    model: str,
    establishment_name: str,
    commune: str,
    departement: str,
    url: str,
    page_text: str,
) -> Dict[str, Any]:
    """Extract address + gestionnaire from page content.

    Returns a dict with keys:
      status: OK|NOT_RELEVANT|ERROR
      confidence: 0..1
      address_line: str
      postal_code: str
      city: str
      gestionnaire: str
      reason: str
    """

    model_name = (model or "").strip() or "gemini-2.0-flash"
    if model_name.startswith("models/"):
        model_name = model_name[len("models/") :]

    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"

    rules = (
        "Tu extrais des informations factuelles depuis une page web. "
        "Objectif: retrouver l'adresse postale complète et le nom du gestionnaire de l'établissement. "
        "Sois strict: si la page ne concerne pas l'établissement, retourne status=NOT_RELEVANT. "
        "Réponds uniquement en JSON strict, sans texte autour."
    )

    prompt = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": (
                            f"{rules}\n\n"
                            f"ETABLISSEMENT: {establishment_name}\n"
                            f"COMMUNE (base): {commune}\n"
                            f"DEPARTEMENT (base): {departement}\n"
                            f"URL: {url}\n\n"
                            "EXTRAIT PAGE (tronqué):\n"
                            f"{page_text}\n\n"
                            "JSON attendu:\n"
                            "{\n"
                            '  "status": "OK"|"NOT_RELEVANT"|"ERROR",\n'
                            '  "confidence": 0.0-1.0,\n'
                            '  "address_line": "",\n'
                            '  "postal_code": "",\n'
                            '  "city": "",\n'
                            '  "gestionnaire": "",\n'
                            '  "reason": ""\n'
                            "}\n"
                        )
                    }
                ],
            }
        ],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 280},
    }

    last_error: Optional[str] = None
    for attempt in range(1, 4):
        try:
            resp = requests.post(endpoint, json=prompt, timeout=45)
            if resp.status_code in {429, 500, 502, 503, 504}:
                last_error = f"Gemini error {resp.status_code}: {resp.text[:200]}"
                if attempt < 3:
                    time.sleep(2 ** (attempt - 1))
                    continue
                raise RuntimeError(last_error)
            if resp.status_code != 200:
                raise RuntimeError(f"Gemini error {resp.status_code}: {resp.text[:200]}")
            data = resp.json()
            text = (
                data.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
                .strip()
            )
            start = text.find("{")
            end = text.rfind("}")
            if start < 0 or end <= start:
                raise ValueError(f"Gemini output non-JSON: {text[:200]}")
            obj = json.loads(text[start : end + 1])
            return obj
        except (requests.Timeout, requests.ConnectionError) as e:
            last_error = f"Gemini network error: {e}"
            if attempt < 3:
                time.sleep(2 ** (attempt - 1))
                continue
            raise

    return {"status": "ERROR", "confidence": 0.0, "reason": last_error or "unknown"}


def adresse_api_lookup(address_line: str, city: str, postal_code: str) -> Dict[str, Any]:
    """Use api-adresse.data.gouv.fr for validation + coordinates."""

    q = " ".join([x for x in [address_line, postal_code, city] if x]).strip()
    if not q:
        return {"valid": False, "error": "missing_query"}

    resp = requests.get(
        "https://api-adresse.data.gouv.fr/search/",
        params={"q": q, "limit": 1},
        timeout=15,
    )
    if resp.status_code != 200:
        return {"valid": False, "error": f"api_status_{resp.status_code}"}

    data = resp.json() or {}
    feats = data.get("features") or []
    if not feats:
        return {"valid": False, "error": "not_found"}

    feat = feats[0]
    props = feat.get("properties") or {}
    coords = (feat.get("geometry") or {}).get("coordinates") or []
    if len(coords) != 2:
        return {"valid": False, "error": "no_coords"}

    lon, lat = coords[0], coords[1]
    score = float(props.get("score") or 0.0)
    typ = str(props.get("type") or "unknown")

    if typ == "housenumber":
        precision = "rooftop"
    elif typ in {"street", "municipality"}:
        precision = "street" if typ == "street" else "locality"
    else:
        precision = "unknown"

    return {
        "valid": True,
        "score": score,
        "label": str(props.get("label") or ""),
        "name": str(props.get("name") or ""),
        "postcode": str(props.get("postcode") or ""),
        "city": str(props.get("city") or ""),
        "lat": float(lat),
        "lon": float(lon),
        "precision": precision,
        "type": typ,
    }


@dataclass
class DiagnosisRow:
    etablissement_id: str
    nom: str
    departement: str
    commune: str
    statut_editorial: str
    can_publish: bool
    missing_address: bool
    missing_gestionnaire: bool
    missing_geom: bool


def load_diagnosis(path: str) -> List[DiagnosisRow]:
    rows: List[DiagnosisRow] = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for d in r:
            rows.append(
                DiagnosisRow(
                    etablissement_id=(d.get("etablissement_id") or "").strip(),
                    nom=(d.get("nom") or "").strip(),
                    departement=(d.get("departement") or "").strip(),
                    commune=(d.get("commune") or "").strip(),
                    statut_editorial=(d.get("statut_editorial") or "").strip(),
                    can_publish=str(d.get("can_publish") or "").lower() == "true",
                    missing_address=str(d.get("missing_address") or "").lower() == "true",
                    missing_gestionnaire=str(d.get("missing_gestionnaire") or "").lower() == "true",
                    missing_geom=str(d.get("missing_geom") or "").lower() == "true",
                )
            )
    return rows


def create_proposition_update(*, cur, etablissement_id: str, payload: Dict[str, Any], review_note: str) -> str:
    cur.execute(
        """
        INSERT INTO propositions (etablissement_id, type_cible, action, statut, source, payload, review_note)
        VALUES (%s, 'etablissement', 'update', 'en_attente', %s, %s::jsonb, %s)
        RETURNING id;
        """,
        (etablissement_id, "publish_fixer", _jsonb(payload), review_note),
    )
    return str(cur.fetchone()[0])


def add_item(*, cur, proposition_id: str, column_name: str, old_value: Any, new_value: Any) -> None:
    allowed = {"adresse_l1", "adresse_l2", "code_postal", "commune", "gestionnaire", "geocode_precision", "geom"}
    if column_name not in allowed:
        raise ValueError(f"Unsupported column_name: {column_name}")

    cur.execute(
        """
        INSERT INTO proposition_items (proposition_id, table_name, column_name, old_value, new_value)
        VALUES (%s, 'etablissements', %s, %s::jsonb, %s::jsonb);
        """,
        (proposition_id, column_name, _jsonb(old_value), _jsonb(new_value)),
    )


def main() -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv(override=False)
    except Exception:
        pass

    p = argparse.ArgumentParser()
    p.add_argument("--diagnosis", required=True, help="CSV generated by diagnose-publish-from-list")
    p.add_argument("--limit", type=int, default=3)
    p.add_argument("--min-confidence", type=float, default=0.78)
    p.add_argument("--gemini-model", default=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"))
    args = p.parse_args()

    gemini_key = os.getenv("GEMINI_API_KEY", "").strip() or os.getenv("GOOGLE_MAPS_API_KEY", "").strip()
    if not gemini_key:
        raise RuntimeError("GEMINI_API_KEY missing in environment")

    scrapingbee_key = os.getenv("SCRAPINGBEE_API_KEY", "").strip()

    rows = load_diagnosis(args.diagnosis)
    candidates = [r for r in rows if (not r.can_publish) and r.statut_editorial == "draft"]

    # Prioritize address blockers
    candidates.sort(key=lambda r: (not r.missing_address, not r.missing_gestionnaire, not r.missing_geom, r.departement, r.nom))
    targets = candidates[: int(args.limit)]
    if not targets:
        print("No targets.")
        return 0

    db = DatabaseManager()

    created_props: List[str] = []

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            for t in targets:
                # Load current values
                cur.execute(
                    """
                    SELECT COALESCE(adresse_l1,''), COALESCE(adresse_l2,''), COALESCE(code_postal,''), COALESCE(commune,''),
                           COALESCE(gestionnaire,''), (geom IS NOT NULL) as has_geom
                    FROM etablissements
                    WHERE id = %s;
                    """,
                    (t.etablissement_id,),
                )
                row = cur.fetchone()
                if not row:
                    continue

                adresse_l1, adresse_l2, code_postal, commune, gestionnaire, has_geom = row

                # Search web for official page with address
                query = f"{t.nom} {t.commune} adresse"
                organic = serper_search(query, num=8)

                best: Optional[Dict[str, Any]] = None
                best_url: str = ""
                best_text: str = ""

                for item in organic:
                    url = (item.get("link") or "").strip()
                    if not url:
                        continue
                    dom = _domain(url)
                    if any(h in dom for h in EXCLUDED_DOMAIN_HINTS):
                        continue

                    status, final_url, text = fetch_page_text(url, scrapingbee_key)
                    if status != 200 or not text:
                        continue

                    extracted = gemini_extract_fundamentals(
                        api_key=gemini_key,
                        model=str(args.gemini_model),
                        establishment_name=t.nom,
                        commune=t.commune,
                        departement=t.departement,
                        url=final_url,
                        page_text=text,
                    )

                    if str(extracted.get("status") or "").upper() != "OK":
                        continue

                    conf = float(extracted.get("confidence") or 0.0)
                    if conf < float(args.min_confidence):
                        continue

                    best = extracted
                    best_url = final_url
                    best_text = text
                    break

                if not best:
                    print(f"SKIP (no confident extraction): {t.nom} ({t.commune})")
                    continue

                new_addr = (best.get("address_line") or "").strip()
                new_cp = (best.get("postal_code") or "").strip()
                new_city = (best.get("city") or "").strip()
                new_gest = (best.get("gestionnaire") or "").strip()

                # Validate address and get coordinates
                addr_valid = adresse_api_lookup(new_addr, new_city or t.commune, new_cp)
                if not addr_valid.get("valid") or float(addr_valid.get("score") or 0.0) < 0.55:
                    print(f"SKIP (adresse API low confidence): {t.nom} -> {addr_valid}")
                    continue

                # Prepare changes only for missing fields
                changes: List[Tuple[str, Any, Any]] = []

                if t.missing_address:
                    # adresse_l1: use api-adresse 'name' when available
                    addr_l1 = (addr_valid.get("name") or new_addr).strip()
                    if addr_l1 and not adresse_l1:
                        changes.append(("adresse_l1", None, addr_l1))

                    cp = (addr_valid.get("postcode") or new_cp).strip()
                    if cp and not code_postal:
                        changes.append(("code_postal", None, cp))

                    city = (addr_valid.get("city") or new_city or t.commune).strip()
                    # We generally keep commune (already in DB). Only set if missing.
                    if city and not commune:
                        changes.append(("commune", None, city))

                if t.missing_gestionnaire and new_gest and not gestionnaire:
                    changes.append(("gestionnaire", None, new_gest))

                if t.missing_geom and not bool(has_geom):
                    changes.append(("geom", None, {"lat": addr_valid.get("lat"), "lon": addr_valid.get("lon")}))
                    changes.append(("geocode_precision", None, addr_valid.get("precision") or "unknown"))

                if not changes:
                    print(f"SKIP (no changes): {t.nom}")
                    continue

                payload = {
                    "workflow": "publish_fixer",
                    "diagnosis": os.path.basename(args.diagnosis),
                    "serper_query": query,
                    "evidence_url": best_url,
                    "gemini": {
                        "model": str(args.gemini_model),
                        "confidence": float(best.get("confidence") or 0.0),
                        "reason": (best.get("reason") or "")[:500],
                    },
                    "adresse_api": {
                        "score": float(addr_valid.get("score") or 0.0),
                        "label": addr_valid.get("label") or "",
                        "type": addr_valid.get("type") or "",
                    },
                }

                review_note = f"publish_fixer: {best_url}"
                prop_id = create_proposition_update(cur=cur, etablissement_id=t.etablissement_id, payload=payload, review_note=review_note)

                for col, old, new in changes:
                    add_item(cur=cur, proposition_id=prop_id, column_name=col, old_value=old, new_value=new)

                created_props.append(prop_id)
                print(f"PROPOSED: {t.nom} ({t.commune}) -> {prop_id} ({len(changes)} items)")

            conn.commit()

    os.makedirs("outputs", exist_ok=True)
    out_ids = os.path.join("outputs", f"publish_fix_proposition_ids_{_now_tag()}.txt")
    with open(out_ids, "w", encoding="utf-8") as f:
        for pid in created_props:
            f.write(pid + "\n")

    print(f"OK: created_propositions={len(created_props)}")
    print(f"- ids: {out_ids}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
