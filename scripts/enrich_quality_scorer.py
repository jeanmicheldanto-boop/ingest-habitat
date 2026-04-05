"""Scoring de qualité de description/presentation.

But
- Détecter automatiquement les descriptions trop courtes, trop génériques ou trop "mécaniques".
- Fournir un score 0-100 (heuristiques) et un indicateur `needs_rewrite`.
- Optionnel: appel Gemini si la heuristique est insuffisante.

Ce module est volontairement léger: pas de dépendance LLM obligatoire.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests


def score_description_quality(description: str, etablissement: Dict[str, Any], *, gemini_api_key: str = "", gemini_model: str = "gemini-2.0-flash") -> Dict[str, Any]:
    """Score qualité 0-100.

    Heuristique (0-70) + (optionnel) LLM (0-30) si la heuristique est faible.
    """

    desc = (description or "").strip()

    breakdown: Dict[str, float] = {}
    breakdown["length"] = score_length(desc)  # 0-20
    breakdown["facts"] = score_fact_density(desc)  # 0-20
    breakdown["repetition"] = score_repetition(desc)  # 0-15
    breakdown["specificity"] = score_specificity(desc, etablissement)  # 0-15

    heuristic_total = sum(breakdown.values())  # 0-70

    llm_points = 0.0
    llm_result: Optional[Dict[str, Any]] = None

    if heuristic_total < 45 and gemini_api_key.strip():
        llm_score_0_10, llm_result = gemini_score_quality(
            description=desc,
            etablissement=etablissement,
            api_key=gemini_api_key.strip(),
            model=gemini_model,
        )
        # Conversion en points (0-30)
        llm_points = max(0.0, min(30.0, (llm_score_0_10 / 10.0) * 30.0))
        breakdown["llm_quality"] = round(llm_points, 2)

    total = heuristic_total + llm_points

    return {
        "total": round(total, 1),
        "breakdown": breakdown,
        "needs_rewrite": total < 60,
        "llm": llm_result,
    }


def score_length(description: str) -> float:
    n = len(description or "")
    if n <= 0:
        return 0.0
    if n < 80:
        return 5.0
    if n < 160:
        return 10.0
    if n < 300:
        return 16.0
    if n < 900:
        return 20.0
    # Trop long => léger malus
    return 18.0


def score_fact_density(description: str) -> float:
    """Cherche des signaux factuels: chiffres, €, m², etc."""

    text = (description or "")
    if not text.strip():
        return 0.0

    euros = len(re.findall(r"\b\d{2,5}\s*€", text))
    numbers = len(re.findall(r"\b\d{2,5}\b", text))
    surfaces = len(re.findall(r"\b\d{1,4}\s*m\s*²\b", text, flags=re.IGNORECASE))
    percents = len(re.findall(r"\b\d{1,3}\s*%\b", text))

    raw = euros * 3 + numbers * 1 + surfaces * 2 + percents * 1
    if raw == 0:
        return 6.0  # description peut être ok sans chiffres
    if raw < 3:
        return 10.0
    if raw < 6:
        return 15.0
    return 20.0


def score_repetition(description: str) -> float:
    """Mesure simple: ratio mots uniques / mots totaux."""

    text = (description or "").lower()
    words = [w for w in re.findall(r"[a-zàâçéèêëîïôûùüÿñæœ']+", text) if len(w) > 2]
    if len(words) < 10:
        return 5.0

    unique = len(set(words))
    ratio = unique / max(1, len(words))

    if ratio > 0.65:
        return 15.0
    if ratio > 0.55:
        return 12.0
    if ratio > 0.45:
        return 8.0
    return 4.0


def score_specificity(description: str, etablissement: Dict[str, Any]) -> float:
    """Vérifie spécificité vs établissement (nom, commune, type)."""

    if not (description or "").strip():
        return 0.0

    desc_l = description.lower()
    score = 0.0

    nom = (etablissement.get("nom") or etablissement.get("nom_etablissement") or "").strip()
    ville = (etablissement.get("commune") or "").strip()
    typ = (etablissement.get("habitat_type") or etablissement.get("sous_categorie") or "").strip()

    if nom and nom.lower() in desc_l:
        score += 5.0
    if ville and ville.lower() in desc_l:
        score += 3.0
    if typ and typ.lower() in desc_l:
        score += 4.0

    specific_services = count_specific_services(description)
    score += min(3.0, float(specific_services))

    return min(15.0, score)


def count_specific_services(description: str) -> int:
    """Compteur très simple de services concrets."""

    text = (description or "").lower()
    keywords = [
        "restauration",
        "portage",
        "kitchenette",
        "animations",
        "atelier",
        "conciergerie",
        "domotique",
        "pmr",
        "téléassistance",
        "salle commune",
        "espace partagé",
        "jardin",
        "terrasse",
        "ascenseur",
    ]
    return sum(1 for k in keywords if k in text)


def gemini_score_quality(*, description: str, etablissement: Dict[str, Any], api_key: str, model: str) -> Tuple[float, Dict[str, Any]]:
    """LLM scoring pour validation (score 0-10)."""

    prompt = f"""
Évalue la qualité de cette description d'établissement senior.

Établissement : {etablissement.get('nom') or etablissement.get('nom_etablissement') or ''} à {etablissement.get('commune') or ''}
Type : {etablissement.get('habitat_type') or etablissement.get('sous_categorie') or ''}

Description :
{description}

Critères d'évaluation (0-10 chacun) :
1. Spécificité (mentionne nom, ville, caractéristiques uniques)
2. Informativité (services concrets, pas généralités)
3. Lisibilité (phrases naturelles, pas robotique)
4. Factualité (évite superlatifs non sourcés)

Réponds UNIQUEMENT en JSON :
{{
  \"specificite\": <0-10>,
  \"informativite\": <0-10>,
  \"lisibilite\": <0-10>,
  \"factualite\": <0-10>,
  \"score_global\": <0-10>,
  \"recommandation\": \"KEEP\"|\"REWRITE\"
}}
""".strip()

    data = _gemini_json(api_key=api_key, model=model, prompt=prompt)
    score = float(data.get("score_global") or 0.0)
    return score, data


def _gemini_json(*, api_key: str, model: str, prompt: str, timeout: int = 45, max_retries: int = 3) -> Dict[str, Any]:
    model_name = (model or "").strip() or "gemini-2.0-flash"
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 512},
    }

    last_error = ""
    for attempt in range(max_retries):
        try:
            resp = requests.post(endpoint, json=payload, timeout=timeout)
            if resp.status_code in {429, 500, 502, 503, 504}:
                time.sleep(1.5 * (attempt + 1))
                last_error = f"Gemini transient {resp.status_code}: {resp.text[:200]}"
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"Gemini error {resp.status_code}: {resp.text[:200]}")

            raw = resp.json()
            text = (
                raw.get("candidates", [{}])[0]
                .get("content", {})
                .get("parts", [{}])[0]
                .get("text", "")
                .strip()
            )
            if not text:
                raise RuntimeError("Gemini output vide")

            try:
                return json.loads(text)
            except Exception:
                # parfois Gemini enveloppe dans ```json
                cleaned = re.sub(r"^```json\s*|\s*```$", "", text, flags=re.IGNORECASE).strip()
                return json.loads(cleaned)

        except Exception as e:
            last_error = str(e)
            time.sleep(1.0 * (attempt + 1))

    raise RuntimeError(last_error or "Gemini error: unknown failure")
