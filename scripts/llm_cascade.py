"""Cascade LLM (flash -> pro) pour extraction structurée.

Idée
- Utiliser `gemini-2.0-flash` par défaut (rapide/peu coûteux)
- Détecter la complexité d'un texte et adapter prompt/modèle
- Re-tenter avec un modèle plus puissant si la qualité de sortie est insuffisante

Ce module fournit:
- `detect_complexity(text)`
- `extract_structured_data(text, prompt_builder, required_keys, ...)`

Les prompts exacts (tarifs/services/etc.) seront construits dans le futur script d'enrichissement.
"""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Callable, Dict, Iterable, Optional

import requests


def detect_complexity(text: str) -> str:
    """Détecte complexité approximative d'un texte."""

    t = (text or "")
    length = len(t)

    has_tables = bool(re.search(r"\|\s*\w+\s*\|", t))
    euro_mentions = len(re.findall(r"\d+\s*€", t))
    has_conditions = any(kw in t.lower() for kw in ["selon", "conditions", "variable", "hors charges", "charges comprises"])

    if length < 1200 and not has_tables and euro_mentions <= 2 and not has_conditions:
        return "simple"
    if length < 6000 and (has_tables or euro_mentions > 5 or has_conditions):
        return "medium"
    return "complex"


def extract_structured_data(
    *,
    text: str,
    prompt_builder: Callable[[str], str],
    required_keys: Optional[Iterable[str]] = None,
    complexity: str = "auto",
    api_key: str = "",
    model_flash: str = "gemini-2.0-flash",
    model_pro: str = "gemini-2.5-pro",
) -> Dict[str, Any]:
    """Extraction structurée avec cascade.

    - `prompt_builder(level)` doit retourner un prompt (level: simple|medium|complex)
    - `required_keys` (optionnel) permet d'évaluer la complétude
    """

    if not api_key:
        raise RuntimeError("GEMINI_API_KEY missing")

    level = complexity
    if level == "auto":
        level = detect_complexity(text)

    # Choix modèle / coût indicatif
    if level in {"simple", "medium"}:
        model = model_flash
        cost = 0.002 if level == "medium" else 0.001
    else:
        model = model_pro
        cost = 0.01

    prompt = prompt_builder(level)
    result = call_gemini_json(api_key=api_key, model=model, prompt=prompt)
    quality = validate_extraction_quality(result, required_keys=required_keys)

    # Retry sur pro si qualité faible
    if quality < 0.7 and model != model_pro:
        prompt2 = prompt_builder("complex")
        result = call_gemini_json(api_key=api_key, model=model_pro, prompt=prompt2)
        quality = validate_extraction_quality(result, required_keys=required_keys)
        model = model_pro
        cost = 0.01

    return {
        "data": result,
        "model_used": model,
        "cost": cost,
        "quality": round(quality, 3),
        "complexity": level,
    }


def validate_extraction_quality(data: Any, *, required_keys: Optional[Iterable[str]] = None) -> float:
    """Évalue grossièrement la qualité/complétude d'un JSON extrait.

    Retourne un score 0..1.
    """

    if not isinstance(data, dict) or not data:
        return 0.0

    if not required_keys:
        # qualité minimale: dict non vide + pas uniquement des null/""
        non_empty = 0
        total = 0
        for v in data.values():
            total += 1
            if v not in (None, "", [], {}):
                non_empty += 1
        return non_empty / max(1, total)

    keys = list(required_keys)
    present = 0
    for k in keys:
        if k in data and data[k] not in (None, "", [], {}):
            present += 1
    return present / max(1, len(keys))


def call_gemini_json(*, api_key: str, model: str, prompt: str, timeout: int = 60, max_retries: int = 3) -> Dict[str, Any]:
    model_name = (model or "").strip() or "gemini-2.0-flash"
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={api_key}"

    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 2048},
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
                cleaned = re.sub(r"^```json\s*|\s*```$", "", text, flags=re.IGNORECASE).strip()
                return json.loads(cleaned)

        except Exception as e:
            last_error = str(e)
            time.sleep(1.0 * (attempt + 1))

    raise RuntimeError(last_error or "Gemini error: unknown failure")
