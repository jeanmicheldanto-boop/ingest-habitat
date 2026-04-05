"""Priorisation simple de crawl multi-pages (même domaine).

But
- Aider l'enrichisseur à choisir quelles pages internes scraper en priorité.
- Éviter de crawler au hasard (coût + temps).

Approche
- Score par mots-clés + profondeur + pénalités (login, mentions légales, etc.).

Ce module est volontairement simple et sans dépendance.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Tuple
from urllib.parse import urlparse


@dataclass
class ScoredUrl:
    url: str
    score: float
    reasons: List[str]


KEYWORD_WEIGHTS: List[Tuple[str, float]] = [
    ("tarif", 6.0),
    ("prix", 5.0),
    ("loyer", 5.0),
    ("charges", 3.0),
    ("prestations", 3.0),
    ("services", 3.0),
    ("restauration", 3.0),
    ("hebergement", 3.0),
    ("hébergement", 3.0),
    ("logement", 3.0),
    ("appartement", 2.0),
    ("studio", 2.0),
    ("t1", 1.0),
    ("t2", 1.0),
    ("brochure", 2.0),
    ("pdf", 1.0),
]

NEGATIVE_HINTS: List[Tuple[str, float]] = [
    ("login", -6.0),
    ("connexion", -6.0),
    ("compte", -4.0),
    ("mentions-legales", -4.0),
    ("politique", -3.0),
    ("cookies", -3.0),
    ("rgpd", -3.0),
    ("recrut", -2.0),
]


def same_domain(a: str, b: str) -> bool:
    try:
        da = urlparse(a).netloc.lower().lstrip("www.")
        db = urlparse(b).netloc.lower().lstrip("www.")
        return da and db and da == db
    except Exception:
        return False


def score_url(url: str) -> ScoredUrl:
    u = (url or "").strip()
    low = u.lower()

    reasons: List[str] = []
    score = 0.0

    # profondeur
    path = urlparse(u).path or ""
    depth = len([p for p in path.split("/") if p])
    if depth <= 1:
        score += 1.5
        reasons.append("shallow")
    elif depth >= 5:
        score -= 1.0
        reasons.append("deep")

    for kw, w in KEYWORD_WEIGHTS:
        if kw in low:
            score += w
            reasons.append(f"+{kw}")

    for kw, w in NEGATIVE_HINTS:
        if kw in low:
            score += w
            reasons.append(f"{w}{kw}")

    # pénalité tracking
    if re.search(r"[?&](utm_|gclid=|fbclid=)", low):
        score -= 0.5
        reasons.append("tracking")

    return ScoredUrl(url=u, score=score, reasons=reasons)


def prioritize_urls(seed_url: str, urls: Iterable[str], *, limit: int = 10) -> List[ScoredUrl]:
    """Filtre sur le domaine du seed + tri par score décroissant."""

    scored: List[ScoredUrl] = []
    for u in urls:
        if not u:
            continue
        if seed_url and not same_domain(seed_url, u):
            continue
        scored.append(score_url(u))

    scored.sort(key=lambda x: x.score, reverse=True)
    return scored[: max(1, int(limit))]
