"""Cohérence des données établissement (scoring + checks).

Objectif
- Centraliser des checks automatiques de cohérence entre:
  - `etablissements` (prix_min/prix_max/fourchette_prix, presentation, etc.)
  - `tarifications` (prix_min/prix_max/loyer_base/charges/periode)
  - `etablissement_service` (couverture services)

Ce module est pensé pour être appelé:
- en mode read-only (audit / scoring)
- en amont d'un workflow de propositions (pour décider quels établissements enrichir / corriger)

Notes
- Les schémas exacts pouvant évoluer, les checks sont tolérants: ils produisent PASS/WARN/FAIL
  et des messages plutôt que lever des exceptions.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# Permet d'exécuter depuis `scripts/`.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from database import DatabaseManager


@dataclass
class CheckResult:
    status: str  # PASS|WARN|FAIL
    message: str
    suggestion: str = ""
    details: Optional[Dict[str, Any]] = None


def validate_etablissement_coherence(etab_id: str) -> Dict[str, Any]:
    """Valide cohérence données établissement et retourne un score 0-100."""

    db = DatabaseManager()
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            etab = get_etablissement(cur, etab_id)
            tarifs = get_tarifications(cur, etab_id)
            services = get_services(cur, etab_id)

    checks: Dict[str, CheckResult] = {}

    checks["tarif_range"] = check_tarif_range(etab, tarifs)
    checks["tarif_units"] = check_tarif_units(tarifs)
    checks["tarif_outliers"] = check_tarif_outliers(etab, tarifs)

    checks["services_completeness"] = check_services_completeness(etab, services)
    checks["description_matches_data"] = check_description_coherence(etab, tarifs, services)

    total_checks = len(checks)
    passed_checks = sum(1 for c in checks.values() if c.status == "PASS")

    # Score simple: PASS=1, WARN=0.5, FAIL=0
    weighted = 0.0
    for c in checks.values():
        if c.status == "PASS":
            weighted += 1.0
        elif c.status == "WARN":
            weighted += 0.5

    score = (weighted / total_checks) * 100 if total_checks else 100.0

    return {
        "etab_id": etab_id,
        "score": round(score, 1),
        "checks": {k: c.__dict__ for k, c in checks.items()},
        "needs_review": any(c.status == "FAIL" for c in checks.values()),
    }


def get_etablissement(cur, etab_id: str) -> Dict[str, Any]:
    cur.execute(
        """
        SELECT
          id,
          nom,
          commune,
          departement,
          fourchette_prix,
          prix_min,
          prix_max,
          presentation,
          habitat_type
        FROM etablissements
        WHERE id = %s;
        """,
        (etab_id,),
    )
    row = cur.fetchone()
    if not row:
        return {"id": etab_id}

    return {
        "id": str(row[0]),
        "nom": row[1] or "",
        "commune": row[2] or "",
        "departement": row[3] or "",
        "fourchette_prix": row[4],
        "prix_min": row[5],
        "prix_max": row[6],
        "presentation": row[7] or "",
        "habitat_type": row[8] or "",
    }


def get_tarifications(cur, etab_id: str) -> List[Dict[str, Any]]:
    # On récupère les champs qui existent dans `tarifications` (observés via check_enrichment_tables.py)
    cur.execute(
        """
        SELECT
          fourchette_prix,
          prix_min,
          prix_max,
          loyer_base,
          charges,
          periode,
          source
        FROM tarifications
        WHERE etablissement_id = %s;
        """,
        (etab_id,),
    )

    out: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        out.append(
            {
                "fourchette_prix": r[0],
                "prix_min": r[1],
                "prix_max": r[2],
                "loyer_base": r[3],
                "charges": r[4],
                "periode": r[5],
                "source": r[6],
            }
        )
    return out


def get_services(cur, etab_id: str) -> List[Dict[str, Any]]:
    # Schéma le plus probable: etablissement_service(etablissement_id, service_id) + services(id, libelle)
    # On garde une requête robuste: si la table `services` n'existe pas / diverge, on tente juste de compter.
    try:
        cur.execute(
            """
            SELECT s.id, COALESCE(s.libelle, '')
            FROM etablissement_service es
            JOIN services s ON s.id = es.service_id
            WHERE es.etablissement_id = %s;
            """,
            (etab_id,),
        )
        return [{"id": str(r[0]), "libelle": r[1] or ""} for r in cur.fetchall()]
    except Exception:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM etablissement_service
            WHERE etablissement_id = %s;
            """,
            (etab_id,),
        )
        n = cur.fetchone()[0] or 0
        return [{"count_only": True, "count": int(n)}]


def _tarif_amount_candidates(t: Dict[str, Any]) -> List[float]:
    vals: List[float] = []
    for k in ["prix_min", "prix_max", "loyer_base"]:
        v = t.get(k)
        if isinstance(v, (int, float)):
            vals.append(float(v))
    # charges peut être additionné si c'est un chiffre
    lb = t.get("loyer_base")
    ch = t.get("charges")
    if isinstance(lb, (int, float)) and isinstance(ch, (int, float)):
        vals.append(float(lb + ch))
    return vals


def _derive_min_max_from_tarifs(tarifs: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    amounts: List[float] = []
    for t in tarifs:
        amounts.extend(_tarif_amount_candidates(t))
    amounts = [a for a in amounts if a > 0]
    if not amounts:
        return None
    return {"min": min(amounts), "max": max(amounts)}


def check_tarif_range(etab: Dict[str, Any], tarifs: List[Dict[str, Any]]) -> CheckResult:
    """Vérifie cohérence entre prix_min/prix_max sur etablissements et les valeurs des tarifications."""

    etab_min = etab.get("prix_min")
    etab_max = etab.get("prix_max")

    if not tarifs:
        if etab_min or etab_max:
            return CheckResult(
                status="WARN",
                message="Fourchette dans etablissements mais tarifications vide",
            )
        return CheckResult(status="PASS", message="Pas de données tarifs")

    derived = _derive_min_max_from_tarifs(tarifs)
    if not derived:
        return CheckResult(status="WARN", message="Tarifications sans montants exploitables")

    tmin = derived["min"]
    tmax = derived["max"]

    issues: List[str] = []
    # tolérance: 10%
    if isinstance(etab_min, (int, float)) and tmin < float(etab_min) * 0.9:
        issues.append(f"Tarif min ({tmin:.0f}) < etablissements.prix_min ({float(etab_min):.0f})")
    if isinstance(etab_max, (int, float)) and tmax > float(etab_max) * 1.1:
        issues.append(f"Tarif max ({tmax:.0f}) > etablissements.prix_max ({float(etab_max):.0f})")

    if issues:
        return CheckResult(
            status="FAIL",
            message="; ".join(issues),
            suggestion=f"Ajuster fourchette etablissements vers [{tmin:.0f}, {tmax:.0f}] (ou corriger tarifications)",
            details={"tarif_min": tmin, "tarif_max": tmax, "etab_min": etab_min, "etab_max": etab_max},
        )

    return CheckResult(status="PASS", message="Cohérence tarifs OK", details={"tarif_min": tmin, "tarif_max": tmax})


def check_tarif_units(tarifs: List[Dict[str, Any]]) -> CheckResult:
    """Vérifie la cohérence des périodes (mensuel/journalier/etc.) et l'absence de valeurs manifestement invalides."""

    if not tarifs:
        return CheckResult(status="PASS", message="Pas de tarifs")

    periodes = sorted({(t.get("periode") or "").strip().lower() for t in tarifs if (t.get("periode") or "").strip()})
    if len(periodes) > 1:
        return CheckResult(
            status="WARN",
            message=f"Périodes multiples dans tarifications: {', '.join(periodes)}",
            suggestion="Normaliser les unités/périodes ou séparer par type de logement",
            details={"periodes": periodes},
        )

    # Détection de valeurs non plausibles (0, négatives)
    bad = 0
    for t in tarifs:
        for a in _tarif_amount_candidates(t):
            if a <= 0:
                bad += 1

    if bad:
        return CheckResult(status="WARN", message=f"{bad} montants non valides (<=0)")

    return CheckResult(status="PASS", message="Unités/périodes OK")


def check_tarif_outliers(etab: Dict[str, Any], tarifs: List[Dict[str, Any]]) -> CheckResult:
    """Détecte valeurs aberrantes (heuristiques métier)."""

    if not tarifs:
        return CheckResult(status="PASS", message="Pas de tarifs")

    # Heuristique: habitat intermédiaire, ordre de grandeur mensuel.
    # Si `periode` est mensuel ou absent, on applique un range.
    amounts: List[float] = []
    for t in tarifs:
        periode = (t.get("periode") or "").strip().lower()
        if periode and periode not in {"mensuel", "mois", "monthly"}:
            # On ne sait pas convertir proprement: on évite les faux positifs.
            continue
        amounts.extend(_tarif_amount_candidates(t))

    amounts = [a for a in amounts if a > 0]
    if not amounts:
        return CheckResult(status="PASS", message="Pas de montants exploitables")

    outliers: List[str] = []
    for a in amounts:
        if a < 300:
            outliers.append(f"{a:.0f}€ trop bas (min attendu ~400€/mois)")
        elif a > 3000:
            outliers.append(f"{a:.0f}€ trop haut (max attendu ~2500€/mois)")

    if outliers:
        return CheckResult(status="WARN", message="Valeurs suspectes: " + "; ".join(outliers))

    return CheckResult(status="PASS", message="Valeurs plausibles")


def check_services_completeness(etab: Dict[str, Any], services: List[Dict[str, Any]]) -> CheckResult:
    """Vérifie que les services ne sont pas vides lorsque l'établissement est censé en avoir."""

    if not services:
        return CheckResult(status="WARN", message="Aucun service renseigné")

    if len(services) == 1 and services[0].get("count_only"):
        n = int(services[0].get("count") or 0)
        if n == 0:
            return CheckResult(status="WARN", message="Aucun service renseigné")
        return CheckResult(status="PASS", message=f"Services présents (count={n})")

    return CheckResult(status="PASS", message=f"Services présents (n={len(services)})")


def check_description_coherence(etab: Dict[str, Any], tarifs: List[Dict[str, Any]], services: List[Dict[str, Any]]) -> CheckResult:
    """Check léger: la présentation mentionne-t-elle des éléments cohérents?"""

    text = (etab.get("presentation") or "").strip().lower()
    if not text:
        return CheckResult(status="WARN", message="Présentation vide")

    signals: List[str] = []
    if tarifs:
        if "tarif" in text or "prix" in text or "loyer" in text:
            signals.append("tarifs")
    if services:
        if any(k in text for k in ["service", "animation", "restauration", "accompagnement", "collectif"]):
            signals.append("services")

    if tarifs and "tarifs" not in signals:
        return CheckResult(status="WARN", message="Présentation ne mentionne pas les tarifs alors que des tarifications existent")

    # Pas de FAIL ici: trop heuristique.
    return CheckResult(status="PASS", message="Présentation cohérente (check léger)")
