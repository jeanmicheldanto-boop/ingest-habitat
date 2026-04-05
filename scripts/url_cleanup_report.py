"""Rapport de stats pour un batch URL cleanup.

Entrées:
- Un CSV de review (celui généré par analyze_suspicious_urls.py --export-review-csv)
- Optionnel: un fichier texte de proposition_id (1 par ligne)

Sorties:
- Un résumé imprimé (console)

Usage:
  python scripts/url_cleanup_report.py --review outputs/url_review_auto_*.csv
  python scripts/url_cleanup_report.py --review outputs/url_review_auto_*.csv --propositions outputs/url_proposition_ids_*.txt
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import dataclass
import os
import sys
from typing import Dict, List, Optional, Set, Tuple

# Permet d'exécuter le script depuis le dossier `scripts/`.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from database import DatabaseManager


@dataclass
class ReviewRow:
    etablissement_id: str
    current_site_web: str
    decision: str
    new_site_web: str


def _read_ids_file(path: str) -> List[str]:
    ids: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            x = line.strip()
            if not x or x.startswith("#"):
                continue
            ids.append(x)
    return ids


def _read_review_csv(path: str) -> List[ReviewRow]:
    rows: List[ReviewRow] = []
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(
                ReviewRow(
                    etablissement_id=(row.get("etablissement_id") or "").strip(),
                    current_site_web=(row.get("current_site_web") or "").strip(),
                    decision=(row.get("decision") or "").strip().upper(),
                    new_site_web=(row.get("new_site_web") or "").strip(),
                )
            )
    return rows


def _fetch_etabs_from_props(db: DatabaseManager, prop_ids: List[str]) -> List[str]:
    if not prop_ids:
        return []
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT etablissement_id::text
                FROM propositions
                WHERE id = ANY(%s::uuid[]);
                """,
                (prop_ids,),
            )
            return [str(r[0]) for r in cur.fetchall() if r and r[0]]


def _fetch_db_state(db: DatabaseManager, etab_ids: List[str]) -> Dict[str, Tuple[str, str, bool]]:
    """Retourne {etab_id: (site_web, statut_editorial, can_publish)}"""
    if not etab_ids:
        return {}
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT e.id::text,
                       COALESCE(e.site_web,''),
                       e.statut_editorial::text,
                       public.can_publish(e.id)
                FROM etablissements e
                WHERE e.id = ANY(%s::uuid[]);
                """,
                (etab_ids,),
            )
            out: Dict[str, Tuple[str, str, bool]] = {}
            for etab_id, site_web, statut, can_pub in cur.fetchall():
                out[str(etab_id)] = (site_web or "", statut or "", bool(can_pub))
            return out


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--review", required=True, help="CSV review/auto-review")
    p.add_argument("--propositions", default="", help="Optionnel: fichier de proposition_id")
    args = p.parse_args()

    review_rows = _read_review_csv(args.review)
    review_rows = [r for r in review_rows if r.etablissement_id]

    decision_counts = Counter([r.decision or "(empty)" for r in review_rows])
    distinct_etabs: Set[str] = set(r.etablissement_id for r in review_rows)

    db = DatabaseManager()

    prop_ids: List[str] = []
    etab_ids_from_props: List[str] = []
    if args.propositions:
        prop_ids = _read_ids_file(args.propositions)
        etab_ids_from_props = _fetch_etabs_from_props(db, prop_ids)

    # Prefer the DB-derived list (covers cases where review csv includes KEEP without proposition)
    if etab_ids_from_props:
        etab_ids = etab_ids_from_props
    else:
        etab_ids = sorted(distinct_etabs)

    db_state = _fetch_db_state(db, etab_ids)

    # Compute change stats by comparing review current_site_web vs DB current
    changed = 0
    dropped_now_null = 0
    replaced = 0
    kept_same = 0

    for rr in review_rows:
        st = db_state.get(rr.etablissement_id)
        if not st:
            continue
        db_site, _, _ = st
        if rr.decision == "DROP":
            if db_site.strip() == "":
                dropped_now_null += 1
        elif rr.decision == "REPLACE":
            replaced += 1
        elif rr.decision == "KEEP":
            # best-effort: consider 'kept' if DB equals previous
            if (db_site or "").strip() == (rr.current_site_web or "").strip():
                kept_same += 1

        if (db_site or "").strip() != (rr.current_site_web or "").strip():
            changed += 1

    statut_counts = Counter([db_state[e][1] for e in db_state.keys()])
    can_publish_true = sum(1 for e in db_state.keys() if db_state[e][2])

    print("=== URL cleanup batch report ===")
    print(f"Review rows: {len(review_rows)}")
    print(f"Distinct etablissements in review: {len(distinct_etabs)}")
    print("Decisions:")
    for k, v in decision_counts.most_common():
        print(f"- {k}: {v}")

    if prop_ids:
        print(f"Propositions listed: {len(prop_ids)}")
        print(f"Distinct etablissements (from propositions): {len(etab_ids_from_props)}")

    print("\nFinal DB state (for tracked etablissements):")
    for k, v in statut_counts.most_common():
        print(f"- statut_editorial={k}: {v}")
    print(f"- can_publish=true: {can_publish_true}/{len(db_state)}")

    print("\nApprox changes vs review current_site_web:")
    print(f"- changed site_web: {changed}")
    print(f"- DROP where site_web now NULL/empty: {dropped_now_null}")
    print(f"- KEEP where DB unchanged: {kept_same}")
    print(f"- REPLACE rows in review: {replaced}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
