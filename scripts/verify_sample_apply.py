"""Small helper to verify that an URL cleanup sample was applied in DB.

Usage:
  python scripts/verify_sample_apply.py --input outputs/url_review_auto_*.csv --limit 10

Prints (id, decision, site_web, source, statut_editorial).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from typing import Dict, List

# Permet d'exécuter le script depuis le dossier `scripts/`.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from database import DatabaseManager


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Review CSV with etablissement_id + decision")
    p.add_argument("--limit", type=int, default=10)
    args = p.parse_args()

    wanted: List[Dict[str, str]] = []
    with open(args.input, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if len(wanted) >= int(args.limit):
                break
            wanted.append(
                {
                    "id": (row.get("etablissement_id") or "").strip(),
                    "decision": (row.get("decision") or "").strip(),
                    "current_site_web": (row.get("current_site_web") or "").strip(),
                    "source": (row.get("source") or "").strip(),
                }
            )

    ids = [r["id"] for r in wanted if r["id"]]
    if not ids:
        print("No ids found.")
        return 1

    db = DatabaseManager()
    by_id = {}
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id::text, COALESCE(site_web,''), COALESCE(source,''), statut_editorial::text
                FROM etablissements
                WHERE id = ANY(%s::uuid[]);
                """,
                (ids,),
            )
            for etab_id, site_web, source, statut in cur.fetchall():
                by_id[str(etab_id)] = {
                    "site_web": site_web,
                    "source": source,
                    "statut_editorial": statut,
                }

    print(f"Checked {len(ids)} etablissements")
    for r in wanted:
        etab_id = r["id"]
        info = by_id.get(etab_id, {})
        print(
            etab_id,
            "| decision=", r["decision"],
            "| site_web=", info.get("site_web"),
            "| source=", info.get("source"),
            "| statut_editorial=", info.get("statut_editorial"),
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
