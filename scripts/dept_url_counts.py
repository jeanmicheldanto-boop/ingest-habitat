"""Liste les départements avec le plus d'URLs éligibles au workflow de nettoyage.

Éligible = site_web non vide + hors sous-catégories RA/RSS/MARPA.

Usage:
  python scripts/dept_url_counts.py --top 20
"""

from __future__ import annotations

import argparse
import os
import sys

# Permet d'exécuter le script depuis le dossier `scripts/`.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
  sys.path.insert(0, REPO_ROOT)

from database import DatabaseManager


EXCLUDED_SOUS_CATEGORIES = (
    "Résidence autonomie",
    "Résidence services seniors",
    "MARPA",
    "residence_autonomie",
    "residence_services_seniors",
    "marpa",
)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--top", type=int, default=15)
    args = p.parse_args()

    db = DatabaseManager()
    sql = """
    SELECT e.departement, COUNT(*)
    FROM etablissements e
    WHERE e.is_test = false
      AND e.site_web IS NOT NULL
      AND trim(e.site_web) != ''
      AND NOT EXISTS (
        SELECT 1
        FROM etablissement_sous_categorie esc2
        JOIN sous_categories sc2 ON sc2.id = esc2.sous_categorie_id
        WHERE esc2.etablissement_id = e.id
          AND sc2.libelle = ANY(%s)
      )
    GROUP BY e.departement
    ORDER BY COUNT(*) DESC
    LIMIT %s;
    """

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (list(EXCLUDED_SOUS_CATEGORIES), int(args.top)))
            rows = cur.fetchall()

    print("TOP départements (URLs éligibles):")
    for dept, count in rows:
        print(f"- {dept}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
