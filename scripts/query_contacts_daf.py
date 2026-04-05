#!/usr/bin/env python3
"""Extrait 30 contacts DAF/finances depuis la base prospection et exporte en CSV.

Usage:
  .venv/Scripts/python.exe scripts/query_contacts_daf.py

Le script lit les credentials DB via `prospection-financeurs/src/supabase_db.py` config.
"""
from pathlib import Path
import csv
import sys
import importlib.util

ROOT = Path(__file__).resolve().parent.parent

# Charger dynamiquement le module prospection-financeurs/src/supabase_db.py
SUPABASE_PATH = ROOT / "prospection-financeurs" / "src" / "supabase_db.py"
spec = importlib.util.spec_from_file_location("supabase_db", str(SUPABASE_PATH))
supabase_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supabase_mod)
ProspectionDB = supabase_mod.ProspectionDB


SQL = """
SELECT
  d.id as dirigeant_id,
  (COALESCE(d.prenom, '') || ' ' || COALESCE(d.nom, '')) as nom_complet,
  COALESCE(d.fonction_normalisee, d.fonction_brute) as poste,
  g.raison_sociale as gestionnaire_nom,
  d.email_reconstitue as email
FROM public.finess_dirigeant d
LEFT JOIN public.finess_gestionnaire g ON g.id_gestionnaire = d.id_gestionnaire
WHERE (
  d.fonction_normalisee ILIKE '%%financ%%' OR
  d.fonction_brute ILIKE '%%financ%%' OR
  d.fonction_normalisee ILIKE '%%tarif%%' OR
  d.fonction_brute ILIKE '%%tarif%%' OR
  d.fonction_normalisee ILIKE '%%compta%%' OR
  d.fonction_brute ILIKE '%%compta%%' OR
  d.fonction_normalisee ILIKE '%%budget%%' OR
  d.fonction_brute ILIKE '%%budget%%' OR
  d.fonction_normalisee ILIKE '%%DAF%%' OR
  d.fonction_brute ILIKE '%%DAF%%'
)
AND d.id_gestionnaire IN (
  SELECT e.id_gestionnaire
  FROM public.finess_etablissement e
  WHERE (
      COALESCE(e.categorie_normalisee, '') ILIKE '%%handicap%%' OR
      COALESCE(e.categorie_normalisee, '') ILIKE '%%enfant%%' OR
      COALESCE(e.categorie_normalisee, '') ILIKE '%%protection%%'
  )
  GROUP BY e.id_gestionnaire
  HAVING COUNT(*) > 10
)
ORDER BY d.id
LIMIT 30
"""


def main():
    out_dir = ROOT / "prospection-financeurs" / "data" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "DAF_30.csv"

    db = ProspectionDB()
    try:
        db.connect()
        with db._conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()

        # écriture CSV
        with out_file.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["dirigeant_id", "nom_complet", "poste", "gestionnaire_nom", "email"])
            for r in rows:
              writer.writerow([r[0], r[1], r[2], r[3], r[4]])

        print(f"Exporté {len(rows)} contacts vers {out_file}")

    finally:
        try:
            db.close()
        except Exception:
            pass


if __name__ == '__main__':
    main()
