#!/usr/bin/env python3
"""Génère prospection-financeurs/data/output/DAF_MULTI_SECTEURS_100.csv
Filtre: gestionnaires avec nb_etablissements>10 et secteur_activite_principal ILIKE '%multi%'.
"""
from pathlib import Path
import csv
import importlib.util

ROOT = Path(__file__).resolve().parent.parent
SUPABASE_PATH = ROOT / "prospection-financeurs" / "src" / "supabase_db.py"
spec = importlib.util.spec_from_file_location("supabase_db", str(SUPABASE_PATH))
supabase_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supabase_mod)
ProspectionDB = supabase_mod.ProspectionDB

SQL = r"""
SELECT g.id_gestionnaire,
  g.raison_sociale,
  g.daf_prenom,
  g.daf_nom,
  g.daf_email,
  g.daf_telephone,
  g.nb_etablissements
FROM public.finess_gestionnaire g
WHERE g.nb_etablissements > 10
  AND COALESCE(g.secteur_activite_principal, '') ILIKE '%multi%'
ORDER BY g.nb_etablissements DESC
LIMIT 100
"""


def main():
    out_dir = ROOT / "prospection-financeurs" / "data" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "DAF_MULTI_SECTEURS_100.csv"

    db = ProspectionDB()
    try:
        db.connect()
        with db._conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()

        with out_file.open('w', encoding='utf-8', newline='') as fh:
            writer = csv.writer(fh)
            writer.writerow(['id_gestionnaire','raison_sociale','daf_prenom','daf_nom','daf_email','daf_telephone','nb_etablissements'])
            for r in rows:
                writer.writerow(r)

        print(f"Exporté {len(rows)} gestionnaires vers {out_file}")
    finally:
        try:
            db.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
