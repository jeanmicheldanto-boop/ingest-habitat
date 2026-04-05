#!/usr/bin/env python3
"""Génère prospection-financeurs/data/output/DAF_HEBERGEMENT_SOCIAL_100.csv
Filtre: gestionnaires avec nb_etablissements>10 et secteur_activite_principal ILIKE '%heberg%'.
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
SELECT DISTINCT ON (g.id_gestionnaire)
  g.id_gestionnaire,
  g.raison_sociale,
  COALESCE(g.daf_prenom, d.prenom) AS prenom,
  COALESCE(g.daf_nom, d.nom) AS nom,
  COALESCE(g.daf_email, d.email_reconstitue) AS email,
  COALESCE(g.daf_telephone, d.telephone_direct) AS telephone,
  COALESCE(d.fonction_normalisee, d.fonction_brute, 'DAF') AS poste
FROM public.finess_gestionnaire g
LEFT JOIN public.finess_dirigeant d
  ON d.id_gestionnaire = g.id_gestionnaire
WHERE g.nb_etablissements > 10
  AND COALESCE(g.secteur_activite_principal, '') ILIKE '%heberg%'
  AND (
    g.daf_email IS NOT NULL
    OR d.fonction_normalisee ILIKE '%financ%'
    OR d.fonction_brute ILIKE '%financ%'
    OR d.fonction_normalisee ILIKE '%DAF%'
    OR d.fonction_brute ILIKE '%DAF%'
    OR d.fonction_normalisee ILIKE '%compta%'
    OR d.fonction_brute ILIKE '%compta%'
    OR d.fonction_normalisee ILIKE '%budget%'
    OR d.fonction_brute ILIKE '%budget%'
  )
ORDER BY g.id_gestionnaire, email DESC
LIMIT 100
"""


def main():
    out_dir = ROOT / "prospection-financeurs" / "data" / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "DAF_HEBERGEMENT_SOCIAL_100.csv"

    db = ProspectionDB()
    try:
        db.connect()
        with db._conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()

        with out_file.open('w', encoding='utf-8', newline='') as fh:
            writer = csv.writer(fh)
            writer.writerow(['id_gestionnaire','raison_sociale','prenom','nom','email','telephone','poste'])
            for r in rows:
                writer.writerow(r)

        print(f"Exporté {len(rows)} contacts vers {out_file}")
    finally:
        try:
            db.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
