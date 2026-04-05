#!/usr/bin/env python3
"""Diagnostic: lister gestionnaires avec nb_etablissements>10 et daf_email, puis agrégats catégories."""
from pathlib import Path
import importlib.util

ROOT = Path(__file__).resolve().parent.parent
SUPABASE_PATH = ROOT / "prospection-financeurs" / "src" / "supabase_db.py"
spec = importlib.util.spec_from_file_location("supabase_db", str(SUPABASE_PATH))
supabase_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supabase_mod)
ProspectionDB = supabase_mod.ProspectionDB

Q_GEST = """
SELECT id_gestionnaire, raison_sociale, nb_etablissements, daf_nom, daf_prenom, daf_email
FROM public.finess_gestionnaire
WHERE nb_etablissements > 10 AND daf_email IS NOT NULL
ORDER BY nb_etablissements DESC
LIMIT 100
"""

Q_CATS = """
SELECT id_gestionnaire, COUNT(*) AS nb_etab, array_agg(DISTINCT COALESCE(categorie_normalisee, '')) AS cats
FROM public.finess_etablissement
WHERE id_gestionnaire = ANY(%s)
GROUP BY id_gestionnaire
ORDER BY nb_etab DESC
"""


def main():
    db = ProspectionDB()
    try:
        db.connect()
        cur = db._conn.cursor()
        cur.execute(Q_GEST)
        gests = cur.fetchall()
        print("--- Gestionnaires (nb_etablissements>10, daf_email not null) ---")
        ids = []
        for r in gests:
            gid, name, nb, daf_nom, daf_prenom, daf_email = r
            ids.append(gid)
            print(f"{gid}\t{nb}\t{name}\t{daf_nom} {daf_prenom}\t{daf_email}")

        if not ids:
            print("Aucun gestionnaire trouvé avec nb_etablissements>10 et daf_email.")
            return

        # second query: categories
        cur.execute(Q_CATS, (ids,))
        cats = cur.fetchall()
        print("\n--- Catégories par gestionnaire (échantillon) ---")
        for r in cats:
            gid, nb, arr = r
            sample = ','.join([c for c in arr if c])[:400]
            print(f"{gid}\t{nb}\t{sample}")

    finally:
        try:
            db.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
