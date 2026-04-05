#!/usr/bin/env python3
from pathlib import Path
import importlib.util

ROOT = Path(__file__).resolve().parent.parent
SUPABASE_PATH = ROOT / "prospection-financeurs" / "src" / "supabase_db.py"
spec = importlib.util.spec_from_file_location("supabase_db", str(SUPABASE_PATH))
supabase_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supabase_mod)
ProspectionDB = supabase_mod.ProspectionDB

Q = """
SELECT COUNT(*) FROM public.finess_gestionnaire g
WHERE COALESCE(g.secteur_activite_principal, '') ILIKE '%heberg%'
"""
Q_SAMPLE = """
SELECT id_gestionnaire, raison_sociale, secteur_activite_principal
FROM public.finess_gestionnaire g
WHERE COALESCE(g.secteur_activite_principal, '') ILIKE '%heberg%'
LIMIT 20
"""

def main():
    db = ProspectionDB()
    try:
        db.connect()
        with db._conn.cursor() as cur:
            cur.execute(Q)
            cnt = cur.fetchone()[0]
            print('count=', cnt)
            cur.execute(Q_SAMPLE)
            rows = cur.fetchall()
            for r in rows:
                print(r)
    finally:
        try:
            db.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
