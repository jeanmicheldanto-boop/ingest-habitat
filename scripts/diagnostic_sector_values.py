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
SELECT DISTINCT COALESCE(secteur_activite_principal,'') AS s, count(*)
FROM public.finess_gestionnaire
GROUP BY s
ORDER BY count(*) DESC
LIMIT 200
"""

def main():
    db = ProspectionDB()
    try:
        db.connect()
        with db._conn.cursor() as cur:
            cur.execute(Q)
            for r in cur.fetchall():
                print(r)
    finally:
        try:
            db.close()
        except Exception:
            pass

if __name__ == '__main__':
    main()
