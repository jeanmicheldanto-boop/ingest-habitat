import sys, os
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import psycopg2
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME","postgres"),
    user=os.getenv("DB_USER","postgres"), password=os.getenv("DB_PASSWORD"), port=int(os.getenv("DB_PORT",5432)))
cur = conn.cursor()

cur.execute("""
    UPDATE prospection_entites
    SET domaine_email = 'alsace.eu'
    WHERE domaine_email = 'collectivite-europeenne-alsace.fr'
    RETURNING code, nom
""")
rows = cur.fetchall()
conn.commit()
conn.close()
for r in rows:
    print(f"  [{r[0]}] {r[1]} -> alsace.eu")
print(f"  {len(rows)} entite(s) mise(s) a jour")
