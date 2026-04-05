import sys, os
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(host=os.getenv("DB_HOST"), database=os.getenv("DB_NAME","postgres"), user=os.getenv("DB_USER","postgres"), password=os.getenv("DB_PASSWORD"), port=int(os.getenv("DB_PORT",5432)))
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='prospection_contacts' ORDER BY ordinal_position")
for r in cur.fetchall():
    print(f"  {r['column_name']:35s} {r['data_type']}")
conn.close()
