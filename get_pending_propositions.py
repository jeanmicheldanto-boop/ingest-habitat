import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    port=os.getenv('DB_PORT')
)

cur = conn.cursor()

# Propositions en_attente des dernières 2 heures
cutoff = datetime.now() - timedelta(hours=2)
cur.execute("""
    SELECT id 
    FROM propositions 
    WHERE statut = 'en_attente' AND created_at > %s
    ORDER BY created_at DESC
""", (cutoff,))

rows = cur.fetchall()

# Écrire dans un fichier
with open('outputs/recent_propositions.txt', 'w') as f:
    for row in rows:
        f.write(f"{row[0]}\n")

print(f"✅ {len(rows)} propositions récupérées et sauvegardées dans outputs/recent_propositions.txt")

cur.close()
conn.close()
