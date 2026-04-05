import os, csv
from dotenv import load_dotenv
load_dotenv()
import psycopg2, psycopg2.extras

def get_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME','postgres'),
        user=os.getenv('DB_USER','postgres'),
        password=os.getenv('DB_PASSWORD'),
        port=int(os.getenv('DB_PORT',5432)),
    )

out_path = os.path.join(os.path.dirname(__file__), 'dir_qual_inno.csv')
conn = get_conn()
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("""
SELECT d.id AS dirigeant_id, d.prenom, d.nom, d.fonction_normalisee, d.fonction_brute, d.confiance, d.email_reconstitue, d.email_organisation,
       g.id_gestionnaire, g.raison_sociale, g.domaine_mail
FROM finess_dirigeant d
LEFT JOIN finess_gestionnaire g ON g.id_gestionnaire = d.id_gestionnaire
WHERE (d.fonction_normalisee ILIKE '%qualit%' OR d.fonction_normalisee ILIKE '%innovation%')
ORDER BY g.raison_sociale NULLS LAST, d.fonction_normalisee NULLS LAST
""")
rows = cur.fetchall()

with open(out_path, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['dirigeant_id','prenom','nom','fonction_normalisee','fonction_brute','confiance','email_reconstitue','email_organisation','id_gestionnaire','raison_sociale','domaine_mail'])
    for r in rows:
        w.writerow([r.get('dirigeant_id'), r.get('prenom'), r.get('nom'), r.get('fonction_normalisee'), r.get('fonction_brute'), r.get('confiance'), r.get('email_reconstitue'), r.get('email_organisation'), r.get('id_gestionnaire'), r.get('raison_sociale'), r.get('domaine_mail')])

print(f"WROTE {len(rows)} rows to {out_path}")
cur.close()
conn.close()
