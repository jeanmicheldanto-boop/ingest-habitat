"""
Audit Phase 3 — vérification des domaines email des départements
avant reconstruction des emails dans prospection_contacts.
"""
import sys, os, socket
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import psycopg2, psycopg2.extras
from dotenv import load_dotenv
load_dotenv()


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", 5432)),
    )


def check_dns(domain: str) -> bool:
    """Retourne True si le domaine résout en DNS (MX ou A)."""
    try:
        socket.getaddrinfo(domain, None)
        return True
    except socket.gaierror:
        return False


def main():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    print("=" * 65)
    print("  ETAT GLOBAL CONTACTS / EMAILS")
    print("=" * 65)
    cur.execute("""
        SELECT
            COUNT(*) tot,
            COUNT(email_principal) avec_email,
            COUNT(CASE WHEN confiance_nom != 'invalide' THEN 1 END) valides,
            COUNT(CASE WHEN confiance_nom != 'invalide' AND email_principal IS NULL THEN 1 END) sans_email
        FROM prospection_contacts
    """)
    r = cur.fetchone()
    print(f"  Total contacts      : {r['tot']}")
    print(f"  Valides             : {r['valides']}")
    print(f"  Avec email          : {r['avec_email']}")
    print(f"  Sans email (valides): {r['sans_email']}")

    cur.execute("""
        SELECT confiance_email, COUNT(*) n
        FROM prospection_contacts
        WHERE confiance_nom != 'invalide'
        GROUP BY confiance_email ORDER BY n DESC
    """)
    print("\n  Confiance email :")
    for row in cur.fetchall():
        print(f"    {str(row['confiance_email']):12s} : {row['n']}")

    print()
    print("=" * 65)
    print("  DOMAINES DES DEPARTEMENTS")
    print("=" * 65)
    cur.execute("""
        SELECT e.code, e.nom, e.domaine_email,
            COUNT(c.id) nb_contacts,
            COUNT(c.email_principal) nb_avec_email
        FROM prospection_entites e
        LEFT JOIN prospection_contacts c
            ON c.entite_id = e.id AND c.confiance_nom != 'invalide'
        WHERE e.type_entite = 'departement'
        GROUP BY e.code, e.nom, e.domaine_email
        ORDER BY e.code
    """)
    rows = cur.fetchall()
    sans_domaine = [r for r in rows if not r["domaine_email"]]
    avec_domaine = [r for r in rows if r["domaine_email"]]

    print(f"  Avec domaine : {len(avec_domaine)}")
    print(f"  Sans domaine : {len(sans_domaine)}")
    if sans_domaine:
        print("\n  Depts SANS domaine email :")
        for r in sans_domaine:
            print(f"    [{r['code']}] {r['nom']}  ({r['nb_contacts']} contacts)")

    print()
    print("=" * 65)
    print("  VERIFICATION DNS DES DOMAINES")
    print("=" * 65)
    cur.execute("""
        SELECT DISTINCT domaine_email
        FROM prospection_entites
        WHERE type_entite = 'departement' AND domaine_email IS NOT NULL
        ORDER BY domaine_email
    """)
    domaines = [r["domaine_email"] for r in cur.fetchall()]
    print(f"  {len(domaines)} domaines distincts\n")

    ok, ko = [], []
    for d in domaines:
        valid = check_dns(d)
        status = "OK " if valid else "KO *** DNS introuvable ***"
        print(f"  {status:5s}  {d}")
        (ok if valid else ko).append(d)

    print()
    print(f"  DNS OK  : {len(ok)}")
    print(f"  DNS KO  : {len(ko)}")
    if ko:
        print("\n  Domaines en erreur DNS :")
        for d in ko:
            print(f"    {d}")

    print()
    print("=" * 65)
    print("  CONTACTS SANS EMAIL PAR DOMAINE (top 20)")
    print("=" * 65)
    cur.execute("""
        SELECT e.domaine_email, COUNT(*) nb_sans_email
        FROM prospection_contacts c
        JOIN prospection_entites e ON e.id = c.entite_id
        WHERE c.email_principal IS NULL
          AND c.confiance_nom != 'invalide'
          AND e.domaine_email IS NOT NULL
        GROUP BY e.domaine_email
        ORDER BY nb_sans_email DESC
        LIMIT 20
    """)
    for row in cur.fetchall():
        print(f"  {row['domaine_email']:45s}  {row['nb_sans_email']} contacts sans email")

    conn.close()


if __name__ == "__main__":
    main()
