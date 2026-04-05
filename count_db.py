"""Script rapide pour compter les établissements dans Supabase"""
from database import DatabaseManager
import psycopg2

db = DatabaseManager()
conn = psycopg2.connect(**db.config)
cur = conn.cursor()

# Total
cur.execute('SELECT COUNT(*) FROM etablissements')
total = cur.fetchone()[0]

# Réels vs test
cur.execute('SELECT COUNT(*) FROM etablissements WHERE is_test=false')
real = cur.fetchone()[0]

# Par type d'habitat
cur.execute('''
    SELECT habitat_type, COUNT(*) 
    FROM etablissements 
    WHERE is_test=false 
    GROUP BY habitat_type 
    ORDER BY habitat_type
''')
types = cur.fetchall()

# Par éligibilité
cur.execute('''
    SELECT eligibilite_statut, COUNT(*) 
    FROM etablissements 
    WHERE is_test=false 
    GROUP BY eligibilite_statut 
    ORDER BY eligibilite_statut
''')
eligibilite = cur.fetchall()

# Par statut éditorial
cur.execute('''
    SELECT statut_editorial, COUNT(*) 
    FROM etablissements 
    WHERE is_test=false 
    GROUP BY statut_editorial 
    ORDER BY statut_editorial
''')
statuts = cur.fetchall()

# Avec données enrichies
cur.execute('''
    SELECT 
        COUNT(*) FILTER (WHERE presentation IS NOT NULL AND trim(presentation) != '') as avec_presentation,
        COUNT(*) FILTER (WHERE telephone IS NOT NULL AND trim(telephone) != '') as avec_telephone,
        COUNT(*) FILTER (WHERE email IS NOT NULL AND trim(email) != '') as avec_email,
        COUNT(*) FILTER (WHERE site_web IS NOT NULL AND trim(site_web) != '') as avec_site_web,
        COUNT(*) FILTER (WHERE geom IS NOT NULL) as avec_geoloc
    FROM etablissements 
    WHERE is_test=false
''')
enrichis = cur.fetchone()

print(f"📊 ÉTAT DE LA BASE SUPABASE")
print(f"{'='*60}")
print(f"\n📈 Total: {total:,} établissements ({real:,} réels, {total-real} test)")

print(f"\n🏠 Par type d'habitat:")
for t in types:
    typ = t[0] if t[0] else "NON DÉFINI"
    print(f"  • {typ}: {t[1]:,}")

print(f"\n✅ Par éligibilité:")
for e in eligibilite:
    elig = e[0] if e[0] else "NON DÉFINI"
    print(f"  • {elig}: {e[1]:,}")

print(f"\n📝 Par statut éditorial:")
for s in statuts:
    stat = s[0] if s[0] else "NON DÉFINI"
    print(f"  • {stat}: {s[1]:,}")

print(f"\n💎 Données enrichies:")
print(f"  • Avec présentation: {enrichis[0]:,} ({enrichis[0]*100/real:.1f}%)")
print(f"  • Avec téléphone: {enrichis[1]:,} ({enrichis[1]*100/real:.1f}%)")
print(f"  • Avec email: {enrichis[2]:,} ({enrichis[2]*100/real:.1f}%)")
print(f"  • Avec site web: {enrichis[3]:,} ({enrichis[3]*100/real:.1f}%)")
print(f"  • Avec géolocalisation: {enrichis[4]:,} ({enrichis[4]*100/real:.1f}%)")

cur.close()
conn.close()
