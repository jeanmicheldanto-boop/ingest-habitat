import psycopg2
from config import DATABASE_CONFIG

conn = psycopg2.connect(**DATABASE_CONFIG)
cur = conn.cursor()

# Départements "Département (XX)"
cur.execute("SELECT COUNT(*) FROM etablissements WHERE departement ~ '^Département'")
dept_dept = cur.fetchone()[0]

# Départements corrects "Nom (XX)"
cur.execute("SELECT COUNT(*) FROM etablissements WHERE departement ~ '^[A-ZÀ-Ÿ][^(]*\\s+\\([0-9]'")
dept_ok = cur.fetchone()[0]

# Total
cur.execute("SELECT COUNT(*) FROM etablissements WHERE departement IS NOT NULL")
dept_total = cur.fetchone()[0]

print(f"Total départements renseignés: {dept_total}")
print(f"   • 'Département (XX)': {dept_dept}")
print(f"   • Format correct: {dept_ok}")

# Compter le nombre d'UPDATE dans le pipeline précédent
cur.execute("SELECT COUNT(*) FROM etablissements WHERE departement ~ '^(Ain|Aisne|Allier|Alpes|Ard|Ari|Aube|Aude|Ave|Bas|Bou|Cal|Can|Cha|Cher|Cor|Corse|Côte|Côtes|Creuse|Dord|Dou|Drô|Eure|Fini|Gard|Gers|Gir|Haut|Héra|Ille|Indr|Isère|Jura|Land|Loire|Loir|Lozè|Main|Man|Marn|May|Meur|Meus|Morb|Mose|Niè|Nord|Oise|Orne|Paris|Pas|Puy|Pyré|Rhône|Sao|Sarth|Savo|Seine|Somm|Tarn|Terr|Val|Var|Vaucl|Vend|Vien|Vosg|Yonn|Yvel|Deux|Esse)'")
dept_nom_complet = cur.fetchone()[0]

print(f"   • Noms complets (Ain, Aisne...): {dept_nom_complet}")

conn.close()
