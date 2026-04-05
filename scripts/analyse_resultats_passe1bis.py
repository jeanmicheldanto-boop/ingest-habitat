import pandas as pd
import os

# Vérifier fichiers
files = [
    'outputs/prospection_250_dirigeants_complet.xlsx',
    'outputs/prospection_250_dirigeants_complet_v2.xlsx'
]

print("=" * 60)
print("ANALYSE RÉSULTATS PASSE 1bis")
print("=" * 60)

for f in files:
    print(f"{f}: {'✅' if os.path.exists(f) else '❌'}")

# Charger fichier v2 (avec PASSE 1bis)
df = pd.read_excel('outputs/prospection_250_dirigeants_complet_v2.xlsx')

print(f"\nTotal gestionnaires: {len(df)}")
print(f"Dirigeants trouvés: {df['dirigeant_nom'].notna().sum()} ({df['dirigeant_nom'].notna().sum()/len(df)*100:.1f}%)")

# Analyser sources
sources = df[df['dirigeant_nom'].notna()]['dirigeant_source'].value_counts()
print("\nDirigéants par source:")
for source, count in sources.items():
    print(f"  {source}: {count}")

# Compter noms avec initiales restants
linkedin_snippet = df[df['dirigeant_source'] == 'linkedin_snippet']
initiales = linkedin_snippet[linkedin_snippet['dirigeant_nom'].str.match(r'^[A-Z][a-z]+\s[A-Z]$', na=False)]
print(f"\nNoms avec initiales restants: {len(initiales)}")

# Noms complets par PASSE 1bis
llm_complete = df[df['dirigeant_source'] == 'linkedin_llm_complete']
print(f"Noms complétés par PASSE 1bis: {len(llm_complete)} / 66 ({len(llm_complete)/66*100:.1f}%)")

print("\n" + "=" * 60)
print("EXEMPLES PASSE 1bis (initiale → nom complet)")
print("=" * 60)
for _, row in llm_complete.head(10).iterrows():
    nom = row['dirigeant_nom']
    org = row['gestionnaire_nom'][:50]
    conf = row['dirigeant_confidence']
    print(f"✅ {nom} - {org}... (conf. {conf})")

print(f"\nFichier final: outputs/prospection_250_dirigeants_complet_v2.xlsx")
