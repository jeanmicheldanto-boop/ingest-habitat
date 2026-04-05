import pandas as pd

df = pd.read_excel('outputs/prospection_250_gestionnaires.xlsx')

print("="*60)
print("RAPPORT D'ENRICHISSEMENT - 250 GESTIONNAIRES")
print("="*60)
print(f"\nTotal gestionnaires enrichis: {len(df)}")

print("\n" + "="*60)
print("TAUX DE REMPLISSAGE PAR CHAMP")
print("="*60)

fields = {
    'nom_public': 'Nom public normalisé',
    'acronyme': 'Acronyme',
    'site_web': 'Site web officiel',
    'domaine': 'Domaine',
    'email_contact': 'Email contact principal',
    'emails_generiques': 'Emails génériques',
    'url_contact': 'URL page contact',
    'url_mentions_legales': 'URL mentions légales',
    'dirigeant_nom': 'Nom du dirigeant',
    'dirigeant_titre': 'Titre du dirigeant',
}

for field, label in fields.items():
    count = df[field].notna().sum()
    pct = count / len(df) * 100
    print(f"{label:30} {count:3}/{len(df)} ({pct:5.1f}%)")

print("\n" + "="*60)
print("QUALITÉ - SCORE CONFIDENCE")
print("="*60)
print(f"Moyenne générale: {df['confidence'].mean():.1f}")
print(f"Médiane: {df['confidence'].median():.1f}")
print(f"\nRépartition:")
for conf in sorted(df['confidence'].unique()):
    count = (df['confidence'] == conf).sum()
    pct = count / len(df) * 100
    print(f"  Confidence {conf:3}: {count:3} gestionnaires ({pct:5.1f}%)")

print("\n" + "="*60)
print("DIRIGEANTS - SCORE CONFIDENCE")
print("="*60)
dir_found = df[df['dirigeant_nom'].notna()]
if len(dir_found) > 0:
    print(f"Dirigeants trouvés: {len(dir_found)}/{len(df)} ({len(dir_found)/len(df)*100:.1f}%)")
    print(f"Confidence moyenne (si trouvé): {dir_found['dirigeant_confidence'].mean():.1f}")
    print(f"\nRépartition confidence dirigeants:")
    for conf in sorted(df['dirigeant_confidence'].unique()):
        if conf > 0:
            count = (df['dirigeant_confidence'] == conf).sum()
            pct = count / len(df) * 100
            print(f"  Confidence {conf:3}: {count:3} dirigeants ({pct:5.1f}%)")
else:
    print("Aucun dirigeant trouvé")

print("\n" + "="*60)
print("EXEMPLES DE RÉSULTATS (10 premiers)")
print("="*60)
cols_display = ['gestionnaire_nom', 'nom_public', 'acronyme', 'site_web', 'dirigeant_nom', 'dirigeant_titre', 'confidence']
print(df[cols_display].head(10).to_string(index=False))

print("\n" + "="*60)
print("CAS À VÉRIFIER (Confidence < 75)")
print("="*60)
low_conf = df[df['confidence'] < 75]
if len(low_conf) > 0:
    print(f"\n{len(low_conf)} gestionnaires avec confidence < 75:")
    print(low_conf[['gestionnaire_nom', 'site_web', 'domaine', 'confidence']].to_string(index=False))
else:
    print("Aucun cas problématique!")

print("\n" + "="*60)
print("TOP 10 - Résultats les plus complets")
print("="*60)
# Score: a un nom public + acronyme + email + dirigeant
df['completeness'] = (
    df['nom_public'].notna().astype(int) * 20 +
    df['acronyme'].notna().astype(int) * 10 +
    df['email_contact'].notna().astype(int) * 30 +
    df['dirigeant_nom'].notna().astype(int) * 40
)
top10 = df.nlargest(10, 'completeness')
cols_top = ['gestionnaire_nom', 'nom_public', 'acronyme', 'email_contact', 'dirigeant_nom', 'confidence']
print(top10[cols_top].to_string(index=False))

print("\n" + "="*60)
print("FICHIER GÉNÉRÉ")
print("="*60)
print(f"📁 outputs/prospection_250_gestionnaires.xlsx")
print(f"📊 {len(df)} lignes × {len(df.columns)} colonnes")
print("✅ Prêt pour import CRM / campagne de prospection")
