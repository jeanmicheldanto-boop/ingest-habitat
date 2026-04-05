import pandas as pd

df = pd.read_excel('outputs/prospection_250_TEST_V2.xlsx')

print('📊 RÉSULTATS DU TEST V2\n')
print('=' * 70)

print(f'\n📈 STATISTIQUES GLOBALES:')
print(f'   Total gestionnaires: {len(df)}')
print(f'   Emails dirigeants générés: {df["Email Dirigeant 1"].notna().sum()}')
print(f'   Emails organisation trouvés: {df["Email Organisation"].notna().sum()}')
print(f'   Patterns détectés (conf > 0%): {(df["Conf. Email"] > 0).sum()}')

print(f'\n📧 DÉTAIL PATTERNS DÉTECTÉS (10 premiers):')
print('=' * 70)
cols = ["nom_public", "Pattern Email", "Conf. Email", "Email Organisation", "Type Email Org"]
print(df[df["Conf. Email"] > 0][cols].head(10).to_string())

print(f'\n✅ VALIDATION CONSERVATION DES COLONNES:')
print('=' * 70)
print(f'   LinkedIn URLs: {df["dirigeant_linkedin_url"].notna().sum()} présents')
print(f'   Type principal: {df["dominante_type"].notna().sum()} présents')
print(f'   Top 5 types: {df["dominante_top5"].notna().sum()} présents')
print(f'   Sites web: {df["site_web"].notna().sum()} présents')

print(f'\n🎯 ÉCHANTILLON RÉSULTATS (3 premiers):')
print('=' * 70)
display_cols = ["nom_public", "dirigeant_nom", "Email Dirigeant 1", "Email Organisation", "Civilité"]
print(df[display_cols].head(3).to_string())

print(f'\n💾 TOTAL COLONNES: {len(df.columns)}')
print('=' * 70)
