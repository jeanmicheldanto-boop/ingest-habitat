import pandas as pd

df = pd.read_excel('outputs/prospection_250_dirigeants_final.xlsx')

print(f'Total: {len(df)} gestionnaires')
print(f'Dirigeants: {df["dirigeant_nom"].notna().sum()}')
print(f'Emails reconstruits: {df["email_dirigeant_1"].notna().sum()}')

print(f'\nColonnes ({len(df.columns)}):')
for col in df.columns:
    print(f'  - {col}')
