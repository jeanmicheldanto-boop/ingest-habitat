import pandas as pd

df = pd.read_excel('outputs/prospection_250_FINAL_FORMATE.xlsx')

print(f'Nombre de lignes: {len(df)}')
print(f'\nColonnes disponibles:')
for i, col in enumerate(df.columns, 1):
    print(f'{i:2d}. {col}')

print(f'\nEchantillon de données (3 premières lignes):')
print(df.head(3).to_string())

print(f'\n\nColonnes emails et patterns:')
email_cols = [c for c in df.columns if 'email' in c.lower() or 'pattern' in c.lower()]
if email_cols:
    print(df[email_cols].head(5).to_string())
