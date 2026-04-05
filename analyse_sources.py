import pandas as pd

print('=== ANALYSE DÉTAILLÉE SOURCES ===')
df = pd.read_csv('data/data_64_1.csv')

print(f'Total: {len(df)} établissements\n')

print('=== RÉPARTITION PAR SOURCE ===')
sources = df['source'].value_counts()
for source, count in sources.items():
    if 'pour-les-personnes-agees.gouv.fr' in source:
        print(f'📋 OFFICIEL: {count} établissements')
    else:
        print(f'🌐 ALTERNATIF: {count} de {source[:60]}...')

print('\n=== DÉTAIL ÉTABLISSEMENTS ALTERNATIFS ===')
alternatifs = df[~df['source'].str.contains('pour-les-personnes-agees.gouv.fr', case=False, na=False)]
for i, row in alternatifs.iterrows():
    print(f'{i+1}. {row["nom"]} - {row["gestionnaire"]} ({row["sous_categories"]})')
    print(f'   Source: {row["source"]}')
    print()

print(f'Total alternatifs: {len(alternatifs)}')