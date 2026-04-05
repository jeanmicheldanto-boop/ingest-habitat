#!/usr/bin/env python3
"""Analyse le diagnostic de publication."""
import pandas as pd

df = pd.read_csv('outputs/publish_diagnosis_enrich_20260201_125958.csv')
non_pub = df[~df['can_publish']]

print(f"Total établissements: {len(df)}")
print(f"Publiables: {df['can_publish'].sum()}")
print(f"Non publiables: {len(non_pub)}\n")

if len(non_pub) > 0:
    print("Raisons de non-publication:")
    cols = ['missing_nom','missing_address','missing_commune','missing_code_postal',
            'missing_geom','missing_gestionnaire','missing_typage','invalid_email']
    for c in cols:
        cnt = non_pub[c].sum()
        if cnt > 0:
            print(f"  {c}: {cnt}")
