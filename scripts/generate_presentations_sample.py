#!/usr/bin/env python3
"""Generate presentations sample using Enricher on CSV exports.
Usage: python scripts/generate_presentations_sample.py -n 5 -i data/data_75_3.csv -o data/data_75_sample_out.csv
"""
import csv
import argparse
import sys
import os
from pathlib import Path

# Ensure project root is on sys.path so imports like `mvp.*` work when running this script directly
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from mvp.scrapers.enricher import Enricher
from mvp.scrapers.mistral_extractor import ExtractedEstablishment


def row_to_est(row):
    return ExtractedEstablishment(
        nom=row.get('nom') or row.get('name') or '',
        commune=row.get('commune') or '',
        code_postal=row.get('code_postal') or row.get('postal_code') or '',
        gestionnaire=row.get('gestionnaire') or '',
        adresse_l1=row.get('adresse_l1') or row.get('adresse') or '',
        telephone=row.get('telephone') or '',
        email=row.get('email') or '',
        site_web=row.get('site_web') or row.get('site') or '',
        sous_categories=row.get('sous_categories') or row.get('type') or 'Habitat inclusif',
        habitat_type=row.get('habitat_type') or 'habitat_partage',
        presentation=row.get('presentation') or '',
        departement=row.get('departement') or '',
        source=row.get('source') or '',
        date_extraction=row.get('date_extraction') or ''
    )


def est_to_row(est, original_row):
    out = dict(original_row)
    out['presentation'] = est.presentation or ''
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument('-n', '--count', type=int, default=5)
    p.add_argument('-i', '--input', default='data/data_75_3.csv')
    p.add_argument('-o', '--output', default='data/data_75_sample_out.csv')
    p.add_argument('-d', '--department', default='Paris')
    args = p.parse_args()

    # Read input CSV
    with open(args.input, encoding='utf-8-sig') as f:
        reader = list(csv.DictReader(f))

    if not reader:
        print('Fichier input vide ou introuvable:', args.input)
        return

    sample = reader[:args.count]
    ests = [row_to_est(r) for r in sample]

    enr = Enricher()
    enriched = enr.enrich_establishments(ests, args.department)

    # Print results
    print('\n--- Résultats échantillon ---')
    for i, est in enumerate(enriched, 1):
        name = est.nom or f"(ligne {i})"
        print(f"\n[{i}] {name}")
        if est.presentation:
            print(f"Présentation ({len(est.presentation)} car.):\n{est.presentation}\n")
        else:
            print("Aucune présentation générée.\n")

    # Save output CSV
    out_path = args.output
    with open(out_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=reader[0].keys())
        writer.writeheader()
        for est, orig in zip(enriched, sample):
            writer.writerow(est_to_row(est, orig))

    print(f"Échantillon sauvegardé: {out_path}")


if __name__ == '__main__':
    main()
