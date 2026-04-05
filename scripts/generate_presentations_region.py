#!/usr/bin/env python3
"""Generate missing presentations for department CSVs.
Usage: python scripts/generate_presentations_region.py 78 91 92 93 94 95

Behavior:
- For each department code given, finds files matching data/data_{dept}_*.csv
- For each file, backs it up to .backup_before_gen, loads rows, selects rows where 'presentation' is empty,
  generates presentations with Enricher, inserts results into rows, and writes the file back.
"""
import csv
import glob
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Ensure stdout can handle UTF-8 characters on Windows consoles
if hasattr(sys, 'stdout') and hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

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


def process_file(path, dept_name=None):
    print(f'Processing {path}')
    backup = path + '.backup_before_gen'
    shutil.copy2(path, backup)
    print(f' Backed up -> {backup}')

    with open(path, encoding='utf-8-sig', newline='') as f:
        reader = list(csv.DictReader(f))
    if not reader:
        print(' Empty file, skipping')
        return
    rows = reader
    fieldnames = list(reader[0].keys())

    # Build establishments for rows missing presentation
    missing_idxs = [i for i, r in enumerate(rows) if not (r.get('presentation') and r.get('presentation').strip())]
    if not missing_idxs:
        print(' No missing presentations in this file')
        return

    ests = []
    for i in missing_idxs:
        ests.append(row_to_est(rows[i]))

    enr = Enricher()
    enriched = enr.enrich_establishments(ests, dept_name or '')

    # Put presentations back
    for idx, est in zip(missing_idxs, enriched):
        rows[idx]['presentation'] = est.presentation or ''

    # Write back to file (atomic)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8', newline='') as outf:
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    shutil.move(tmp, path)
    print(f' Wrote updated file -> {path} (presentations generated: {len(enriched)})')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Specify department codes, e.g. 78 91 92')
        sys.exit(1)
    depts = sys.argv[1:]
    for d in depts:
        pattern = os.path.join('data', f'data_{d}_*.csv')
        matches = [p for p in glob.glob(pattern) if not p.endswith('.backup_original') and not p.endswith('.backup_before_gen')]
        if not matches:
            print(f'No files found for dept {d} (pattern {pattern})')
            continue
        for p in matches:
            try:
                process_file(p, dept_name=d)
            except Exception as e:
                print(f'Error processing {p}: {e}')

    print('Done')
