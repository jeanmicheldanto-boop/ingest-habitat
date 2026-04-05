#!/usr/bin/env python3
"""Fix presentation placement and clean newlines for Paris enriched CSVs.
Usage: python scripts/fix_paris_presentations.py
This will read files named data/data_75_*_enriched.csv and data/data_75_sample_out.csv
and write fixed versions replacing the originals with a _fixed suffix before replacing.
"""
import csv
import glob
import re
from pathlib import Path

# Desired field order (matches pipeline_v3_cli._write_csv_file)
FIELDNAMES = [
    'nom', 'commune', 'code_postal', 'gestionnaire', 'adresse_l1',
    'telephone', 'email', 'site_web', 'sous_categories', 'habitat_type',
    'eligibilite_avp', 'presentation', 'departement', 'source',
    'date_extraction', 'public_cible'
]

PATTERN = 'data/data_75_*_enriched.csv'
FILES = glob.glob(PATTERN) + ['data/data_75_sample_out.csv']

clean_re = re.compile(r"\s+", flags=re.UNICODE)

for f in FILES:
    path = Path(f)
    if not path.exists():
        continue
    print(f"Processing {f}...")
    rows = []
    with path.open(encoding='utf-8-sig', newline='') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            # Normalize keys to expected fieldnames: if presentation exists under other key, try to find it
            # Clean presentation: replace internal newlines/tabs with spaces and collapse spaces
            pres = ''
            # Try common keys
            for key in ['presentation', 'description', 'présentation']:
                if key in r and r[key] is not None and str(r[key]).strip():
                    pres = str(r[key])
                    break
            # If still empty, check any long text in row values that seems like a presentation
            if not pres:
                for k, v in r.items():
                    if v and len(v) > 80 and '\n' in v:
                        pres = v
                        # remove from original field to avoid duplication
                        r[k] = ''
                        break
            # Clean presentation text
            if pres:
                pres_clean = pres.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
                pres_clean = clean_re.sub(' ', pres_clean).strip()
            else:
                pres_clean = ''

            # Build new row with desired order
            newr = {k: '' for k in FIELDNAMES}
            for k, v in r.items():
                key = k
                if key not in newr:
                    # skip unknown keys
                    continue
                newr[key] = v
            # Ensure presentation placed after eligibilite_avp
            newr['presentation'] = pres_clean

            rows.append(newr)

    # Write fixed file (temporary), then replace original
    out_path = path.with_name(path.stem + '_fixed.csv')
    with out_path.open('w', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDNAMES)
        writer.writeheader()
        for r in rows:
            # clean any stray whitespace in all fields
            clean_row = {k: (re.sub(r"\s+", ' ', (v or '')).strip()) for k, v in r.items()}
            writer.writerow(clean_row)

    # Replace original file
    backup = path.with_name(path.stem + '_fixed_backup.csv')
    path.rename(backup)
    out_path.rename(path)
    print(f"Replaced {f} (backup at {backup})")

print('Done')
