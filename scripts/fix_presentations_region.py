#!/usr/bin/env python3
"""Fix presentation field for department files.
Usage: python scripts/fix_presentations_region.py 78 91 92 93 94 95

Behavior:
- For each department code passed, finds files matching
  data/data_{dept}_*_enriched.csv and processes them.
- For each enriched file:
  - Cleans `presentation` by collapsing whitespace/newlines/tabs.
  - Ensures `presentation` column is placed immediately after `eligibilite_avp` when present.
  - Backs up existing target (if present) to `{target}.backup_original`.
  - Writes fixed CSV to target filename (enriched filename with `_enriched` removed).
  - Removes the enriched file after replacement.
"""
import csv
import glob
import os
import shutil
import sys
from collections import OrderedDict


def clean_text(s):
    if s is None:
        return ""
    return " ".join(s.split())


def process_file(enriched_path):
    target = enriched_path.replace('_enriched', '')
    backup = None
    if os.path.exists(target):
        backup = target + '.backup_original'
        shutil.copy2(target, backup)
        print(f'Backed up existing {target} -> {backup}')

    tmp_path = target + '.tmp'

    with open(enriched_path, newline='', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        rows = list(reader)
        if not rows:
            print(f'No rows in {enriched_path}, skipping')
            return
        fieldnames = list(reader.fieldnames)

    # Clean presentations and compute new field order
    for r in rows:
        if 'presentation' in r:
            r['presentation'] = clean_text(r.get('presentation', ''))

    # Move presentation after eligibilite_avp if both exist
    if 'presentation' in fieldnames and 'eligibilite_avp' in fieldnames:
        new_fields = []
        for f in fieldnames:
            if f == 'presentation':
                continue
            new_fields.append(f)
            if f == 'eligibilite_avp':
                new_fields.append('presentation')
        # ensure presentation included
        if 'presentation' not in new_fields:
            new_fields.append('presentation')
        fieldnames = new_fields

    # Write tmp then replace
    with open(tmp_path, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            # Ensure all keys exist
            out_row = {k: r.get(k, '') for k in fieldnames}
            writer.writerow(out_row)

    shutil.move(tmp_path, target)
    print(f'Wrote fixed file -> {target}')
    try:
        os.remove(enriched_path)
        print(f'Removed enriched file {enriched_path}')
    except Exception:
        pass


def process_plain(path):
    # Backup original
    backup = path + '.backup_original'
    shutil.copy2(path, backup)
    print(f'Backed up {path} -> {backup}')

    tmp_path = path + '.tmp'
    with open(path, newline='', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        rows = list(reader)
        if not rows:
            print(f'No rows in {path}, skipping')
            return
        fieldnames = list(reader.fieldnames)

    for r in rows:
        if 'presentation' in r:
            r['presentation'] = clean_text(r.get('presentation', ''))

    if 'presentation' in fieldnames and 'eligibilite_avp' in fieldnames:
        new_fields = []
        for f in fieldnames:
            if f == 'presentation':
                continue
            new_fields.append(f)
            if f == 'eligibilite_avp':
                new_fields.append('presentation')
        if 'presentation' not in new_fields:
            new_fields.append('presentation')
        fieldnames = new_fields

    with open(tmp_path, 'w', newline='', encoding='utf-8') as outf:
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            out_row = {k: r.get(k, '') for k in fieldnames}
            writer.writerow(out_row)

    shutil.move(tmp_path, path)
    print(f'Wrote fixed file -> {path}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Specify at least one department code, e.g. 78 91 92')
        sys.exit(1)
    depts = sys.argv[1:]
    for d in depts:
        pattern = os.path.join('data', f'data_{d}_*_enriched.csv')
        matches = glob.glob(pattern)
        if not matches:
            print(f'No enriched files found for dept {d} (pattern {pattern})')
        else:
            for p in matches:
                try:
                    process_file(p)
                except Exception as e:
                    print(f'Error processing {p}: {e}')

        # Also process plain data files for this dept (if any)
        plain_pattern = os.path.join('data', f'data_{d}_*.csv')
        plain_matches = [p for p in glob.glob(plain_pattern) if '_enriched' not in p and not p.endswith('.backup_original')]
        for p in plain_matches:
            try:
                process_plain(p)
            except Exception as e:
                print(f'Error processing plain file {p}: {e}')

    print('Done')
