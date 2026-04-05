#!/usr/bin/env python3
"""Scan data files for specified department codes and report presentation counts."""
import csv
import glob
from collections import defaultdict

# Departments in Île-de-France
dept_codes = ['75','77','78','91','92','93','94','95']

results = defaultdict(lambda: {'rows':0,'missing':0,'present':0,'files':[]} )

for code in dept_codes:
    pattern = f'data/data_{code}_*.csv'
    files = glob.glob(pattern)
    for f in files:
        results[code]['files'].append(f)
        with open(f, encoding='utf-8-sig') as fh:
            reader = list(csv.DictReader(fh))
            for r in reader:
                results[code]['rows'] += 1
                p = (r.get('presentation') or '').strip()
                if p:
                    results[code]['present'] += 1
                else:
                    results[code]['missing'] += 1

# Print summary
for code in dept_codes:
    r = results[code]
    if not r['files']:
        print(f"Dept {code}: no files found")
    else:
        print(f"Dept {code}: files={len(r['files'])} rows={r['rows']} present={r['present']} missing={r['missing']}")
        for ff in r['files']:
            print(f"  - {ff}")
