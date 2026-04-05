from __future__ import annotations

import argparse
import csv
import json
import os
import re
from pathlib import Path
from typing import Dict, List, Tuple

import requests

MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"


def load_dotenv(path: Path = Path('.env')) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding='utf-8').splitlines():
        s = line.strip()
        if not s or s.startswith('#') or '=' not in s:
            continue
        k, v = s.split('=', 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def read_csv(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    with path.open('r', encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        rows = list(r)
        return rows, (r.fieldnames or [])


def write_csv(path: Path, rows: List[Dict[str, str]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def normalize_name(s: str) -> str:
    s = (s or '').strip()
    s = re.sub(r'\s+', ' ', s)
    return s


def detect_name_fields(fields: List[str]) -> Tuple[str, str]:
    candidates_first = ['drh_prenom', 'prenom', 'first_name']
    candidates_last = ['drh_nom', 'nom', 'last_name']

    first = next((c for c in candidates_first if c in fields), '')
    last = next((c for c in candidates_last if c in fields), '')
    if not first or not last:
        raise SystemExit(f'Impossible de trouver les colonnes prenom/nom dans: {fields}')
    return first, last


def pick_person_name(row: Dict[str, str], first_col: str, last_col: str) -> Tuple[str, str]:
    # Default target is DRH for PME and most files.
    first = normalize_name(row.get(first_col, ''))
    last = normalize_name(row.get(last_col, ''))

    # If source indicates DGS or DRH name is missing, fallback to DGS fields when available.
    source = (row.get('bouncer_email_source') or '').strip().lower()
    dgs_first = normalize_name(row.get('dgs_prenom', ''))
    dgs_last = normalize_name(row.get('dgs_nom', ''))

    if source.startswith('dgs_') and dgs_first:
        return dgs_first, dgs_last

    if not first and dgs_first:
        return dgs_first, dgs_last

    return first, last


def chunk(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def extract_json(text: str) -> Dict[str, str]:
    text = text.strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return {str(k): str(v) for k, v in obj.items()}
    except Exception:
        pass

    m = re.search(r'\{[\s\S]*\}', text)
    if not m:
        return {}
    try:
        obj = json.loads(m.group(0))
        if isinstance(obj, dict):
            return {str(k): str(v) for k, v in obj.items()}
    except Exception:
        return {}
    return {}


def classify_with_mistral(names: List[str], api_key: str, model: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    }

    for batch in chunk(names, 120):
        prompt = (
            'Classe ces prenoms pour une formule de politesse email en francais. '\
            'Retourne uniquement un JSON strict {"prenom":"M|F|U"}. '\
            'M=monsieur, F=madame, U=inconnu. '\
            'Conserve exactement la casse des cles fournies. '\
            f'Prenoms: {json.dumps(batch, ensure_ascii=False)}'
        )
        payload = {
            'model': model,
            'temperature': 0,
            'messages': [
                {'role': 'system', 'content': 'Tu reponds uniquement en JSON valide.'},
                {'role': 'user', 'content': prompt},
            ],
        }

        r = requests.post(MISTRAL_URL, headers=headers, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        content = data['choices'][0]['message']['content']
        if isinstance(content, list):
            content = ''.join(part.get('text', '') for part in content if isinstance(part, dict))

        parsed = extract_json(str(content))
        for name in batch:
            val = str(parsed.get(name, 'U')).strip().upper()
            out[name] = val if val in {'M', 'F', 'U'} else 'U'

    return out


def fallback_guess(names: List[str]) -> Dict[str, str]:
    # Leger fallback: inconnu par defaut, quelques prenoms tres frequents.
    female = {
        'marie', 'sophie', 'anne', 'caroline', 'julie', 'pauline', 'audrey', 'alice', 'chloe', 'nathalie',
        'laurence', 'delphine', 'sabine', 'cecile', 'stephanie', 'megane', 'isabelle', 'aline', 'lila', 'lea',
    }
    male = {
        'jean', 'pierre', 'thomas', 'nicolas', 'philippe', 'alexandre', 'francois', 'hugo', 'bruno', 'didier',
        'benjamin', 'loic', 'cyril', 'patrice', 'vincent', 'remi', 'thierry', 'mathieu', 'wayne', 'james',
    }
    out: Dict[str, str] = {}
    for n in names:
        low = n.lower()
        if low in female:
            out[n] = 'F'
        elif low in male:
            out[n] = 'M'
        else:
            out[n] = 'U'
    return out


def civilite_from_code(code: str) -> str:
    if code == 'M':
        return 'Monsieur'
    if code == 'F':
        return 'Madame'
    return 'Madame / Monsieur'


def main() -> None:
    ap = argparse.ArgumentParser(description='Qualification legere de civilite sur CSV prospection')
    ap.add_argument('--inputs', nargs='+', required=True)
    ap.add_argument('--use-mistral', action='store_true')
    ap.add_argument('--output-suffix', default='_civilite')
    args = ap.parse_args()

    load_dotenv()
    api_key = (os.getenv('MISTRAL_API_KEY') or '').strip()
    model = (os.getenv('MISTRAL_MODEL') or 'mistral-small-latest').strip()

    # Collect unique first names across files.
    files_rows = []
    unique_first_names = set()
    for p in args.inputs:
        path = Path(p)
        rows, fields = read_csv(path)
        first_col, last_col = detect_name_fields(fields)
        files_rows.append((path, rows, fields, first_col, last_col))
        for row in rows:
            first, last = pick_person_name(row, first_col, last_col)
            if first:
                unique_first_names.add(first)

    sorted_names = sorted(unique_first_names)
    if args.use_mistral and api_key:
        print(f'[INFO] Classification Mistral de {len(sorted_names)} prenoms')
        mapping = classify_with_mistral(sorted_names, api_key, model)
    else:
        print(f'[INFO] Classification fallback de {len(sorted_names)} prenoms')
        mapping = fallback_guess(sorted_names)

    for path, rows, fields, first_col, last_col in files_rows:
        extra = ['civilite_guess', 'formule_appel']
        out_fields = fields + [c for c in extra if c not in fields]

        for row in rows:
            first, last = pick_person_name(row, first_col, last_col)
            code = mapping.get(first, 'U') if first else 'U'
            civ = civilite_from_code(code)

            row['civilite_guess'] = civ
            if civ == 'Madame / Monsieur':
                row['formule_appel'] = f'Bonjour {civ}'
            else:
                surname = last if last else first
                row['formule_appel'] = f'Bonjour {civ} {surname}'.strip()

        out_path = path.with_name(f'{path.stem}{args.output_suffix}{path.suffix}')
        write_csv(out_path, rows, out_fields)
        print(f'[INFO] output: {out_path}')


if __name__ == '__main__':
    main()
