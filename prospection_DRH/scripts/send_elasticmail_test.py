from __future__ import annotations

import argparse
import os
from pathlib import Path

import requests


ELASTICMAIL_ENDPOINT_V2 = 'https://api.elasticemail.com/v2/email/send'


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


def build_body(salutation: str) -> str:
    return (
        f"{salutation},\n\n"
        "Ceci est un email de test ElasticMail pour valider la personnalisation des formules de politesse.\n"
        "Si vous recevez bien ce message, le setup d'envoi est opérationnel.\n\n"
        "Bien cordialement,\n"
        "Alex pour l'equipe Dashboard RH\n"
        "www.dashboard-rh.net\n"
        "07 54 59 88 45 · 06 98 39 70 66\n"
    )


def send_one(api_key: str, sender: str, reply_to: str, to_email: str, salutation: str, subject: str) -> tuple[bool, str]:
    payload = {
        'apikey': api_key,
        'from': sender,
        'fromName': 'Alex - Dashboard RH',
        'replyTo': reply_to,
        'to': to_email,
        'subject': subject,
        'bodyText': build_body(salutation),
    }
    try:
        r = requests.post(ELASTICMAIL_ENDPOINT_V2, data=payload, timeout=30)
    except requests.RequestException as exc:
        return False, f'network_error: {exc}'

    if r.status_code != 200:
        return False, f'http_{r.status_code}: {r.text[:200]}'

    try:
        data = r.json()
    except ValueError:
        return True, 'ok_200'

    if data.get('success'):
        tid = (data.get('data') or {}).get('transactionid', '')
        return True, f'ok tid={tid}'
    return False, f"api_error: {data.get('error', 'unknown')}"


def main() -> None:
    ap = argparse.ArgumentParser(description='Envoi de 2 emails de test via ElasticMail')
    ap.add_argument('--sender', default='alex@dashboard-rh.net')
    ap.add_argument('--reply-to', default='alexglele12@gmail.com')
    ap.add_argument('--subject', default='[TEST] Verification salutation - Dashboard RH')
    args = ap.parse_args()

    load_dotenv()
    api_key = (os.getenv('ELASTICMAIL_API_KEY') or '').strip()
    if not api_key:
        raise SystemExit('ELASTICMAIL_API_KEY manquante dans .env')

    tests = [
        ('alexglele12@gmail.com', 'Bonjour Monsieur Djegnonde'),
        ('patrick.danto@confidensia.fr', 'Bonjour Monsieur Danto'),
    ]

    for to_email, salutation in tests:
        ok, msg = send_one(api_key, args.sender, args.reply_to, to_email, salutation, args.subject)
        status = 'OK' if ok else 'FAIL'
        print(f'[{status}] {to_email} -> {msg}')


if __name__ == '__main__':
    main()
