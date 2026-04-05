from __future__ import annotations

import os
from pathlib import Path
from typing import Tuple

import requests

ELASTICMAIL_ENDPOINT_V2 = "https://api.elasticemail.com/v2/email/send"

COLLECTIVITES_BODY = """{salutation},

Dans un contexte de tensions sur l'emploi public, le suivi des candidatures et le reporting RH deviennent des leviers essentiels du service rendu aux habitants et de la maitrise budgetaire.
Or, les logiciels de gestion du recrutement (ATS) restent couteux, et leur integration dans les processus specifiques des collectivites — RIFSEEP, contraintes statutaires, circuits de validation — rarement satisfaisante.
C'est pourquoi nous avons concu CollectivRH : une solution simple, securisee, pensee par des professionnels RH du secteur public. Elle couvre la gestion des candidatures, l'integration des agents, le pilotage budgetaire en coherence avec vos cadres de gestion, et produit des indicateurs directement exploitables par votre direction generale et vos elus.
Nous assurons la reprise de vos donnees existantes, dans le respect de la conformite RGPD.
Seriez-vous disponible pour un echange de 15 minutes, par telephone ou en visio, afin que je vous fasse une presentation concrete ? Sans engagement, bien sur.
Je vous souhaite une tres bonne journee.

Bien cordialement,
Alex pour l'equipe Dashboard RH
www.dashboard-rh.net
07 54 59 88 45 - 06 98 39 70 66

P.S. Nous accompagnons egalement les recrutements sur profils en tension, avec un suivi humain jusqu'a la prise de poste. N'hesitez pas a nous solliciter.
"""

PME_BODY = """{salutation},

Dans un marche de l'emploi tendu, le suivi des candidats et le pilotage RH font souvent la difference entre une organisation qui recrute efficacement et une qui subit ses delais.
Or, les ATS du marche restent couteux et rarement adaptes aux realites des PME, start-up et ESN.
C'est a partir de ce constat que nous avons concu TalentTracker : une solution simple, securisee, pensee par des professionnels RH pour un usage quotidien. Elle couvre l'ensemble du cycle de recrutement — gestion des candidatures, onboarding, pilotage du budget recrutement — et produit des tableaux de bord analytiques directement exploitables.
Nous assurons la reprise de vos donnees existantes, dans le respect de la conformite RGPD.
Seriez-vous disponible pour un echange de 15 minutes, par telephone ou en visio, afin que je vous fasse une demonstration concrete ? Sans engagement, bien sur.
Je vous souhaite une tres bonne journee.

Bien cordialement,
Alex pour l'equipe Dashboard RH
www.dashboard-rh.net
07 54 59 88 45 - 06 98 39 70 66

P.S. Nous accompagnons egalement les recrutements sur profils en tension, avec un suivi humain jusqu'a la prise de poste. N'hesitez pas a nous solliciter.
"""


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


def send_one(api_key: str, sender: str, reply_to: str, to_email: str, subject: str, body: str) -> Tuple[bool, str]:
    payload = {
        'apikey': api_key,
        'from': sender,
        'fromName': 'Alex - Dashboard RH',
        'replyTo': reply_to,
        'to': to_email,
        'subject': subject,
        'bodyText': body,
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
    load_dotenv()
    api_key = (os.getenv('ELASTICMAIL_API_KEY') or '').strip()
    if not api_key:
        raise SystemExit('ELASTICMAIL_API_KEY manquante')

    sender = 'alex@dashboard-rh.net'
    reply_to = 'alexglele12@gmail.com'

    recipients = [
        ('alexglele12@gmail.com', 'Bonjour Monsieur Djegnonde'),
        ('a.djegnonde@mairie-champigny94.fr', 'Bonjour Monsieur Djegnonde'),
        ('patrick.danto@confidensia.fr', 'Bonjour Monsieur Danto'),
    ]

    campaigns = [
        ('CollectivRH - Echange 15 minutes', COLLECTIVITES_BODY),
        ('TalentTracker - Echange 15 minutes', PME_BODY),
    ]

    for to_email, salutation in recipients:
        for subject, template in campaigns:
            body = template.format(salutation=salutation)
            ok, msg = send_one(api_key, sender, reply_to, to_email, subject, body)
            status = 'OK' if ok else 'FAIL'
            print(f'[{status}] to={to_email} subject={subject} -> {msg}')


if __name__ == '__main__':
    main()
