from __future__ import annotations

import argparse
import csv
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

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


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v


def read_csv(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        return rows, (r.fieldnames or [])


def write_csv(path: Path, rows: List[Dict[str, str]], fields: List[str]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def is_deliverable(row: Dict[str, str]) -> bool:
    return (row.get("bouncer_status") or "").strip().lower() == "deliverable"


def is_already_sent(row: Dict[str, str]) -> bool:
    return (row.get("send_status") or "").strip().lower() == "sent"


def pick_email(row: Dict[str, str]) -> str:
    return (row.get("bouncer_email_checked") or "").strip()


def pick_salutation(row: Dict[str, str]) -> str:
    s = (row.get("formule_appel") or "").strip()
    return s if s else "Bonjour Madame / Monsieur"


def send_email(api_key: str, sender: str, reply_to: str, to_email: str, subject: str, body: str) -> Tuple[bool, str]:
    payload = {
        "apikey": api_key,
        "from": sender,
        "fromName": "Alex - Dashboard RH",
        "replyTo": reply_to,
        "to": to_email,
        "subject": subject,
        "bodyText": body,
    }
    try:
        r = requests.post(ELASTICMAIL_ENDPOINT_V2, data=payload, timeout=30)
    except requests.RequestException as exc:
        return False, f"network_error: {exc}"

    if r.status_code != 200:
        return False, f"http_{r.status_code}: {r.text[:200]}"

    try:
        data = r.json()
    except ValueError:
        return True, "ok_200"

    if data.get("success"):
        tid = (data.get("data") or {}).get("transactionid", "")
        return True, f"ok tid={tid}"
    return False, f"api_error: {data.get('error', 'unknown')}"


def process_file(
    path: Path,
    per_file: int,
    template_body: str,
    subject: str,
    sender: str,
    reply_to: str,
    api_key: str,
    dry_run: bool,
    delay: float,
) -> Tuple[int, int, int]:
    rows, fields = read_csv(path)
    extra_fields = ["send_status", "send_at", "send_message", "send_campaign"]
    out_fields = fields + [f for f in extra_fields if f not in fields]

    campaign_id = datetime.now().strftime("wave_%Y%m%d_%H%M%S")
    selected_idx: List[int] = []
    for i, row in enumerate(rows):
        if len(selected_idx) >= per_file:
            break
        if not is_deliverable(row):
            continue
        if is_already_sent(row):
            continue
        if not pick_email(row):
            continue
        selected_idx.append(i)

    sent_ok = 0
    sent_fail = 0

    for idx in selected_idx:
        row = rows[idx]
        to_email = pick_email(row)
        salutation = pick_salutation(row)
        body = template_body.format(salutation=salutation)

        if dry_run:
            ok, msg = True, "dry_run"
        else:
            ok, msg = send_email(api_key, sender, reply_to, to_email, subject, body)

        if ok:
            row["send_status"] = "sent"
            row["send_at"] = datetime.now().isoformat(timespec="seconds")
            row["send_message"] = msg
            row["send_campaign"] = campaign_id
            sent_ok += 1
        else:
            row["send_status"] = "failed"
            row["send_at"] = datetime.now().isoformat(timespec="seconds")
            row["send_message"] = msg
            row["send_campaign"] = campaign_id
            sent_fail += 1

        if delay > 0:
            time.sleep(delay)

    write_csv(path, rows, out_fields)
    return len(selected_idx), sent_ok, sent_fail


def main() -> None:
    ap = argparse.ArgumentParser(description="Send daily wave and mark records as sent")
    ap.add_argument("--collectivites", default="data/mails_collectivites_FINAl_bouncer_civilite.csv")
    ap.add_argument("--pme", default="data/mails_pme_FINAL_bouncer_civilite.csv")
    ap.add_argument("--per-file", type=int, default=20)
    ap.add_argument("--sender", default="alex@dashboard-rh.net")
    ap.add_argument("--reply-to", default="alexglele12@gmail.com")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--delay", type=float, default=0.8)
    args = ap.parse_args()

    load_dotenv()
    api_key = (os.getenv("ELASTICMAIL_API_KEY") or "").strip()
    if not api_key and not args.dry_run:
        raise SystemExit("ELASTICMAIL_API_KEY manquante dans .env")

    c_total, c_ok, c_fail = process_file(
        path=Path(args.collectivites),
        per_file=args.per_file,
        template_body=COLLECTIVITES_BODY,
        subject="CollectivRH - Echange 15 minutes",
        sender=args.sender,
        reply_to=args.reply_to,
        api_key=api_key,
        dry_run=args.dry_run,
        delay=args.delay,
    )

    p_total, p_ok, p_fail = process_file(
        path=Path(args.pme),
        per_file=args.per_file,
        template_body=PME_BODY,
        subject="TalentTracker - Echange 15 minutes",
        sender=args.sender,
        reply_to=args.reply_to,
        api_key=api_key,
        dry_run=args.dry_run,
        delay=args.delay,
    )

    print("[SUMMARY]")
    print(f"collectivites selected={c_total} sent={c_ok} failed={c_fail}")
    print(f"pme          selected={p_total} sent={p_ok} failed={p_fail}")
    print(f"mode={'DRY_RUN' if args.dry_run else 'REAL'}")


if __name__ == "__main__":
    main()
