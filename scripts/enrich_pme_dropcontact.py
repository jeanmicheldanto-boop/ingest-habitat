from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

import requests

# Ensure workspace root is importable when running this script directly.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.enrich_communes_idf_contacts import load_dotenv_if_present

POST_URL = "https://api.dropcontact.com/v1/enrich/all"
GET_URL_PREFIX = "https://api.dropcontact.com/v1/enrich/all/"

GENERIC_LOCAL_PARTS = {"contact", "info", "hello", "rh", "recrutement", "jobs", "career", "careers"}


def read_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(f)]


def normalize_domain(value: str) -> str:
    v = (value or "").strip().lower()
    if not v:
        return ""
    if v.startswith("http://"):
        v = v[len("http://") :]
    if v.startswith("https://"):
        v = v[len("https://") :]
    if v.startswith("www."):
        v = v[len("www.") :]
    return v.strip("/")


def choose_best_email(email_items: List[Dict[str, str]]) -> Tuple[str, str]:
    if not email_items:
        return "", ""

    def score(item: Dict[str, str]) -> int:
        email = (item.get("email") or "").strip().lower()
        qual = (item.get("qualification") or "").strip().lower()
        local = email.split("@")[0] if "@" in email else ""

        if qual == "nominative@pro":
            return 100
        if qual == "catch_all@pro":
            return 80
        if qual == "generic@pro":
            return 30 if local in GENERIC_LOCAL_PARTS else 45
        if qual.endswith("@pro"):
            return 20
        return 0

    best = max(email_items, key=score)
    return (best.get("email") or "").strip(), (best.get("qualification") or "").strip()


def build_contact_payload(row: Dict[str, str], row_id: str) -> Dict[str, object] | None:
    first_name = (row.get("drh_prenom") or row.get("first_name") or "").strip()
    last_name = (row.get("drh_nom") or row.get("last_name") or "").strip()
    company = (row.get("nom_entreprise") or row.get("company") or "").strip()

    if not first_name or not last_name or not company:
        return None

    website = normalize_domain(row.get("domain", ""))
    if website:
        website = f"https://{website}"

    email_seed = (row.get("drh_email_public") or row.get("drh_email_reconstitue") or row.get("email") or "").strip()

    payload: Dict[str, object] = {
        "first_name": first_name,
        "last_name": last_name,
        "company": company,
        "custom_fields": {
            "row_id": row_id,
            "siren": (row.get("siren") or "").strip(),
        },
    }
    if website:
        payload["website"] = website
    if email_seed:
        payload["email"] = email_seed
    return payload


def post_batch(api_key: str, batch_contacts: List[Dict[str, object]]) -> Tuple[str, int]:
    resp = requests.post(
        POST_URL,
        json={"data": batch_contacts, "siren": True, "language": "fr"},
        headers={"Content-Type": "application/json", "X-Access-Token": api_key},
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("error") or not body.get("success"):
        raise RuntimeError(f"Dropcontact POST error: {json.dumps(body, ensure_ascii=False)}")
    request_id = (body.get("request_id") or "").strip()
    if not request_id:
        raise RuntimeError(f"Dropcontact request_id manquant: {json.dumps(body, ensure_ascii=False)}")
    credits_left = int(body.get("credits_left", -1))
    return request_id, credits_left


def poll_results(api_key: str, request_id: str, poll_seconds: int, max_wait_seconds: int) -> Dict[str, object]:
    deadline = time.time() + max_wait_seconds
    last_body: Dict[str, object] = {}

    while time.time() < deadline:
        resp = requests.get(
            f"{GET_URL_PREFIX}{request_id}",
            headers={"X-Access-Token": api_key},
            timeout=60,
        )
        resp.raise_for_status()
        body = resp.json()
        last_body = body

        if body.get("success") and not body.get("error"):
            return body

        reason = str(body.get("reason", "")).lower()
        if "not ready" in reason or "try again" in reason:
            time.sleep(max(1, poll_seconds))
            continue

        time.sleep(max(1, poll_seconds))

    raise TimeoutError(f"Timeout Dropcontact request_id={request_id}, last_body={json.dumps(last_body, ensure_ascii=False)}")


def write_output(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Valide/enrichit contacts DRH PME via Dropcontact")
    parser.add_argument("--input", default="data/pme_idf_50_500_drh_serper_resume307.csv")
    parser.add_argument("--output", default="data/pme_idf_50_500_dropcontact_test.csv")
    parser.add_argument("--limit", type=int, default=60)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--max-wait-seconds", type=int, default=600)
    args = parser.parse_args()

    load_dotenv_if_present(Path(".env"))
    api_key = os.getenv("DROPCONTACT_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("DROPCONTACT_API_KEY manquante dans .env")

    input_path = Path(args.input)
    rows = read_rows(input_path)
    if args.limit > 0:
        rows = rows[: args.limit]

    if not rows:
        raise SystemExit(f"Aucune ligne dans {input_path}")

    base_fields = list(rows[0].keys())
    extra_fields = [
        "dropcontact_status",
        "dropcontact_request_id",
        "dropcontact_email_selected",
        "dropcontact_email_selected_qualification",
        "dropcontact_email_candidates",
        "dropcontact_credits_left_after_post",
    ]
    out_fields = base_fields + [f for f in extra_fields if f not in base_fields]

    for row in rows:
        row["dropcontact_status"] = "skipped_missing_required_fields"
        row["dropcontact_request_id"] = ""
        row["dropcontact_email_selected"] = ""
        row["dropcontact_email_selected_qualification"] = ""
        row["dropcontact_email_candidates"] = ""
        row["dropcontact_credits_left_after_post"] = ""

    eligible: List[Tuple[int, Dict[str, object]]] = []
    for idx, row in enumerate(rows):
        row_id = str(idx)
        payload = build_contact_payload(row, row_id=row_id)
        if payload is not None:
            eligible.append((idx, payload))

    if not eligible:
        write_output(Path(args.output), rows, out_fields)
        print(f"Aucun contact eligible Dropcontact dans {input_path}. Fichier ecrit: {args.output}")
        return

    batch_size = max(1, min(250, int(args.batch_size)))

    for i in range(0, len(eligible), batch_size):
        chunk = eligible[i : i + batch_size]
        contacts = [payload for _, payload in chunk]
        request_id, credits_left = post_batch(api_key=api_key, batch_contacts=contacts)

        for row_index, _ in chunk:
            rows[row_index]["dropcontact_request_id"] = request_id
            rows[row_index]["dropcontact_credits_left_after_post"] = str(credits_left)
            rows[row_index]["dropcontact_status"] = "processing"

        result = poll_results(
            api_key=api_key,
            request_id=request_id,
            poll_seconds=max(1, int(args.poll_seconds)),
            max_wait_seconds=max(30, int(args.max_wait_seconds)),
        )

        data = result.get("data") or []
        if not isinstance(data, list):
            data = []

        by_row_id: Dict[str, Dict[str, object]] = {}
        for item in data:
            if not isinstance(item, dict):
                continue
            cf = item.get("custom_fields") or {}
            if isinstance(cf, dict):
                row_id = str(cf.get("row_id", "")).strip()
                if row_id:
                    by_row_id[row_id] = item

        for row_index, _ in chunk:
            row_id = str(row_index)
            item = by_row_id.get(row_id)
            if not item:
                rows[row_index]["dropcontact_status"] = "no_result"
                continue

            emails = item.get("email") or []
            if not isinstance(emails, list):
                emails = []

            chosen_email, chosen_qualification = choose_best_email(emails)
            candidates = []
            for e in emails:
                if not isinstance(e, dict):
                    continue
                addr = (e.get("email") or "").strip()
                qual = (e.get("qualification") or "").strip()
                if addr:
                    candidates.append(f"{addr}|{qual}")

            rows[row_index]["dropcontact_email_selected"] = chosen_email
            rows[row_index]["dropcontact_email_selected_qualification"] = chosen_qualification
            rows[row_index]["dropcontact_email_candidates"] = "; ".join(candidates)
            rows[row_index]["dropcontact_status"] = "ok" if chosen_email else "ok_no_email"

        print(
            f"Batch {(i // batch_size) + 1}: request_id={request_id} processed={len(chunk)} credits_left={credits_left}"
        )

    write_output(Path(args.output), rows, out_fields)

    total = len(rows)
    eligible_n = len(eligible)
    ok_n = sum(1 for r in rows if (r.get("dropcontact_status") or "") == "ok")
    no_email_n = sum(1 for r in rows if (r.get("dropcontact_status") or "") == "ok_no_email")
    skipped_n = sum(1 for r in rows if (r.get("dropcontact_status") or "") == "skipped_missing_required_fields")

    print(f"Done. total={total} eligible={eligible_n} ok={ok_n} ok_no_email={no_email_n} skipped={skipped_n}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
