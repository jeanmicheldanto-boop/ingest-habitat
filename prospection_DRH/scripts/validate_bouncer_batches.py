from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


DEFAULT_API_URL = "https://api.usebouncer.com/v1.1/email/verify"


def load_dotenv(path: Path = Path(".env")) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def read_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fields = reader.fieldnames or []
    return rows, fields


def write_rows(path: Path, rows: List[Dict[str, str]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def pick_email_and_source(row: Dict[str, str]) -> Tuple[str, str]:
    candidates = [
        ("dropcontact_email_selected", row.get("dropcontact_email_selected", "")),
        ("drh_email_public", row.get("drh_email_public", "")),
        ("drh_email_reconstitue", row.get("drh_email_reconstitue", "")),
        ("drh_email_generic", row.get("drh_email_generic", "")),
        ("email", row.get("email", "")),
    ]
    for source, value in candidates:
        email = (value or "").strip().lower()
        if email and "@" in email and "." in email.split("@")[-1]:
            return email, source
    return "", ""


def load_cache(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return {str(k): dict(v) for k, v in data.items() if isinstance(v, dict)}
    except Exception:
        pass
    return {}


def save_cache(path: Path, cache: Dict[str, Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def verify_email(api_url: str, api_key: str, email: str, timeout: int) -> Dict[str, str]:
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        resp = requests.get(api_url, headers=headers, params={"email": email}, timeout=timeout)
    except requests.RequestException as exc:
        return {
            "bouncer_status": "error_network",
            "bouncer_sub_status": str(exc),
            "bouncer_score": "",
            "bouncer_is_deliverable": "",
        }

    if resp.status_code >= 400:
        body = ""
        try:
            body = resp.text[:300]
        except Exception:
            body = ""
        return {
            "bouncer_status": f"error_http_{resp.status_code}",
            "bouncer_sub_status": body,
            "bouncer_score": "",
            "bouncer_is_deliverable": "",
        }

    try:
        data = resp.json()
    except ValueError:
        return {
            "bouncer_status": "error_invalid_json",
            "bouncer_sub_status": resp.text[:300],
            "bouncer_score": "",
            "bouncer_is_deliverable": "",
        }

    status = str(data.get("status", "")).strip() or str(data.get("result", "")).strip()
    sub_status = str(data.get("sub_status", "")).strip() or str(data.get("reason", "")).strip()
    score_raw = data.get("score", "")
    score = "" if score_raw in (None, "") else str(score_raw)

    status_lower = status.lower()
    is_deliverable = "1" if status_lower in {"deliverable", "safe", "valid"} else "0"

    return {
        "bouncer_status": status,
        "bouncer_sub_status": sub_status,
        "bouncer_score": score,
        "bouncer_is_deliverable": is_deliverable,
    }


def output_path_for(input_path: Path, output_dir: Optional[Path]) -> Path:
    if output_dir:
        return output_dir / f"{input_path.stem}_bouncer{input_path.suffix}"
    return input_path.with_name(f"{input_path.stem}_bouncer{input_path.suffix}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bouncer verification for one or many CSV lots")
    parser.add_argument("--inputs", nargs="+", required=True, help="Input CSV files")
    parser.add_argument("--output-dir", default="", help="Optional output directory")
    parser.add_argument("--cache", default="data/cache_bouncer/results.json", help="JSON cache path")
    parser.add_argument("--timeout", type=int, default=25)
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--max-checks", type=int, default=0, help="0 = all")
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    args = parser.parse_args()

    load_dotenv(Path(".env"))
    api_key = (os.getenv("BOUNCER_API_KEY") or "").strip()
    if not api_key:
        raise SystemExit("BOUNCER_API_KEY manquante dans .env")

    cache_path = Path(args.cache)
    cache = load_cache(cache_path)
    checked_now = 0

    extra_fields = [
        "bouncer_email_checked",
        "bouncer_email_source",
        "bouncer_status",
        "bouncer_sub_status",
        "bouncer_score",
        "bouncer_is_deliverable",
        "bouncer_checked_at",
    ]

    output_dir = Path(args.output_dir) if args.output_dir else None

    for input_str in args.inputs:
        input_path = Path(input_str)
        if not input_path.exists():
            print(f"[WARN] missing input: {input_path}")
            continue

        rows, fields = read_rows(input_path)
        out_fields = fields + [f for f in extra_fields if f not in fields]

        # Build unique checks for this file.
        needed: Dict[str, None] = {}
        for row in rows:
            email, _ = pick_email_and_source(row)
            if email and email not in cache:
                needed[email] = None

        emails_to_check = list(needed.keys())
        if args.max_checks > 0:
            emails_to_check = emails_to_check[: args.max_checks]

        total_to_check = len(emails_to_check)
        print(f"[INFO] {input_path.name}: rows={len(rows)} unique_new_checks={total_to_check}")

        for idx, email in enumerate(emails_to_check, start=1):
            result = verify_email(args.api_url, api_key, email, timeout=args.timeout)
            result["bouncer_checked_at"] = now_iso()
            cache[email] = result
            checked_now += 1

            if idx % 25 == 0 or idx == total_to_check:
                print(f"[INFO] {input_path.name}: verified {idx}/{total_to_check}")
                save_cache(cache_path, cache)

            if args.sleep > 0:
                time.sleep(args.sleep)

        # Populate rows from cache
        for row in rows:
            email, source = pick_email_and_source(row)
            row["bouncer_email_checked"] = email
            row["bouncer_email_source"] = source

            if not email:
                row["bouncer_status"] = "no_email"
                row["bouncer_sub_status"] = ""
                row["bouncer_score"] = ""
                row["bouncer_is_deliverable"] = ""
                row["bouncer_checked_at"] = ""
                continue

            info = cache.get(email, {})
            row["bouncer_status"] = str(info.get("bouncer_status", "not_checked"))
            row["bouncer_sub_status"] = str(info.get("bouncer_sub_status", ""))
            row["bouncer_score"] = str(info.get("bouncer_score", ""))
            row["bouncer_is_deliverable"] = str(info.get("bouncer_is_deliverable", ""))
            row["bouncer_checked_at"] = str(info.get("bouncer_checked_at", ""))

        out_path = output_path_for(input_path, output_dir)
        write_rows(out_path, rows, out_fields)
        print(f"[INFO] output: {out_path}")

    save_cache(cache_path, cache)
    print(f"[DONE] checked_now={checked_now} cache_size={len(cache)} cache={cache_path}")


if __name__ == "__main__":
    main()
