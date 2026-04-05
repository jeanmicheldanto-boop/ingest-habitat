from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import re
import time
import unicodedata
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
log = logging.getLogger("prepare_population")


class JsonCache:
    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, Any] = {}
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}

    def get(self, key: str) -> Any:
        return self.data.get(key)

    def has(self, key: str) -> bool:
        return key in self.data

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")


def backoff_sleep(attempt: int, base: float = 1.2) -> None:
    time.sleep(base * (2 ** max(0, attempt - 1)) + random.uniform(0, 0.7))


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = re.sub(r"[^0-9]", "", s)
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def normalize_name(value: str) -> str:
    s = (value or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]", "", s)


def fetch_population(code_insee: str, expected_commune: str, cache: JsonCache) -> Optional[int]:
    code = (code_insee or "").strip()
    if not code:
        return None
    expected_norm = normalize_name(expected_commune)

    if cache.has(code):
        val = cache.get(code)
        # Legacy cache values were raw ints and may be wrong when INSEE/name mapping is broken.
        # We only trust the new structured cache format.
        if isinstance(val, dict):
            cached_name = normalize_name(str(val.get("nom", "")))
            cached_pop = val.get("population")
            if expected_norm and cached_name and cached_name != expected_norm:
                return None
            return cached_pop if isinstance(cached_pop, int) else None

    url = f"https://geo.api.gouv.fr/communes/{code}?fields=nom,population&format=json&geometry=centre"
    for attempt in range(1, 4):
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code == 200:
                body = resp.json() or {}
                api_name = str(body.get("nom", "") or "")
                pop = body.get("population")
                if expected_norm and normalize_name(api_name) != expected_norm:
                    cache.set(code, {"nom": api_name, "population": None, "reason": "name_mismatch"})
                    return None
                if isinstance(pop, int):
                    cache.set(code, {"nom": api_name, "population": pop})
                    return pop
                cache.set(code, {"nom": api_name, "population": None})
                return None
            if resp.status_code in {429, 500, 502, 503, 504}:
                backoff_sleep(attempt)
                continue
            break
        except Exception:
            backoff_sleep(attempt)

    cache.set(code, {"nom": "", "population": None, "reason": "request_failed"})
    return None


def fetch_population_by_name_dept(commune: str, dept_code: str, cache: JsonCache) -> Optional[int]:
    name = (commune or "").strip()
    dept = (dept_code or "").strip()
    if not name or not dept:
        return None

    cache_key = f"{normalize_name(name)}|{dept}"
    if cache.has(cache_key):
        val = cache.get(cache_key)
        return val if isinstance(val, int) else None

    encoded_name = urllib.parse.quote(name)
    url = (
        "https://geo.api.gouv.fr/communes"
        f"?nom={encoded_name}&codeDepartement={dept}"
        "&fields=nom,population,code,codesPostaux"
        "&boost=population&limit=8"
    )

    for attempt in range(1, 4):
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code == 200:
                arr = resp.json() or []
                if not isinstance(arr, list) or not arr:
                    cache.set(cache_key, None)
                    return None

                target = normalize_name(name)
                chosen: Optional[Dict[str, Any]] = None

                # Prefer exact normalized name match within the same department list.
                for cand in arr:
                    if not isinstance(cand, dict):
                        continue
                    if normalize_name(str(cand.get("nom", ""))) == target:
                        chosen = cand
                        break

                if chosen is None:
                    chosen = arr[0] if isinstance(arr[0], dict) else None

                pop = chosen.get("population") if isinstance(chosen, dict) else None
                if isinstance(pop, int):
                    cache.set(cache_key, pop)
                    return pop

                cache.set(cache_key, None)
                return None

            if resp.status_code in {429, 500, 502, 503, 504}:
                backoff_sleep(attempt)
                continue
            break
        except Exception:
            backoff_sleep(attempt)

    cache.set(cache_key, None)
    return None


def read_rows(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("code_insee") or not row.get("commune"):
                continue
            rows.append({k: (v or "").strip() for k, v in row.items()})

    seen = set()
    dedup: List[Dict[str, str]] = []
    for row in rows:
        code = row.get("code_insee", "")
        if code in seen:
            continue
        seen.add(code)
        dedup.append(row)
    return dedup


def write_rows(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build communes CSV with real population values.")
    parser.add_argument("--input", default="data/communes_idf.csv", help="Input CSV file")
    parser.add_argument("--output", default="data/communes_idf_with_population.csv", help="Output CSV file")
    parser.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between API calls")
    parser.add_argument("--flush-every", type=int, default=50, help="Flush cache every N rows")
    parser.add_argument("--log-level", default="INFO", help="DEBUG|INFO|WARNING|ERROR")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.getLogger().setLevel(getattr(logging, str(args.log_level).upper(), logging.INFO))

    input_path = Path(args.input)
    output_path = Path(args.output)
    if not input_path.exists():
        log.error("Input not found: %s", input_path)
        return 1

    cache = JsonCache(Path("data") / "cache_communes_idf_contacts" / "population_cache_by_insee.json")
    fallback_cache = JsonCache(Path("data") / "cache_communes_idf_contacts" / "population_cache_by_name_dept.json")
    rows = read_rows(input_path)
    log.info("Rows to enrich: %d", len(rows))

    out: List[Dict[str, Any]] = []
    with_pop = 0
    missing_pop = 0
    recovered_by_fallback = 0

    for idx, row in enumerate(rows, start=1):
        current_pop = parse_int(row.get("population"))
        if current_pop is None:
            current_pop = fetch_population(row.get("code_insee", ""), row.get("commune", ""), cache)
        if current_pop is None:
            current_pop = fetch_population_by_name_dept(row.get("commune", ""), row.get("code_departement", ""), fallback_cache)
            if current_pop is not None:
                recovered_by_fallback += 1
            if args.sleep > 0:
                time.sleep(max(0.0, args.sleep))

        if current_pop is None:
            missing_pop += 1
        else:
            with_pop += 1

        new_row = dict(row)
        new_row["population"] = current_pop if current_pop is not None else ""
        out.append(new_row)

        if idx % 50 == 0:
            log.info(
                "Progress %d/%d | with_population=%d missing=%d recovered_fallback=%d",
                idx,
                len(rows),
                with_pop,
                missing_pop,
                recovered_by_fallback,
            )
        if idx % max(1, args.flush_every) == 0:
            cache.flush()
            fallback_cache.flush()

    write_rows(output_path, out)
    cache.flush()
    fallback_cache.flush()
    log.info(
        "Done. Output=%s | rows=%d with_population=%d missing=%d recovered_fallback=%d",
        output_path,
        len(out),
        with_pop,
        missing_pop,
        recovered_by_fallback,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
