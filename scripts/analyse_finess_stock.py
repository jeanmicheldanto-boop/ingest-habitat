from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyse a FINESS stock CSV (etalab) and print its structure.")
    parser.add_argument(
        "path",
        nargs="?",
        default="database/etalab-cs1100502-stock-20260107-0343.csv",
        help="Path to the FINESS stock CSV",
    )
    parser.add_argument(
        "--show-index",
        default=None,
        help="Record type for which to print the full index mapping (e.g. structureet, structureej)",
    )
    args = parser.parse_args()

    path = Path(args.path)
    if not path.exists():
        raise SystemExit(f"File not found: {path}")

    type_counts: Counter[str] = Counter()
    len_by_type: dict[str, Counter[int]] = defaultdict(Counter)
    examples: dict[str, list[str]] = {}

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for i, row in enumerate(reader, start=1):
            if not row:
                continue
            rec_type = row[0]
            type_counts[rec_type] += 1
            len_by_type[rec_type][len(row)] += 1
            if rec_type != "finess" and rec_type not in examples:
                examples[rec_type] = row

    print(f"File: {path} (lines: {sum(type_counts.values())})")
    print("\nRecord types (count):")
    for t, c in type_counts.most_common():
        print(f"- {t}: {c}")

    print("\nColumn-count distribution (top 5 per type):")
    for t, _ in type_counts.most_common():
        dist = ", ".join(f"{k}→{v}" for k, v in len_by_type[t].most_common(5))
        print(f"- {t}: {dist}")

    print("\nExamples (first 20 fields):")
    for t in sorted(examples.keys()):
        row = examples[t]
        print(f"\n[{t}] cols={len(row)}")
        print(";".join(row[:20]))

    if args.show_index:
        record_type = args.show_index
        if record_type not in examples:
            available = ", ".join(sorted(examples.keys()))
            raise SystemExit(f"No example found for record type '{record_type}'. Available: {available}")
        row = examples[record_type]
        print(f"\nIndex map for sample [{record_type}]:")
        for idx, value in enumerate(row):
            print(f"{idx:02d}: {value}")


if __name__ == "__main__":
    main()
