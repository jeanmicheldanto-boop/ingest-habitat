from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class EJ:
    finess_ej: str
    nom: str
    adresse: str


@dataclass(frozen=True)
class ETCategory:
    finess_et: str
    categorie_code: str
    categorie_lib: str
    groupe_code: str
    groupe_lib: str


def _join_nonempty(parts: Iterable[str], sep: str = " ") -> str:
    return sep.join([p.strip() for p in parts if p and p.strip()])


def read_ej(path_cs1100501: Path) -> dict[str, EJ]:
    """Reads EJ (structureej) file.

    Expected format (23 cols):
    01 finess_ej
    02 rs (short)
    03 rs (long)
    04 sigle
    05 numvoie
    06 typevoie
    07 libvoie
    08 compl1
    09 compl2
    10 compl3
    11 code_commune
    12 cp_ville
    13 dep_code
    14 dep_nom
    15 telephone
    16 statut_juridique_code
    17 statut_juridique_lib
    18 ...
    19 ...
    20 siren
    21 naf
    22 date_creation
    """
    ej_by_id: dict[str, EJ] = {}

    with path_cs1100501.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if not row:
                continue
            if row[0] != "structureej":
                continue
            # Be defensive if format changes
            finess_ej = row[1].strip() if len(row) > 1 else ""
            if not finess_ej:
                continue

            rs_short = row[2].strip() if len(row) > 2 else ""
            rs_long = row[3].strip() if len(row) > 3 else ""
            nom = rs_long or rs_short

            numvoie = row[5].strip() if len(row) > 5 else ""
            typevoie = row[6].strip() if len(row) > 6 else ""
            libvoie = row[7].strip() if len(row) > 7 else ""
            compl1 = row[8].strip() if len(row) > 8 else ""
            compl2 = row[9].strip() if len(row) > 9 else ""
            compl3 = row[10].strip() if len(row) > 10 else ""
            cp_ville = row[12].strip() if len(row) > 12 else ""
            dep_nom = row[14].strip() if len(row) > 14 else ""

            ligne_voie = _join_nonempty([numvoie, typevoie, libvoie])
            ligne_compl = _join_nonempty([compl1, compl2, compl3], sep=", ")

            adresse = _join_nonempty(
                [
                    ligne_voie,
                    ligne_compl,
                    cp_ville,
                    dep_nom,
                ],
                sep=", ",
            )

            ej_by_id[finess_ej] = EJ(finess_ej=finess_ej, nom=nom, adresse=adresse)

    return ej_by_id


def read_essms_mapping(path_cs1100505: Path) -> tuple[dict[str, str], set[str]]:
    """Reads cs1100505 file and returns:

    - mapping_et_to_ej: finess_et -> finess_ej (from record type 'structureet')
    - essms_et_ids: set of finess_et present in 'equipementsocial' records

    We count unique finess_et to avoid double-counting multiple activities.
    """
    mapping_et_to_ej: dict[str, str] = {}
    essms_et_ids: set[str] = set()

    with path_cs1100505.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if not row:
                continue
            t = row[0]
            if t == "structureet":
                if len(row) >= 3:
                    et = row[1].strip()
                    ej = row[2].strip()
                    if et and ej:
                        mapping_et_to_ej[et] = ej
            elif t == "equipementsocial":
                if len(row) >= 2:
                    et = row[1].strip()
                    if et:
                        essms_et_ids.add(et)

    return mapping_et_to_ej, essms_et_ids


def read_et_categories_from_cs1100502(path_cs1100502: Path, wanted_ets: set[str]) -> dict[str, ETCategory]:
    """Reads cs1100502 (structureet details) and extracts category info for wanted ET ids.

    Mapping derived from observed 32-column structureet rows:
    - 01: finess_et
    - 18: categorie_code (3 digits)
    - 19: categorie_lib
    - 20: groupe_code (4 digits)
    - 21: groupe_lib
    """
    by_et: dict[str, ETCategory] = {}

    with path_cs1100502.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        for row in reader:
            if not row or row[0] != "structureet":
                continue
            if len(row) < 22:
                continue
            finess_et = row[1].strip()
            if not finess_et or finess_et not in wanted_ets:
                continue
            cat_code = row[18].strip()
            cat_lib = row[19].strip()
            grp_code = row[20].strip()
            grp_lib = row[21].strip()
            by_et[finess_et] = ETCategory(
                finess_et=finess_et,
                categorie_code=cat_code,
                categorie_lib=cat_lib,
                groupe_code=grp_code,
                groupe_lib=grp_lib,
            )
    return by_et


def classify_dominant_type(cat: ETCategory | None) -> str:
    """Very light heuristic mapping to a coarse 'type' bucket.

    We intentionally keep it simple and transparent (keyword-based).
    """
    if cat is None:
        return "INCONNU"

    text = " ".join([cat.categorie_lib, cat.groupe_lib]).lower()

    # Common ESSMS types (examples requested)
    if "hébergement pour personnes âgées dépendantes" in text or "ehpad" in text:
        return "EHPAD"
    if "centre d'hébergement et de réinsertion sociale" in text or "chrs" in text:
        return "CHRS"
    if "maison d'enfants" in text or "mecs" in text:
        return "MECS"
    if "foyer" in text and "hébergement" in text:
        return "FOYER HÉBERGEMENT"

    # Useful fallbacks (still medico-social)
    if "esat" in text or "aide par le travail" in text:
        return "ESAT"
    if "service de soins infirmiers" in text or "ssiad" in text:
        return "SSIAD"
    if "se" in text and "ssad" in text:
        return "SESSAD"

    # Default: keep FINESS category label (readable)
    return cat.categorie_lib or cat.groupe_lib or "INCONNU"


def size_bucket(nb_essms: int) -> str:
    # Strictly "plus de" as requested
    if nb_essms > 100:
        return ">100"
    if nb_essms > 50:
        return ">50"
    if nb_essms > 20:
        return ">20"
    if nb_essms > 10:
        return ">10"
    return "<=10"


def build_report(
    ej_by_id: dict[str, EJ],
    mapping_et_to_ej: dict[str, str],
    essms_et_ids: set[str],
    et_categories: dict[str, ETCategory],
    min_essms: int,
) -> pd.DataFrame:
    effective_essms = [et for et in essms_et_ids if et in mapping_et_to_ej]
    counts = Counter(mapping_et_to_ej[et] for et in effective_essms)

    # dominant type per EJ
    type_counts_by_ej: dict[str, Counter[str]] = {}
    for et in effective_essms:
        ej = mapping_et_to_ej[et]
        t = classify_dominant_type(et_categories.get(et))
        if ej not in type_counts_by_ej:
            type_counts_by_ej[ej] = Counter()
        type_counts_by_ej[ej][t] += 1

    rows: list[dict[str, object]] = []
    for finess_ej, nb in counts.items():
        if nb <= min_essms:
            continue
        ej = ej_by_id.get(finess_ej)

        dominant_type = "INCONNU"
        dominant_count = 0
        top5_detail = ""
        if finess_ej in type_counts_by_ej and type_counts_by_ej[finess_ej]:
            most_common = type_counts_by_ej[finess_ej].most_common(5)
            dominant_type, dominant_count = most_common[0]
            top5_detail = ", ".join(f"{t} ({c})" for t, c in most_common)

        rows.append(
            {
                "finess_ej": finess_ej,
                "gestionnaire_nom": ej.nom if ej else "",
                "gestionnaire_adresse": ej.adresse if ej else "",
                "nb_essms": int(nb),
                "categorie_taille": size_bucket(int(nb)),
                "dominante_type": dominant_type,
                "dominante_nb": int(dominant_count),
                "dominante_top5": top5_detail,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df = df.sort_values(["nb_essms", "gestionnaire_nom", "finess_ej"], ascending=[False, True, True])
    return df


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Export FINESS gestionnaires (EJ) that manage more than N ESSMS (social & medico-social). "
            "Uses cs1100505 (equipementsocial + ET->EJ) and cs1100501 (EJ details)."
        )
    )
    parser.add_argument(
        "--cs1100501",
        default="database/etalab-cs1100501-stock-20260107-0343.csv",
        help="Path to cs1100501 stock (structureej)",
    )
    parser.add_argument(
        "--cs1100505",
        default="database/etalab-cs1100505-stock-20260107-0343.csv",
        help="Path to cs1100505 stock (equipementsocial + structureet ET->EJ)",
    )
    parser.add_argument(
        "--cs1100502",
        default="database/etalab-cs1100502-stock-20260107-0343.csv",
        help="Path to cs1100502 stock (structureet details: categories)",
    )
    parser.add_argument(
        "--min-essms",
        type=int,
        default=10,
        help="Threshold: keep EJ with strictly more than this number of ESSMS (default: 10)",
    )
    parser.add_argument(
        "--out",
        default="outputs/finess_gestionnaires_essms_gt10.csv",
        help="Output file path (.csv or .xlsx)",
    )

    args = parser.parse_args()

    path_ej = Path(args.cs1100501)
    path_essms = Path(args.cs1100505)
    path_et_detail = Path(args.cs1100502)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ej_by_id = read_ej(path_ej)
    mapping_et_to_ej, essms_et_ids = read_essms_mapping(path_essms)
    et_categories = read_et_categories_from_cs1100502(path_et_detail, wanted_ets=essms_et_ids)
    df = build_report(
        ej_by_id,
        mapping_et_to_ej,
        essms_et_ids,
        et_categories,
        min_essms=args.min_essms,
    )

    if out_path.suffix.lower() == ".xlsx":
        df.to_excel(out_path, index=False)
    else:
        df.to_csv(out_path, index=False, encoding="utf-8")

    print(f"Wrote {len(df)} rows to {out_path}")


if __name__ == "__main__":
    main()
