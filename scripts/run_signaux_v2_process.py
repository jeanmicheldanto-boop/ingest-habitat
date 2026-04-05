"""Orchestration pratique du process Signaux V2.

Ce script permet de lancer la mise en place V2 sans passer par l'éditeur SQL :
- Application optionnelle des scripts SQL (migration/backfill/recalibration)
- Calcul de KPI de suivi
- Génération d'un lot QA 20/20/10 (ou tailles personnalisées)
- Export CSV + mémo markdown dans outputs/

Exemples:
    python scripts/run_signaux_v2_process.py --generate-qa
    python scripts/run_signaux_v2_process.py --apply-sql 16 18 --generate-qa
    python scripts/run_signaux_v2_process.py --apply-sql 17 18 --sample-keywords 30 --sample-excluded 30 --sample-angle-mort 15
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import psycopg2.extras

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from database import DatabaseManager

OUTPUTS_DIR = REPO_ROOT / "outputs"
SCRIPTS_SQL_DIR = REPO_ROOT / "scripts_sql"

SQL_FILES = {
    "16": "16_ajouter_signaux_v2.sql",
    "17": "17_backfill_signaux_v2_keywords.sql",
    "18": "18_recalibrate_signaux_v2_keywords_strict.sql",
    "19": "19_schema_signaux_v2_exhaustif.sql",
}

KPI_QUERY = """
SELECT
    COUNT(*) FILTER (WHERE signal_v2_methode = 'keywords_v1') AS nb_keywords_v1,
    COUNT(*) FILTER (WHERE signal_v2_methode = 'keywords_v1_excluded') AS nb_keywords_v1_excluded,
    COUNT(*) FILTER (WHERE signal_v2_methode IS NULL) AS nb_signal_v2_methode_null,
    COUNT(*) FILTER (WHERE signal_financier) AS nb_financier,
    COUNT(*) FILTER (WHERE signal_rh) AS nb_rh,
    COUNT(*) FILTER (WHERE signal_qualite) AS nb_qualite,
    COUNT(*) FILTER (WHERE signal_juridique) AS nb_juridique,
    COUNT(*) FILTER (WHERE signal_financier OR signal_rh OR signal_qualite OR signal_juridique) AS nb_any_axis,
    COUNT(*) FILTER (
        WHERE (signal_financier::int + signal_rh::int + signal_qualite::int + signal_juridique::int) >= 2
    ) AS nb_multi_axes,
    COUNT(*) FILTER (
        WHERE COALESCE(signal_tension, FALSE) = FALSE
          AND COALESCE(nb_etablissements, 0) <= 10
          AND signal_v2_methode IS NULL
          AND EXISTS (
              SELECT 1
              FROM finess_etablissement e
              WHERE e.id_gestionnaire = finess_gestionnaire.id_gestionnaire
                AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
          )
    ) AS nb_angle_mort_structurel
FROM finess_gestionnaire;
"""

SAMPLE_BASE_SELECT = """
SELECT
    {bucket} AS bucket,
    g.id_gestionnaire,
    g.raison_sociale,
    g.sigle,
    g.secteur_activite_principal,
    g.nb_etablissements,
    g.signal_tension,
    g.signal_v2_methode,
    g.signal_financier,
    g.signal_rh,
    g.signal_qualite,
    g.signal_juridique,
    CASE
        WHEN jsonb_typeof(g.signaux_recents) = 'array' THEN jsonb_array_length(g.signaux_recents)
        ELSE 0
    END AS nb_signaux_recents,
    CASE
        WHEN g.signal_tension_detail IS NULL THEN NULL
        ELSE left(g.signal_tension_detail, 220)
    END AS signal_tension_detail_excerpt
FROM finess_gestionnaire g
WHERE {where_clause}
ORDER BY md5(g.id_gestionnaire::text || %s)
LIMIT %s;
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lancer le process Signaux V2 (SQL + QA).")
    parser.add_argument(
        "--apply-sql",
        nargs="*",
        choices=["16", "17", "18", "19"],
        default=[],
        help="Scripts SQL a executer dans l'ordre (16, 17, 18, 19).",
    )
    parser.add_argument(
        "--generate-qa",
        action="store_true",
        help="Genere le lot QA (CSV + memo markdown).",
    )
    parser.add_argument("--sample-keywords", type=int, default=20, help="Taille echantillon keywords_v1.")
    parser.add_argument("--sample-excluded", type=int, default=20, help="Taille echantillon keywords_v1_excluded.")
    parser.add_argument("--sample-angle-mort", type=int, default=10, help="Taille echantillon angle mort structurel.")
    parser.add_argument(
        "--seed",
        default=datetime.now().strftime("%Y%m%d"),
        help="Seed texte pour un echantillonnage stable (defaut: date du jour).",
    )
    parser.add_argument(
        "--prefill-review",
        action="store_true",
        help="Pre-remplit les colonnes de revue humaine dans le CSV.",
    )
    return parser.parse_args()


def execute_sql_file(cur: psycopg2.extras.RealDictCursor, sql_path: Path) -> None:
    sql = sql_path.read_text(encoding="utf-8")
    cur.execute(sql)


def choose_axis(row: Dict[str, Any]) -> str:
    if row.get("signal_juridique"):
        return "juridique"
    if row.get("signal_financier"):
        return "financier"
    if row.get("signal_qualite"):
        return "qualite"
    if row.get("signal_rh"):
        return "rh"
    return "aucun"


def build_review_prefill(row: Dict[str, Any]) -> Dict[str, str]:
    axis_flags = [
        ("financier", bool(row.get("signal_financier"))),
        ("rh", bool(row.get("signal_rh"))),
        ("qualite", bool(row.get("signal_qualite"))),
        ("juridique", bool(row.get("signal_juridique"))),
    ]
    active_axes = [name for name, is_on in axis_flags if is_on]

    if row["bucket"] == "keywords_v1":
        lecture = "oui"
        source = "a_verifier"
        commentaire = (
            f"Pre-remplissage auto: {len(active_axes)} axe(s) detecte(s)"
            + (f" ({', '.join(active_axes)})." if active_axes else ".")
            + " A confirmer manuellement sur sources."
        )
    else:
        lecture = "non"
        source = "aucune"
        commentaire = (
            "Pre-remplissage auto: classe exclusion/angle mort. "
            "Verifier qu'il ne s'agit pas d'une difficulte individuelle utile."
        )

    return {
        "review_lecture_humaine_signal": lecture,
        "review_axe_principal": choose_axis(row),
        "review_niveau_confiance_humain": "moyen" if row["bucket"] == "keywords_v1" else "faible",
        "review_source_verifiee": source,
        "review_commentaire": commentaire,
    }


def sample_rows(cur: psycopg2.extras.RealDictCursor, seed: str, limit: int, bucket: str, where_clause: str) -> List[Dict[str, Any]]:
    sql = SAMPLE_BASE_SELECT.format(bucket="%s", where_clause=where_clause)
    cur.execute(sql, (bucket, seed, limit))
    return [dict(r) for r in cur.fetchall()]


def fetch_kpi(cur: psycopg2.extras.RealDictCursor) -> Dict[str, Any]:
    cur.execute(KPI_QUERY)
    row = cur.fetchone() or {}
    return dict(row)


def write_csv(rows: List[Dict[str, Any]], csv_path: Path, prefill_review: bool) -> None:
    fields = [
        "bucket",
        "id_gestionnaire",
        "raison_sociale",
        "sigle",
        "secteur_activite_principal",
        "nb_etablissements",
        "signal_tension",
        "signal_v2_methode",
        "signal_financier",
        "signal_rh",
        "signal_qualite",
        "signal_juridique",
        "nb_signaux_recents",
        "signal_tension_detail_excerpt",
        "review_lecture_humaine_signal",
        "review_axe_principal",
        "review_niveau_confiance_humain",
        "review_source_verifiee",
        "review_commentaire",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        for row in rows:
            out = dict(row)
            if prefill_review:
                out.update(build_review_prefill(row))
            else:
                out.update(
                    {
                        "review_lecture_humaine_signal": "",
                        "review_axe_principal": "",
                        "review_niveau_confiance_humain": "",
                        "review_source_verifiee": "",
                        "review_commentaire": "",
                    }
                )
            writer.writerow(out)


def write_memo(
    memo_path: Path,
    csv_path: Path,
    kpi: Dict[str, Any],
    rows: List[Dict[str, Any]],
    sample_keywords: int,
    sample_excluded: int,
    sample_angle_mort: int,
    seed: str,
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nb_keywords = sum(1 for r in rows if r["bucket"] == "keywords_v1")
    nb_excluded = sum(1 for r in rows if r["bucket"] == "keywords_v1_excluded")
    nb_angle_mort = sum(1 for r in rows if r["bucket"] == "angle_mort_structurel")

    content = f"""# Batch QA Signaux V2

Date generation: {now}
Seed echantillonnage: {seed}

## Fichiers
- CSV: {csv_path.relative_to(REPO_ROOT)}
- Memo: {memo_path.relative_to(REPO_ROOT)}

## Composition du batch
- keywords_v1: {nb_keywords} (cible: {sample_keywords})
- keywords_v1_excluded: {nb_excluded} (cible: {sample_excluded})
- angle_mort_structurel: {nb_angle_mort} (cible: {sample_angle_mort})
- total: {len(rows)}

## KPI instantanes
- keywords_v1: {kpi.get('nb_keywords_v1', 0)}
- keywords_v1_excluded: {kpi.get('nb_keywords_v1_excluded', 0)}
- signal_v2_methode IS NULL: {kpi.get('nb_signal_v2_methode_null', 0)}
- signal_financier = TRUE: {kpi.get('nb_financier', 0)}
- signal_rh = TRUE: {kpi.get('nb_rh', 0)}
- signal_qualite = TRUE: {kpi.get('nb_qualite', 0)}
- signal_juridique = TRUE: {kpi.get('nb_juridique', 0)}
- au moins 1 axe V2: {kpi.get('nb_any_axis', 0)}
- multi-axes: {kpi.get('nb_multi_axes', 0)}
- angle mort structurel: {kpi.get('nb_angle_mort_structurel', 0)}

## Prochaine etape recommandee
1. Relecture manuelle des 20/20/10 cas.
2. Ajustement des regles RH/Juridique selon faux positifs observes.
3. Lancement d'une passe Serper/LLM ciblee sur multi-axes + grands gestionnaires + angle mort.
"""

    memo_path.write_text(content, encoding="utf-8")


def main() -> None:
    args = parse_args()
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    db = DatabaseManager()

    with db.get_connection() as conn:
        conn.autocommit = False
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # 1) Application optionnelle des scripts SQL
            for sql_key in args.apply_sql:
                sql_name = SQL_FILES[sql_key]
                sql_path = SCRIPTS_SQL_DIR / sql_name
                if not sql_path.exists():
                    raise FileNotFoundError(f"Script SQL introuvable: {sql_path}")
                print(f"[SQL] Execution {sql_name} ...")
                execute_sql_file(cur, sql_path)
                conn.commit()
                print(f"[OK] {sql_name}")

            # 2) KPI
            kpi = fetch_kpi(cur)
            print("[KPI]", kpi)

            if not args.generate_qa:
                return

            # 3) Echantillons QA
            rows_keywords = sample_rows(
                cur,
                seed=args.seed,
                limit=args.sample_keywords,
                bucket="keywords_v1",
                where_clause="g.signal_v2_methode = 'keywords_v1'",
            )
            rows_excluded = sample_rows(
                cur,
                seed=args.seed,
                limit=args.sample_excluded,
                bucket="keywords_v1_excluded",
                where_clause="g.signal_v2_methode = 'keywords_v1_excluded'",
            )
            rows_angle_mort = sample_rows(
                cur,
                seed=args.seed,
                limit=args.sample_angle_mort,
                bucket="angle_mort_structurel",
                where_clause=(
                    "COALESCE(g.signal_tension, FALSE) = FALSE "
                    "AND COALESCE(g.nb_etablissements, 0) <= 10 "
                    "AND g.signal_v2_methode IS NULL "
                    "AND EXISTS ("
                    "SELECT 1 FROM finess_etablissement e "
                    "WHERE e.id_gestionnaire = g.id_gestionnaire "
                    "AND e.categorie_normalisee IS DISTINCT FROM 'SAA')"
                ),
            )

            all_rows = rows_keywords + rows_excluded + rows_angle_mort

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            suffix = "_prefill" if args.prefill_review else ""
            csv_path = OUTPUTS_DIR / f"qa_v2_review_{args.sample_keywords}_{args.sample_excluded}_{args.sample_angle_mort}{suffix}_{ts}.csv"
            memo_path = OUTPUTS_DIR / f"qa_v2_review_{args.sample_keywords}_{args.sample_excluded}_{args.sample_angle_mort}{suffix}_{ts}.md"

            write_csv(all_rows, csv_path, prefill_review=args.prefill_review)
            write_memo(
                memo_path=memo_path,
                csv_path=csv_path,
                kpi=kpi,
                rows=all_rows,
                sample_keywords=args.sample_keywords,
                sample_excluded=args.sample_excluded,
                sample_angle_mort=args.sample_angle_mort,
                seed=args.seed,
            )

            print(f"[OK] CSV: {csv_path}")
            print(f"[OK] Memo: {memo_path}")


if __name__ == "__main__":
    main()
