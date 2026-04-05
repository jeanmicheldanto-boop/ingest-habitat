#!/usr/bin/env python3
"""Stats rapides de la base FINESS sur les signaux et profils gestionnaires.

Sorties:
- Affichage console lisible
- Exports CSV optionnels

Exemples:
    python scripts/stats_signaux_v2.py
    python scripts/stats_signaux_v2.py --all-gestionnaires --top 50 --out-dir outputs/stats_signaux_v2
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

import psycopg2
import psycopg2.extras

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

load_dotenv(override=False)

# ET-type to sector mapping for reclassifying mixed gestionnaires
ET_TYPE_SECTORS = {
    # Personnes Âgées
    "EHPAD": "Personnes Âgées",
    "RA": "Personnes Âgées",
    "Accueil temporaire": "Personnes Âgées",
    "EHPA": "Personnes Âgées",
    "Marpa": "Personnes Âgées",
    "Foyer": "Personnes Âgées",
    # Protection de l'Enfance
    "MECS": "Protection de l'Enfance",
    "AEMO": "Protection de l'Enfance",
    "AJ": "Protection de l'Enfance",
    "SESSAD": "Protection de l'Enfance",
    "CAMSP": "Protection de l'Enfance",
    "Pouponnière": "Protection de l'Enfance",
    # Handicap
    "ESAT": "Handicap",
    "IME": "Handicap",
    "MAS": "Handicap",
    "FA": "Handicap",
    "SAVS": "Handicap",
    "IDA": "Handicap",
    "SAAS": "Handicap",
    # Insertion Sociale
    "CHRS": "Insertion Sociale",
    "Résidence Sociale": "Insertion Sociale",
    "Pension de Famille": "Insertion Sociale",
    "FV": "Insertion Sociale",
    "FJT": "Insertion Sociale",
    "CSAPA": "Insertion Sociale",
    "Centre d'Accueil": "Insertion Sociale",
    # Aide à Domicile
    "SSIAD": "Aide à Domicile",
    "SAAD-Famille": "Aide à Domicile",
    "LVA": "Aide à Domicile",
    "SAAD": "Aide à Domicile",
    # Autres
    "EA": "Autres",
    "EAM": "Autres",
    "SPI": "Autres",
    "SPAS": "Autres",
    "CAT": "Autres",
    "CP": "Autres",
    "SMJPM": "Autres",
}

def get_db_config() -> Dict[str, Any]:
    return {
        "host": os.getenv("DB_HOST", ""),
        "database": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", ""),
        "port": int(os.getenv("DB_PORT", "5432")),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stats signaux: types de signaux, type/taille de gestionnaires, secteur."
    )
    parser.add_argument(
        "--all-gestionnaires",
        action="store_true",
        help="Inclut tous les gestionnaires (sinon scope non-SAA uniquement).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=30,
        help="Nombre max de lignes pour les repartitions type/secteur (defaut: 30).",
    )
    parser.add_argument(
        "--out-dir",
        default="",
        help="Dossier de sortie CSV (optionnel).",
    )
    parser.add_argument(
        "--no-markdown",
        action="store_true",
        help="Desactive la generation du rapport markdown.",
    )
    parser.add_argument(
        "--top-terms",
        type=int,
        default=40,
        help="Nombre de termes recurrents a afficher/exporter (defaut: 40).",
    )
    parser.add_argument(
        "--only-visible",
        action="store_true",
        help="Exclut les gestionnaires sans aucun contact (telephone/site_web/email).",
    )
    parser.add_argument(
        "--financial-only",
        action="store_true",
        help="Restreint has_signal au seul signal_financier.",
    )
    return parser.parse_args()


def scope_where_clause(include_all: bool, only_visible: bool = False) -> str:
    base = "TRUE" if include_all else """
EXISTS (
    SELECT 1
    FROM public.finess_etablissement e
    WHERE e.id_gestionnaire = g.id_gestionnaire
      AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
)
""".strip()
    if only_visible:
        visibility_cond = "(g.telephone IS NOT NULL AND g.telephone != '') OR (g.site_web IS NOT NULL AND g.site_web != '')"
        if include_all:
            return visibility_cond
        else:
            return f"({base}) AND {visibility_cond}"
    return base


def get_et_sector_cte() -> str:
    """Returns CTE that maps ET-types to sectors for reclassifying 'Mixte' gestionnaires (>3 ET types)."""
    return """gest_et_sectors AS (
    SELECT
        g.id_gestionnaire,
        COUNT(DISTINCT e.categorie_normalisee) as n_et_types,
        CASE 
            WHEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('EHPAD', 'RA', 'Accueil temporaire', 'EHPA', 'Marpa')
                    THEN 1 END) > 0 
            THEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('EHPAD', 'RA', 'Accueil temporaire', 'EHPA', 'Marpa')
                    THEN 1 END)
            ELSE 0
        END as cnt_personnes_agees,
        CASE 
            WHEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('MECS', 'AEMO', 'AJ', 'SESSAD', 'CAMSP')
                    THEN 1 END) > 0 
            THEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('MECS', 'AEMO', 'AJ', 'SESSAD', 'CAMSP')
                    THEN 1 END)
            ELSE 0
        END as cnt_enfance,
        CASE 
            WHEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('ESAT', 'IME', 'MAS', 'FA', 'SAVS', 'IDA', 'SAAS')
                    THEN 1 END) > 0 
            THEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('ESAT', 'IME', 'MAS', 'FA', 'SAVS', 'IDA', 'SAAS')
                    THEN 1 END)
            ELSE 0
        END as cnt_handicap,
        CASE 
            WHEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('CHRS', 'Résidence Sociale', 'Pension de Famille', 'FV', 'FJT', 'CSAPA', 'Centre d''Accueil')
                    THEN 1 END) > 0 
            THEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('CHRS', 'Résidence Sociale', 'Pension de Famille', 'FV', 'FJT', 'CSAPA', 'Centre d''Accueil')
                    THEN 1 END)
            ELSE 0
        END as cnt_insertion,
        CASE 
            WHEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('SSIAD', 'SAAD-Famille', 'LVA', 'SAAD')
                    THEN 1 END) > 0 
            THEN COUNT(DISTINCT CASE 
                    WHEN e.categorie_normalisee IN ('SSIAD', 'SAAD-Famille', 'LVA', 'SAAD')
                    THEN 1 END)
            ELSE 0
        END as cnt_aide_domicile
    FROM finess_gestionnaire g
    LEFT JOIN finess_etablissement e ON g.id_gestionnaire = e.id_gestionnaire
        AND e.categorie_normalisee <> 'SAA'
    WHERE EXISTS(SELECT 1 FROM finess_etablissement e2 WHERE e2.id_gestionnaire = g.id_gestionnaire AND e2.categorie_normalisee <> 'SAA')
    GROUP BY g.id_gestionnaire
)"""


def build_base_cte(include_all: bool, only_visible: bool = False, financial_only: bool = False) -> str:
    where_clause = scope_where_clause(include_all, only_visible)
    et_sector_cte = get_et_sector_cte()
    if financial_only:
        has_signal_expr = "COALESCE(g.signal_financier, FALSE)"
    else:
        has_signal_expr = (
            "COALESCE(g.signal_financier, FALSE)\n"
            "            OR COALESCE(g.signal_rh, FALSE)\n"
            "            OR COALESCE(g.signal_qualite, FALSE)\n"
            "            OR COALESCE(g.signal_juridique, FALSE)\n"
            "            OR COALESCE(g.signal_v2_niveau_suspicion, 'aucun') IN ('possible', 'probable', 'certain')\n"
            "            OR COALESCE(g.signal_v2_statut_couverture, '') IN ('signal_tension_probable', 'signal_ambigu_review')"
        )
    return f"""
WITH {et_sector_cte},
base AS (
    SELECT
        g.*,
        CASE 
            WHEN g.forme_juridique_libelle IN ('Commune', 'Département', 'Etat', 'Autre Collectivité Territoriale')
                 OR g.forme_juridique_libelle ILIKE '%%Communal%%'
                 OR g.forme_juridique_libelle ILIKE '%%Départemental%%'
                 OR g.forme_juridique_libelle ILIKE '%%Intercommunal%%'
                 OR g.forme_juridique_libelle = 'Centre Communal d''Action Sociale'
                 OR g.forme_juridique_libelle = 'Centre Intercommunal d''Action Sociale (CIAS)'
                 THEN 'Secteur Public'
            WHEN ges.n_et_types > 3 THEN
                CASE 
                    WHEN ges.cnt_personnes_agees >= ges.cnt_enfance 
                         AND ges.cnt_personnes_agees >= ges.cnt_handicap 
                         AND ges.cnt_personnes_agees >= ges.cnt_insertion 
                         AND ges.cnt_personnes_agees >= ges.cnt_aide_domicile
                         AND ges.cnt_personnes_agees > 0
                        THEN 'Mixte (Personnes Âgées)'
                    WHEN ges.cnt_enfance >= ges.cnt_handicap 
                         AND ges.cnt_enfance >= ges.cnt_insertion 
                         AND ges.cnt_enfance >= ges.cnt_aide_domicile
                         AND ges.cnt_enfance > 0
                        THEN 'Mixte (Protection de l''Enfance)'
                    WHEN ges.cnt_handicap >= ges.cnt_insertion 
                         AND ges.cnt_handicap >= ges.cnt_aide_domicile
                         AND ges.cnt_handicap > 0
                        THEN 'Mixte (Handicap)'
                    WHEN ges.cnt_insertion >= ges.cnt_aide_domicile
                         AND ges.cnt_insertion > 0
                        THEN 'Mixte (Insertion Sociale)'
                    WHEN ges.cnt_aide_domicile > 0
                        THEN 'Mixte (Aide à Domicile)'
                    ELSE 'Mixte (Autres)'
                END
            ELSE COALESCE(NULLIF(g.forme_juridique_libelle, ''), 'NON_RENSEIGNE')
        END AS type_gestionnaire_final,
        (
            COALESCE(g.signal_financier, FALSE)::int
            + COALESCE(g.signal_rh, FALSE)::int
            + COALESCE(g.signal_qualite, FALSE)::int
            + COALESCE(g.signal_juridique, FALSE)::int
        ) AS nb_axes,
        (
            {has_signal_expr}
        ) AS has_signal
    FROM public.finess_gestionnaire g
    LEFT JOIN gest_et_sectors ges ON g.id_gestionnaire = ges.id_gestionnaire
    WHERE {where_clause}
)
"""


def et_scope_clause(include_all: bool, only_visible: bool = False) -> str:
    base = "TRUE" if include_all else "e.categorie_normalisee IS DISTINCT FROM 'SAA'"
    if only_visible:
        visibility_cond = "(b.telephone IS NOT NULL AND b.telephone != '') OR (b.site_web IS NOT NULL AND b.site_web != '')"
        if include_all:
            return visibility_cond
        else:
            return f"({base}) AND {visibility_cond}"
    return base


def fetch_rows(cur: Any, query: str, params: Iterable[Any] | None = None) -> List[Dict[str, Any]]:
    try:
        cur.execute(query, tuple(params or ()))
    except (IndexError, psycopg2.errors.ProgrammingError) as e:
        print(f"\nERROR in fetch_rows: {e}")
        print(f"Query length: {len(query)}")
        print(f"Params: {params}")
        print(f"Query first 500 chars:\n{query[:500]}")
        raise
    return [dict(r) for r in cur.fetchall()]


def print_kv_section(title: str, rows: List[Dict[str, Any]]) -> None:
    print(f"\n=== {title} ===")
    for row in rows:
        print(f"- {row['label']}: {row['valeur']}")


def print_group_section(title: str, rows: List[Dict[str, Any]]) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("(aucune ligne)")
        return

    for r in rows:
        key = r.get("categorie", "NON_RENSEIGNE")
        nb = r.get("nb_gestionnaires", 0)
        pct_total = r.get("pct_total", 0)
        nb_signal = r.get("nb_avec_signal", 0)
        pct_signal = r.get("pct_signal", 0)
        print(
            f"- {key}: {nb} gestionnaires ({pct_total}%), "
            f"dont {nb_signal} avec signal ({pct_signal}%)"
        )


def print_group_section_with_gap(title: str, rows: List[Dict[str, Any]]) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("(aucune ligne)")
        return

    for r in rows:
        key = r.get("categorie", "NON_RENSEIGNE")
        nb = r.get("nb_gestionnaires", 0)
        pct_signal = r.get("pct_signal", 0)
        gap_med = r.get("ecart_pp_mediane", 0)
        gap_top = r.get("ecart_pp_plus_representee", 0)
        print(
            f"- {key}: {nb} gestionnaires, {pct_signal}% avec signal "
            f"(ecart mediane: {gap_med:+} pp, ecart cat. la plus representee: {gap_top:+} pp)"
        )


def maybe_export_csv(out_dir: Path | None, filename: str, rows: List[Dict[str, Any]]) -> None:
    if out_dir is None:
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename

    fieldnames: List[str] = []
    if rows:
        for row in rows:
            for k in row.keys():
                if k not in fieldnames:
                    fieldnames.append(k)
    else:
        fieldnames = ["info"]

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)
        else:
            writer.writerow({"info": "no_data"})


def _md_table(rows: List[Dict[str, Any]], columns: List[str]) -> str:
    if not rows:
        return "_Aucune donnee._\n"

    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = []
    for row in rows:
        vals = [str(row.get(c, "")) for c in columns]
        body.append("| " + " | ".join(vals) + " |")
    return "\n".join([header, sep] + body) + "\n"


def write_markdown_report(
    out_dir: Path,
    scope_label: str,
    et_tension_rows: List[Dict[str, Any]],
    global_rows: List[Dict[str, Any]],
    signal_type_rows: List[Dict[str, Any]],
    type_rows: List[Dict[str, Any]],
    taille_rows: List[Dict[str, Any]],
    secteur_rows: List[Dict[str, Any]],
    secteur_gaps_rows: List[Dict[str, Any]],
    statut_gaps_rows: List[Dict[str, Any]],
    taille_gaps_rows: List[Dict[str, Any]],
    terms_detail_rows: List[Dict[str, Any]],
    terms_snippet_rows: List[Dict[str, Any]],
) -> Path:
    report_path = out_dir / "rapport_stats_signaux_v2.md"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = []
    lines.append("# Rapport stats signaux V2")
    lines.append("")
    lines.append(f"- Genere le: {now}")
    lines.append(f"- Scope: {scope_label}")
    lines.append("")

    lines.append("## KPI globaux")
    lines.append("")
    lines.append(_md_table(global_rows, ["label", "valeur"]))

    lines.append("## ET rattaches a des gestionnaires en tension")
    lines.append("")
    lines.append(
        _md_table(
            et_tension_rows,
            [
                "nb_et_total",
                "nb_et_gest_tension",
                "pct_et_gest_tension",
                "nb_et_gest_signal",
                "pct_et_gest_signal",
            ],
        )
    )

    lines.append("## Types de signaux V2")
    lines.append("")
    lines.append(_md_table(signal_type_rows, ["dimension", "categorie", "nb_gestionnaires", "pct_total"]))

    lines.append("## Repartition par type de gestionnaire")
    lines.append("")
    lines.append(_md_table(type_rows, ["categorie", "nb_gestionnaires", "pct_total", "nb_avec_signal", "pct_signal"]))

    lines.append("## Repartition par taille de gestionnaire")
    lines.append("")
    lines.append(_md_table(taille_rows, ["categorie", "nb_gestionnaires", "pct_total", "nb_avec_signal", "pct_signal"]))

    lines.append("## Repartition par secteur d activite")
    lines.append("")
    lines.append(_md_table(secteur_rows, ["categorie", "nb_gestionnaires", "pct_total", "nb_avec_signal", "pct_signal"]))

    lines.append("## % gestionnaires avec signaux par secteur (ecarts)")
    lines.append("")
    lines.append(
        _md_table(
            secteur_gaps_rows,
            [
                "categorie",
                "nb_gestionnaires",
                "pct_signal",
                "ecart_pp_mediane",
                "ecart_pp_plus_representee",
                "categorie_plus_representee",
            ],
        )
    )

    lines.append("## % gestionnaires avec signaux par statut juridique (ecarts)")
    lines.append("")
    lines.append(
        _md_table(
            statut_gaps_rows,
            [
                "categorie",
                "nb_gestionnaires",
                "pct_signal",
                "ecart_pp_mediane",
                "ecart_pp_plus_representee",
                "categorie_plus_representee",
            ],
        )
    )

    lines.append("## % gestionnaires avec signaux par taille (ecarts)")
    lines.append("")
    lines.append(
        _md_table(
            taille_gaps_rows,
            [
                "categorie",
                "nb_gestionnaires",
                "pct_signal",
                "ecart_pp_mediane",
                "ecart_pp_plus_representee",
                "categorie_plus_representee",
            ],
        )
    )

    lines.append("## Termes recurrents dans signal_tension_detail")
    lines.append("")
    lines.append(_md_table(terms_detail_rows, ["terme", "occurrences"]))

    lines.append("## Termes recurrents dans les snippets de decision")
    lines.append("")
    lines.append(_md_table(terms_snippet_rows, ["terme", "occurrences"]))

    lines.append("## Insights rapides")
    lines.append("")

    if global_rows:
        global_map = {r.get("label"): r.get("valeur") for r in global_rows}
        lines.append(
            "- Volume scope: "
            + str(global_map.get("Gestionnaires dans le scope", "n/a"))
            + ", avec signal: "
            + str(global_map.get("Gestionnaires avec signal", "n/a"))
            + " ("
            + str(global_map.get("Pct gestionnaires avec signal", "n/a"))
            + "%)."
        )

    if et_tension_rows:
        et = et_tension_rows[0]
        lines.append(
            "- ET rattaches a des gestionnaires en tension: "
            + str(et.get("pct_et_gest_tension", "n/a"))
            + "% ("
            + str(et.get("nb_et_gest_tension", "n/a"))
            + "/"
            + str(et.get("nb_et_total", "n/a"))
            + ")."
        )

    if type_rows:
        top_type = type_rows[0]
        lines.append(
            f"- Type de gestionnaire le plus represente: {top_type.get('categorie')} "
            f"({top_type.get('nb_gestionnaires')} gestionnaires, {top_type.get('pct_signal')}% avec signal)."
        )

    if taille_rows:
        top_taille = taille_rows[0]
        lines.append(
            f"- Taille la plus representee: {top_taille.get('categorie')} "
            f"({top_taille.get('nb_gestionnaires')} gestionnaires, {top_taille.get('pct_signal')}% avec signal)."
        )

    if secteur_rows:
        top_secteur = secteur_rows[0]
        lines.append(
            f"- Secteur principal dominant: {top_secteur.get('categorie')} "
            f"({top_secteur.get('nb_gestionnaires')} gestionnaires, {top_secteur.get('pct_signal')}% avec signal)."
        )

    if terms_detail_rows:
        top_terms = ", ".join([str(r.get("terme")) for r in terms_detail_rows[:8]])
        lines.append(f"- Mots recurrents dans les details gestionnaire: {top_terms}.")

    if terms_snippet_rows:
        top_terms_snip = ", ".join([str(r.get("terme")) for r in terms_snippet_rows[:8]])
        lines.append(f"- Mots recurrents dans les snippets: {top_terms_snip}.")

    lines.append("")
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def main() -> None:
    args = parse_args()
    base_cte = build_base_cte(include_all=args.all_gestionnaires, only_visible=args.only_visible, financial_only=args.financial_only)

    out_dir: Path | None = None
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = REPO_ROOT / "outputs" / f"stats_signaux_v2_{timestamp}"

    q_global = f"""
{base_cte}
SELECT
    'Gestionnaires dans le scope' AS label,
    COUNT(*)::text AS valeur
FROM base
UNION ALL
SELECT
    'Gestionnaires avec signal' AS label,
    COUNT(*) FILTER (WHERE has_signal)::text AS valeur
FROM base
UNION ALL
SELECT
    'Pct gestionnaires avec signal' AS label,
    ROUND(100.0 * COUNT(*) FILTER (WHERE has_signal) / NULLIF(COUNT(*), 0), 2)::text AS valeur
FROM base
UNION ALL
SELECT
    'Signal financier' AS label,
    COUNT(*) FILTER (WHERE COALESCE(signal_financier, FALSE))::text AS valeur
FROM base
UNION ALL
SELECT
    'Signal RH' AS label,
    COUNT(*) FILTER (WHERE COALESCE(signal_rh, FALSE))::text AS valeur
FROM base
UNION ALL
SELECT
    'Signal qualite' AS label,
    COUNT(*) FILTER (WHERE COALESCE(signal_qualite, FALSE))::text AS valeur
FROM base
UNION ALL
SELECT
    'Signal juridique' AS label,
    COUNT(*) FILTER (WHERE COALESCE(signal_juridique, FALSE))::text AS valeur
FROM base
UNION ALL
SELECT
    'Multi-axes (>=2)' AS label,
    COUNT(*) FILTER (WHERE nb_axes >= 2)::text AS valeur
FROM base;
"""

    q_et_tension = f"""
{base_cte}
SELECT
    COUNT(*) AS nb_et_total,
    COUNT(*) FILTER (WHERE COALESCE(b.signal_tension, FALSE)) AS nb_et_gest_tension,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE COALESCE(b.signal_tension, FALSE)) / NULLIF(COUNT(*), 0),
        2
    ) AS pct_et_gest_tension,
    COUNT(*) FILTER (WHERE b.has_signal) AS nb_et_gest_signal,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE b.has_signal) / NULLIF(COUNT(*), 0),
        2
    ) AS pct_et_gest_signal
FROM public.finess_etablissement e
JOIN base b ON b.id_gestionnaire = e.id_gestionnaire
WHERE {et_scope_clause(args.all_gestionnaires, args.only_visible)};
"""

    q_signal_types = f"""
{base_cte}
SELECT
    'niveau_suspicion' AS dimension,
    COALESCE(NULLIF(signal_v2_niveau_suspicion, ''), 'NON_RENSEIGNE') AS categorie,
    COUNT(*) AS nb_gestionnaires,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_total
FROM base
GROUP BY 1, 2
UNION ALL
SELECT
    'statut_couverture' AS dimension,
    COALESCE(NULLIF(signal_v2_statut_couverture, ''), 'NON_RENSEIGNE') AS categorie,
    COUNT(*) AS nb_gestionnaires,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_total
FROM base
GROUP BY 1, 2
UNION ALL
SELECT
    'imputabilite' AS dimension,
    COALESCE(NULLIF(signal_v2_imputabilite, ''), 'NON_RENSEIGNE') AS categorie,
    COUNT(*) AS nb_gestionnaires,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_total
FROM base
GROUP BY 1, 2
ORDER BY dimension, nb_gestionnaires DESC, categorie ASC;
"""

    q_type_gestionnaire = f"""
{base_cte}
SELECT
    COALESCE(NULLIF(type_gestionnaire_final, ''), 'NON_RENSEIGNE') AS categorie,
    COUNT(*) AS nb_gestionnaires,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_total,
    COUNT(*) FILTER (WHERE has_signal) AS nb_avec_signal,
    ROUND(100.0 * COUNT(*) FILTER (WHERE has_signal) / NULLIF(COUNT(*), 0), 2) AS pct_signal
FROM base
GROUP BY 1
ORDER BY nb_gestionnaires DESC, categorie ASC
LIMIT %s;
"""

    q_taille = f"""
{base_cte}
SELECT
    COALESCE(NULLIF(categorie_taille, ''), 'NON_RENSEIGNE') AS categorie,
    COUNT(*) AS nb_gestionnaires,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_total,
    COUNT(*) FILTER (WHERE has_signal) AS nb_avec_signal,
    ROUND(100.0 * COUNT(*) FILTER (WHERE has_signal) / NULLIF(COUNT(*), 0), 2) AS pct_signal
FROM base
GROUP BY 1
ORDER BY nb_gestionnaires DESC, categorie ASC;
"""

    q_secteur = f"""
{base_cte}
SELECT
    COALESCE(NULLIF(secteur_activite_principal, ''), 'NON_RENSEIGNE') AS categorie,
    COUNT(*) AS nb_gestionnaires,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 2) AS pct_total,
    COUNT(*) FILTER (WHERE has_signal) AS nb_avec_signal,
    ROUND(100.0 * COUNT(*) FILTER (WHERE has_signal) / NULLIF(COUNT(*), 0), 2) AS pct_signal
FROM base
GROUP BY 1
ORDER BY nb_gestionnaires DESC, categorie ASC
LIMIT %s;
"""

    q_secteur_gaps = f"""
{base_cte},
agg AS (
    SELECT
        COALESCE(NULLIF(secteur_activite_principal, ''), 'NON_RENSEIGNE') AS categorie,
        COUNT(*) AS nb_gestionnaires,
        ROUND(100.0 * COUNT(*) FILTER (WHERE has_signal) / NULLIF(COUNT(*), 0), 2) AS pct_signal
    FROM base
    GROUP BY 1
),
med AS (
    SELECT ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_signal))::numeric, 2) AS mediane_pct_signal
    FROM agg
),
topcat AS (
    SELECT categorie AS categorie_plus_representee, pct_signal AS pct_signal_cat_plus_representee
    FROM agg
    ORDER BY nb_gestionnaires DESC, categorie ASC
    LIMIT 1
)
SELECT
    a.categorie,
    a.nb_gestionnaires,
    a.pct_signal,
    ROUND(a.pct_signal - m.mediane_pct_signal, 2) AS ecart_pp_mediane,
    ROUND(a.pct_signal - t.pct_signal_cat_plus_representee, 2) AS ecart_pp_plus_representee,
    t.categorie_plus_representee
FROM agg a
CROSS JOIN med m
CROSS JOIN topcat t
ORDER BY a.nb_gestionnaires DESC, a.categorie ASC
LIMIT %s;
"""

    q_statut_gaps = f"""
{base_cte},
agg AS (
    SELECT
        COALESCE(NULLIF(forme_juridique_libelle, ''), 'NON_RENSEIGNE') AS categorie,
        COUNT(*) AS nb_gestionnaires,
        ROUND(100.0 * COUNT(*) FILTER (WHERE has_signal) / NULLIF(COUNT(*), 0), 2) AS pct_signal
    FROM base
    GROUP BY 1
),
med AS (
    SELECT ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_signal))::numeric, 2) AS mediane_pct_signal
    FROM agg
),
topcat AS (
    SELECT categorie AS categorie_plus_representee, pct_signal AS pct_signal_cat_plus_representee
    FROM agg
    ORDER BY nb_gestionnaires DESC, categorie ASC
    LIMIT 1
)
SELECT
    a.categorie,
    a.nb_gestionnaires,
    a.pct_signal,
    ROUND(a.pct_signal - m.mediane_pct_signal, 2) AS ecart_pp_mediane,
    ROUND(a.pct_signal - t.pct_signal_cat_plus_representee, 2) AS ecart_pp_plus_representee,
    t.categorie_plus_representee
FROM agg a
CROSS JOIN med m
CROSS JOIN topcat t
ORDER BY a.nb_gestionnaires DESC, a.categorie ASC
LIMIT %s;
"""

    q_taille_gaps = f"""
{base_cte},
agg AS (
    SELECT
        COALESCE(NULLIF(categorie_taille, ''), 'NON_RENSEIGNE') AS categorie,
        COUNT(*) AS nb_gestionnaires,
        ROUND(100.0 * COUNT(*) FILTER (WHERE has_signal) / NULLIF(COUNT(*), 0), 2) AS pct_signal
    FROM base
    GROUP BY 1
),
med AS (
    SELECT ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pct_signal))::numeric, 2) AS mediane_pct_signal
    FROM agg
),
topcat AS (
    SELECT categorie AS categorie_plus_representee, pct_signal AS pct_signal_cat_plus_representee
    FROM agg
    ORDER BY nb_gestionnaires DESC, categorie ASC
    LIMIT 1
)
SELECT
    a.categorie,
    a.nb_gestionnaires,
    a.pct_signal,
    ROUND(a.pct_signal - m.mediane_pct_signal, 2) AS ecart_pp_mediane,
    ROUND(a.pct_signal - t.pct_signal_cat_plus_representee, 2) AS ecart_pp_plus_representee,
    t.categorie_plus_representee
FROM agg a
CROSS JOIN med m
CROSS JOIN topcat t
ORDER BY a.nb_gestionnaires DESC, a.categorie ASC;
"""

    stopwords = [
        "avec", "dans", "pour", "sans", "plus", "moins", "tres", "etre", "avoir", "apres",
        "avant", "entre", "ainsi", "suite", "fait", "toute", "toutes", "tout", "leurs", "leur",
        "cette", "cet", "ceux", "celle", "elles", "elle", "nous", "vous", "ils", "elles",
        "des", "les", "une", "sur", "par", "est", "pas", "aux", "ses", "son", "mais",
        "dans", "depuis", "aussi", "comme", "etre", "ont", "sont", "ete", "ces", "afin",
        "dune", "dun", "cote", "cas", "dont", "via", "qui", "que", "quoi", "leur", "leurs",
        "gestionnaire", "gestionnaires", "etablissement", "etablissements", "association", "fondation",
        "service", "services", "social", "medico", "medical", "france", "departement", "region",
    ]
    stopwords_sql = ", ".join(["'" + w.replace("'", "''") + "'" for w in stopwords])

    q_terms_detail = f"""
{base_cte},
tokens AS (
    SELECT lower(token) AS terme
    FROM base
    CROSS JOIN LATERAL regexp_split_to_table(
        regexp_replace(COALESCE(signal_tension_detail, ''), '[^[:alnum:] ]', ' ', 'g'),
        '\\s+'
    ) AS token
    WHERE has_signal
)
SELECT
    terme,
    COUNT(*) AS occurrences
FROM tokens
WHERE terme <> ''
  AND char_length(terme) >= 4
  AND terme !~ '^[0-9]+$'
  AND terme NOT IN ({stopwords_sql})
GROUP BY terme
ORDER BY occurrences DESC, terme ASC
LIMIT %s;
"""

    q_terms_snippets = f"""
{base_cte},
tokens AS (
    SELECT lower(token) AS terme
    FROM public.finess_signal_v2_snippet s
    JOIN base b ON b.id_gestionnaire = s.id_gestionnaire
    CROSS JOIN LATERAL regexp_split_to_table(
        regexp_replace(COALESCE(s.title, '') || ' ' || COALESCE(s.snippet, ''), '[^[:alnum:] ]', ' ', 'g'),
        '\\s+'
    ) AS token
    WHERE b.has_signal
      AND COALESCE(s.used_for_decision, FALSE)
)
SELECT
    terme,
    COUNT(*) AS occurrences
FROM tokens
WHERE terme <> ''
  AND char_length(terme) >= 4
  AND terme !~ '^[0-9]+$'
  AND terme NOT IN ({stopwords_sql})
GROUP BY terme
ORDER BY occurrences DESC, terme ASC
LIMIT %s;
"""

    with psycopg2.connect(**get_db_config()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            global_rows = fetch_rows(cur, q_global)
            et_tension_rows = fetch_rows(cur, q_et_tension)
            signal_type_rows = fetch_rows(cur, q_signal_types)
            type_rows = fetch_rows(cur, q_type_gestionnaire, [args.top])
            taille_rows = fetch_rows(cur, q_taille)
            secteur_rows = fetch_rows(cur, q_secteur, [args.top])
            secteur_gaps_rows = fetch_rows(cur, q_secteur_gaps, [args.top])
            statut_gaps_rows = fetch_rows(cur, q_statut_gaps, [args.top])
            taille_gaps_rows = fetch_rows(cur, q_taille_gaps)
            terms_detail_rows = fetch_rows(cur, q_terms_detail, [args.top_terms])
            terms_snippet_rows = fetch_rows(cur, q_terms_snippets, [args.top_terms])

    print("\n###############################################")
    print("# STATS SIGNAUX / GESTIONNAIRES (SNAPSHOT DB) #")
    print("###############################################")
    print(
        "Scope:",
        "tous gestionnaires" if args.all_gestionnaires else "gestionnaires avec au moins un ESSMS non-SAA",
    )

    print_kv_section("KPI globaux", global_rows)
    print_kv_section("ET rattaches a des gestionnaires en tension", [
        {
            "label": "ET total",
            "valeur": et_tension_rows[0]["nb_et_total"],
        },
        {
            "label": "ET sur gestionnaires en tension",
            "valeur": f"{et_tension_rows[0]['nb_et_gest_tension']} ({et_tension_rows[0]['pct_et_gest_tension']}%)",
        },
        {
            "label": "ET sur gestionnaires avec signal",
            "valeur": f"{et_tension_rows[0]['nb_et_gest_signal']} ({et_tension_rows[0]['pct_et_gest_signal']}%)",
        },
    ])

    print("\n=== Types de signaux (distributions V2) ===")
    for row in signal_type_rows:
        print(f"- [{row['dimension']}] {row['categorie']}: {row['nb_gestionnaires']} ({row['pct_total']}%)")

    print_group_section("Repartition par type de gestionnaire (forme juridique)", type_rows)
    print_group_section("Repartition par taille de gestionnaire", taille_rows)
    print_group_section("Repartition par secteur d'activite", secteur_rows)
    print_group_section_with_gap("% avec signal par secteur (ecart mediane et cat. plus representee)", secteur_gaps_rows)
    print_group_section_with_gap("% avec signal par statut juridique (ecart mediane et cat. plus representee)", statut_gaps_rows)
    print_group_section_with_gap("% avec signal par taille (ecart mediane et cat. plus representee)", taille_gaps_rows)

    print("\n=== Top termes recurrents (signal_tension_detail) ===")
    for row in terms_detail_rows:
        print(f"- {row['terme']}: {row['occurrences']}")

    print("\n=== Top termes recurrents (snippets utilises pour decision) ===")
    for row in terms_snippet_rows:
        print(f"- {row['terme']}: {row['occurrences']}")

    maybe_export_csv(out_dir, "kpi_globaux.csv", global_rows)
    maybe_export_csv(out_dir, "kpi_et_rattaches_tension.csv", et_tension_rows)
    maybe_export_csv(out_dir, "types_signaux_v2.csv", signal_type_rows)
    maybe_export_csv(out_dir, "repartition_type_gestionnaire.csv", type_rows)
    maybe_export_csv(out_dir, "repartition_taille_gestionnaire.csv", taille_rows)
    maybe_export_csv(out_dir, "repartition_secteur_activite.csv", secteur_rows)
    maybe_export_csv(out_dir, "pct_signal_par_secteur_ecarts.csv", secteur_gaps_rows)
    maybe_export_csv(out_dir, "pct_signal_par_statut_ecarts.csv", statut_gaps_rows)
    maybe_export_csv(out_dir, "pct_signal_par_taille_ecarts.csv", taille_gaps_rows)
    maybe_export_csv(out_dir, "top_termes_signal_tension_detail.csv", terms_detail_rows)
    maybe_export_csv(out_dir, "top_termes_snippets_decision.csv", terms_snippet_rows)

    scope_label = (
        "tous gestionnaires"
        if args.all_gestionnaires
        else "gestionnaires avec au moins un ESSMS non-SAA"
    )
    if not args.no_markdown:
        report_path = write_markdown_report(
            out_dir=out_dir,
            scope_label=scope_label,
            et_tension_rows=et_tension_rows,
            global_rows=global_rows,
            signal_type_rows=signal_type_rows,
            type_rows=type_rows,
            taille_rows=taille_rows,
            secteur_rows=secteur_rows,
            secteur_gaps_rows=secteur_gaps_rows,
            statut_gaps_rows=statut_gaps_rows,
            taille_gaps_rows=taille_gaps_rows,
            terms_detail_rows=terms_detail_rows,
            terms_snippet_rows=terms_snippet_rows,
        )
        print(f"Rapport markdown: {report_path}")

    print(f"\nExports CSV: {out_dir}")


if __name__ == "__main__":
    main()
