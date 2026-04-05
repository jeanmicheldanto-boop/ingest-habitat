from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any, Dict, List

import psycopg2.extras

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from database import DatabaseManager


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export CSV d'un run G0 avec qualification + snippets")
    parser.add_argument("--run-id", required=True, help="Run ID G0 a exporter")
    parser.add_argument(
        "--output",
        default="",
        help="Chemin du CSV de sortie (defaut: outputs/signaux_v2_g0_<run-id>.csv)",
    )
    return parser.parse_args()


def fetch_rows(cur: psycopg2.extras.RealDictCursor, run_id: str) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT
            g.id_gestionnaire,
            g.raison_sociale,
            g.sigle,
            g.departement_code,
            g.departement_nom,
            g.secteur_activite_principal,
            g.nb_etablissements,
            g.signal_v2_phase,
            g.signal_v2_run_id,
            g.signal_v2_statut_couverture,
            g.signal_v2_niveau_suspicion,
            g.signal_v2_imputabilite,
            g.signal_v2_review_required,
            g.signal_v2_scope_issue,
            g.signal_v2_decision_detail,
            g.signal_v2_queries_count,
            g.signal_v2_snippets_count,
            g.signal_v2_last_query_at,
            s.id AS snippet_id,
            s.query_text,
            s.domain,
            s.url,
            s.title,
            s.snippet,
            s.alias_hit_type,
            s.alias_hit_value,
            s.scope_label,
            s.imputabilite AS snippet_imputabilite,
            s.suspicion_level AS snippet_suspicion_level,
            s.risk_score,
            s.scope_score,
            s.source_confidence,
            s.used_for_decision,
            s.discarded_reason,
            s.retrieved_at
        FROM public.finess_gestionnaire g
        LEFT JOIN public.finess_signal_v2_snippet s
               ON s.id_gestionnaire = g.id_gestionnaire
              AND s.run_id = %s
              AND s.phase = 'G0'
        WHERE g.signal_v2_run_id = %s
          AND g.signal_v2_phase = 'G0'
        ORDER BY g.id_gestionnaire ASC, s.used_for_decision DESC, s.risk_score DESC NULLS LAST, s.id ASC
        """,
        (run_id, run_id),
    )
    return [dict(row) for row in cur.fetchall()]


def main() -> None:
    args = parse_args()
    run_id = args.run_id.strip()

    output_path = Path(args.output).expanduser() if args.output else (REPO_ROOT / "outputs" / f"signaux_v2_g0_{run_id}.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    db = DatabaseManager()
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            rows = fetch_rows(cur, run_id)

    if not rows:
        print(f"Aucune ligne trouvee pour run_id={run_id}")
        return

    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Export termine: {output_path}")
    print(f"Lignes exportees: {len(rows)}")


if __name__ == "__main__":
    main()
