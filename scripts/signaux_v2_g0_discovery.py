"""Passe G0 V2: criblage large et stockage des snippets.

Objectif:
- Couvrir exhaustivement les gestionnaires sans axes V2
- Exclure ceux deja traites en serper_passe_b
- Lancer une requete large a faible cout
- Stocker tous les snippets en base pour recalibration offline
- Classer chaque gestionnaire en aucun/possible/probable/certain
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2.extras

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from database import DatabaseManager
from signaux_v2_exhaustif_common import (
    build_light_tension_query,
    build_run_id,
    choose_public_name,
    create_run,
    fetch_candidates_g0,
    finish_run,
    store_snippets,
    summarize_g0,
    update_g0_row,
)
from signaux_v2_passe_b import _clean_secret, collect_snippets
from signaux_v2_passe_b import llm_json
from enrich_finess_config import GEMINI_CONFIG, MISTRAL_CONFIG


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Passe G0 exhaustive des signaux V2")
    parser.add_argument("--batch-offset", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--dept", default="", help="Liste de departements, ex: 75,69")
    parser.add_argument("--max-serper-results", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-rerun", action="store_true")
    parser.add_argument("--disable-llm-snippet-gate", action="store_true", help="Desactive le gate LLM binaire en G0")
    parser.add_argument("--run-id", default="", help="Identifiant logique de run")
    return parser.parse_args()


def _snippet_gate_prompt(row: Dict[str, Any], public_name: str, snippet: Dict[str, Any]) -> str:
    return f"""
Tu fais un tri binaire de snippet pour signaux de tension d'un gestionnaire medico-social.

Gestionnaire cible:
- nom_public: {public_name}
- raison_sociale: {row.get('raison_sociale') or ''}
- sigle: {row.get('sigle') or ''}
- departement: {row.get('departement_nom') or row.get('departement_code') or ''}

Snippet:
- title: {snippet.get('title') or ''}
- snippet: {snippet.get('snippet') or ''}
- url: {snippet.get('url') or ''}

Question:
Le snippet decrit-il un signal de difficulte reel (pas annuaire, pas prevention, pas offre d'emploi)
et imputable au gestionnaire cible ?

Reponds en JSON strict:
{{
  "signal_reel": true|false,
  "imputable": true|false,
  "reason": "texte court"
}}
""".strip()


def _apply_llm_snippet_gate(
    cur: psycopg2.extras.RealDictCursor,
    row: Dict[str, Any],
    run_id: str,
    public_name: str,
    provider: str,
    llm_key: str,
    llm_model: str,
    classified_snippets: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not llm_key:
        return classified_snippets

    adjusted: List[Dict[str, Any]] = []
    for item in classified_snippets:
        candidate = dict(item)
        if not candidate.get("used_for_decision"):
            adjusted.append(candidate)
            continue

        prompt = _snippet_gate_prompt(row, public_name, candidate)
        gate = llm_json(provider, llm_key, llm_model, prompt, max_tokens=220)
        signal_reel = bool(gate.get("signal_reel")) if isinstance(gate, dict) else False
        imputable = bool(gate.get("imputable")) if isinstance(gate, dict) else False
        keep = signal_reel and imputable

        candidate["llm_payload"] = gate if isinstance(gate, dict) else {"raw": gate}
        if not keep:
            reason = "llm_gate_reject"
            if isinstance(gate, dict) and isinstance(gate.get("reason"), str) and gate.get("reason").strip():
                reason = f"llm_gate:{gate.get('reason').strip()[:140]}"
            candidate["used_for_decision"] = False
            candidate["suspicion_level"] = "aucun"
            candidate["discarded_reason"] = reason

        cur.execute(
            """
            UPDATE public.finess_signal_v2_snippet
            SET used_for_decision = %s,
                suspicion_level = %s,
                discarded_reason = %s,
                llm_payload = %s::jsonb
            WHERE id_gestionnaire = %s
              AND run_id = %s
              AND phase = 'G0'
              AND query_hash = %s
              AND COALESCE(url, '') = COALESCE(%s, '')
            """,
            (
                candidate.get("used_for_decision"),
                candidate.get("suspicion_level"),
                candidate.get("discarded_reason"),
                json.dumps(candidate.get("llm_payload") or {}, ensure_ascii=False),
                row["id_gestionnaire"],
                run_id,
                candidate.get("query_hash"),
                candidate.get("url"),
            ),
        )
        adjusted.append(candidate)

    return adjusted


def main() -> None:
    args = parse_args()
    provider = os.getenv("LLM_PROVIDER", "gemini").strip().lower()
    serper_key = _clean_secret(os.getenv("SERPER_API_KEY", ""))
    if not args.dry_run and not serper_key:
        raise RuntimeError("SERPER_API_KEY manquante")

    llm_gate_enabled = not args.disable_llm_snippet_gate
    llm_key = ""
    llm_model = ""
    if llm_gate_enabled:
        if provider == "gemini":
            llm_key = _clean_secret(os.getenv("GEMINI_API_KEY", ""))
            llm_model = os.getenv("GEMINI_MODEL", str(GEMINI_CONFIG["model"]))
        else:
            llm_key = _clean_secret(os.getenv("MISTRAL_API_KEY", ""))
            llm_model = os.getenv("MISTRAL_MODEL", str(MISTRAL_CONFIG["model"]))

    dept_list = [item.strip() for item in args.dept.split(",") if item.strip()] if args.dept else None
    run_id = args.run_id.strip() or build_run_id("G0")

    print("=" * 72)
    print("PASSE G0 - TENSION LEGERE EXHAUSTIVE")
    print(f"run_id={run_id}")
    print(f"batch={args.batch_offset}+{args.batch_size} dry_run={args.dry_run} force_rerun={args.force_rerun}")
    print(f"llm_snippet_gate={llm_gate_enabled}")
    if dept_list:
        print(f"departements={','.join(dept_list)}")
    print("filtre: exclusion stricte des gestionnaires deja en serper_passe_b")
    print("=" * 72)

    stats = {
        "processed": 0,
        "updated": 0,
        "no_signal_public": 0,
        "signal_public_non_tension": 0,
        "signal_ambigu_review": 0,
        "signal_tension_probable": 0,
        "llm_gate_rejected": 0,
        "errors": 0,
    }

    db = DatabaseManager()
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            create_run(cur, run_id, "G0", "Passe large exhaustive hors gestionnaires deja serper_passe_b")
            conn.commit()

            rows = fetch_candidates_g0(
                cur=cur,
                batch_offset=args.batch_offset,
                batch_size=args.batch_size,
                dept_list=dept_list,
                force_rerun=args.force_rerun,
            )
            print(f"Candidats G0: {len(rows)}")

            for idx, row in enumerate(rows, start=1):
                gid = row["id_gestionnaire"]
                public_name = choose_public_name(row.get("raison_sociale"), row.get("sigle"))
                query = build_light_tension_query(row, public_name)
                print(f"[{idx}/{len(rows)}] {gid} | {public_name} | query={query}")
                stats["processed"] += 1

                try:
                    # Prevent two concurrent runs from processing the same manager at the same time.
                    cur.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS locked", (str(gid),))
                    lock_row = cur.fetchone() or {}
                    if not lock_row.get("locked"):
                        print("  -> skip: verrou deja pris par une autre execution")
                        continue

                    snippets: List[Dict[str, Any]] = []
                    if not args.dry_run:
                        snippets = collect_snippets(cur, serper_key, [query], args.max_serper_results)
                    stored = store_snippets(cur, row, public_name, run_id, "G0", snippets)
                    if not args.dry_run and llm_gate_enabled and llm_key:
                        before = sum(1 for item in stored if item.get("used_for_decision"))
                        stored = _apply_llm_snippet_gate(
                            cur=cur,
                            row=row,
                            run_id=run_id,
                            public_name=public_name,
                            provider=provider,
                            llm_key=llm_key,
                            llm_model=llm_model,
                            classified_snippets=stored,
                        )
                        after = sum(1 for item in stored if item.get("used_for_decision"))
                        if before > after:
                            stats["llm_gate_rejected"] += (before - after)

                    summary = summarize_g0(stored)

                    print(
                        "  ->",
                        f"statut={summary['statut']}",
                        f"suspicion={summary['suspicion']}",
                        f"imput={summary['imputabilite']}",
                        f"snippets={len(stored)}",
                    )

                    if not args.dry_run:
                        update_g0_row(
                            cur,
                            gid=gid,
                            run_id=run_id,
                            query_count=1,
                            snippet_count=len(stored),
                            summary=summary,
                        )
                        conn.commit()
                    stats[summary["statut"]] += 1
                    stats["updated"] += 1
                except Exception as exc:
                    conn.rollback()
                    stats["errors"] += 1
                    print(f"  -> erreur: {exc}")
                finally:
                    try:
                        cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (str(gid),))
                    except Exception:
                        # Ignore unlock errors after rollback; the session cleanup will release locks.
                        pass

            finish_run(cur, run_id, "completed" if stats["errors"] == 0 else "failed", stats)
            conn.commit()

    print("=" * 72)
    print("STATUT:", stats)
    print("=" * 72)


if __name__ == "__main__":
    main()
