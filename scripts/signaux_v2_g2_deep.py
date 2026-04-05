"""Passe G2 V2: qualification approfondie des cas suspects issus de G0.

Objectif:
- Reprendre les gestionnaires classes possible/probable/certain
- Exclure strictement ceux deja traites en serper_passe_b
- Reutiliser les snippets G0 stockes et ajouter des requetes ciblees
- Produire une qualification profonde equivalente a la passe B
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

import psycopg2.extras
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from database import DatabaseManager
from enrich_finess_config import GEMINI_CONFIG, MISTRAL_CONFIG
from signaux_v2_exhaustif_common import (
    build_run_id,
    choose_public_name,
    create_run,
    fetch_candidates_g2,
    finish_run,
    load_recent_snippets,
    store_snippets,
    update_g2_row,
)
from signaux_v2_passe_b import (
    _clean_secret,
    apply_scope_filter_llm,
    build_prompt,
    build_queries,
    collect_snippets,
    has_sensitive_v1_type,
    infer_from_snippets,
    llm_json,
    normalize_result,
    summarize_v1,
    tighten_result,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Passe G2 approfondie des signaux V2")
    parser.add_argument("--batch-offset", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--dept", default="", help="Liste de departements, ex: 75,69")
    parser.add_argument("--max-serper-results", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force-rerun", action="store_true")
    parser.add_argument("--scope-filter-llm", action="store_true")
    parser.add_argument("--skip-serper-llm", action="store_true")
    parser.add_argument("--run-id", default="", help="Identifiant logique de run")
    return parser.parse_args()


def infer_imputabilite(snippets: List[Dict[str, Any]]) -> str:
    if any((snippet.get("imputabilite") == "gestionnaire_certain") for snippet in snippets):
        return "gestionnaire_certain"
    if any((snippet.get("imputabilite") == "gestionnaire_probable") for snippet in snippets):
        return "gestionnaire_probable"
    return "inconnue"


def infer_scope_issue(snippets: List[Dict[str, Any]]) -> str | None:
    for candidate in ["groupe_non_imputable", "ancrage_faible", "signal_local_limite", "preuves_insuffisantes"]:
        if any((snippet.get("discarded_reason") == candidate) for snippet in snippets):
            return candidate
    return None


def send_completion_email_cloud(
    *,
    run_id: str,
    provider: str,
    llm_model: str,
    status: str,
    stats: Dict[str, int],
    duration_sec: float,
) -> bool:
    """Send completion email directly from Cloud Run execution."""
    enabled = os.getenv("ENABLE_COMPLETION_EMAIL", "1").strip().lower() not in {"0", "false", "no", "off"}
    if not enabled:
        print("[EMAIL] Desactive (ENABLE_COMPLETION_EMAIL=0)")
        return False

    api_key = _clean_secret(os.getenv("ELASTICMAIL_API_KEY", ""))
    if not api_key:
        print("[EMAIL] ELASTICMAIL_API_KEY manquante, envoi ignore")
        return False

    recipient = (os.getenv("NOTIFICATION_EMAIL", "") or "").strip() or "patrick.danto@confidensia.fr"
    sender = (os.getenv("SENDER_EMAIL", "") or "").strip() or "patrick.danto@bmse.fr"
    project = (os.getenv("GOOGLE_CLOUD_PROJECT", "") or os.getenv("GCP_PROJECT", "") or "").strip()

    subject_prefix = "OK" if status == "completed" else "ALERTE"
    subject = f"[{subject_prefix}] Signaux V2 G2 termine - run_id={run_id}"
    body = (
        "Campagne Signaux V2 G2 terminee.\n\n"
        f"run_id: {run_id}\n"
        f"status: {status}\n"
        f"provider/model: {provider}/{llm_model}\n"
        f"project: {project or 'n/a'}\n"
        f"duree: {round(duration_sec / 60.0, 1)} min\n\n"
        f"processed: {stats.get('processed', 0)}\n"
        f"updated: {stats.get('updated', 0)}\n"
        f"with_axis: {stats.get('with_axis', 0)}\n"
        f"no_axis: {stats.get('no_axis', 0)}\n"
        f"review_required: {stats.get('review_required', 0)}\n"
        f"llm_empty: {stats.get('llm_empty', 0)}\n"
        f"errors: {stats.get('errors', 0)}\n"
    )

    for attempt in range(1, 4):
        try:
            resp = requests.post(
                "https://api.elasticemail.com/v2/email/send",
                data={
                    "apikey": api_key,
                    "from": sender,
                    "fromName": "FINESS Pipeline",
                    "to": recipient,
                    "subject": subject,
                    "bodyText": body,
                },
                timeout=20,
            )
            if resp.ok and "true" in resp.text.lower():
                print(f"[EMAIL] Envoye via v2 a {recipient}")
                return True
        except Exception as exc:
            print(f"[EMAIL] v2 tentative {attempt}/3 en erreur: {exc}")
        time.sleep(min(10, attempt * 2))

    for attempt in range(1, 4):
        try:
            payload = {
                "Recipients": {"To": [recipient]},
                "Content": {
                    "From": sender,
                    "Subject": subject,
                    "Body": [{"ContentType": "PlainText", "Content": body}],
                },
            }
            resp = requests.post(
                "https://api.elasticemail.com/v4/emails/transactional",
                headers={
                    "X-ElasticEmail-ApiKey": api_key,
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=20,
            )
            if resp.ok:
                print(f"[EMAIL] Envoye via v4 a {recipient}")
                return True
        except Exception as exc:
            print(f"[EMAIL] v4 tentative {attempt}/3 en erreur: {exc}")
        time.sleep(min(10, attempt * 2))

    print("[EMAIL] Echec envoi apres tentatives")
    return False


def main() -> None:
    started_ts = time.time()
    args = parse_args()
    provider = os.getenv("LLM_PROVIDER", "gemini").strip().lower()
    serper_key = _clean_secret(os.getenv("SERPER_API_KEY", ""))
    if provider == "gemini":
        llm_key = _clean_secret(os.getenv("GEMINI_API_KEY", ""))
        llm_model = os.getenv("GEMINI_MODEL", str(GEMINI_CONFIG["model"]))
    else:
        llm_key = _clean_secret(os.getenv("MISTRAL_API_KEY", ""))
        llm_model = os.getenv("MISTRAL_MODEL", str(MISTRAL_CONFIG["model"]))

    if not args.dry_run and not args.skip_serper_llm:
        if not serper_key:
            raise RuntimeError("SERPER_API_KEY manquante")
        if not llm_key:
            raise RuntimeError(f"Cle LLM manquante pour provider={provider}")

    dept_list = [item.strip() for item in args.dept.split(",") if item.strip()] if args.dept else None
    run_id = args.run_id.strip() or build_run_id("G2")

    print("=" * 72)
    print("PASSE G2 - QUALIFICATION APPROFONDIE")
    print(f"run_id={run_id}")
    print(f"batch={args.batch_offset}+{args.batch_size} dry_run={args.dry_run} force_rerun={args.force_rerun}")
    print(f"provider={provider} llm_model={llm_model}")
    print(f"scope_filter_llm={args.scope_filter_llm} skip_serper_llm={args.skip_serper_llm}")
    print("filtre: exclusion stricte des gestionnaires deja en serper_passe_b")
    print("=" * 72)

    stats = {
        "processed": 0,
        "updated": 0,
        "no_axis": 0,
        "with_axis": 0,
        "review_required": 0,
        "llm_empty": 0,
        "errors": 0,
    }

    status = "completed"

    try:
        db = DatabaseManager()
        with db.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                create_run(cur, run_id, "G2", "Qualification approfondie des cas suspects hors gestionnaires deja serper_passe_b")
                conn.commit()

                rows = fetch_candidates_g2(
                    cur=cur,
                    batch_offset=args.batch_offset,
                    batch_size=args.batch_size,
                    dept_list=dept_list,
                    force_rerun=args.force_rerun,
                )
                print(f"Candidats G2: {len(rows)}")

                for idx, row in enumerate(rows, start=1):
                    gid = row["id_gestionnaire"]
                    public_name = choose_public_name(row.get("raison_sociale"), row.get("sigle"))
                    include_rh = bool(row.get("signal_rh")) or has_sensitive_v1_type(row.get("signaux_recents"))
                    queries = build_queries(public_name, include_rh_query=include_rh)
                    print(f"[{idx}/{len(rows)}] {gid} | {public_name} | suspicion={row.get('signal_v2_niveau_suspicion')}")
                    stats["processed"] += 1

                    try:
                        g0_snippets = load_recent_snippets(cur, gid, phases=["G0"], limit=6)
                        new_snippets: List[Dict[str, Any]] = []
                        norm: Dict[str, Any] | None = None
                        if not args.skip_serper_llm and not args.dry_run:
                            new_snippets = collect_snippets(cur, serper_key, queries, args.max_serper_results)

                        stored_g2 = store_snippets(cur, row, public_name, run_id, "G2", new_snippets)
                        combined_snippets: List[Dict[str, Any]] = []
                        combined_snippets.extend(g0_snippets)
                        combined_snippets.extend(
                            {
                                "query": item.get("query_text") or "",
                                "title": item.get("title") or "",
                                "snippet": item.get("snippet") or "",
                                "url": item.get("url") or "",
                                "source": item.get("domain") or "",
                                "imputabilite": item.get("imputabilite"),
                                "discarded_reason": item.get("discarded_reason"),
                            }
                            for item in stored_g2
                        )

                        if args.scope_filter_llm and not args.skip_serper_llm and combined_snippets:
                            filtered = apply_scope_filter_llm(
                                provider=provider,
                                llm_key=llm_key,
                                llm_model=llm_model,
                                row=row,
                                nom_req=public_name,
                                snippets=combined_snippets,
                            )
                            combined_snippets = filtered["kept"]
                            if filtered.get("used"):
                                print(f"  -> scope filter: dropped={filtered['dropped']} kept={len(combined_snippets)}")

                        if not combined_snippets:
                            norm = {
                                "signal_financier": False,
                                "signal_financier_detail": None,
                                "signal_rh": False,
                                "signal_rh_detail": None,
                                "signal_qualite": False,
                                "signal_qualite_detail": None,
                                "signal_juridique": False,
                                "signal_juridique_detail": None,
                                "sources": [],
                                "confiance": "basse",
                                "review_required": True,
                            }
                        elif args.skip_serper_llm or args.dry_run:
                            norm = {
                                "signal_financier": False,
                                "signal_financier_detail": None,
                                "signal_rh": False,
                                "signal_rh_detail": None,
                                "signal_qualite": False,
                                "signal_qualite_detail": None,
                                "signal_juridique": False,
                                "signal_juridique_detail": None,
                                "sources": [snippet.get("url") for snippet in combined_snippets if snippet.get("url")][:5],
                                "confiance": "basse",
                                "review_required": True,
                            }
                        else:
                            prompt = build_prompt(row, public_name, summarize_v1(row.get("signaux_recents")), combined_snippets)
                            result = llm_json(provider, llm_key, llm_model, prompt, max_tokens=900)
                            if not result:
                                stats["llm_empty"] += 1
                                fallback = infer_from_snippets(combined_snippets)
                                if any(
                                    [
                                        fallback["signal_financier"],
                                        fallback["signal_rh"],
                                        fallback["signal_qualite"],
                                        fallback["signal_juridique"],
                                    ]
                                ):
                                    result = fallback
                                else:
                                    norm = {
                                        "signal_financier": False,
                                        "signal_financier_detail": None,
                                        "signal_rh": False,
                                        "signal_rh_detail": None,
                                        "signal_qualite": False,
                                        "signal_qualite_detail": None,
                                        "signal_juridique": False,
                                        "signal_juridique_detail": None,
                                        "sources": [snippet.get("url") for snippet in combined_snippets if snippet.get("url")][:5],
                                        "confiance": "basse",
                                        "review_required": True,
                                    }
                            if norm is None:
                                norm = normalize_result(result, combined_snippets)
                                norm = tighten_result(row, norm, combined_snippets)

                        imputabilite = infer_imputabilite(stored_g2 or g0_snippets)
                        scope_issue = infer_scope_issue(stored_g2 or g0_snippets)
                        decision_detail = (
                            f"G2 sur {len(combined_snippets)} snippet(s), confiance={norm['confiance']}, "
                            f"review={int(bool(norm.get('review_required')))}"
                        )

                        if any([norm["signal_financier"], norm["signal_rh"], norm["signal_qualite"], norm["signal_juridique"]]):
                            stats["with_axis"] += 1
                        else:
                            stats["no_axis"] += 1
                        if norm.get("review_required"):
                            stats["review_required"] += 1

                        print(
                            "  -> axes",
                            f"fin={int(norm['signal_financier'])}",
                            f"rh={int(norm['signal_rh'])}",
                            f"qual={int(norm['signal_qualite'])}",
                            f"jur={int(norm['signal_juridique'])}",
                            f"conf={norm['confiance']}",
                            f"imput={imputabilite}",
                        )

                        if not args.dry_run:
                            update_g2_row(
                                cur,
                                gid=gid,
                                run_id=run_id,
                                query_count=len(queries),
                                snippet_count=len(g0_snippets) + len(stored_g2),
                                norm=norm,
                                imputabilite=imputabilite,
                                scope_issue=scope_issue,
                                decision_detail=decision_detail,
                            )
                            conn.commit()
                        stats["updated"] += 1
                    except Exception as exc:
                        conn.rollback()
                        stats["errors"] += 1
                        print(f"  -> erreur: {exc}")

                status = "completed" if stats["errors"] == 0 else "failed"
                finish_run(cur, run_id, status, stats)
                conn.commit()
    except Exception:
        status = "failed"
        raise
    finally:
        send_completion_email_cloud(
            run_id=run_id,
            provider=provider,
            llm_model=llm_model,
            status=status,
            stats=stats,
            duration_sec=time.time() - started_ts,
        )

    print("=" * 72)
    print("STATUT:", stats)
    print("=" * 72)


if __name__ == "__main__":
    main()