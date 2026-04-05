from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

import psycopg2.extras

from signaux_v2_passe_b import (
    _alias_profile,
    _passes_alias_guard,
    choose_public_name,
    normalize_text,
)

LIGHT_TENSION_PATTERNS = [
    r"difficultes? financieres?",
    r"administration provisoire",
    r"fermeture",
    r"souffrance",
    r"maltraitance",
]

STRONG_SIGNAL_PATTERNS = [
    r"administration provisoire",
    r"difficultes? financieres?",
    r"condamnation",
    r"mise en demeure",
    r"sanction",
]

LIGHT_TENSION_QUERY_CLAUSE = (
    '"difficultes financieres" OR "administration provisoire" OR fermeture OR souffrance OR maltraitance'
)

HIGH_CONFIDENCE_DOMAINS = {
    "apmnews.fr",
    "hospimedia.fr",
    "lien-social.com",
    "directions.fr",
    "gazette-sante-social.fr",
    "sanitaire-social.com",
    "lesechos.fr",
    "lemonde.fr",
    "liberation.fr",
    "actulegales.fr",
    "infogreffe.fr",
}

MEDIUM_CONFIDENCE_DOMAINS = {
    "ouest-france.fr",
    "sudouest.fr",
    "francebleu.fr",
    "france3-regions.francetvinfo.fr",
    "actu.fr",
    "linkedin.com",
}

NOISE_PATTERNS = [
    r"\bcpom\b",
    r"appel a projets",
    r"extension",
    r"renovation",
    r"inauguration",
    r"nomination",
    r"changement de direction",
    r"recrutement",
    r"offre d'emploi",
    r"mobilisation",
    r"manifestation",
    r"petition",
    r"nexem",
    r"fehap",
    r"uniopss",
    r"synerpa",
]

ALLOWED_SCOPE_ISSUES = {
    "groupe_non_imputable",
    "ancrage_faible",
    "signal_local_limite",
    "conflit_entite",
    "bruit_sectoriel",
    "preuves_insuffisantes",
    "polarite_ambigue",
}

DETERMINISTIC_DISQUALIFICATION_RULES: List[Tuple[str, List[str]]] = [
    (
        "formation_prevention_maltraitance",
        [r"formation", r"(maltraitance|bientraitance|prevention|sensibilisation|lutte contre)"],
    ),
    (
        "journee_mondiale_sensibilisation",
        [r"journee mondiale|semaine", r"(sensibilisation|prevention|lutte)", r"maltraitance"],
    ),
    (
        "accompagnement_souffrance",
        [r"accompagnement", r"souffrance", r"(psychique|psychologique|mentale|au travail|des aidants)"],
    ),
    (
        "offre_emploi_recrutement",
        [r"offre d'emploi|recrutement|nous recrutons|cdi|cdd|postulez"],
    ),
]

LOCAL_MARKER_PATTERN = re.compile(r"\b(ehpad|ime|mas|foyer|residence|site|etablissement|centre|mecs|esat)\b")


def build_run_id(phase: str) -> str:
    return f"{phase.lower()}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"


def build_light_tension_query(row: Dict[str, Any], public_name: str) -> str:
    name = (public_name or "").strip()
    dept = (row.get("departement_nom") or row.get("departement_code") or "").strip()
    is_short_or_ambiguous = len(name) <= 5
    if is_short_or_ambiguous and dept:
        return f'"{name}" "{dept}" ({LIGHT_TENSION_QUERY_CLAUSE})'
    return f'"{name}" ({LIGHT_TENSION_QUERY_CLAUSE})'


def extract_domain(url: str) -> str:
    raw = (url or "").strip().lower()
    if not raw:
        return ""
    try:
        host = urlparse(raw).netloc or raw
    except Exception:
        host = raw
    host = host.split("/")[0].strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def source_confidence_for_domain(domain: str) -> str:
    if domain in HIGH_CONFIDENCE_DOMAINS:
        return "haute"
    if domain in MEDIUM_CONFIDENCE_DOMAINS:
        return "moyenne"
    return "basse"


def count_patterns(patterns: Sequence[str], text: str) -> int:
    return sum(1 for pat in patterns if re.search(pat, text))


def deterministic_disqualification_reason(text: str) -> Optional[str]:
    for reason, patterns in DETERMINISTIC_DISQUALIFICATION_RULES:
        if all(re.search(pattern, text) for pattern in patterns):
            return reason
    return None


def scope_issue_from_scope(scope_label: str) -> Optional[str]:
    if scope_label == "entite_du_groupe":
        return "groupe_non_imputable"
    if scope_label == "secteur_general":
        return "bruit_sectoriel"
    if scope_label == "hors_perimetre":
        return "conflit_entite"
    return None


def sanitize_scope_issue(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value if value in ALLOWED_SCOPE_ISSUES else "ancrage_faible"


def classify_scope(row: Dict[str, Any], public_name: str, snippet: Dict[str, Any]) -> Tuple[str, str, str, str, float]:
    profile = _alias_profile(row, public_name)
    text = normalize_text(((snippet.get("title") or "") + " " + (snippet.get("snippet") or "")))
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    strong_aliases = profile.get("strong_aliases") or []
    weak_tokens = profile.get("weak_tokens") or []
    local_marker = bool(LOCAL_MARKER_PATTERN.search(text))

    strong_hit = next((alias for alias in strong_aliases if alias and alias in text), None)
    weak_hit = next((tok for tok in weak_tokens if tok and re.search(rf"\b{re.escape(tok)}\b", text)), None)
    passes_guard = _passes_alias_guard(snippet, profile)

    if strong_hit and passes_guard:
        return "gestionnaire_exact", "strong", strong_hit, "gestionnaire_certain", 1.0
    if local_marker and weak_hit and passes_guard:
        return "etablissement_local", "weak", weak_hit, "gestionnaire_probable", 0.75
    if weak_hit:
        return "entite_du_groupe", "weak", weak_hit, "inconnue", 0.35
    if any(token in text for token in ("ehpad", "ime", "mas", "medico social", "handicap", "social")):
        return "secteur_general", "none", "", "inconnue", 0.15
    return "hors_perimetre", "none", "", "inconnue", 0.0


def classify_light_tension_snippet(row: Dict[str, Any], public_name: str, snippet: Dict[str, Any]) -> Dict[str, Any]:
    scope_label, alias_hit_type, alias_hit_value, imputabilite, scope_score = classify_scope(row, public_name, snippet)
    domain = extract_domain(snippet.get("url") or "")
    source_confidence = source_confidence_for_domain(domain)

    text = normalize_text(((snippet.get("title") or "") + " " + (snippet.get("snippet") or "")))
    disqualification_reason = deterministic_disqualification_reason(text)
    negative_hits = count_patterns(LIGHT_TENSION_PATTERNS, text)
    strong_hits = [pat for pat in STRONG_SIGNAL_PATTERNS if re.search(pat, text)]
    strong_signal_hit = bool(strong_hits)
    noise_hits = count_patterns(NOISE_PATTERNS, text)

    risk_score = float(negative_hits * 2)
    if source_confidence == "haute":
        risk_score += 1.0
    elif source_confidence == "moyenne":
        risk_score += 0.5
    risk_score -= float(noise_hits)
    if scope_label in {"entite_du_groupe", "secteur_general", "hors_perimetre"}:
        risk_score -= 1.0

    suspicion_level = "aucun"
    used_for_decision = False
    discarded_reason = scope_issue_from_scope(scope_label)

    if disqualification_reason:
        return {
            "query_hash": hashlib.sha256((snippet.get("query") or "").encode("utf-8")).hexdigest(),
            "query_text": snippet.get("query") or "",
            "title": snippet.get("title") or "",
            "snippet": snippet.get("snippet") or "",
            "url": snippet.get("url") or "",
            "domain": domain,
            "alias_hit_type": alias_hit_type,
            "alias_hit_value": alias_hit_value,
            "scope_label": scope_label,
            "imputabilite": imputabilite,
            "suspicion_level": "aucun",
            "risk_score": 0.0,
            "scope_score": round(scope_score, 2),
            "freshness_score": None,
            "source_confidence": source_confidence,
            "used_for_decision": False,
            "discarded_reason": disqualification_reason,
            "llm_payload": None,
            "metadata": json.dumps({"source": snippet.get("source") or ""}, ensure_ascii=False),
        }

    if negative_hits <= 0:
        if noise_hits > 0:
            discarded_reason = discarded_reason or "bruit_sectoriel"
        else:
            discarded_reason = discarded_reason or "preuves_insuffisantes"
    else:
        if imputabilite == "gestionnaire_certain" and risk_score >= 3.0:
            suspicion_level = "certain"
            used_for_decision = True
        elif imputabilite in {"gestionnaire_certain", "gestionnaire_probable"} and risk_score >= 1.5:
            suspicion_level = "probable"
            used_for_decision = True
        else:
            suspicion_level = "possible"
            used_for_decision = True
            discarded_reason = discarded_reason or "ancrage_faible"

    if suspicion_level == "possible" and scope_label == "entite_du_groupe":
        discarded_reason = "groupe_non_imputable"
    if suspicion_level == "possible" and scope_label == "etablissement_local":
        discarded_reason = "signal_local_limite"

    # Promote snippets with strong tension cues when imputability points to the manager.
    if (
        not disqualification_reason
        and strong_signal_hit
        and imputabilite in {"gestionnaire_probable", "gestionnaire_certain"}
        and suspicion_level in {"aucun", "possible"}
    ):
        suspicion_level = "probable"
        used_for_decision = True
        if discarded_reason in {"ancrage_faible", "preuves_insuffisantes", "source_basse_retrograde"}:
            discarded_reason = None

    # Strongly limit false positives from very weak sources.
    if source_confidence == "basse" and suspicion_level == "certain":
        suspicion_level = "probable"
        discarded_reason = discarded_reason or "source_basse_retrograde"
    elif (
        source_confidence == "basse"
        and suspicion_level == "probable"
        and not (strong_signal_hit and imputabilite in {"gestionnaire_probable", "gestionnaire_certain"})
    ):
        suspicion_level = "possible"
        discarded_reason = discarded_reason or "source_basse_retrograde"

    return {
        "query_hash": hashlib.sha256((snippet.get("query") or "").encode("utf-8")).hexdigest(),
        "query_text": snippet.get("query") or "",
        "title": snippet.get("title") or "",
        "snippet": snippet.get("snippet") or "",
        "url": snippet.get("url") or "",
        "domain": domain,
        "alias_hit_type": alias_hit_type,
        "alias_hit_value": alias_hit_value,
        "scope_label": scope_label,
        "imputabilite": imputabilite,
        "suspicion_level": suspicion_level,
        "risk_score": round(risk_score, 2),
        "scope_score": round(scope_score, 2),
        "freshness_score": None,
        "source_confidence": source_confidence,
        "used_for_decision": used_for_decision,
        "discarded_reason": discarded_reason,
        "llm_payload": None,
        "metadata": json.dumps(
            {
                "source": snippet.get("source") or "",
                "strong_signal_hit": strong_signal_hit,
                "strong_signal_patterns": strong_hits,
            },
            ensure_ascii=False,
        ),
    }


def _snippet_has_strong_signal(item: Dict[str, Any]) -> bool:
    metadata_raw = item.get("metadata")
    if isinstance(metadata_raw, str):
        try:
            metadata = json.loads(metadata_raw)
        except json.JSONDecodeError:
            metadata = {}
    elif isinstance(metadata_raw, dict):
        metadata = metadata_raw
    else:
        metadata = {}
    return bool(metadata.get("strong_signal_hit"))


def create_run(cur: psycopg2.extras.RealDictCursor, run_id: str, phase: str, notes: str, initiated_by: str = "script") -> None:
    cur.execute(
        """
        INSERT INTO public.finess_signal_v2_run (run_id, phase, initiated_by, notes)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (run_id) DO NOTHING
        """,
        (run_id, phase, initiated_by, notes),
    )


def finish_run(cur: psycopg2.extras.RealDictCursor, run_id: str, status: str, metrics: Dict[str, Any]) -> None:
    cur.execute(
        """
        UPDATE public.finess_signal_v2_run
        SET status = %s,
            finished_at = NOW(),
            metrics = %s::jsonb
        WHERE run_id = %s
        """,
        (status, json.dumps(metrics, ensure_ascii=False), run_id),
    )


def fetch_candidates_g0(
    cur: psycopg2.extras.RealDictCursor,
    batch_offset: int,
    batch_size: int,
    dept_list: Optional[List[str]],
    force_rerun: bool,
) -> List[Dict[str, Any]]:
    dept_filter_sql = ""
    params: List[Any] = []
    if dept_list:
        dept_filter_sql = " AND g.departement_code = ANY(%s)"
        params.append(dept_list)

    rerun_sql = ""
    if not force_rerun:
        rerun_sql = " AND COALESCE(g.signal_v2_phase, '') NOT IN ('G0', 'G1', 'G2')"

    sql = f"""
    SELECT
        g.id_gestionnaire,
        g.raison_sociale,
        g.sigle,
        g.departement_code,
        g.departement_nom,
        g.secteur_activite_principal,
        g.nb_etablissements,
        g.signal_v2_methode,
        g.signal_v2_phase,
        g.signal_v2_statut_couverture
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
      AND COALESCE(g.signal_v2_methode, '') <> 'serper_passe_b'
      AND NOT (COALESCE(g.signal_financier, FALSE) OR COALESCE(g.signal_rh, FALSE)
               OR COALESCE(g.signal_qualite, FALSE) OR COALESCE(g.signal_juridique, FALSE))
      {rerun_sql}
      {dept_filter_sql}
    ORDER BY COALESCE(g.nb_etablissements, 0) DESC, g.id_gestionnaire ASC
    OFFSET %s LIMIT %s
    """
    params.extend([batch_offset, batch_size])
    cur.execute(sql, tuple(params))
    return [dict(row) for row in cur.fetchall()]


def fetch_candidates_g2(
    cur: psycopg2.extras.RealDictCursor,
    batch_offset: int,
    batch_size: int,
    dept_list: Optional[List[str]],
    force_rerun: bool,
) -> List[Dict[str, Any]]:
    dept_filter_sql = ""
    params: List[Any] = []
    if dept_list:
        dept_filter_sql = " AND g.departement_code = ANY(%s)"
        params.append(dept_list)

    rerun_sql = ""
    if not force_rerun:
        rerun_sql = " AND COALESCE(g.signal_v2_methode, '') <> 'serper_passe_b'"

    sql = f"""
    SELECT
        g.id_gestionnaire,
        g.raison_sociale,
        g.sigle,
        g.departement_code,
        g.departement_nom,
        g.secteur_activite_principal,
        g.nb_etablissements,
        g.signal_v2_methode,
        g.signal_v2_phase,
        g.signal_v2_statut_couverture,
        g.signal_v2_niveau_suspicion,
        g.signal_v2_imputabilite,
        g.signal_v2_review_required,
        g.signal_v2_scope_issue,
        g.signaux_recents,
        g.signal_rh
    FROM public.finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1
        FROM public.finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
      AND COALESCE(g.signal_v2_niveau_suspicion, 'aucun') IN ('possible', 'probable', 'certain')
      AND COALESCE(g.signal_v2_statut_couverture, '') IN ('signal_tension_probable', 'signal_ambigu_review')
      {rerun_sql}
      {dept_filter_sql}
    ORDER BY
      CASE COALESCE(g.signal_v2_niveau_suspicion, 'aucun')
        WHEN 'certain' THEN 0
        WHEN 'probable' THEN 1
        WHEN 'possible' THEN 2
        ELSE 9
      END,
      COALESCE(g.nb_etablissements, 0) DESC,
      g.id_gestionnaire ASC
    OFFSET %s LIMIT %s
    """
    params.extend([batch_offset, batch_size])
    cur.execute(sql, tuple(params))
    return [dict(row) for row in cur.fetchall()]


def store_snippets(
    cur: psycopg2.extras.RealDictCursor,
    row: Dict[str, Any],
    public_name: str,
    run_id: str,
    phase: str,
    snippets: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    stored: List[Dict[str, Any]] = []
    for rank, snippet in enumerate(snippets, start=1):
        classified = classify_light_tension_snippet(row, public_name, snippet)
        cur.execute(
            """
            INSERT INTO public.finess_signal_v2_snippet (
                id_gestionnaire,
                run_id,
                phase,
                query_hash,
                query_text,
                serper_rank,
                title,
                snippet,
                url,
                domain,
                alias_hit_type,
                alias_hit_value,
                scope_label,
                imputabilite,
                suspicion_level,
                risk_score,
                scope_score,
                freshness_score,
                source_confidence,
                used_for_decision,
                discarded_reason,
                llm_payload,
                metadata
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s::jsonb
            )
            ON CONFLICT DO NOTHING
            """,
            (
                row["id_gestionnaire"],
                run_id,
                phase,
                classified["query_hash"],
                classified["query_text"],
                rank,
                classified["title"],
                classified["snippet"],
                classified["url"],
                classified["domain"],
                classified["alias_hit_type"],
                classified["alias_hit_value"],
                classified["scope_label"],
                classified["imputabilite"],
                classified["suspicion_level"],
                classified["risk_score"],
                classified["scope_score"],
                classified["freshness_score"],
                classified["source_confidence"],
                classified["used_for_decision"],
                classified["discarded_reason"],
                classified["llm_payload"],
                classified["metadata"],
            ),
        )
        stored.append(classified)
    return stored


def summarize_g0(stored_snippets: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not stored_snippets:
        return {
            "statut": "no_signal_public",
            "suspicion": "aucun",
            "imputabilite": "inconnue",
            "review_required": False,
            "scope_issue": None,
            "decision_detail": "Aucun resultat exploitable sur la requete tension legere.",
        }

    order = {"aucun": 0, "possible": 1, "probable": 2, "certain": 3}
    imput_order = {"inconnue": 0, "gestionnaire_probable": 1, "gestionnaire_certain": 2}

    best = max(stored_snippets, key=lambda item: (order[item["suspicion_level"]], item["risk_score"]))
    best_suspicion = best["suspicion_level"]
    best_imput = max((item["imputabilite"] for item in stored_snippets), key=lambda value: imput_order.get(value, 0))
    used_snippets = [item for item in stored_snippets if item["used_for_decision"]]
    used_count = len(used_snippets)
    has_strong_signal_in_used = any(_snippet_has_strong_signal(item) for item in used_snippets)
    all_used_are_possible = bool(used_snippets) and all(item["suspicion_level"] == "possible" for item in used_snippets)

    if best_suspicion == "aucun":
        return {
            "statut": "signal_public_non_tension",
            "suspicion": "aucun",
            "imputabilite": "inconnue",
            "review_required": False,
            "scope_issue": None,
            "decision_detail": f"Aucun signal faible retenu sur la requete tension legere ({len(stored_snippets)} snippets).",
        }

    if best_suspicion == "possible":
        if all_used_are_possible and not has_strong_signal_in_used:
            return {
                "statut": "signal_public_non_tension",
                "suspicion": "aucun",
                "imputabilite": "inconnue",
                "review_required": False,
                "scope_issue": "preuves_insuffisantes",
                "decision_detail": f"{used_count} snippet(s) possibles sans terme fort: reclasse en non-tension.",
            }
        scope_issue = sanitize_scope_issue(best.get("discarded_reason") or "ancrage_faible")
        return {
            "statut": "signal_ambigu_review",
            "suspicion": "possible",
            "imputabilite": best_imput,
            "review_required": True,
            "scope_issue": scope_issue,
            "decision_detail": f"{used_count} snippet(s) suspects a confirmer; portee encore ambigue.",
        }

    return {
        "statut": "signal_tension_probable",
        "suspicion": best_suspicion,
        "imputabilite": best_imput,
        "review_required": best_suspicion == "probable" and best_imput != "gestionnaire_certain",
        "scope_issue": sanitize_scope_issue(best.get("discarded_reason")) if best_suspicion == "probable" and best_imput != "gestionnaire_certain" else None,
        "decision_detail": f"{used_count} snippet(s) suspects retenus, meilleur niveau={best_suspicion}.",
    }


def update_g0_row(
    cur: psycopg2.extras.RealDictCursor,
    gid: str,
    run_id: str,
    query_count: int,
    snippet_count: int,
    summary: Dict[str, Any],
) -> None:
    cur.execute(
        """
        UPDATE public.finess_gestionnaire
        SET signal_v2_phase = 'G0',
            signal_v2_run_id = %s,
            signal_v2_statut_couverture = %s,
            signal_v2_niveau_suspicion = %s,
            signal_v2_imputabilite = %s,
            signal_v2_review_required = %s,
            signal_v2_scope_issue = %s,
            signal_v2_queries_count = %s,
            signal_v2_snippets_count = %s,
            signal_v2_last_query_at = NOW(),
            signal_v2_decision_detail = %s
        WHERE id_gestionnaire = %s
        """,
        (
            run_id,
            summary["statut"],
            summary["suspicion"],
            summary["imputabilite"],
            summary["review_required"],
            summary["scope_issue"],
            query_count,
            snippet_count,
            summary["decision_detail"],
            gid,
        ),
    )


def load_recent_snippets(
    cur: psycopg2.extras.RealDictCursor,
    gid: str,
    phases: Optional[Sequence[str]] = None,
    limit: int = 8,
) -> List[Dict[str, Any]]:
    params: List[Any] = [gid]
    phase_sql = ""
    if phases:
        phase_sql = " AND phase = ANY(%s)"
        params.append(list(phases))
    params.append(limit)
    cur.execute(
        f"""
        SELECT query_text AS query,
               title,
               snippet,
               url,
               domain,
               source_confidence,
               scope_label,
               imputabilite,
               suspicion_level,
               used_for_decision,
               discarded_reason
        FROM public.finess_signal_v2_snippet
        WHERE id_gestionnaire = %s
          {phase_sql}
        ORDER BY used_for_decision DESC, risk_score DESC NULLS LAST, retrieved_at DESC
        LIMIT %s
        """,
        tuple(params),
    )
    return [dict(row) for row in cur.fetchall()]


def update_g2_row(
    cur: psycopg2.extras.RealDictCursor,
    gid: str,
    run_id: str,
    query_count: int,
    snippet_count: int,
    norm: Dict[str, Any],
    imputabilite: str,
    scope_issue: Optional[str],
    decision_detail: str,
) -> None:
    active_axis = any(
        [
            norm.get("signal_financier"),
            norm.get("signal_rh"),
            norm.get("signal_qualite"),
            norm.get("signal_juridique"),
        ]
    )
    suspicion = "certain" if active_axis and imputabilite == "gestionnaire_certain" and norm.get("confiance") == "haute" else "probable" if active_axis else "aucun"
    statut = "signal_tension_probable" if active_axis else "signal_ambigu_review" if norm.get("review_required") else "signal_public_non_tension"

    cur.execute(
        """
        UPDATE public.finess_gestionnaire
        SET signal_financier = %s,
            signal_financier_detail = %s,
            signal_financier_sources = %s,
            signal_rh = %s,
            signal_rh_detail = %s,
            signal_qualite = %s,
            signal_qualite_detail = %s,
            signal_qualite_sources = %s,
            signal_juridique = %s,
            signal_juridique_detail = %s,
            signal_v2_confiance = %s,
            signal_v2_date = NOW(),
            signal_v2_methode = 'serper_passe_b',
            signal_v2_phase = 'G2',
            signal_v2_run_id = %s,
            signal_v2_statut_couverture = %s,
            signal_v2_niveau_suspicion = %s,
            signal_v2_imputabilite = %s,
            signal_v2_review_required = %s,
            signal_v2_scope_issue = %s,
            signal_v2_queries_count = %s,
            signal_v2_snippets_count = %s,
            signal_v2_last_query_at = NOW(),
            signal_v2_decision_detail = %s
        WHERE id_gestionnaire = %s
        """,
        (
            norm["signal_financier"],
            norm["signal_financier_detail"],
            norm["sources"],
            norm["signal_rh"],
            norm["signal_rh_detail"],
            norm["signal_qualite"],
            norm["signal_qualite_detail"],
            norm["sources"],
            norm["signal_juridique"],
            norm["signal_juridique_detail"],
            norm["confiance"],
            run_id,
            statut,
            suspicion,
            imputabilite,
            bool(norm.get("review_required")),
            scope_issue,
            query_count,
            snippet_count,
            decision_detail,
            gid,
        ),
    )
