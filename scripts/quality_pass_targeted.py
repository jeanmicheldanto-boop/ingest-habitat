"""Passe qualité ciblée — enrichissement ultra-qualitatif des gestionnaires prioritaires.

Cible : gestionnaires >5 établissements dans les secteurs :
  Personnes Âgées, Protection de l'Enfance, Multi-secteurs,
  Handicap Adulte, Handicap Enfant, Hébergement Social

3 phases :
  Phase A : Découverte domaines manquants (Serper + LLM)
  Phase B : Détection agressive de patterns email (3 queries Serper + LLM scoring)
  Phase C : Recherche directeurs qualité / innovation / DSI (>20 étab, hors PA)

Usage :
    # Dry-run complet
    python scripts/quality_pass_targeted.py --dry-run

    # Phase A seule
    python scripts/quality_pass_targeted.py --phase A --dry-run

    # Phase B sur un département test
    python scripts/quality_pass_targeted.py --phase B --dept 47 --dry-run

    # Tout en live
    python scripts/quality_pass_targeted.py --phase A,B,C

Env vars :
    LLM_PROVIDER   (gemini|mistral, default: gemini)
    GEMINI_API_KEY / MISTRAL_API_KEY
    SERPER_API_KEY
    DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

# ── Path setup ───────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from database import DatabaseManager
from fix_data_quality import (
    _clean_secret,
    _domain_matches_org,
    _extract_org_domain,
    _is_person_email,
    _normalise_for_email,
    _reconstituer_email,
    _store_domain,
    discover_domain_for_gestionnaire,
    get_or_search_serper,
    llm_json,
    llm_generate,
    PROMPT_VALIDATE_DOMAIN,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("quality_pass")

# ── Configuration ────────────────────────────────────────────────────────────

# Secteurs cibles pour phases A et B
TARGET_SECTORS = {
    "Personnes Âgées",
    "Protection de l'Enfance",
    "Multi-secteurs",
    "Handicap Adulte",
    "Handicap Enfant",
    "Hébergement Social",
}

# Secteurs cibles pour phase C (hors Personnes Âgées)
TARGET_SECTORS_C = TARGET_SECTORS - {"Personnes Âgées"}

MIN_ETAB_AB = 6       # nb_etablissements > 5
MIN_ETAB_C = 21       # nb_etablissements > 20

# Fonctions recherchées en phase C
QUALITY_FUNCTIONS = [
    "directeur qualité",
    "responsable qualité",
    "directrice qualité",
    "responsable qualité gestion des risques",
]

INNOVATION_FUNCTIONS = [
    "directeur des systèmes d'information",
    "DSI",
    "directeur numérique",
    "directeur innovation",
    "responsable informatique",
    "directeur SI",
    "directrice des systèmes d'information",
    "responsable SI",
]

# Emails génériques à ignorer dans la détection de pattern
GENERIC_EMAIL_PREFIXES = {
    "contact", "info", "accueil", "direction", "admin", "rh",
    "secretariat", "comptabilite", "communication", "siege",
    "recrutement", "formation", "services", "courrier",
    "noreply", "no-reply", "webmaster", "postmaster",
}


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE A : Découverte domaines manquants
# ═══════════════════════════════════════════════════════════════════════════════

def phase_a_discover_domains(
    conn, serper_key: str, llm_key: str, llm_model: str,
    dry_run: bool, dept_filter: str = None,
) -> Dict[str, int]:
    """Découvre les domaines pour les gestionnaires cibles sans domaine."""
    stats = {"total": 0, "found": 0, "not_found": 0, "errors": 0}

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        query = """
            SELECT id_gestionnaire, raison_sociale, sigle, commune,
                   departement_nom, departement_code, site_web, siren
            FROM finess_gestionnaire
            WHERE (domaine_mail IS NULL OR domaine_mail = '')
              AND enrichissement_statut = 'enrichi'
              AND nb_etablissements >= %s
              AND secteur_activite_principal IN %s
        """
        params: list = [MIN_ETAB_AB, tuple(TARGET_SECTORS)]
        if dept_filter:
            query += " AND departement_code = %s"
            params.append(dept_filter)
        query += " ORDER BY nb_etablissements DESC, id_gestionnaire"

        cur.execute(query, params)
        gestionnaires = cur.fetchall()
        stats["total"] = len(gestionnaires)
        log.info("Phase A : %d gestionnaires sans domaine à traiter", stats["total"])

    with conn.cursor() as cur:
        for i, gest in enumerate(gestionnaires):
            gid = gest["id_gestionnaire"]
            try:
                domain = discover_domain_for_gestionnaire(
                    gest, serper_key, llm_key, llm_model, cur, dry_run,
                )
                if domain:
                    stats["found"] += 1
                    log.debug("  [%s] %s → %s", gid, gest["raison_sociale"], domain)
                else:
                    stats["not_found"] += 1
            except Exception as e:
                log.warning("  [%s] Erreur: %s", gid, e)
                stats["errors"] += 1

            if (i + 1) % 25 == 0:
                log.info("  Phase A progression: %d/%d (trouvés=%d)",
                         i + 1, stats["total"], stats["found"])
                if not dry_run:
                    conn.commit()

    if not dry_run:
        conn.commit()
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE B : Détection agressive de patterns email
# ═══════════════════════════════════════════════════════════════════════════════

PROMPT_SCORE_PATTERN = """Tu analyses des adresses email trouvées pour le domaine @{domaine} d'un organisme médico-social.

Organisme : "{raison_sociale}"

Emails trouvés :
{emails_list}

Identifie le modèle de construction des emails PERSONNELS (pas contact@, info@, etc.).
Exemples de modèles : prenom.nom, p.nom, nom.prenom, prenom-nom, prenom_nom

Réponds UNIQUEMENT en JSON :
{{
  "structure": "<prenom.nom | p.nom | nom.prenom | prenom-nom | prenom_nom | inconnu>",
  "confiance": "haute" | "moyenne" | "basse",
  "raison": "<courte justification>"
}}"""


def _extract_emails_from_text(text: str, domaine: str) -> set[str]:
    """Extrait les emails @domaine depuis un texte (snippet, titre, URL)."""
    pattern = r"[a-zA-Z0-9._%+-]+@" + re.escape(domaine)
    return {m.lower() for m in re.findall(pattern, text, re.IGNORECASE)}


def _detect_pattern_from_emails(person_emails: list[str]) -> Tuple[Optional[str], str]:
    """Détecte le pattern email depuis une liste d'emails personnels.
    
    Returns (structure, confiance).
    """
    if not person_emails:
        return None, "basse"

    from collections import Counter
    patterns: list[str] = []
    for email in person_emails:
        local = email.split("@")[0]
        if "." in local:
            parts = local.split(".")
            if len(parts) == 2:
                patterns.append("p.nom" if len(parts[0]) == 1 else "prenom.nom")
            else:
                patterns.append("prenom.nom")
        elif "-" in local:
            patterns.append("prenom-nom")
        elif "_" in local:
            patterns.append("prenom_nom")

    if not patterns:
        return None, "basse"

    most_common = Counter(patterns).most_common(1)[0]
    structure = most_common[0]
    confiance = "haute" if most_common[1] >= 2 else "moyenne"
    return structure, confiance


def phase_b_email_patterns(
    conn, serper_key: str, llm_key: str, llm_model: str,
    dry_run: bool, dept_filter: str = None,
) -> Dict[str, int]:
    """Détection agressive de patterns email avec 3 queries Serper + LLM scoring."""
    stats = {
        "total": 0, "with_pattern": 0, "no_pattern": 0,
        "emails_built": 0, "llm_scored": 0, "errors": 0,
    }

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        query = """
            SELECT id_gestionnaire, domaine_mail, raison_sociale
            FROM finess_gestionnaire
            WHERE domaine_mail IS NOT NULL AND domaine_mail != ''
              AND (structure_mail IS NULL OR structure_mail = '')
              AND nb_etablissements >= %s
              AND secteur_activite_principal IN %s
        """
        params: list = [MIN_ETAB_AB, tuple(TARGET_SECTORS)]
        if dept_filter:
            query += " AND departement_code = %s"
            params.append(dept_filter)
        query += " ORDER BY nb_etablissements DESC, id_gestionnaire"

        cur.execute(query, params)
        gestionnaires = cur.fetchall()
        stats["total"] = len(gestionnaires)
        log.info("Phase B : %d gestionnaires avec domaine, sans pattern", stats["total"])

    with conn.cursor() as cur:
        for i, gest in enumerate(gestionnaires):
            gid = gest["id_gestionnaire"]
            domaine = gest["domaine_mail"]
            rs = gest["raison_sociale"] or ""

            try:
                # ── 3 queries Serper pour trouver des emails ──
                found_emails: set[str] = set()

                queries = [
                    f'"@{domaine}"',
                    f'"@{domaine}" "{rs}"',
                    f'site:{domaine} contact email',
                ]
                for q in queries:
                    results = get_or_search_serper(q, serper_key, cur)
                    for r in results:
                        text = f'{r.get("snippet", "")} {r.get("title", "")} {r.get("link", "")}'
                        found_emails.update(_extract_emails_from_text(text, domaine))

                person_emails = [e for e in found_emails if _is_person_email(e)]
                all_emails = list(found_emails)

                # ── Détection rule-based ──
                structure, confiance = _detect_pattern_from_emails(person_emails)

                # ── LLM scoring systématique si on a des emails ──
                if all_emails and llm_key:
                    # Même avec confiance haute → le LLM peut confirmer/infirmer
                    if confiance != "haute" or len(person_emails) < 3:
                        emails_str = "\n".join(f"  - {e}" for e in all_emails[:15])
                        prompt = PROMPT_SCORE_PATTERN.format(
                            domaine=domaine,
                            raison_sociale=rs,
                            emails_list=emails_str,
                        )
                        result = llm_json(llm_key, llm_model, prompt, max_tokens=300)
                        llm_struct = (result.get("structure") or "").strip().lower()
                        llm_conf = (result.get("confiance") or "").strip().lower()
                        valid_structs = {"prenom.nom", "p.nom", "nom.prenom", "prenom-nom", "prenom_nom"}

                        if llm_struct in valid_structs and llm_conf in ("haute", "moyenne"):
                            # LLM override si rule-based était basse, ou si confiance LLM > rule
                            if confiance == "basse" or (confiance == "moyenne" and llm_conf == "haute"):
                                structure = llm_struct
                                confiance = llm_conf
                            elif not structure:
                                structure = llm_struct
                                confiance = llm_conf

                        stats["llm_scored"] += 1

                # ── Stocker le résultat ──
                if structure and confiance != "basse":
                    stats["with_pattern"] += 1
                    if not dry_run:
                        cur.execute(
                            "UPDATE finess_gestionnaire SET structure_mail = %s WHERE id_gestionnaire = %s",
                            (structure, gid),
                        )

                    # Reconstruire les emails des dirigeants
                    cur.execute("""
                        SELECT id, prenom, nom FROM finess_dirigeant
                        WHERE id_gestionnaire = %s
                          AND email_reconstitue IS NULL
                          AND prenom IS NOT NULL AND LOWER(TRIM(prenom)) NOT IN ('', 'null')
                          AND nom IS NOT NULL AND LOWER(TRIM(nom)) NOT IN ('', 'null')
                          AND confiance IN ('haute', 'moyenne')
                    """, (gid,))
                    dirigeants = cur.fetchall()

                    for d in dirigeants:
                        email = _reconstituer_email(d[1], d[2], structure, domaine)
                        if email and not dry_run:
                            cur.execute(
                                "UPDATE finess_dirigeant SET email_reconstitue = %s, "
                                "email_organisation = %s WHERE id = %s",
                                (email, f"@{domaine}", d[0]),
                            )
                            stats["emails_built"] += 1
                        elif email:
                            stats["emails_built"] += 1
                else:
                    stats["no_pattern"] += 1

            except Exception as e:
                log.warning("  [%s] Erreur phase B: %s", gid, e)
                stats["errors"] += 1

            if (i + 1) % 25 == 0:
                log.info("  Phase B progression: %d/%d (pattern=%d, emails=%d)",
                         i + 1, stats["total"], stats["with_pattern"], stats["emails_built"])
                if not dry_run:
                    conn.commit()

    if not dry_run:
        conn.commit()
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE C : Recherche directeurs qualité / innovation / DSI
# ═══════════════════════════════════════════════════════════════════════════════

PROMPT_EXTRACT_DIRECTOR = """Tu analyses des résultats de recherche Google pour trouver un {role_type} d'un organisme médico-social.

Organisme : "{raison_sociale}"
Commune : {commune}

Résultats de recherche :
{snippets}

Extrais les personnes qui occupent un poste de {role_type} dans cet organisme.
Ne confonds pas avec des postes dans d'autres organisations.
Réponds UNIQUEMENT en JSON :
{{
  "personnes": [
    {{
      "prenom": "...",
      "nom": "...",
      "fonction": "...",
      "email": null ou "email trouvé",
      "confiance": "haute" | "moyenne" | "basse",
      "source": "snippet / LinkedIn / annuaire"
    }}
  ]
}}
Si aucune personne trouvée, retourne {{"personnes": []}}"""


def phase_c_search_directors(
    conn, serper_key: str, llm_key: str, llm_model: str,
    dry_run: bool, dept_filter: str = None,
) -> Dict[str, int]:
    """Recherche directeurs qualité / innovation / DSI pour grosses structures."""
    stats = {
        "total": 0, "quality_found": 0, "innovation_found": 0,
        "total_directors": 0, "errors": 0,
    }

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        query = """
            SELECT id_gestionnaire, raison_sociale, sigle, commune,
                   departement_nom, departement_code, domaine_mail,
                   nb_etablissements
            FROM finess_gestionnaire
            WHERE nb_etablissements >= %s
              AND secteur_activite_principal IN %s
              AND enrichissement_statut = 'enrichi'
        """
        params: list = [MIN_ETAB_C, tuple(TARGET_SECTORS_C)]
        if dept_filter:
            query += " AND departement_code = %s"
            params.append(dept_filter)
        query += " ORDER BY nb_etablissements DESC, id_gestionnaire"

        cur.execute(query, params)
        gestionnaires = cur.fetchall()
        stats["total"] = len(gestionnaires)
        log.info("Phase C : %d gestionnaires >20 étab (hors PA) à chercher", stats["total"])

    with conn.cursor() as cur:
        for i, gest in enumerate(gestionnaires):
            gid = gest["id_gestionnaire"]
            rs = gest["raison_sociale"] or ""
            commune = gest["commune"] or ""
            domaine = gest["domaine_mail"] or ""
            structure_mail = None

            # Récupérer le structure_mail si existant
            if domaine:
                cur.execute(
                    "SELECT structure_mail FROM finess_gestionnaire WHERE id_gestionnaire = %s",
                    (gid,),
                )
                row = cur.fetchone()
                structure_mail = row[0] if row else None

            try:
                # ── Query 1 : Directeur qualité ──
                q_quality = f'"directeur qualité" OR "responsable qualité" "{rs}"'
                results_q = get_or_search_serper(q_quality, serper_key, cur)

                # ── Query 2 : DSI / Innovation ──
                q_innov = f'"DSI" OR "directeur numérique" OR "directeur innovation" OR "directeur SI" "{rs}"'
                results_i = get_or_search_serper(q_innov, serper_key, cur)

                # ── Extraire via LLM ──
                for role_type, results, stat_key in [
                    ("directeur qualité / responsable qualité", results_q, "quality_found"),
                    ("DSI / directeur innovation / directeur numérique", results_i, "innovation_found"),
                ]:
                    if not results or not llm_key:
                        continue

                    snippets_text = "\n".join(
                        f"- {r.get('title', '')} — {r.get('snippet', '')}"
                        for r in results[:8]
                    )
                    prompt = PROMPT_EXTRACT_DIRECTOR.format(
                        role_type=role_type,
                        raison_sociale=rs,
                        commune=commune,
                        snippets=snippets_text[:2000],
                    )
                    result = llm_json(llm_key, llm_model, prompt, max_tokens=500)
                    personnes = result.get("personnes") or []

                    for p in personnes:
                        prenom = (p.get("prenom") or "").strip()
                        nom = (p.get("nom") or "").strip()
                        fonction = (p.get("fonction") or "").strip()
                        email_found = (p.get("email") or "").strip() or None
                        conf = (p.get("confiance") or "moyenne").strip().lower()

                        if not prenom or not nom or not fonction:
                            continue
                        if conf == "basse":
                            continue

                        # Vérifier que ce dirigeant n'existe pas déjà
                        cur.execute("""
                            SELECT id FROM finess_dirigeant
                            WHERE id_gestionnaire = %s
                              AND LOWER(TRIM(nom)) = LOWER(TRIM(%s))
                              AND LOWER(TRIM(prenom)) = LOWER(TRIM(%s))
                        """, (gid, nom, prenom))
                        if cur.fetchone():
                            continue

                        # Reconstruire email si domaine + pattern connus
                        email_recon = None
                        if not email_found and domaine and structure_mail:
                            email_recon = _reconstituer_email(prenom, nom, structure_mail, domaine)

                        if not dry_run:
                            cur.execute("""
                                INSERT INTO finess_dirigeant
                                    (id_gestionnaire, prenom, nom, fonction_normalisee,
                                     fonction_brute, email_reconstitue, email_organisation,
                                     source_type, confiance)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                gid, prenom, nom, fonction,
                                fonction, email_found or email_recon,
                                f"@{domaine}" if domaine else None,
                                "serper_quality_pass", conf,
                            ))

                        stats[stat_key] += 1
                        stats["total_directors"] += 1
                        log.debug("  [%s] +%s: %s %s (%s)",
                                  gid, fonction, prenom, nom, conf)

            except Exception as e:
                log.warning("  [%s] Erreur phase C: %s", gid, e)
                stats["errors"] += 1

            if (i + 1) % 10 == 0:
                log.info("  Phase C progression: %d/%d (qualité=%d, innov=%d)",
                         i + 1, stats["total"], stats["quality_found"], stats["innovation_found"])
                if not dry_run:
                    conn.commit()

    if not dry_run:
        conn.commit()
    return stats


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Passe qualité ciblée FINESS")
    p.add_argument("--phase", default="A,B,C",
                   help="Phase(s) à exécuter : A, B, C ou combinaison (défaut: A,B,C)")
    p.add_argument("--dry-run", action="store_true",
                   help="Ne pas écrire en base, afficher les stats seulement")
    p.add_argument("--dept", default=None,
                   help="Code département unique (ex: 47)")
    p.add_argument("--depts", default=None,
                   help="Liste de départements comma-séparée (ex: 01,02,2A,9B)")
    return p.parse_args()


def _send_completion_email(dept_list: List[str], results: Dict[str, Any], elapsed: float, dry_run: bool) -> None:
    """Envoie un email de fin de traitement via ElasticMail."""
    api_key = os.getenv("ELASTICMAIL_API_KEY", "").strip()
    recipient = os.getenv("NOTIFICATION_EMAIL", "").strip()
    sender = os.getenv("SENDER_EMAIL", "noreply@bmse.fr").strip()
    if not api_key or not recipient:
        log.info("Email non configuré (ELASTICMAIL_API_KEY ou NOTIFICATION_EMAIL manquant)")
        return

    pa = results.get("phase_a", {})
    pb = results.get("phase_b", {})
    pc = results.get("phase_c", {})
    mode = "DRY-RUN" if dry_run else "LIVE"
    dept_label = ",".join(dept_list[:5]) + (f"...+{len(dept_list)-5}" if len(dept_list) > 5 else "")

    subject = f"✅ Quality Pass {mode} terminé — {dept_label} ({len(dept_list)} depts)"
    body = f"""Passe qualité ciblée FINESS — {mode}

Départements traités : {', '.join(dept_list)}
Durée totale : {elapsed:.1f} min

Phase A (domaines) : {pa.get('found', 0)}/{pa.get('total', 0)} trouvés
Phase B (patterns email) : {pb.get('with_pattern', 0)}/{pb.get('total', 0)} patterns, {pb.get('emails_built', 0)} emails reconstruits
Phase C (directeurs) : {pc.get('total_directors', 0)} dirigeants ajoutés ({pc.get('quality_found', 0)} qualité, {pc.get('innovation_found', 0)} DSI/innovation)

Logs Cloud Run : https://console.cloud.google.com/run/jobs?project=gen-lang-client-0230548399
"""
    try:
        import requests as _req
        resp = _req.post(
            "https://api.elasticemail.com/v2/email/send",
            data={"apikey": api_key, "from": sender, "to": recipient,
                  "subject": subject, "bodyText": body},
            timeout=15,
        )
        if resp.status_code == 200:
            log.info("Email de notification envoyé à %s", recipient)
        else:
            log.warning("Échec email (%s): %s", resp.status_code, resp.text[:200])
    except Exception as e:
        log.warning("Erreur envoi email: %s", e)


def _merge_stats(a: Dict, b: Dict) -> Dict:
    """Additionne les compteurs de deux dicts stats."""
    merged = dict(a)
    for k, v in b.items():
        if isinstance(v, int):
            merged[k] = merged.get(k, 0) + v
    return merged


def main():
    import fix_data_quality as fdq
    import time as _time
    args = parse_args()

    # Les env vars PHASE_FILTER et DEPTS_FILTER ont priorité sur les args CLI
    # (utilisés pour les jobs Cloud Run via --update-env-vars)
    phase_str = os.getenv("PHASE_FILTER") or args.phase
    phases = {x.strip().upper() for x in phase_str.split(",")}
    mode = "DRY-RUN" if args.dry_run else "LIVE"

    # Construire la liste des départements à traiter
    depts_env = os.getenv("DEPTS_FILTER", "").strip()
    if depts_env:
        dept_list = [d.strip() for d in depts_env.split(",") if d.strip()]
    elif args.depts:
        dept_list = [d.strip() for d in args.depts.split(",") if d.strip()]
    elif args.dept:
        dept_list = [args.dept.strip()]
    else:
        dept_list = [None]  # None = tous

    # Config LLM/Serper
    fdq._llm_provider = os.getenv("LLM_PROVIDER", "gemini").lower()
    serper_key = _clean_secret(os.getenv("SERPER_API_KEY", ""))
    if fdq._llm_provider == "gemini":
        llm_key = _clean_secret(os.getenv("GEMINI_API_KEY", ""))
        llm_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
    else:
        llm_key = _clean_secret(os.getenv("MISTRAL_API_KEY", ""))
        llm_model = os.getenv("MISTRAL_MODEL", "mistral-small-latest")

    dept_label = ",".join(d for d in dept_list if d) or "ALL"
    log.info("=" * 60)
    log.info("PASSE QUALITÉ CIBLÉE — phases=%s mode=%s depts=%s",
             phases, mode, dept_label)
    log.info("  LLM=%s serper=%s", fdq._llm_provider, "OK" if serper_key else "MANQUANT")
    log.info("  Cible: >%d étab (A,B), >%d étab (C)", MIN_ETAB_AB - 1, MIN_ETAB_C - 1)
    log.info("=" * 60)

    totals: Dict[str, Any] = {}
    t0 = _time.time()

    db = DatabaseManager()
    with db.get_connection() as conn:
        for dept_filter in dept_list:
            if dept_filter:
                log.info("── Département %s ──", dept_filter)

            if "A" in phases:
                r = phase_a_discover_domains(conn, serper_key, llm_key, llm_model, args.dry_run, dept_filter)
                totals["phase_a"] = _merge_stats(totals.get("phase_a", {}), r)

            if "B" in phases:
                r = phase_b_email_patterns(conn, serper_key, llm_key, llm_model, args.dry_run, dept_filter)
                totals["phase_b"] = _merge_stats(totals.get("phase_b", {}), r)

            if "C" in phases:
                r = phase_c_search_directors(conn, serper_key, llm_key, llm_model, args.dry_run, dept_filter)
                totals["phase_c"] = _merge_stats(totals.get("phase_c", {}), r)

    elapsed = (_time.time() - t0) / 60
    log.info("=" * 60)
    log.info("RÉSUMÉ COMPLET (%d depts, %.1f min):", len([d for d in dept_list if d] or ["all"]), elapsed)
    log.info(json.dumps(totals, indent=2, ensure_ascii=False))
    log.info("=" * 60)

    # Email de notification
    effective_depts = [d for d in dept_list if d] or ["ALL"]
    _send_completion_email(effective_depts, totals, elapsed, args.dry_run)


if __name__ == "__main__":
    main()
