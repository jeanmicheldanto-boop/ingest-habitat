#!/usr/bin/env python3
"""
pipeline_quality.py — Post-run quality toolkit

Modes disponibles (cumulables) :
  --check-names          Flagge / supprime les noms fantômes et les initiales seules
  --audit-postes         Détecte les doublons de poste par entité (CSV)
  --validate-dirs        Double-validation des directeurs autonomie/enfance via organigramme + Mistral
  --check-territorial    Détecte les directeurs « territoriaux » hors scope ESSMS (CSV + flag invalide)
  --apply-replacements   Applique les remplacements de noms issus de --validate-dirs (action=REMPLACE)
  --reclassify-niveaux      Reclasse les contacts mal nivellés via LLM (niveau + scope ESSMS)
  --requalify-tarification  Re-vérifie via LLM tous les contacts avec 'tarif' dans le poste
  --find-missing-contacts   Recherche Serper+LLM les contacts manquants (requiert --niveau)
  --enrich-linkedin         Re-lance la recherche LinkedIn pour les contacts sans profil
  --all                     Active les modes 1-7 (hors find-missing-contacts)

Options communes :
  --dry-run          Affiche les actions sans modifier la base
  --out-dir PATH     Dossier de sortie pour les rapports (défaut : outputs/quality)
  --limit N          Limite le nombre de contacts traités par --enrich-linkedin
"""
import argparse
import csv
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

# ── Chemins ─────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[2]   # ingest-habitat/
SRC  = Path(__file__).resolve().parent       # prospection-financeurs/src/
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SRC.parent))          # prospection-financeurs/

from src.supabase_db import ProspectionDB
from src.linkedin_finder import LinkedInFinder
from src.serper_client import SerperClient
from src.mistral_client import MistralClient
from src.normalizer import remove_accents

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_DIR = ROOT / "prospection-financeurs" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "quality.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Constantes ───────────────────────────────────────────────────────────────
# Noms qui ne désignent pas une vraie personne
_GHOST_PATTERNS = [
    "non communiqué",
    "non trouvé",
    "n/a",
    "vacance",
    "à définir",
    "en cours",
    "poste vacant",
    "à recruter",
    "inconnu",
    "inconnue",
]


def _is_ghost(nom_complet: str) -> bool:
    """Retourne True si nom_complet correspond à un nom fantôme."""
    nc = (nom_complet or "").strip().lower()
    return any(nc == p or nc.startswith(p) for p in _GHOST_PATTERNS)


def _is_initial_only(prenom: str, nom: str) -> bool:
    """Retourne True si le prénom ou le nom est une initiale isolée (1-2 lettres)."""
    p = (prenom or "").strip()
    n = (nom or "").strip()
    # Prénom = 1 lettre ou minuscule unique
    if len(p) <= 1:
        return True
    # Nom = 1 lettre
    if len(n) == 1:
        return True
    # Nom = 2 lettres majuscules (ex : "DU", "LE")
    if len(n) == 2 and n.isupper():
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# MODE 1 — CHECK NAMES
# ─────────────────────────────────────────────────────────────────────────────

def run_check_names(db: ProspectionDB, dry_run: bool) -> dict[str, int]:
    """
    Parcourt tous les contacts et :
      - Supprime les noms fantômes (aucune personne réelle)
      - Marque 'invalide' les initiales isolées
    Retourne un résumé {deleted, flagged}.
    """
    logger.info("=== MODE : check-names ===")
    stats = {"deleted": 0, "flagged": 0, "inspected": 0}

    with db._conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.nom_complet, c.prenom, c.nom, c.confiance_nom,
                   e.nom AS entite, e.type_entite, e.code
            FROM prospection_contacts c
            JOIN prospection_entites e ON e.id = c.entite_id
            ORDER BY e.nom, c.nom_complet
        """)
        rows = cur.fetchall()

    logger.info("  %d contacts à inspecter", len(rows))

    to_delete: list[tuple[int, str, str]] = []  # (id, nom_complet, entite)
    to_flag:   list[tuple[int, str, str]] = []

    for row in rows:
        cid, nom_complet, prenom, nom, confiance, entite, _, _ = row
        stats["inspected"] += 1

        if _is_ghost(nom_complet):
            to_delete.append((cid, nom_complet, entite))
        elif _is_initial_only(prenom or "", nom or ""):
            if confiance != "invalide":
                to_flag.append((cid, nom_complet, entite))

    # Affichage
    if to_delete:
        logger.info("  Noms fantômes à SUPPRIMER (%d) :", len(to_delete))
        for cid, nc, ent in to_delete:
            logger.info("    [%s] id=%d  '%s'", ent, cid, nc)
    else:
        logger.info("  Aucun nom fantôme trouvé.")

    if to_flag:
        logger.info("  Initiales isolées à FLAGUER 'invalide' (%d) :", len(to_flag))
        for cid, nc, ent in to_flag:
            logger.info("    [%s] id=%d  '%s'", ent, cid, nc)
    else:
        logger.info("  Aucune initiale isolée trouvée.")

    if dry_run:
        logger.info("  [dry-run] Aucune modification effectuée.")
        stats["deleted"] = len(to_delete)
        stats["flagged"] = len(to_flag)
        return stats

    with db._conn.cursor() as cur:
        for cid, nc, ent in to_delete:
            cur.execute("DELETE FROM prospection_contacts WHERE id = %s", (cid,))
            logger.info("  SUPPRIMÉ id=%d '%s' [%s]", cid, nc, ent)
            stats["deleted"] += 1

        for cid, nc, ent in to_flag:
            cur.execute(
                "UPDATE prospection_contacts SET confiance_nom = 'invalide' WHERE id = %s",
                (cid,),
            )
            logger.info("  FLAGUÉ 'invalide' id=%d '%s' [%s]", cid, nc, ent)
            stats["flagged"] += 1

    db._conn.commit()
    logger.info("  Résultat : %d supprimés, %d flagués.", stats["deleted"], stats["flagged"])
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MODE 2 — AUDIT POSTES
# ─────────────────────────────────────────────────────────────────────────────

def _keyword_overlap(a: str, b: str) -> float:
    """Ratio de mots en commun entre deux chaînes (normalisées)."""
    if not a or not b:
        return 0.0
    stopwords = {"de", "du", "la", "le", "les", "des", "et", "en", "au", "aux", "l", "d"}
    wa = {w.lower() for w in a.split() if len(w) > 2 and w.lower() not in stopwords}
    wb = {w.lower() for w in b.split() if len(w) > 2 and w.lower() not in stopwords}
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def run_audit_postes(db: ProspectionDB, out_dir: Path) -> dict[str, int]:
    """
    Détecte les doublons de poste (même entité + même niveau + postes similaires).
    Écrit un rapport CSV dans out_dir/doublons_postes.csv.
    Ne modifie jamais la base.
    """
    logger.info("=== MODE : audit-postes ===")
    stats = {"entites_avec_doublons": 0, "paires": 0}

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "doublons_postes.csv"

    with db._conn.cursor() as cur:
        # Récupère tous les contacts avec leur entité
        cur.execute("""
            SELECT c.id, c.nom_complet, c.niveau, c.poste_exact,
                   e.id AS entite_id, e.nom AS entite, e.type_entite, e.code
            FROM prospection_contacts c
            JOIN prospection_entites e ON e.id = c.entite_id
            ORDER BY e.nom, c.niveau, c.nom_complet
        """)
        rows = cur.fetchall()

    # Regrouper par entite_id + niveau
    from collections import defaultdict
    groups: dict[tuple, list] = defaultdict(list)
    for row in rows:
        cid, nom_complet, niveau, poste_exact, entite_id, entite, etype, ecode = row
        if not niveau:
            continue
        groups[(entite_id, entite, niveau)].append({
            "id": cid,
            "nom_complet": nom_complet,
            "poste_exact": poste_exact or "",
        })

    paires = []
    entites_vues = set()

    for (entite_id, entite, niveau), contacts in groups.items():
        if len(contacts) < 2:
            continue
        # Vérifier les paires pour similarité de poste
        for i in range(len(contacts)):
            for j in range(i + 1, len(contacts)):
                c1, c2 = contacts[i], contacts[j]
                overlap = _keyword_overlap(c1["poste_exact"], c2["poste_exact"])
                if overlap >= 0.5 or not c1["poste_exact"] or not c2["poste_exact"]:
                    paires.append({
                        "entite": entite,
                        "type": "",
                        "code": "",
                        "niveau": niveau,
                        "contact_1_id": c1["id"],
                        "contact_1_nom": c1["nom_complet"],
                        "contact_1_poste": c1["poste_exact"],
                        "contact_2_id": c2["id"],
                        "contact_2_nom": c2["nom_complet"],
                        "contact_2_poste": c2["poste_exact"],
                        "overlap_score": round(overlap, 2),
                    })
                    entites_vues.add(entite_id)

    stats["paires"] = len(paires)
    stats["entites_avec_doublons"] = len(entites_vues)

    # Tri par entité puis overlap décroissant
    paires.sort(key=lambda x: (x["entite"], -x["overlap_score"]))

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        if not paires:
            f.write("Aucun doublon détecté.\n")
        else:
            fieldnames = [
                "entite", "niveau",
                "contact_1_id", "contact_1_nom", "contact_1_poste",
                "contact_2_id", "contact_2_nom", "contact_2_poste",
                "overlap_score",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(paires)

    logger.info("  %d paires suspectes dans %d entités → %s",
                stats["paires"], stats["entites_avec_doublons"], csv_path)
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MODE 2b — CHECK TERRITORIAL
# ─────────────────────────────────────────────────────────────────────────────

# Mots-clés qui indiquent une compétence ESSMS réelle malgré le mot « territorial »
_ESSMS_KEYWORDS = [
    "autonomie",
    "handicap",
    "personnes âgées",
    "personnes agées",
    "personnes agees",
    "pa/ph", "pa-ph",
    "dépendance",
    "dependance",
    "mdph",
    "essms",
    "établissements sociaux",
    "etablissements sociaux",
    "médico-social",
    "medico-social",
    "insertion",
    "enfance",   # directeur territorial ASE peut gérer des ESSMS enfance
    "protection de l'enfance",
]


def _has_essms_scope(poste: str) -> bool:
    """Retourne True si le poste contient un mot-clé de compétence ESSMS."""
    p = (poste or "").lower()
    return any(kw in p for kw in _ESSMS_KEYWORDS)


def run_check_territorial(
    db: ProspectionDB,
    out_dir: Path,
    dry_run: bool,
) -> dict[str, int]:
    """
    Détecte les contacts « directeurs territoriaux » qui ne gèrent probablement
    pas le financement des ESSMS :
      - Poste contenant « territorial / territoriale / territoriaux »
      - Sans mot-clé ESSMS (autonomie, handicap, insertion, enfance…)

    Actions :
      - Écrit un CSV de rapport dans out_dir/territorial_suspects.csv
      - Hors --dry-run : marque confiance_nom = 'invalide' pour les hors-scope

    Retourne {"total": N, "hors_scope": N, "scope_ok": N, "flagged": N}.
    """
    logger.info("=== check-territorial ===")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "territorial_suspects.csv"

    with db._conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.nom_complet, c.poste_exact, c.niveau,
                   c.confiance_nom, e.nom AS entite, e.code AS code_dept
            FROM prospection_contacts c
            JOIN prospection_entites e ON e.id = c.entite_id
            WHERE LOWER(c.poste_exact) LIKE '%territorial%'
              AND c.confiance_nom != 'invalide'
            ORDER BY e.code, c.nom_complet
        """)
        rows = cur.fetchall()

    stats = {"total": len(rows), "hors_scope": 0, "scope_ok": 0, "flagged": 0}
    rapport: list[dict] = []

    hors_scope_ids: list[int] = []

    for cid, nom, poste, niveau, confiance, entite, dept in rows:
        # Tout poste contenant "territorial" est hors scope ESSMS
        stats["hors_scope"] += 1
        rapport.append({
            "dept":       dept,
            "entite":     entite,
            "id":         cid,
            "nom":        nom,
            "poste":      poste,
            "niveau":     niveau,
            "confiance":  confiance,
            "categorie":  "hors_scope",
            "raison":     "poste territorial — ne gère pas le financement des ESSMS",
        })
        hors_scope_ids.append(cid)
        logger.info("  [hors_scope] id=%-4d | %-30s | %s (%s)", cid, nom, poste, entite)

    # ── CSV rapport ──────────────────────────────────────────────────────────
    rapport.sort(key=lambda r: (r["categorie"] != "hors_scope", r["dept"] or ""))
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["dept", "entite", "id", "nom", "poste", "niveau",
                      "confiance", "categorie", "raison"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rapport)
    logger.info("  Rapport → %s", csv_path)

    # ── Flag hors_scope → invalide ───────────────────────────────────────────
    if hors_scope_ids and not dry_run:
        with db._conn.cursor() as cur:
            cur.execute(
                "UPDATE prospection_contacts SET confiance_nom = 'invalide' "
                "WHERE id = ANY(%s)",
                (hors_scope_ids,)
            )
        db._conn.commit()
        stats["flagged"] = len(hors_scope_ids)
        logger.info("  %d contacts hors-scope flagués 'invalide'.", stats["flagged"])
    elif dry_run:
        logger.info("  [dry-run] %d auraient été flagués 'invalide'.", len(hors_scope_ids))

    logger.info(
        "  Résultat : %d total, %d hors_scope, %d scope_ok.",
        stats["total"], stats["hors_scope"], stats["scope_ok"]
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MODE 3 — VALIDATE DIRECTORS
# ─────────────────────────────────────────────────────────────────────────────

def _domaine_from_poste(poste: str) -> str:
    """Dérive le mot-clé thématique principal depuis poste_exact pour la requête Serper."""
    p = remove_accents((poste or "").lower())
    if "autonomie" in p:
        return "autonomie"
    if "enfance" in p or "famille" in p:
        return "enfance famille"
    if "personnes agees" in p or "personnes âgées" in p or "pa/" in p or "pa-ph" in p:
        return "personnes agees"
    if "insertion" in p:
        return "insertion"
    if "handicap" in p or "mdph" in p:
        return "handicap"
    return "solidarites"


def _llm_triage_extract(
    mistral: MistralClient,
    nom_db: str,
    poste_db: str,
    entity_name: str,
    snippets: list[str],
) -> dict:
    """
    Appel Mistral unique qui fait extraction ET triage automatique en un seul prompt.

    Retourne un dict :
      {
        "contacts":  [...],          # liste extraite (même format qu'extract_contacts)
        "triage": {
          "action":             "CONFIRME" | "REMPLACE" | "PARTIEL" | "VERIFIER" | "INCONNU",
          "raison":             "<explication courte>",
          "confiance_decision": "haute" | "moyenne" | "basse",
          "nom_retenu":         "<nom de la personne qui semble en poste>",
          "source_retenue":     "<URL source principale>",
          "annee_source":       "<année détectée ou vide>",
          "type_source":        "officielle" | "linkedin" | "annuaire" | "presse" | "autre"
        }
      }

    Actions :
      CONFIRME  → même personne qu'en base, toujours en poste
      REMPLACE  → une autre personne occupe clairement le poste (source officielle récente)
      PARTIEL   → même personne mais intitulé / périmètre de poste a évolué
      VERIFIER  → données insuffisantes, contradictoires, source trop ancienne ou tierce
      INCONNU   → aucune personne identifiée dans les snippets
    """
    import json as _json

    snippets_str = "\n\n---\n".join(snippets) if snippets else "(aucun résultat)"

    prompt = f"""Tu es un assistant expert en organigrammes administratifs français.

## Contact actuellement en base de données
- Nom complet : {nom_db}
- Poste : {poste_db}
- Entité : {entity_name}

## Extraits de résultats de recherche (organigramme)
{snippets_str}

## Tâche 1 — Extraction
Identifie toutes les personnes occupant un poste de direction {poste_db.split()[0].lower() if poste_db else ''} au sein de {entity_name}.
Pour chaque personne extraite, fournis : nom_complet, poste_exact, source (URL), confiance (haute/moyenne/basse).
- haute : organigramme officiel du site institutionnel du département, daté de moins de 18 mois
- moyenne : LinkedIn, nomination récente, annuaire tiers fiable
- basse : source ancienne (>2 ans), blog, forum, source indirecte ou non datée

## Tâche 2 — Triage automatique
Compare la personne extraite avec le contact en base ({nom_db} / {poste_db}).
Choisis UNE action parmi :
- CONFIRME  : c'est la même personne, toujours en poste (même si l'intitulé varie légèrement)
- REMPLACE  : une autre personne occupe clairement ce poste selon une source officielle récente (haute confiance)
- PARTIEL   : semble être la même personne mais le poste ou périmètre a changé
- VERIFIER  : données insuffisantes, source trop ancienne, résultats contradictoires ou ambigus
- INCONNU   : aucune personne clairement identifiable dans les snippets

Pour le champ type_source, utilise : officielle / linkedin / annuaire / presse / autre
Pour annee_source, indique l'année si tu la détectes dans les snippets (ex: "2024"), sinon "".

## Format de réponse
Réponds UNIQUEMENT en JSON valide, sans commentaire ni markdown.
{{
  "contacts": [
    {{"nom_complet": "...", "poste_exact": "...", "source": "...", "confiance": "..."}}
  ],
  "triage": {{
    "action": "...",
    "raison": "...",
    "confiance_decision": "...",
    "nom_retenu": "...",
    "source_retenue": "...",
    "annee_source": "...",
    "type_source": "..."
  }}
}}"""

    try:
        raw = mistral._raw_complete([{"role": "user", "content": prompt}])
        data = _json.loads(raw)
        return data
    except Exception as exc:
        logger.error("    LLM triage erreur pour %s : %s", nom_db, exc)
        return {
            "contacts": [],
            "triage": {
                "action": "VERIFIER",
                "raison": f"Erreur LLM : {exc}",
                "confiance_decision": "basse",
                "nom_retenu": "",
                "source_retenue": "",
                "annee_source": "",
                "type_source": "autre",
            },
        }


# Mapping action LLM → tag confiance_nom en base
_ACTION_TO_CONFIANCE = {
    "CONFIRME": "haute",
    "REMPLACE": "a_remplacer",
    "PARTIEL":  "a_verifier",
    "VERIFIER": "a_verifier",
    "INCONNU":  None,  # pas de mise à jour
}

# Icône pour les logs
_ACTION_ICON = {
    "CONFIRME": "✓",
    "REMPLACE": "↻",
    "PARTIEL":  "~",
    "VERIFIER": "?",
    "INCONNU":  "-",
}


def run_validate_dirs(
    db: ProspectionDB,
    out_dir: Path,
    dry_run: bool,
) -> dict[str, int]:
    """
    Pour chaque directeur autonomie/enfance/social d'un département :
      1. Requête Serper : organigramme "[entité]" directeur {domaine}
      2. Mistral (appel unique) : extraction + triage LLM automatisé
      3. Mise à jour confiance_nom en base + rapport CSV détaillé

    Actions LLM → confiance_nom :
      CONFIRME  → 'haute'
      REMPLACE  → 'a_remplacer'   (nouvelle personne selon source officielle)
      PARTIEL   → 'a_verifier'    (même personne, poste évolué)
      VERIFIER  → 'a_verifier'    (source insuffisante)
      INCONNU   → pas de mise à jour
    """
    logger.info("=== MODE : validate-dirs (triage LLM) ===")
    stats = {
        "processed":  0,
        "confirmed":  0,
        "replaced":   0,
        "partial":    0,
        "to_verify":  0,
        "not_found":  0,
        "errors":     0,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "validation_dirs.csv"

    # Contacts éligibles : directeurs de département avec poste à enjeu
    with db._conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.nom_complet, c.prenom, c.nom, c.poste_exact, c.confiance_nom,
                   e.nom AS entite_nom, e.code
            FROM prospection_contacts c
            JOIN prospection_entites e ON e.id = c.entite_id
            WHERE e.type_entite = 'departement'
              AND c.niveau = 'direction'
              AND c.confiance_nom != 'invalide'
              AND (
                LOWER(c.poste_exact) LIKE '%autonomie%'
                OR LOWER(c.poste_exact) LIKE '%enfance%'
                OR LOWER(c.poste_exact) LIKE '%famille%'
                OR LOWER(c.poste_exact) LIKE '%insertion%'
                OR LOWER(c.poste_exact) LIKE '%solidarité%'
                OR LOWER(c.poste_exact) LIKE '%solidarites%'
                OR LOWER(c.poste_exact) LIKE '%handicap%'
                OR LOWER(c.poste_exact) LIKE '%mdph%'
                OR LOWER(c.poste_exact) LIKE '%personnes%'
              )
            ORDER BY e.nom, c.nom_complet
        """)
        contacts = cur.fetchall()

    logger.info("  %d directeurs à valider", len(contacts))

    serper  = SerperClient()
    mistral = MistralClient()

    report_rows: list[dict] = []
    updates: list[tuple[str, int]] = []  # (nouvelle confiance, contact_id)

    for i, row in enumerate(contacts, 1):
        cid, nom_complet, prenom, nom, poste_exact, confiance, entite_nom, code = row
        stats["processed"] += 1
        domaine = _domaine_from_poste(poste_exact)

        logger.info("  [%d/%d] %s | %s | %s",
                    i, len(contacts), entite_nom, nom_complet, (poste_exact or "")[:55])

        # ── 1. Serper organigramme ────────────────────────────────────────────
        query = f'organigramme "{entite_nom}" directeur {domaine}'
        try:
            _, snippets = serper.search_and_extract(query, num=7)
        except Exception as exc:
            logger.error("    Serper erreur : %s", exc)
            stats["errors"] += 1
            time.sleep(1)
            continue

        time.sleep(1)  # throttle Serper

        if not snippets:
            stats["not_found"] += 1
            continue

        # ── 2. Extraction + triage LLM (appel unique) ────────────────────────
        result = _llm_triage_extract(mistral, nom_complet, poste_exact, entite_nom, snippets)
        time.sleep(1)  # throttle Mistral

        triage = result.get("triage", {})
        action           = triage.get("action", "VERIFIER").upper()
        raison           = triage.get("raison", "")
        confiance_dec    = triage.get("confiance_decision", "basse")
        nom_retenu       = triage.get("nom_retenu", "")
        source_retenue   = triage.get("source_retenue", "")
        annee_source     = triage.get("annee_source", "")
        type_source      = triage.get("type_source", "")

        if action not in _ACTION_TO_CONFIANCE:
            action = "VERIFIER"

        icon = _ACTION_ICON.get(action, "?")
        logger.info("    %s %s | nom_retenu='%s' | source=%s (%s %s) | raison: %s",
                    icon, action, nom_retenu, type_source, annee_source,
                    confiance_dec, raison[:80])

        # ── 3. Stats ─────────────────────────────────────────────────────────
        if action == "CONFIRME":
            stats["confirmed"] += 1
        elif action == "REMPLACE":
            stats["replaced"] += 1
        elif action == "PARTIEL":
            stats["partial"] += 1
        elif action == "INCONNU":
            stats["not_found"] += 1
        else:
            stats["to_verify"] += 1

        # ── 4. Mise à jour confiance_nom ──────────────────────────────────────
        new_confiance = _ACTION_TO_CONFIANCE.get(action)
        if new_confiance and new_confiance != confiance:
            updates.append((new_confiance, cid))

        # ── 5. Ligne CSV ──────────────────────────────────────────────────────
        report_rows.append({
            "entite":            entite_nom,
            "code":              code,
            "id_contact":        cid,
            "nom_db":            nom_complet,
            "poste_db":          poste_exact or "",
            "action":            action,
            "raison":            raison,
            "confiance_decision": confiance_dec,
            "nom_retenu":        nom_retenu,
            "source_retenue":    source_retenue,
            "annee_source":      annee_source,
            "type_source":       type_source,
        })

    # ── Persistance ───────────────────────────────────────────────────────────
    if not dry_run and updates:
        with db._conn.cursor() as cur:
            for new_confiance, contact_id in updates:
                cur.execute(
                    "UPDATE prospection_contacts SET confiance_nom = %s WHERE id = %s",
                    (new_confiance, contact_id),
                )
        db._conn.commit()
        logger.info("  %d contacts mis à jour en base.", len(updates))
    elif dry_run:
        logger.info("  [dry-run] %d mises à jour simulées.", len(updates))

    # ── Rapport CSV ───────────────────────────────────────────────────────────
    # Tri : REMPLACE en premier, puis VERIFIER/PARTIEL, puis CONFIRME
    _order = {"REMPLACE": 0, "VERIFIER": 1, "PARTIEL": 2, "CONFIRME": 3, "INCONNU": 4}
    report_rows.sort(key=lambda r: (_order.get(r["action"], 9), r["entite"]))

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        if not report_rows:
            f.write("Aucun résultat.\n")
        else:
            fieldnames = [
                "entite", "code", "id_contact", "nom_db", "poste_db",
                "action", "raison", "confiance_decision",
                "nom_retenu", "source_retenue", "annee_source", "type_source",
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(report_rows)

    logger.info(
        "  Résultat : %d traités → %d confirmés, %d à remplacer, "
        "%d partiel, %d à vérifier, %d non trouvés, %d erreurs",
        stats["processed"], stats["confirmed"], stats["replaced"],
        stats["partial"], stats["to_verify"], stats["not_found"], stats["errors"],
    )
    logger.info("  Rapport → %s", csv_path)
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MODE 4 — APPLY REPLACEMENTS
# ─────────────────────────────────────────────────────────────────────────────

def _split_nom(nom_complet: str) -> tuple[str, str]:
    """Divise 'Prénom NOM' en (prénom, nom). Dernière partie = nom."""
    parts = nom_complet.strip().split()
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def _is_valid_replacement(nom_retenu: str) -> tuple[bool, str]:
    """
    Vérifie qu'un nom_retenu est utilisable pour un remplacement en base.
    Retourne (valide, raison_rejet).
    """
    n = (nom_retenu or "").strip()
    if not n:
        return False, "nom vide"
    if "[" in n or "]" in n:
        return False, f"nom incomplet : {n!r}"
    parts = n.split()
    if len(parts) < 2:
        return False, f"prénom seul sans nom de famille : {n!r}"
    prenom = parts[0]
    # Initiale isolée type "E." ou "J"
    if len(prenom.rstrip(".")) <= 1:
        return False, f"initiale sans prénom complet : {n!r}"
    return True, ""


def run_apply_replacements(
    db: ProspectionDB,
    out_dir: Path,
    dry_run: bool,
) -> dict[str, int]:
    """
    Lit outputs/quality/validation_dirs.csv et applique les remplacement de noms
    pour toutes les lignes action=REMPLACE dont le nom_retenu est complet.

    Mises à jour DB :
      - nom_complet, prenom, nom
      - confiance_nom = 'haute' si confiance_decision='haute', sinon 'a_verifier'

    Retourne {applied, skipped, errors}.
    """
    logger.info("=== apply-replacements ===")
    csv_path = out_dir / "validation_dirs.csv"
    if not csv_path.exists():
        logger.error("  CSV introuvable : %s", csv_path)
        return {"applied": 0, "skipped": 0, "errors": 1}

    rows_remplace = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("action") == "REMPLACE":
                rows_remplace.append(row)

    logger.info("  %d lignes REMPLACE trouvées dans le CSV.", len(rows_remplace))
    stats = {"applied": 0, "skipped": 0, "errors": 0}

    for row in rows_remplace:
        contact_id = row.get("id_contact", "").strip()
        nom_retenu  = (row.get("nom_retenu") or "").strip()
        nom_db      = row.get("nom_db", "")
        entite      = row.get("entite", "")
        conf_dec    = (row.get("confiance_decision") or "").strip().lower()

        valid, raison = _is_valid_replacement(nom_retenu)
        if not valid:
            logger.warning("  [SKIP] id=%-4s | %-25s | %s (%s)",
                           contact_id, nom_db, raison, entite)
            stats["skipped"] += 1
            continue

        prenom, nom = _split_nom(nom_retenu)
        new_confiance = "haute" if conf_dec == "haute" else "a_verifier"

        logger.info("  [REMPLACE] id=%-4s | %-25s → %-25s | %s (%s)",
                    contact_id, nom_db, nom_retenu, new_confiance, entite)

        if not dry_run:
            try:
                with db._conn.cursor() as cur:
                    cur.execute("""
                        UPDATE prospection_contacts
                        SET nom_complet  = %s,
                            prenom       = %s,
                            nom          = %s,
                            confiance_nom = %s
                        WHERE id = %s
                    """, (nom_retenu, prenom, nom, new_confiance, int(contact_id)))
                db._conn.commit()
                stats["applied"] += 1
            except Exception as exc:
                logger.error("  ERREUR id=%s : %s", contact_id, exc)
                db._conn.rollback()
                stats["errors"] += 1
        else:
            stats["applied"] += 1

    tag = " [dry-run]" if dry_run else ""
    logger.info("  %d appliqués%s, %d sautés (noms incomplets), %d erreurs.",
                stats["applied"], tag, stats["skipped"], stats["errors"])
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MODE 5 — RECLASSIFY NIVEAUX
# ─────────────────────────────────────────────────────────────────────────────

_VALID_NIVEAUX = {
    "dga", "direction", "direction_adjointe",
    "responsable_tarification", "operationnel",
}

_RECLASSIFY_PROMPT = """\
Tu es un expert en organisation des collectivités territoriales françaises.
Tu analyses un contact dans une base de prospection B2G pour le financement \
des ESSMS (Établissements et Services Sociaux et Médico-Sociaux).

Contact :
  Nom    : {nom}
  Poste  : {poste}
  Entité : {entite} (type : {type_entite})

Réponds UNIQUEMENT en JSON strict, sans commentaire :
{{
  "niveau": "<niveau>",
  "scope_essms": <true|false>,
  "raison": "<explication courte>"
}}

Règles pour "niveau" (choisis parmi ces valeurs exactes) :
- "dga"                    : Directeur Général Adjoint ou DGA
- "direction"              : Directeur(trice) de service / pôle / direction
- "direction_adjointe"     : Directeur(trice) adjoint(e)
- "responsable_tarification" : Responsable ou chef de service dédié au \
financement / tarification / contrôle qualité des ESSMS
- "operationnel"           : Chargé(e) de mission, gestionnaire, coordinateur, \
inspecteur opérationnel, cadre technique

Règles pour "scope_essms" :
- true  : le poste porte directement sur le pilotage, financement, tarification \
ou contrôle des ESSMS (autonomie, enfance, handicap, PA/PH, insertion + ESSMS)
- false : poste sans lien direct avec les ESSMS (insertion pure sans \
établissements, RH, communication, SI, juridique, finances générales, routes…)
"""


def _llm_reclassify(mistral: MistralClient, nom: str, poste: str,
                    entite: str, type_entite: str) -> dict:
    """Appel Mistral pour reclasser un contact. Retourne dict avec niveau/scope_essms/raison."""
    import json as _json
    prompt = _RECLASSIFY_PROMPT.format(
        nom=nom, poste=poste, entite=entite, type_entite=type_entite
    )
    try:
        raw = mistral._raw_complete([{"role": "user", "content": prompt}])
        # Extraire le JSON de la réponse
        text = raw.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        data = _json.loads(text.strip())
        niveau = data.get("niveau", "").strip().lower()
        if niveau not in _VALID_NIVEAUX:
            niveau = "operationnel"  # fallback conservateur
        return {
            "niveau": niveau,
            "scope_essms": bool(data.get("scope_essms", True)),
            "raison": str(data.get("raison", "")),
        }
    except Exception as exc:
        logger.warning("  LLM reclassify erreur : %s", exc)
        return {"niveau": None, "scope_essms": True, "raison": f"erreur: {exc}"}


def run_reclassify_niveaux(
    db: ProspectionDB,
    out_dir: Path,
    dry_run: bool,
) -> dict[str, int]:
    """
    Reclasse les contacts en niveau='direction' dont le poste n'est pas
    un titre de directeur/directrice (responsables, chefs de service,
    chargés, cadres, coordinateurs…).

    Pour chaque contact :
      1. Appel Mistral → niveau correct + scope_essms
      2. Si hors scope ESSMS → confiance_nom='invalide'
      3. Sinon → met à jour niveau

    Ecrit un CSV rapport dans out_dir/reclassify_niveaux.csv.
    """
    logger.info("=== reclassify-niveaux ===")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "reclassify_niveaux.csv"

    with db._conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.nom_complet, c.poste_exact, c.niveau,
                   c.confiance_nom, e.nom AS entite, e.type_entite
            FROM prospection_contacts c
            JOIN prospection_entites e ON e.id = c.entite_id
            WHERE c.niveau = 'direction'
              AND c.confiance_nom != 'invalide'
              AND LOWER(c.poste_exact) NOT LIKE '%direct%'
            ORDER BY e.nom, c.poste_exact
        """)
        rows = cur.fetchall()

    logger.info("  %d contacts à reclasser (niveau=direction sans 'direct' dans poste).", len(rows))

    mistral = MistralClient()
    stats = {"processed": 0, "reclassified": 0, "hors_scope": 0, "unchanged": 0, "errors": 0}
    rapport: list[dict] = []

    for i, (cid, nom, poste, niveau, confiance, entite, type_entite) in enumerate(rows, 1):
        logger.info("  [%d/%d] %s | %s | %s", i, len(rows), entite, nom, poste)
        stats["processed"] += 1
        time.sleep(0.8)

        result = _llm_reclassify(mistral, nom or "", poste or "", entite or "", type_entite or "")

        nouveau_niveau = result["niveau"]
        scope = result["scope_essms"]
        raison = result["raison"]

        if nouveau_niveau is None:
            stats["errors"] += 1
            action = "erreur"
        elif not scope:
            action = "hors_scope"
            stats["hors_scope"] += 1
            logger.info("    → HORS SCOPE | %s", raison)
        elif nouveau_niveau != niveau:
            action = "reclassifié"
            stats["reclassified"] += 1
            logger.info("    → %s | %s", nouveau_niveau, raison)
        else:
            action = "inchangé"
            stats["unchanged"] += 1
            logger.info("    → inchangé (%s) | %s", niveau, raison)

        rapport.append({
            "entite":           entite,
            "id":               cid,
            "nom":              nom,
            "poste":            poste,
            "niveau_avant":     niveau,
            "niveau_apres":     nouveau_niveau or niveau,
            "scope_essms":      scope,
            "action":           action,
            "raison":           raison,
        })

        if dry_run or nouveau_niveau is None:
            continue

        try:
            with db._conn.cursor() as cur:
                if not scope:
                    cur.execute(
                        "UPDATE prospection_contacts SET confiance_nom='invalide' WHERE id=%s",
                        (cid,)
                    )
                elif nouveau_niveau != niveau:
                    cur.execute(
                        "UPDATE prospection_contacts SET niveau=%s WHERE id=%s",
                        (nouveau_niveau, cid)
                    )
            db._conn.commit()
        except Exception as exc:
            logger.error("  ERREUR id=%s : %s", cid, exc)
            db._conn.rollback()
            stats["errors"] += 1

    # CSV
    rapport.sort(key=lambda r: (r["action"] == "inchangé", r["entite"]))
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["entite", "id", "nom", "poste", "niveau_avant",
                      "niveau_apres", "scope_essms", "action", "raison"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rapport)
    logger.info("  Rapport → %s", csv_path)

    tag = " [dry-run]" if dry_run else ""
    logger.info(
        "  %d traités%s → %d reclassifiés, %d hors_scope, %d inchangés, %d erreurs.",
        stats["processed"], tag, stats["reclassified"],
        stats["hors_scope"], stats["unchanged"], stats["errors"]
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MODE 6 — REQUALIFY TARIFICATION
# ─────────────────────────────────────────────────────────────────────────────

def run_requalify_tarification(
    db: ProspectionDB,
    out_dir: Path,
    dry_run: bool,
) -> dict[str, int]:
    """
    Re-vérifie via LLM tous les contacts actifs dont le poste contient 'tarif'
    OU dont le niveau est déjà 'responsable_tarification'.

    Décide pour chacun le bon niveau parmi les 5 valides + scope_essms.
    Applique les corrections en base et génère un CSV rapport.
    """
    logger.info("=== requalify-tarification ===")
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / "requalify_tarification.csv"

    with db._conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.nom_complet, c.poste_exact, c.niveau,
                   c.confiance_nom, e.nom AS entite, e.type_entite
            FROM prospection_contacts c
            JOIN prospection_entites e ON e.id = c.entite_id
            WHERE c.confiance_nom != 'invalide'
              AND (
                c.niveau = 'responsable_tarification'
                OR LOWER(c.poste_exact) LIKE '%tarif%'
              )
            ORDER BY e.nom, c.poste_exact
        """)
        rows = cur.fetchall()

    logger.info("  %d contacts 'tarif' ou 'responsable_tarification' à requalifier.", len(rows))

    mistral = MistralClient()
    stats = {"processed": 0, "reclassified": 0, "hors_scope": 0, "unchanged": 0, "errors": 0}
    rapport: list[dict] = []

    for i, (cid, nom, poste, niveau, confiance, entite, type_entite) in enumerate(rows, 1):
        logger.info("  [%d/%d] %s | %s | %s (niveau=%s)", i, len(rows), entite, nom, poste, niveau)
        stats["processed"] += 1
        time.sleep(0.8)

        result = _llm_reclassify(mistral, nom or "", poste or "", entite or "", type_entite or "")
        nouveau_niveau = result["niveau"]
        scope = result["scope_essms"]
        raison = result["raison"]

        if nouveau_niveau is None:
            stats["errors"] += 1
            action = "erreur"
        elif not scope:
            action = "hors_scope"
            stats["hors_scope"] += 1
            logger.info("    → HORS SCOPE | %s", raison)
        elif nouveau_niveau != niveau:
            action = "reclassifié"
            stats["reclassified"] += 1
            logger.info("    → %s | %s", nouveau_niveau, raison)
        else:
            action = "inchangé"
            stats["unchanged"] += 1
            logger.info("    → inchangé (%s) | %s", niveau, raison)

        rapport.append({
            "entite":       entite,
            "id":           cid,
            "nom":          nom,
            "poste":        poste,
            "niveau_avant": niveau,
            "niveau_apres": nouveau_niveau or niveau,
            "scope_essms":  scope,
            "action":       action,
            "raison":       raison,
        })

        if dry_run or nouveau_niveau is None:
            continue

        try:
            with db._conn.cursor() as cur:
                if not scope:
                    cur.execute(
                        "UPDATE prospection_contacts SET confiance_nom='invalide' WHERE id=%s",
                        (cid,)
                    )
                elif nouveau_niveau != niveau:
                    cur.execute(
                        "UPDATE prospection_contacts SET niveau=%s WHERE id=%s",
                        (nouveau_niveau, cid)
                    )
            db._conn.commit()
        except Exception as exc:
            logger.error("  ERREUR id=%s : %s", cid, exc)
            db._conn.rollback()
            stats["errors"] += 1

    rapport.sort(key=lambda r: (r["action"] == "inchangé", r["entite"]))
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "entite", "id", "nom", "poste", "niveau_avant",
            "niveau_apres", "scope_essms", "action", "raison"
        ])
        writer.writeheader()
        writer.writerows(rapport)
    logger.info("  Rapport → %s", csv_path)

    tag = " [dry-run]" if dry_run else ""
    logger.info(
        "  %d traités%s → %d reclassifiés, %d hors_scope, %d inchangés, %d erreurs.",
        stats["processed"], tag, stats["reclassified"],
        stats["hors_scope"], stats["unchanged"], stats["errors"]
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MODE 7 — FIND MISSING CONTACTS
# ─────────────────────────────────────────────────────────────────────────────

_FIND_CONTACT_PROMPT = """\
Tu es un expert en organigrammes administratifs français.
Tu dois identifier la personne occupant un poste précis au sein d'une entité publique.

## Entité
- Nom : {entite}
- Type : {type_entite}

## Poste recherché
{poste_cible}

## Extraits de résultats de recherche web
{snippets}

## Tâche
Identifie la personne (Prénom + Nom) qui occupe "{poste_cible}" ou un poste équivalent \
dans {entite}. Si plusieurs candidats, prends le plus récent et le plus fiable.

Réponds UNIQUEMENT en JSON strict, sans commentaire ni markdown :
{{
  "action": "INSERER" | "INSERER_ADJOINT" | "VERIFIER" | "INCONNU",
  "niveau_insere": "<niveau exact à insérer en base>",
  "nom_complet": "<Prénom NOM ou vide si inconnu>",
  "prenom": "<prénom ou vide>",
  "nom": "<nom de famille ou vide>",
  "poste_exact": "<intitulé exact trouvé dans les sources>",
  "source": "<URL principale>",
  "confiance": "haute" | "moyenne" | "basse",
  "annee_source": "<année détectée ou vide>",
  "raison": "<explication courte>"
}}

Règles pour "action" et "niveau_insere" :
- "INSERER" + niveau_insere="{niveau}" : DGA/Responsable clairement identifié \
(titre principal == DGA ou Directeur Général Adjoint ou équivalent direct)
- "INSERER_ADJOINT" + niveau_insere="direction_adjointe" : aucun DGA strict trouvé, \
mais un "Adjoint au DGA", "Directeur de la solidarité" sans DGA explicite, ou \
"Directeur adjoint solidarités" est disponible avec source fiable
- "VERIFIER" : source ambigüe, trop ancienne (>2 ans), ou plusieurs candidats \
contradictoires → niveau_insere="{niveau}"
- "INCONNU" : aucune personne identifiable → niveau_insere="{niveau}"
- Pour responsable_tarification : chercher un(e) Responsable ou Chef de service \
dédié(e) à la tarification / financement des ESSMS (établissements et services \
sociaux et médico-sociaux). Intitulés valides pour INSERER : "Responsable \
tarification ESSMS", "Chef de service tarification autonomie", "Responsable \
financement de l'offre", "Chef de service financement ESSMS", "Contrôleur \
tarificateur", "Responsable tarification enfance". \
ATTENTION : un directeur de pôle généraliste, un DGA, ou un responsable \
administratif sans lien explicite avec la tarification ESSMS → INCONNU.
"""

# Requêtes Serper adaptées par cible
_SEARCH_QUERIES = {
    "dga": [
        '"{entite}" "directeur général adjoint" solidarités',
        '"{entite}" DGA solidarités site:fr',
        'organigramme "{entite}" DGA solidarités',
    ],
    "responsable_tarification": [
        '"{entite}" ("chef de service" OR "responsable") "tarification" ESSMS',
        '"{entite}" "chef de service" tarification autonomie OR enfance',
        '"{entite}" "responsable" "financement de l\'offre" médico-social OR ESSMS',
    ],
}

_POSTE_CIBLE = {
    "dga": "Directeur(trice) Général(e) Adjoint(e) chargé(e) des solidarités / action sociale",
    "responsable_tarification": "Responsable ou chef de service tarification / financement des ESSMS (établissements sociaux et médico-sociaux), dans le pôle autonomie (personnes âgées / handicap) ou enfance",
}


def run_find_missing_contacts(
    db: ProspectionDB,
    out_dir: Path,
    dry_run: bool,
    niveau: str,
) -> dict[str, int]:
    """
    Pour chaque entité qui n'a pas encore de contact avec le niveau demandé
    (dga ou responsable_tarification) :
      1. Serper : 1-3 requêtes ciblées
      2. Mistral : extraction du contact
      3. Si confiance haute/moyenne et action=INSERER → insertion en base
      4. CSV rapport complet

    Seules les entités de type 'departement' sont ciblées pour
    responsable_tarification ; tous les types pour dga.
    """
    logger.info("=== find-missing-contacts [niveau=%s] ===", niveau)
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"find_missing_{niveau}.csv"

    if niveau not in _SEARCH_QUERIES:
        logger.error("  niveau '%s' non supporté. Valeurs: %s", niveau, list(_SEARCH_QUERIES))
        return {}

    # Entités qui n'ont PAS de contact actif avec ce niveau
    # Pour dga et responsable_tarification : uniquement les départements
    type_filter = "AND e.type_entite = 'departement'"

    with db._conn.cursor() as cur:
        cur.execute(f"""
            SELECT e.id, e.nom, e.type_entite, e.code
            FROM prospection_entites e
            WHERE NOT EXISTS (
                SELECT 1 FROM prospection_contacts c
                WHERE c.entite_id = e.id
                  AND c.niveau = %s
                  AND c.confiance_nom != 'invalide'
            )
            {type_filter}
            ORDER BY e.nom
        """, (niveau,))
        entites = cur.fetchall()

    logger.info("  %d entités sans contact '%s'.", len(entites), niveau)

    serper  = SerperClient()
    mistral = MistralClient()
    stats   = {"processed": 0, "inserted": 0, "to_verify": 0, "inconnu": 0, "errors": 0}
    rapport: list[dict] = []

    import json as _json

    poste_cible = _POSTE_CIBLE[niveau]
    queries_tpl = _SEARCH_QUERIES[niveau]

    for i, (eid, entite_nom, type_entite, code) in enumerate(entites, 1):
        logger.info("  [%d/%d] %s (%s)", i, len(entites), entite_nom, type_entite)
        stats["processed"] += 1

        # Agrège les snippets des différentes requêtes (max 3 requêtes, stop si assez)
        all_snippets: list[str] = []
        for q_tpl in queries_tpl:
            query = q_tpl.format(entite=entite_nom)
            try:
                _, snips = serper.search_and_extract(query, num=5)
                all_snippets.extend(snips)
            except Exception as exc:
                logger.warning("    Serper erreur (%s) : %s", query[:40], exc)
            time.sleep(0.8)
            if len(all_snippets) >= 8:
                break

        if not all_snippets:
            stats["inconnu"] += 1
            rapport.append({
                "entite": entite_nom, "type_entite": type_entite, "code": code,
                "action": "INCONNU", "niveau_insere": niveau,
                "nom_complet": "", "prenom": "", "nom": "",
                "poste_exact": "", "source": "", "confiance": "", "annee_source": "",
                "raison": "Aucun snippet Serper", "inserted": False,
            })
            continue

        snippets_str = "\n\n---\n".join(all_snippets[:10])
        prompt = _FIND_CONTACT_PROMPT.format(
            entite=entite_nom, type_entite=type_entite,
            poste_cible=poste_cible, snippets=snippets_str,
            niveau=niveau,
        )

        try:
            raw = mistral._raw_complete([{"role": "user", "content": prompt}])
            text = raw.strip()
            if "```" in text:
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            data = _json.loads(text.strip())
        except Exception as exc:
            logger.error("    LLM erreur : %s", exc)
            stats["errors"] += 1
            rapport.append({
                "entite": entite_nom, "type_entite": type_entite, "code": code,
                "action": "ERREUR", "niveau_insere": niveau,
                "nom_complet": "", "prenom": "", "nom": "",
                "poste_exact": "", "source": "", "confiance": "", "annee_source": "",
                "raison": str(exc), "inserted": False,
            })
            time.sleep(1)
            continue

        time.sleep(0.8)

        action        = str(data.get("action", "VERIFIER")).upper()
        niveau_insere = str(data.get("niveau_insere", niveau)).strip()
        if niveau_insere not in _VALID_NIVEAUX:
            niveau_insere = niveau
        nom_complet = str(data.get("nom_complet", "")).strip()
        prenom      = str(data.get("prenom", "")).strip()
        nom_fam     = str(data.get("nom", "")).strip()
        poste_exact = str(data.get("poste_exact", "")).strip()
        source      = str(data.get("source", "")).strip()
        confiance   = str(data.get("confiance", "basse")).strip()
        annee       = str(data.get("annee_source", "")).strip()
        raison      = str(data.get("raison", "")).strip()

        logger.info("    → %s | %s (%s) | niveau=%s | conf=%s | %s",
                    action, nom_complet, poste_exact[:35], niveau_insere, confiance, raison[:50])

        inserted = False
        if action in ("INSERER", "INSERER_ADJOINT") and confiance in ("haute", "moyenne") and nom_complet:
            stats["inserted"] += 1
            if not dry_run:
                try:
                    with db._conn.cursor() as cur:
                        # Vérifie doublon
                        cur.execute("""
                            SELECT id FROM prospection_contacts
                            WHERE entite_id=%s AND LOWER(nom_complet)=LOWER(%s)
                        """, (eid, nom_complet))
                        if cur.fetchone():
                            logger.info("    → doublon déjà en base, ignoré.")
                        else:
                            cur.execute("""
                                INSERT INTO prospection_contacts
                                  (entite_id, nom_complet, prenom, nom, poste_exact,
                                   niveau, confiance_nom, source_nom)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            """, (
                                eid, nom_complet, prenom or None, nom_fam or None,
                                poste_exact or None, niveau_insere,
                                "haute" if confiance == "haute" else "moyenne",
                                source or None,
                            ))
                            inserted = True
                    db._conn.commit()
                    if inserted:
                        logger.info("    ✓ Inséré en base.")
                except Exception as exc:
                    logger.error("    INSERT erreur : %s", exc)
                    db._conn.rollback()
                    stats["errors"] += 1
            else:
                logger.info("    [dry-run] aurait inséré %s.", nom_complet)
        elif action == "VERIFIER":
            stats["to_verify"] += 1
        else:
            stats["inconnu"] += 1

        rapport.append({
            "entite": entite_nom, "type_entite": type_entite, "code": code,
            "action": action, "niveau_insere": niveau_insere,
            "nom_complet": nom_complet, "prenom": prenom, "nom": nom_fam,
            "poste_exact": poste_exact, "source": source, "confiance": confiance,
            "annee_source": annee, "raison": raison, "inserted": inserted,
        })

    # CSV
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["entite", "type_entite", "code", "action", "niveau_insere",
                      "nom_complet", "prenom", "nom", "poste_exact", "source",
                      "confiance", "annee_source", "raison", "inserted"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rapport)
    logger.info("  Rapport → %s", csv_path)

    tag = " [dry-run]" if dry_run else ""
    logger.info(
        "  %d entités traitées%s → %d insérés, %d à vérifier, %d inconnu, %d erreurs.",
        stats["processed"], tag, stats["inserted"],
        stats["to_verify"], stats["inconnu"], stats["errors"]
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MODE 8 — ENRICH LINKEDIN
# ─────────────────────────────────────────────────────────────────────────────

def run_enrich_linkedin(
    db: ProspectionDB,
    dry_run: bool,
    limit: int | None = None,
) -> dict[str, int]:
    """
    Pour chaque contact sans linkedin_url (et avec un nom valide),
    lance find_profile (Q1 contexte + Q2 fallback).
    Met à jour prospection_contacts.linkedin_url en place.
    """
    logger.info("=== MODE : enrich-linkedin ===")
    stats = {"processed": 0, "found": 0, "skipped": 0, "errors": 0}

    # Récupération des contacts éligibles
    with db._conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.prenom, c.nom, c.nom_complet, c.poste_exact, c.confiance_nom,
                   e.nom AS entite_nom, e.type_entite, e.code
            FROM prospection_contacts c
            JOIN prospection_entites e ON e.id = c.entite_id
            WHERE c.linkedin_url IS NULL
              AND c.prenom IS NOT NULL
              AND c.nom IS NOT NULL
              AND LENGTH(TRIM(c.nom)) > 1
              AND NOT (LENGTH(TRIM(c.nom)) = 2 AND c.nom = UPPER(c.nom))
              AND c.confiance_nom != 'invalide'
              AND LOWER(c.nom_complet) NOT LIKE '%non communiqué%'
              AND LOWER(c.nom_complet) NOT LIKE '%non trouvé%'
              AND LOWER(c.nom_complet) NOT LIKE '%vacance%'
              AND LOWER(c.nom_complet) NOT LIKE '%à définir%'
              AND LOWER(c.nom_complet) NOT LIKE '%en cours%'
            ORDER BY e.nom, c.nom_complet
        """)
        contacts = cur.fetchall()

    total = len(contacts)
    if limit:
        contacts = contacts[:limit]

    logger.info("  %d contacts sans LinkedIn (traitement : %d)", total, len(contacts))

    if not contacts:
        logger.info("  Rien à enrichir.")
        return stats

    serper = SerperClient()
    finder = LinkedInFinder(serper=serper)

    found_ids: list[tuple[str, int]] = []  # (url, contact_id)

    for row in contacts:
        cid, prenom, nom, nom_complet, poste_exact, confiance, entite_nom, entity_type, entity_code = row
        stats["processed"] += 1

        # Skip noms avec initiale (au cas où check-names n'a pas tourné)
        if len((nom or "").strip()) <= 1:
            stats["skipped"] += 1
            continue

        logger.info(
            "  [%d/%d] %s | %s %s (%s)",
            stats["processed"], len(contacts), entite_nom, prenom, nom, poste_exact or "-"
        )

        try:
            url = finder.find_profile(
                prenom=prenom,
                nom=nom,
                entity_name=entite_nom,
                entity_type=entity_type,
                entity_code=entity_code or "",
            )
        except Exception as exc:
            logger.error("    Erreur pour %s %s : %s", prenom, nom, exc)
            stats["errors"] += 1
            time.sleep(1)
            continue

        if url:
            logger.info("    → TROUVÉ : %s", url)
            stats["found"] += 1
            found_ids.append((url, cid))
        else:
            logger.debug("    → non trouvé")

        time.sleep(1)  # throttle

    # Persistance
    if not dry_run and found_ids:
        with db._conn.cursor() as cur:
            for url, cid in found_ids:
                cur.execute(
                    "UPDATE prospection_contacts SET linkedin_url = %s WHERE id = %s",
                    (url, cid),
                )
        db._conn.commit()
        logger.info("  %d LinkedIn mis à jour en base.", len(found_ids))
    elif dry_run:
        logger.info("  [dry-run] %d LinkedIn auraient été mis à jour.", len(found_ids))

    logger.info(
        "  Résultat : %d traités, %d trouvés, %d ignorés, %d erreurs.",
        stats["processed"], stats["found"], stats["skipped"], stats["errors"]
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline quality toolkit — check-names, audit-postes, validate-dirs, check-territorial, enrich-linkedin"
    )
    parser.add_argument("--check-names",          action="store_true", help="Flagge/supprime les noms fantômes")
    parser.add_argument("--audit-postes",         action="store_true", help="Rapport CSV des doublons de postes")
    parser.add_argument("--validate-dirs",        action="store_true", help="Double-validation directeurs autonomie/enfance via organigramme")
    parser.add_argument("--check-territorial",    action="store_true", help="Flagge les directeurs territoriaux hors scope ESSMS")
    parser.add_argument("--apply-replacements",   action="store_true", help="Applique les noms REMPLACE issus du CSV validation_dirs")
    parser.add_argument("--reclassify-niveaux",      action="store_true", help="Reclasse les contacts mal nivellés (LLM + scope ESSMS)")
    parser.add_argument("--requalify-tarification",  action="store_true", help="Re-vérifie les contacts avec 'tarif' dans le poste")
    parser.add_argument("--find-missing-contacts",   action="store_true", help="Recherche Serper+LLM les contacts manquants (requiert --niveau)")
    parser.add_argument("--enrich-linkedin",         action="store_true", help="Re-enrichit les contacts sans LinkedIn")
    parser.add_argument("--all",                     action="store_true", help="Active les modes 1-7 (hors find-missing-contacts)")
    parser.add_argument("--dry-run",          action="store_true", help="Simulation sans écriture en base")
    parser.add_argument("--out-dir",          default="outputs/quality", help="Dossier de sortie (défaut: outputs/quality)")
    parser.add_argument("--limit",            type=int, default=None, help="Limite pour --enrich-linkedin")
    parser.add_argument("--niveau",           default=None,
                        choices=["dga", "responsable_tarification"],
                        help="Niveau cible pour --find-missing-contacts")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.all:
        args.check_names          = True
        args.audit_postes         = True
        args.validate_dirs        = True
        args.check_territorial    = True
        args.apply_replacements   = True
        args.reclassify_niveaux      = True
        args.requalify_tarification  = True
        args.enrich_linkedin         = True
        # find-missing-contacts non inclus dans --all (requiert --niveau explicite)

    if not any([args.check_names, args.audit_postes,
                getattr(args, "validate_dirs", False),
                getattr(args, "check_territorial", False),
                getattr(args, "apply_replacements", False),
                getattr(args, "reclassify_niveaux", False),
                getattr(args, "requalify_tarification", False),
                getattr(args, "find_missing_contacts", False),
                args.enrich_linkedin]):
        print("Aucun mode sélectionné. Utilisez --check-names, --audit-postes, --validate-dirs, --check-territorial,"
              " --reclassify-niveaux, --requalify-tarification, --find-missing-contacts, --enrich-linkedin ou --all.")
        print("Utilisez --help pour plus d'informations.")
        sys.exit(1)

    out_dir = ROOT / args.out_dir

    start = datetime.now()
    logger.info("pipeline_quality démarré — %s%s",
                ", ".join([
                    m for m, flag in [
                        ("check-names",         getattr(args, "check_names",          False)),
                        ("audit-postes",        getattr(args, "audit_postes",         False)),
                        ("validate-dirs",       getattr(args, "validate_dirs",        False)),
                        ("check-territorial",   getattr(args, "check_territorial",    False)),
                        ("apply-replacements",  getattr(args, "apply_replacements",   False)),
                        ("reclassify-niveaux",     getattr(args, "reclassify_niveaux",      False)),
                        ("requalify-tarification", getattr(args, "requalify_tarification", False)),
                        ("find-missing-contacts",  getattr(args, "find_missing_contacts",  False)),
                        ("enrich-linkedin",        getattr(args, "enrich_linkedin",         False)),
                    ] if flag
                ]),
                " [DRY-RUN]" if args.dry_run else "")

    results: dict[str, Any] = {}


    with ProspectionDB() as db:
        if args.check_names:
            results["check_names"] = run_check_names(db, dry_run=args.dry_run)

        if args.audit_postes:
            results["audit_postes"] = run_audit_postes(db, out_dir=out_dir)

        if getattr(args, "validate_dirs", False):
            results["validate_dirs"] = run_validate_dirs(
                db, out_dir=out_dir, dry_run=args.dry_run
            )

        if getattr(args, "check_territorial", False):
            results["check_territorial"] = run_check_territorial(
                db, out_dir=out_dir, dry_run=args.dry_run
            )

        if getattr(args, "apply_replacements", False):
            results["apply_replacements"] = run_apply_replacements(
                db, out_dir=out_dir, dry_run=args.dry_run
            )

        if getattr(args, "reclassify_niveaux", False):
            results["reclassify_niveaux"] = run_reclassify_niveaux(
                db, out_dir=out_dir, dry_run=args.dry_run
            )

        if getattr(args, "requalify_tarification", False):
            results["requalify_tarification"] = run_requalify_tarification(
                db, out_dir=out_dir, dry_run=args.dry_run
            )

        if getattr(args, "find_missing_contacts", False):
            if not args.niveau:
                logger.error("--find-missing-contacts requiert --niveau dga|responsable_tarification")
            else:
                results["find_missing_contacts"] = run_find_missing_contacts(
                    db, out_dir=out_dir, dry_run=args.dry_run, niveau=args.niveau
                )

        if args.enrich_linkedin:
            results["enrich_linkedin"] = run_enrich_linkedin(
                db,
                dry_run=args.dry_run,
                limit=args.limit,
            )

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("=== TERMINÉ en %.1fs ===", elapsed)

    if "check_names" in results:
        s = results["check_names"]
        logger.info("  check-names     : %d supprimés, %d flagués", s["deleted"], s["flagged"])
    if "audit_postes" in results:
        s = results["audit_postes"]
        logger.info("  audit-postes    : %d paires suspectes dans %d entités", s["paires"], s["entites_avec_doublons"])
    if "validate_dirs" in results:
        s = results["validate_dirs"]
        logger.info("  validate-dirs      : %d traités → %d confirmés, %d à remplacer, %d à vérifier",
                    s["processed"], s["confirmed"], s["replaced"], s["to_verify"])
    if "check_territorial" in results:
        s = results["check_territorial"]
        logger.info("  check-territorial  : %d total → %d hors_scope flagués, %d scope_ok",
                    s["total"], s["flagged"], s["scope_ok"])
    if "apply_replacements" in results:
        s = results["apply_replacements"]
        logger.info("  apply-replacements : %d appliqués, %d sautés (incomplets), %d erreurs",
                    s["applied"], s["skipped"], s["errors"])
    if "reclassify_niveaux" in results:
        s = results["reclassify_niveaux"]
        logger.info("  reclassify-niveaux : %d traités → %d reclassifiés, %d hors_scope, %d inchangés",
                    s["processed"], s["reclassified"], s["hors_scope"], s["unchanged"])
    if "requalify_tarification" in results:
        s = results["requalify_tarification"]
        logger.info("  requalify-tarification : %d traités → %d reclassifiés, %d hors_scope, %d inchangés",
                    s["processed"], s["reclassified"], s["hors_scope"], s["unchanged"])
    if "find_missing_contacts" in results:
        s = results["find_missing_contacts"]
        logger.info("  find-missing-contacts : %d entités → %d insérés, %d à vérifier, %d inconnu",
                    s["processed"], s["inserted"], s["to_verify"], s["inconnu"])
    if "enrich_linkedin" in results:
        s = results["enrich_linkedin"]
        logger.info("  enrich-linkedin : %d traités → %d trouvés", s["processed"], s["found"])


if __name__ == "__main__":
    main()
