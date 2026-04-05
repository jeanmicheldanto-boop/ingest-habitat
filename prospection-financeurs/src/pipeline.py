"""
Orchestrateur principal du pipeline de prospection financeurs ESSMS.

Séquence :
  1. Chargement du référentiel des entités (JSON)
  2. Pour chaque entité :
     a. Upsert entité dans Supabase
     b. Identification des contacts (Serper + Mistral)
     c. Recherche LinkedIn
     d. Détection pattern email (Serper + Mistral, avec cache DB)
     e. Reconstruction des emails
     f. Validation croisée optionnelle
     g. Upsert contacts + patterns dans Supabase
     h. Export JSON/CSV incrémental
  3. Résumé final + stats Supabase
"""
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).resolve().parent.parent / "logs" / "pipeline.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("pipeline")

# ── Imports internes ──────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    CONFIG_DIR,
    OUTPUT_DIR,
    ENTITY_TYPES,
    VALIDATE_EMAILS,
)
from src.serper_client import SerperClient
from src.mistral_client import MistralClient
from src.contact_finder import ContactFinder
from src.email_reconstructor import EmailReconstructor
from src.linkedin_finder import LinkedInFinder
from src.exporter import Exporter
from src.supabase_db import ProspectionDB


# ── Chargement des référentiels ───────────────────────────────────────────────

def load_referentiel(entity_type: str) -> list[dict]:
    mapping = {
        "departement": "departements.json",
        "dirpjj":      "dirpjj.json",
        "ars":         "ars.json",
    }
    path = CONFIG_DIR / mapping[entity_type]
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_postes_cibles(entity_type: str) -> list[dict]:
    path = CONFIG_DIR / "postes_cibles.json"
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return data.get(entity_type, [])


# ── Traitement d'une entité ───────────────────────────────────────────────────

def process_entity(
    entity: dict[str, Any],
    entity_type: str,
    target_posts: list[dict],
    contact_finder: ContactFinder,
    linkedin_finder: LinkedInFinder,
    email_recon: EmailReconstructor,
    exporter: Exporter,
    db: ProspectionDB,
    validate_emails: bool = True,
) -> int:
    """
    Traite une entité complète.
    Retourne le nombre de contacts ajoutés.
    """
    nom = entity.get("nom", entity.get("code", "?"))
    domain = entity.get("domaine_email") or _infer_domain(entity, entity_type)

    logger.info("── Traitement : [%s] %s (domaine: %s)", entity_type, nom, domain)

    # 1. Upsert entité dans Supabase
    entity["domaine_email"] = domain
    entite_id = db.upsert_entite(entity, entity_type)

    # 2. Identification des contacts
    contacts = contact_finder.find_contacts(entity, entity_type, target_posts)
    logger.info("  → %d contact(s) identifié(s)", len(contacts))

    if not contacts:
        return 0

    # 3. LinkedIn
    contacts = linkedin_finder.enrich_contacts(
        contacts, nom, entity_type, entity_code=entity.get("code", "")
    )

    # 4. Pattern email (d'abord depuis la DB, sinon via Serper+Mistral)
    pattern_info = db.get_email_pattern(domain)
    if not pattern_info:
        pattern_info = email_recon.get_email_pattern(domain, nom)
        db.upsert_email_pattern(domain, pattern_info)
    else:
        logger.debug("  Pattern email récupéré depuis Supabase pour @%s", domain)
        # Alimenter le cache local de email_recon
        email_recon._pattern_cache[domain] = pattern_info

    # 5. Reconstruction des emails
    for contact in contacts:
        contact = email_recon.reconstruct_for_contact(contact, domain, pattern_info)

        # 6. Validation croisée (optionnelle, prioritaire = confiance haute sur nom)
        if validate_emails and contact.get("email_principal") and contact.get("confiance_nom") != "basse":
            is_valid = email_recon.validate_email(contact["email_principal"])
            contact["email_valide_web"] = is_valid
            if is_valid:
                contact["confiance_email"] = "haute"

    # 7. Upsert contacts Supabase + export fichiers
    for contact in contacts:
        contact_id = db.upsert_contact(entite_id, contact)
        if contact.get("email_valide_web"):
            db.mark_email_validated(contact_id)
        exporter.add_contact(entity, entity_type, contact, pattern_info)

    logger.info("  → %d contact(s) sauvegardé(s) (entite_id=%d)", len(contacts), entite_id)
    return len(contacts)


def _infer_domain(entity: dict, entity_type: str) -> str:
    """Déduit le domaine email si non fourni explicitement."""
    if entity_type == "dirpjj":
        return "justice.fr"
    if entity_type == "ars":
        return "ars.sante.fr"
    # Département : extraire depuis site_web
    site = entity.get("site_web", "")
    if site:
        return site.removeprefix("www.").removeprefix("http://").removeprefix("https://").split("/")[0]
    return ""


# ── Point d'entrée ────────────────────────────────────────────────────────────

def main(args: argparse.Namespace) -> None:
    entity_types = args.types or ENTITY_TYPES
    skip_done = not args.force

    exporter = Exporter(OUTPUT_DIR)
    already_done_keys = exporter.get_processed_entity_keys()

    with SerperClient() as serper, ProspectionDB() as db:
        # Créer les tables si nécessaire
        db.create_tables()

        mistral = MistralClient()
        contact_finder = ContactFinder(serper, mistral)
        linkedin_finder = LinkedInFinder(serper)

        # Charger le cache pattern email depuis la DB pour éviter des requêtes redondantes
        email_pattern_cache: dict = {}
        email_recon = EmailReconstructor(serper, mistral, cache=email_pattern_cache)

        total_contacts = 0
        total_entities = 0
        total_skipped = 0

        for entity_type in entity_types:
            entities = load_referentiel(entity_type)
            target_posts = load_postes_cibles(entity_type)

            # Filtrer selon --codes si fourni
            if args.codes:
                entities = [e for e in entities if e.get("code") in args.codes]

            # Récupérer les entités déjà traitées dans Supabase
            done_codes_db = db.get_processed_entity_codes(entity_type) if skip_done else set()
            done_keys_file = {
                k.split(":")[1]
                for k in already_done_keys
                if k.startswith(f"{entity_type}:")
            }
            done_codes = done_codes_db | done_keys_file

            logger.info(
                "=== %s : %d entités (%d déjà traitées, %d à traiter) ===",
                entity_type.upper(), len(entities), len(done_codes),
                len([e for e in entities if e.get("code") not in done_codes])
            )

            for entity in entities:
                code = entity.get("code", "")
                if skip_done and code in done_codes:
                    logger.debug("Skip %s %s (déjà traité)", entity_type, code)
                    total_skipped += 1
                    continue

                try:
                    n = process_entity(
                        entity=entity,
                        entity_type=entity_type,
                        target_posts=target_posts,
                        contact_finder=contact_finder,
                        linkedin_finder=linkedin_finder,
                        email_recon=email_recon,
                        exporter=exporter,
                        db=db,
                        validate_emails=args.validate_emails,
                    )
                    total_contacts += n
                    total_entities += 1
                except Exception as exc:
                    logger.error("Erreur sur %s %s : %s", entity_type, code, exc, exc_info=True)

        exporter.finalize()

        # Statistiques finales
        stats = db.stats()
        logger.info(
            "\n══ Résumé ══\n"
            "  Entités traitées ce run : %d (skip : %d)\n"
            "  Contacts ajoutés        : %d\n"
            "  Requêtes Serper totales : %d\n"
            "  Appels Mistral totaux   : %d\n"
            "  ── Supabase ──\n"
            "  Total entités en base   : %d %s\n"
            "  Total contacts en base  : %d\n"
            "  Avec email              : %d\n"
            "  Avec LinkedIn           : %d\n"
            "  Email validé web        : %d",
            total_entities, total_skipped,
            total_contacts,
            serper.request_count,
            mistral.request_count,
            stats.get("total_entites", 0),
            stats.get("entites_par_type", {}),
            stats.get("total_contacts", 0),
            stats.get("contacts_avec_email", 0),
            stats.get("contacts_avec_linkedin", 0),
            stats.get("contacts_email_valide", 0),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline de prospection financeurs ESSMS"
    )
    parser.add_argument(
        "--types",
        nargs="+",
        choices=["departement", "dirpjj", "ars"],
        default=None,
        help="Types d'entités à traiter (défaut : tous)",
    )
    parser.add_argument(
        "--codes",
        nargs="+",
        default=None,
        help="Liste de codes spécifiques à traiter (ex: 77 91 ARS-IDF)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Retraiter les entités déjà traitées",
    )
    parser.add_argument(
        "--no-validate-emails",
        dest="validate_emails",
        action="store_false",
        help="Désactiver la validation croisée des emails",
    )
    parser.set_defaults(validate_emails=VALIDATE_EMAILS)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(args)
