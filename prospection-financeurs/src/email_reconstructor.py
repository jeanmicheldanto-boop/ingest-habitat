"""
Reconstruction et validation des emails professionnels.
"""
import logging
import re
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.serper_client import SerperClient
from src.mistral_client import MistralClient
from src.normalizer import build_email_variants

logger = logging.getLogger(__name__)

# Préfixes d'adresses génériques à exclure lors de la recherche de pattern
_GENERIC_PREFIXES = [
    "contact", "accueil", "dpo", "direction", "drh", "communication",
    "webmaster", "secretariat", "info", "courrier", "mairie", "rh",
    "ars-", "dirpjj-",
]

_EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-.]+\.[a-zA-Z]{2,}")


def _is_generic(email: str) -> bool:
    local = email.split("@")[0].lower()
    return any(local.startswith(p) for p in _GENERIC_PREFIXES)


def _extract_emails_from_snippets(snippets: list[str]) -> list[str]:
    """Extrait toutes les adresses email des snippets Serper."""
    emails: list[str] = []
    for s in snippets:
        found = _EMAIL_RE.findall(s)
        for e in found:
            if not _is_generic(e) and e not in emails:
                emails.append(e)
    return emails


class EmailReconstructor:
    """
    Détecte le pattern email d'un domaine et reconstruit les emails des contacts.
    """

    KNOWN_PATTERNS = {
        "justice.fr": {"pattern": "prenom.nom", "accents": "supprimés", "tirets_noms": "point", "confiance": "haute"},
        "ars.sante.fr": {"pattern": "prenom.nom", "accents": "supprimés", "tirets_noms": "point", "confiance": "haute"},
    }

    def __init__(self, serper: SerperClient, mistral: MistralClient, cache: dict | None = None):
        self.serper = serper
        self.mistral = mistral
        # Cache des patterns par domaine
        self._pattern_cache: dict[str, dict] = cache if cache is not None else {}

    def get_email_pattern(self, domain: str, entity_name: str = "") -> dict[str, Any]:
        """
        Détecte (et met en cache) le pattern email pour un domaine.
        """
        if domain in self._pattern_cache:
            return self._pattern_cache[domain]

        # Patterns connus — pas besoin de rechercher
        if domain in self.KNOWN_PATTERNS:
            pattern_info = self.KNOWN_PATTERNS[domain].copy()
            self._pattern_cache[domain] = pattern_info
            logger.info("Pattern connu pour %s: %s", domain, pattern_info["pattern"])
            return pattern_info

        # Recherche Serper pour trouver des exemples d'emails
        exclusions = " ".join(f'-"{p}@"' for p in _GENERIC_PREFIXES[:8])
        query = f'"@{domain}" {exclusions}'
        try:
            _, snippets = self.serper.search_and_extract(query)
            emails_found = _extract_emails_from_snippets(snippets)
            # Filtrer pour ne garder que les emails du bon domaine
            emails_found = [e for e in emails_found if e.endswith(f"@{domain}")]
            logger.info("[%s] %d exemples emails trouvés pour @%s", entity_name, len(emails_found), domain)
        except Exception as exc:
            logger.error("[%s] Erreur recherche pattern email: %s", entity_name, exc)
            emails_found = []

        pattern_info = self.mistral.detect_email_pattern(domain, emails_found)
        self._pattern_cache[domain] = pattern_info
        return pattern_info

    def reconstruct_for_contact(
        self,
        contact: dict[str, Any],
        domain: str,
        pattern_info: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Injecte email_principal et email_variantes dans le dict contact.
        """
        prenom = contact.get("prenom", "")
        nom = contact.get("nom", "")

        if not prenom or not nom:
            logger.warning("Prénom ou nom manquant pour '%s'", contact.get("nom_complet"))
            contact["email_principal"] = None
            contact["email_variantes"] = []
            contact["confiance_email"] = "inconnue"
            return contact

        variantes = build_email_variants(
            prenom=prenom,
            nom=nom,
            domain=domain,
            pattern=pattern_info.get("pattern", "prenom.nom"),
            accents=pattern_info.get("accents", "supprimés"),
            tirets_noms=pattern_info.get("tirets_noms", "point"),
        )

        contact["email_principal"] = variantes[0] if variantes else None
        contact["email_variantes"] = variantes[1:5] if len(variantes) > 1 else []
        # La confiance email hérite de la confiance du pattern
        pat_conf = pattern_info.get("confiance", "basse")
        nom_conf = contact.get("confiance_nom", "basse")
        # Prendre le niveau le plus bas entre pattern et nom
        conf_order = {"haute": 2, "moyenne": 1, "basse": 0, "inconnue": -1}
        lvl = min(conf_order.get(pat_conf, 0), conf_order.get(nom_conf, 0))
        rev = {v: k for k, v in conf_order.items()}
        contact["confiance_email"] = rev.get(lvl, "basse")

        return contact

    def validate_email(self, email: str) -> bool:
        """
        Valide un email en cherchant son apparition sur le web (via Serper).
        Retourne True si trouvé, False sinon.
        """
        try:
            results = self.serper.search(f'"{email}"', num=5)
            found = bool(results.get("organic"))
            if found:
                logger.info("Email validé par recherche web : %s", email)
            return found
        except Exception as exc:
            logger.error("Erreur validation email %s: %s", email, exc)
            return False
