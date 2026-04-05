"""
Client Mistral avec retry exponentiel, gestion des timeouts et prompts structurés.
"""
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

from mistralai import Mistral
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import (
    MISTRAL_API_KEY,
    MISTRAL_MODEL,
    MISTRAL_DELAY_SECONDS,
    MAX_RETRIES,
    RETRY_BASE_DELAY,
)

logger = logging.getLogger(__name__)


class MistralClient:
    """
    Encapsule les appels à l'API Mistral avec retry et prompts structurés.
    """

    def __init__(self):
        self._client = Mistral(api_key=MISTRAL_API_KEY)
        self._last_call: float = 0.0
        self.request_count: int = 0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_call
        wait = MISTRAL_DELAY_SECONDS - elapsed
        if wait > 0:
            time.sleep(wait)

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=RETRY_BASE_DELAY, min=RETRY_BASE_DELAY, max=60),
        retry=retry_if_exception_type(Exception),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _raw_complete(self, messages: list[dict]) -> str:
        """Appel brut à l'API Mistral."""
        self._throttle()
        response = self._client.chat.complete(
            model=MISTRAL_MODEL,
            messages=messages,
            response_format={"type": "json_object"},
        )
        self._last_call = time.time()
        self.request_count += 1
        content = response.choices[0].message.content
        logger.debug("Mistral [%d] → %d chars", self.request_count, len(content or ""))
        return content or ""

    def extract_contacts(
        self,
        entity_type: str,
        entity_name: str,
        target_posts: list[str],
        snippets: list[str],
    ) -> list[dict[str, Any]]:
        """
        Extrait les contacts depuis les snippets Serper.
        Retourne une liste de dicts avec : nom_complet, poste_exact, source, confiance.
        """
        posts_str = "\n".join(f"- {p}" for p in target_posts)
        snippets_str = "\n\n".join(snippets) if snippets else "(aucun résultat)"

        prompt = f"""Tu es un assistant spécialisé dans l'analyse d'organigrammes administratifs français.
À partir des extraits de résultats de recherche ci-dessous, identifie les personnes
occupant les postes suivants au sein du {entity_type} de {entity_name} :

Postes recherchés :
{posts_str}

Pour chaque personne trouvée, extrais :
- nom_complet : (prénom et nom)
- poste_exact : (intitulé tel qu'il apparaît)
- source : (URL d'où provient l'information)
- confiance : (haute/moyenne/basse selon la fraîcheur et la fiabilité de la source)

Résultats de recherche :
{snippets_str}

Règles :
- Confiance haute : organigramme officiel du site institutionnel, daté de moins d'un an
- Confiance moyenne : LinkedIn, article de presse, annuaire tiers, annonce de nomination
- Confiance basse : source ancienne (>2 ans), blog, forum, source indirecte
- Si plusieurs personnes occupent le même type de poste, inclure les deux
- Si une personne n'est pas trouvée, ne pas l'inventer

Réponds UNIQUEMENT en JSON valide, sans commentaire.
Format : {{"contacts": [{{"nom_complet": "...", "poste_exact": "...", "source": "...", "confiance": "..."}}]}}"""

        raw = self._raw_complete([{"role": "user", "content": prompt}])
        try:
            data = json.loads(raw)
            return data.get("contacts", [])
        except json.JSONDecodeError:
            logger.error("JSON invalide de Mistral pour %s: %s", entity_name, raw[:200])
            return []

    def detect_email_pattern(self, domain: str, emails_found: list[str]) -> dict[str, Any]:
        """
        Identifie le pattern email d'un domaine à partir d'exemples trouvés.
        Retourne : pattern, accents, tirets_noms, exemples_trouvés, confiance.
        """
        if not emails_found:
            return {
                "pattern": "prenom.nom",
                "accents": "supprimés",
                "tirets_noms": "point",
                "exemples_trouves": [],
                "confiance": "basse",
            }

        emails_str = "\n".join(f"- {e}" for e in emails_found)

        prompt = f"""À partir des adresses email trouvées ci-dessous pour le domaine {domain},
identifie le pattern de construction des adresses email nominatives.

Adresses trouvées :
{emails_str}

Détermine le pattern parmi :
- prenom.nom@{domain}
- pnom@{domain} (première lettre + nom)
- p.nom@{domain}
- nom.prenom@{domain}
- prenom-nom@{domain}
- premiere_lettre_prenom.nom@{domain}
- autre (préciser)

Indique aussi si les accents sont conservés ou supprimés,
et si les tirets dans les noms composés sont conservés ou remplacés par un point/supprimés.

Réponds en JSON :
{{"pattern": "...", "accents": "conservés|supprimés", "tirets_noms": "conservés|point|supprimés", "exemples_trouves": [...], "confiance": "haute|moyenne|basse"}}"""

        raw = self._raw_complete([{"role": "user", "content": prompt}])
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.error("JSON invalide de Mistral pour pattern email %s: %s", domain, raw[:200])
            return {
                "pattern": "prenom.nom",
                "accents": "supprimés",
                "tirets_noms": "point",
                "exemples_trouves": [],
                "confiance": "basse",
            }

    def analyze_signals(
        self,
        entity_name: str,
        code: str,
        snippets: list[str],
        sources: list[str],
    ) -> dict[str, Any]:
        """
        Synthétise des signaux de tension sur la tarification/financement ESSMS
        pour un département donné, à partir de snippets Serper.

        Retourne : resume, tags, niveau_alerte, sources_utilisees,
                   confiance, periode_couverte.
        """
        tags_ref = [
            "retards_paiement", "restriction_budgetaire", "contentieux",
            "reforme_tarifaire", "tension_operateurs", "sous_dotation",
            "controle_ou_audit", "reconfiguration_cpom",
            "fermeture_etablissement", "crise_sectorielle",
            "coupes_budgetaires", "non_revalorisation", "surcharge_administrative",
        ]
        snippets_str = "\n\n".join(snippets) if snippets else "(aucun extrait)"
        sources_str = "\n".join(sources[:10])
        tags_str = ", ".join(tags_ref)

        prompt = f"""Tu es un expert du secteur médico-social français, spécialisé dans le financement et la tarification des ESSMS (établissements et services sociaux et médico-sociaux).

Analyse les extraits de résultats de recherche ci-dessous concernant le département {entity_name} (code {code}).
Identifie les signaux de tension, difficultés ou enjeux récents liés à :
- La tarification des ESSMS (EHPAD, SAAD, IME, ESAT, etc.)
- Le financement par le Conseil Départemental
- Les relations avec les opérateurs / associations gestionnaires
- Les réformes tarifaires (SERAFIN-PH, CPOM, dotation globale, etc.)
- Les contentieux, retards de paiement, coupes budgétaires

Extraits de recherche :
{snippets_str}

Sources consultées :
{sources_str}

Tags disponibles (utilise ceux qui sont pertinents, tu peux aussi en créer d'autres) :
{tags_str}

Réponds UNIQUEMENT en JSON valide :
{{
  "resume": "2 à 4 phrases synthétisant les tensions ou enjeux identifiés. Si aucun signal clair, indiquer 'Aucun signal notable détecté.'",
  "tags": ["tag1", "tag2"],
  "niveau_alerte": "faible|moyen|fort",
  "sources_utilisees": ["url1", "url2"],
  "confiance": "faible|moyen|fort",
  "periode_couverte": "ex: 2023-2025"
}}

Critères niveau_alerte :
- fort : signaux explicites et récents (retards > 6 mois, contentieux en cours, coupes budgétaires annoncées)
- moyen : tensions mentionnées mais peu documentées ou anciennes (> 1 an)
- faible : aucun signal ou sources trop vagues"""

        raw = self._raw_complete([{"role": "user", "content": prompt}])
        try:
            data = json.loads(raw)
            # Garantir les champs attendus
            return {
                "resume":            data.get("resume", "Aucun signal identifié."),
                "tags":              data.get("tags", []),
                "niveau_alerte":     data.get("niveau_alerte", "faible"),
                "sources_utilisees": data.get("sources_utilisees", sources[:5]),
                "confiance":         data.get("confiance", "faible"),
                "periode_couverte":  data.get("periode_couverte", "2023-2025"),
            }
        except json.JSONDecodeError:
            logger.error("JSON invalide de Mistral pour signaux dept %s: %s", code, raw[:200])
            return {
                "resume": "Erreur d'analyse (réponse LLM invalide).",
                "tags": [],
                "niveau_alerte": "faible",
                "sources_utilisees": sources[:5],
                "confiance": "faible",
                "periode_couverte": "2023-2025",
            }
