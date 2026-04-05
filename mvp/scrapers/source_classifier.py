"""
Module de classification des sources web
Distingue : site officiel / gestionnaire / annuaire / article / autre
Version: 1.0
"""

import requests
import os
from typing import Dict, Optional
from dataclasses import dataclass
from dotenv import load_dotenv
from urllib.parse import urlparse

# Charger .env
load_dotenv()


@dataclass
class SourceClassification:
    """Résultat de classification d'une source"""
    type_source: str  # officiel_etablissement, site_gestionnaire, annuaire, article, autre
    confidence: float  # 0-100
    reasoning: str = ""  # Explication du choix


class SourceClassifier:
    """
    Classifie les sources web pour politique d'extraction
    - officiel_etablissement : site officiel d'une résidence/structure
    - site_gestionnaire : site d'association/réseau (CCAS, Habitat & Humanisme, etc.)
    - annuaire : site qui liste/compare des établissements
    - article : page d'info/actualité
    - autre : reste
    """
    
    def __init__(self, groq_api_key: Optional[str] = None):
        """
        Initialise le classificateur
        
        Args:
            groq_api_key: Clé API Groq (ou depuis .env)
        """
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        
        # Modèle léger pour classification
        self.model = "llama-3.1-8b-instant"
        
        # Pricing Groq
        self.pricing = {
            "input": 0.05,   # $/1M tokens
            "output": 0.08   # $/1M tokens
        }
        
        # Patterns d'annuaires connus (non exhaustif, mais doit couvrir les gros agrégateurs)
        # Objectif: empêcher qu'un annuaire se fasse passer pour un "site officiel".
        self.annuaire_patterns = [
            'essentiel-autonomie',
            'papyhappy',
            'pour-les-personnes-agees.gouv.fr',
            'retraiteplus',
            'logement-seniors.com',
            'capresidencesseniors.com',
            'capgeris.com',
            'france-maison-de-retraite',
            'lesmaisonsderetraite.fr',
            'ascellianceresidence.fr',
            'ascelliance-retraite.fr',
            'conseildependance.fr',
            'tarif-senior.com',
            'colocation-adulte.fr',
            'vizzit.fr',
            'capcampus.com',
            'pap.fr',
            # Nouveaux détectés en prod
            'cohabiting-seniors.com',
            'cohabiting-seniors',
            'capretraite.fr',
            'trouver-maison-de-retraite.fr',
            'sanitaire-social.com',
            'annuaire-retraite.com',
            'nosresidences.',
            'seloger.com',
            'logic-immo.com',
            'avendrealouer.fr',
            'explorimmoneuf.com',
            'immoneuf.com',
            'pagesjaunes.fr',
            'mappy.com',
            # Mots clés génériques
            'annuaire',
            'comparateur',
            'guide-maison-retraite'
        ]

        # Patterns presse / médias (non exhaustif → heuristiques génériques aussi)
        self.press_patterns = [
            'actu.fr',
            'oisehebdo.fr',
            'lobservateurdebeauvais.fr',
            'lebonhommepicard.fr',
            'leparisien.fr',
            'ouest-france.fr',
            'france3-regions.franceinfo.fr',
            'francebleu.fr',
            'bfmtv.com',
            '20minutes.fr',
            'lavoixdunord.fr'
        ]

        # Réseaux sociaux / plateformes (jamais un site officiel d'établissement)
        self.social_patterns = [
            'facebook.com',
            'instagram.com',
            'linkedin.com',
            'twitter.com',
            'x.com',
            'youtube.com',
            'tiktok.com'
        ]

        self.file_ext_blocklist = ['.pdf']
        
        # Statistiques
        self.stats = {
            'sources_classified': 0,
            'classification_cost': 0.0
        }
    
    def classify_source(self, url: str, title: str, snippet: str, 
                       page_excerpt: Optional[str] = None) -> SourceClassification:
        """Classifie une source web.

        Priorités (gating dur AVANT LLM):
        1) Extensions (PDF) / réseaux sociaux → jamais officiel
        2) Annuaires → jamais officiel
        3) Presse/médias → article (peut être une source d'entrée, mais pas un site officiel)
        4) Sinon: LLM (Groq) + fallback heuristique
        """
        self.stats['sources_classified'] += 1

        url_lower = (url or "").lower()
        title_lower = (title or "").lower()

        # 0) Extensions bloquantes
        for ext in self.file_ext_blocklist:
            if url_lower.split('?')[0].endswith(ext):
                return SourceClassification(
                    type_source="article",
                    confidence=95.0,
                    reasoning=f"Fichier {ext} (jamais site officiel)"
                )

        # 1) Réseaux sociaux
        if any(pattern in url_lower for pattern in self.social_patterns):
            return SourceClassification(
                type_source="article",
                confidence=95.0,
                reasoning="Réseau social/plateforme (jamais site officiel)"
            )

        # 2) Détection rapide annuaire par URL
        if any(pattern in url_lower for pattern in self.annuaire_patterns):
            return SourceClassification(
                type_source="annuaire",
                confidence=95.0,
                reasoning="URL correspond à un annuaire connu"
            )

        # 3) Presse/médias: heuristique robuste (liste + mots clés)
        # NB: on ne peut pas lister toute la presse FR -> on combine patterns + signaux.
        if self._looks_like_press_site(url_lower, title_lower):
            return SourceClassification(
                type_source="article",
                confidence=85.0,
                reasoning="Heuristique presse/média"
            )

        # Classification LLM
        prompt = self._build_classification_prompt(url, title, snippet, page_excerpt)
        
        try:
            response = self._call_groq_api(prompt, max_tokens=200)
            
            if not response:
                # Fallback : heuristique basique
                return self._heuristic_classification(url, title, snippet)
            
            # Parser la réponse JSON
            classification = self._parse_classification(response['content'])
            
            return classification
            
        except Exception as e:
            print(f"         ⚠️ Erreur classification: {e}")
            return self._heuristic_classification(url, title, snippet)
    
    def _build_classification_prompt(self, url: str, title: str, snippet: str, 
                                    page_excerpt: Optional[str]) -> str:
        """Construit le prompt de classification"""
        
        excerpt_text = f"\n\nEXTRAIT_PAGE:\n{page_excerpt[:1000]}" if page_excerpt else ""
        
        return f"""Tu dois classifier la page suivante selon sa nature.

CATEGORIES POSSIBLES:
- officiel_etablissement : site officiel d'une résidence ou structure localisée, avec ses informations propres.
- site_gestionnaire : site d'une association, fondation, réseau (ex: CCAS, Habitat & Humanisme, Ages & Vie, CetteFamille).
- annuaire : site qui liste de nombreux établissements, compare des offres, propose des formulaires de contact centralisés.
- article : article d'actualité, page d'information générale, fiche "mode d'emploi".
- autre : tout le reste.

REGLES DE DECISION:
- Si le site propose de "comparer les résidences", "demander un devis", "être rappelé" → annuaire
- Si le site présente plusieurs résidences du même réseau (Ages & Vie, Habitat & Humanisme…) → site_gestionnaire
- Si la page est centrée sur un seul lieu avec rubriques "Nos services", "Actualités", "Accès", "Tarifs" → probablement officiel_etablissement
- Si URL contient le nom d'un établissement spécifique → probablement officiel_etablissement
- Si domaine généraliste (mairie, ville-, ccas-) → peut être officiel ou gestionnaire selon contenu

DONNEES:
URL: {url}
TITRE: {title}
SNIPPET: {snippet}{excerpt_text}

Reponds UNIQUEMENT en JSON strict:
{{"type_source": "officiel_etablissement|site_gestionnaire|annuaire|article|autre", "confidence": 0-100, "reasoning": "explication courte"}}"""
    
    def _heuristic_classification(self, url: str, title: str, snippet: str) -> SourceClassification:
        """Classification heuristique de secours"""
        
        url_lower = url.lower()
        title_lower = title.lower()
        snippet_lower = snippet.lower()
        
        combined = f"{url_lower} {title_lower} {snippet_lower}"
        
        # Détection annuaire
        annuaire_keywords = [
            'comparer', 'annuaire', 'liste', 'guide', 'formulaire', 'devis', 'rappel',
            'trouvez votre', 'choisir', 'sélection de', 'tous les établissements',
            'maisons de retraite', 'résidences seniors', 'comparatif',
            'gratuit', 'sans engagement', 'conseiller'
        ]
        if any(kw in combined for kw in annuaire_keywords):
            return SourceClassification(
                type_source="annuaire",
                confidence=75.0,
                reasoning="Mots-clés d'annuaire détectés"
            )
        
        # Détection gestionnaire
        gestionnaire_keywords = ['ages & vie', 'ages et vie', 'cettefamille', 'habitat humanisme', 
                                'ccas', 'mairie', 'ville-', 'commune', 'admr', 'apf', 'udaf']
        if any(kw in combined for kw in gestionnaire_keywords):
            return SourceClassification(
                type_source="site_gestionnaire",
                confidence=65.0,
                reasoning="Gestionnaire identifié par mots-clés"
            )
        
        # Détection article
        article_keywords = ['actualité', 'article', 'publié', 'journal', 'presse', 'info']
        if any(kw in combined for kw in article_keywords):
            return SourceClassification(
                type_source="article",
                confidence=60.0,
                reasoning="Indicateurs d'article détectés"
            )
        
        # Par défaut : autre
        return SourceClassification(
            type_source="autre",
            confidence=40.0,
            reasoning="Classification incertaine"
        )

    def _looks_like_press_site(self, url_lower: str, title_lower: str) -> bool:
        """Heuristique presse/média.

        On combine:
        - liste partielle de domaines connus
        - tokens fréquents dans la presse
        - pattern d'URL d'article (date / /2025/12/.. etc.)
        """
        if any(p in url_lower for p in self.press_patterns):
            return True

        press_tokens = [
            'actualite', 'actualité', 'journal', 'hebdo', 'presse', 'reportage',
            '/actualite/', '/actualites/', '/news/', '/article/', '/articles/'
        ]
        if any(t in url_lower for t in press_tokens):
            return True

        # URLs d'articles: /YYYY/MM/DD/ ou /YYYY/MM/
        import re
        if re.search(r'/(19|20)\d{2}/\d{1,2}(/\d{1,2})?/', url_lower):
            return True

        # Titre typique média
        if any(t in title_lower for t in [' - actu', ' - journal', ' | actu', ' | journal']):
            return True

        return False
    
    def _parse_classification(self, content: str) -> SourceClassification:
        """Parse la réponse JSON de classification (robuste aux échappements)."""

        import json

        try:
            if not content:
                raise ValueError("empty content")

            # Nettoyer le contenu (```json ... ```)
            if '```json' in content:
                content = content.split('```json', 1)[1].split('```', 1)[0]
            elif '```' in content:
                content = content.split('```', 1)[1].split('```', 1)[0]

            cleaned = content.strip()

            # Fix erreurs fréquentes: backslashes non échappés
            # Exemple log: Invalid \escape ...
            cleaned = cleaned.replace('\\', '\\\\')

            data = json.loads(cleaned)

            type_source = data.get('type_source', 'autre')
            confidence = float(data.get('confidence', 0))
            reasoning = data.get('reasoning', '')

            # Sécurité: jamais retourner "officiel" si heuristique annuaire/presse aurait matché.
            # (utile si LLM se trompe)
            url_field = str(data.get('url', ''))
            merged_url = (url_field or '').lower()
            if merged_url:
                if any(p in merged_url for p in self.annuaire_patterns):
                    type_source = 'annuaire'
                if self._looks_like_press_site(merged_url, ''):
                    type_source = 'article'

            return SourceClassification(
                type_source=type_source,
                confidence=confidence,
                reasoning=reasoning
            )

        except Exception as e:
            print(f"         ⚠️ Erreur parsing classification: {e}")
            return SourceClassification(
                type_source="autre",
                confidence=0.0,
                reasoning="Erreur parsing"
            )
    
    def _call_groq_api(self, prompt: str, max_tokens: int = 200) -> Optional[Dict]:
        """Appel API Groq"""
        
        if not self.groq_api_key:
            return None
        
        # Nettoyer le prompt
        prompt = prompt.replace('\x00', '').replace('\r\n', ' ').replace('\n', ' ')
        while '  ' in prompt:
            prompt = prompt.replace('  ', ' ')
        
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "model": self.model,
            "temperature": 0.1,  # Bas pour cohérence
            "max_tokens": max_tokens
        }
        
        try:
            response = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers=headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code == 200:
                data = response.json()
                content = data["choices"][0]["message"]["content"]
                usage = data.get("usage", {})
                
                # Calcul coût
                input_tokens = usage.get("prompt_tokens", 0)
                output_tokens = usage.get("completion_tokens", 0)
                cost = (input_tokens * self.pricing["input"] + 
                       output_tokens * self.pricing["output"]) / 1_000_000
                
                self.stats['classification_cost'] += cost
                
                return {
                    "content": content,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost": cost
                }
            else:
                print(f"         ❌ Erreur Groq API: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"         ❌ Erreur appel Groq: {e}")
            return None
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques"""
        return self.stats.copy()


if __name__ == "__main__":
    """Test du module"""
    
    classifier = SourceClassifier()
    
    # Test 1: Annuaire évident
    result1 = classifier.classify_source(
        url="https://www.essentiel-autonomie.com/residence/test",
        title="Résidence Test - Essentiel Autonomie",
        snippet="Comparez les résidences seniors et demandez un devis gratuit"
    )
    print(f"Test 1: {result1.type_source} (confiance: {result1.confidence}%)")
    
    # Test 2: Site officiel probable
    result2 = classifier.classify_source(
        url="https://www.residencetest.fr/accueil",
        title="Résidence Test - Accueil",
        snippet="Bienvenue à la Résidence Test située à Troyes. Nos services, tarifs et actualités."
    )
    print(f"Test 2: {result2.type_source} (confiance: {result2.confidence}%)")
    
    # Test 3: Gestionnaire
    result3 = classifier.classify_source(
        url="https://www.agesetvie.fr/maisons-partagees/troyes",
        title="Ages & Vie - Maison partagée Troyes",
        snippet="Ages & Vie propose des colocations seniors dans toute la France"
    )
    print(f"Test 3: {result3.type_source} (confiance: {result3.confidence}%)")
    
    print(f"\nStatistiques: {classifier.get_stats()}")
