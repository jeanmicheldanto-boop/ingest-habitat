"""
AlternativeSearchScraper - Module 3 pour capturer toutes les autres catégories
Cible : 25% des établissements (habitat inclusif, intergénérationnel, accueil familial, etc.)
Stratégie hybride : Recherche ciblée (50%) + Institutionnelle (35%) + Générique (15%)
"""

import requests
import time
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Set
from dataclasses import dataclass, asdict
from datetime import datetime
import pandas as pd
import sys
import os

# Configuration
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config_mvp import scraping_config, ai_config

@dataclass
class SearchStrategy:
    """Configuration d'une stratégie de recherche"""
    name: str
    queries: List[str]
    target_categories: List[str]
    weight: float
    validation_level: str  # 'strict', 'medium', 'flexible'

@dataclass 
class EstablishmentCandidate:
    """Candidat établissement avec métadonnées"""
    nom: str
    url: str
    snippet: str
    commune: str
    departement: str
    confidence_score: float = 0.0
    validation_status: str = "pending"
    page_content: str = ""  # NOUVEAU: Contenu ScrapingBee
    validation_status: str = "pending"  # pending, validated, rejected

class AlternativeSearchScraper:
    """
    Scraper pour habitat inclusif, intergénérationnel et autres catégories non-officielles
    """
    
    def __init__(self, rate_limit_delay: float = 2.0):
        self.rate_limit_delay = rate_limit_delay
        self.serper_api_key = scraping_config.serper_api_key
        self.scrapingbee_api_key = scraping_config.scrapingbee_api_key
        
        # Stratégies de recherche
        self.strategies = self._initialize_search_strategies()
        
        # Patterns de validation
        self._initialize_validation_patterns()
        
        # Cache pour éviter les doublons
        self.processed_urls = set()
        self.found_establishments = []

    def _initialize_search_strategies(self) -> List[SearchStrategy]:
        """Initialise les 3 stratégies de recherche"""
        
        strategies = []
        
        # Stratégie A : Réseaux Spécifiques Optimisés (30%) - 8 résultats
        strategies.append(SearchStrategy(
            name="réseaux_spécifiques",
            queries=[
                # Groupement optimisé avec OR - 8 résultats chacune
                "habitat inclusif {department} (APEI OR ADMR OR APF OR UDAF OR \"Habitat & Humanisme\")",
                "habitat partagé {department} (\"Ages & Vie\" OR CetteFamille OR Gurekin)"
            ],
            target_categories=["Habitat inclusif", "Accueil familial"],
            weight=0.3,
            validation_level="medium"
        ))
        
        # Stratégie B : Types d'Habitat Groupés (50%) - 10 résultats
        strategies.append(SearchStrategy(
            name="types_habitat_groupés",
            queries=[
                # Groupement par types d'habitat - 10 résultats chacune
                "(\"habitat inclusif\" OR \"habitat partagé\" OR \"colocation seniors\") {department}",
                "(béguinage OR \"maison partagée\" OR \"logement intergénérationnel\") {department}"
            ],
            target_categories=["Habitat inclusif", "Habitat intergénérationnel"],
            weight=0.5,
            validation_level="flexible"
        ))
        
        # Stratégie C : Recherche Institutionnelle Légère (20%)
        strategies.append(SearchStrategy(
            name="institutionnelle_légère",
            queries=[
                # PDF officiel départemental - PRIORITÉ
                "liste habitat inclusif {department} filetype:pdf",
                "commune {department} habitat inclusif filetype:pdf",
                "service accueil familial départemental {department}",
            ],
            target_categories=["Opérateur institutionnel"],
            weight=0.2,
            validation_level="strict"
        ))
        
        return strategies

    def _initialize_validation_patterns(self):
        """Initialise les patterns de validation IA"""
        
        # Mots-clés positifs obligatoires (au moins 1)
        self.positive_keywords = [
            # Personnes âgées et aliases
            "senior", "sénior", "âgé", "âgée", "personnes âgées", "3ème âge", 
            "troisième âge", "retraité", "aîné", "ainés", "vieillissement",
            
            # Personnes handicapées et aliases  
            "handicap", "handicapé", "handicapée", "en situation de handicap",
            "pmr", "mobilité réduite", "autisme", "autiste", "déficient",
            "malvoyant", "malentendant", "polyhandicap", "trisomie",
            
            # Concepts généraux
            "autonomie", "dépendance", "intergénérationnel", "solidaire", 
            "inclusif", "famille d'accueil", "colocation", "habitat partagé"
        ]
        
        # Mots-clés négatifs (exclusion si seuls présents)
        self.negative_keywords_exclusive = [
            "étudiant seulement", "jeune exclusivement", "famille nucléaire",
            "professionnel", "entreprise", "bureau", "commercial uniquement",
            # Ajout exclusions foyers spécialisés non-inclusifs
            "foyer travailleurs", "foyer d'hébergement", "chrs", "cada", 
            "centre hébergement", "foyer jeunes travailleurs", "fjt",
            "réinsertion professionnelle", "formation professionnelle"
        ]
        
        # Exception : intergénérationnel autorise "jeune" si + "âgé"
        self.intergenerational_patterns = [
            "intergénérationnel", "solidaire", "mixte générations", "lien social",
            "entre générations", "toutes générations"
        ]
        
        # Domaines à exclure
        self.excluded_domains = [
            "linkedin.com", "facebook.com", "twitter.com", "instagram.com",
            "leboncoin.fr", "seloger.com", "pap.fr", "lacentrale.fr",
            "youtube.com", "tousbenevoles.org", "jeveuxaider.gouv.fr",
            # Ajout exclusions administratives évidentes
            "service-public.fr", "caf.fr", "ameli.fr", "pole-emploi.fr"
        ]
        
        # Termes à exclure (établissements déjà couverts par Modules 1&2)
        self.excluded_establishment_types = [
            "ehpad", "maison de retraite", "résidence autonomie", "marpa",
            "résidence services seniors", "domitys", "senioriales", "espace & vie",
            "api résidence", "happy senior"
        ]
        
        # Filtres géographiques
        self.out_of_area_indicators = [
            "haute-garonne", "toulouse", "gers", "condom", "tarn", "albi",
            "gironde", "bordeaux", "pyrénées", "ariège"
        ]

    def search_establishments(self, department: str, department_num: str) -> List[EstablishmentCandidate]:
        """
        Lance la recherche multi-stratégie pour un département
        """
        print(f"🔍 Recherche alternative département {department} ({department_num})")
        
        all_candidates = []
        
        # Exécuter chaque stratégie
        for strategy in self.strategies:
            print(f"\n📈 Stratégie {strategy.name} (poids: {strategy.weight:.1%})")
            
            strategy_candidates = self._execute_strategy(
                strategy, 
                department, 
                department_num
            )
            
            # Ajouter métadonnées stratégie
            for candidate in strategy_candidates:
                # candidate.source_query supprimé - info dans departement
                candidate.confidence_score = strategy.weight
            
            all_candidates.extend(strategy_candidates)
            
            print(f"   📊 {len(strategy_candidates)} candidats trouvés")
            
            # Rate limiting entre stratégies
            time.sleep(self.rate_limit_delay)
        
        # Déduplication et validation
        unique_candidates = self._deduplicate_candidates(all_candidates)
        validated_candidates = self._validate_candidates(unique_candidates)
        
        print(f"\n✅ {len(validated_candidates)} établissements validés sur {len(all_candidates)} candidats")
        
        return validated_candidates

    def _execute_strategy(self, strategy: SearchStrategy, department: str, department_num: str) -> List[EstablishmentCandidate]:
        """Exécute une stratégie de recherche"""
        
        candidates = []
        
        for query_template in strategy.queries:
            # Personnaliser la requête - plus simple, sans hardcoding
            query = query_template.format(
                department=department,
                department_num=department_num
            )
            
            print(f"   🔍 Requête: {query}")
            
            try:
                # Recherche via Serper
                search_results = self._search_with_serper(query)
                
                # Extraire candidats
                query_candidates = self._extract_candidates_from_results(search_results, query)
                candidates.extend(query_candidates)
                
                print(f"      📋 {len(query_candidates)} résultats")
                
                # Rate limiting entre requêtes
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                print(f"      ❌ Erreur requête: {e}")
                continue
        
        return candidates

    def _search_with_serper(self, query: str) -> Dict:
        """Recherche via Serper.dev"""
        
        url = "https://google.serper.dev/search"
        
        payload = {
            "q": query,
            "gl": "fr",  # France
            "hl": "fr",  # Français
            "lr": "lang_fr",  # Force résultats francophones
            # Stratégie optimisée : num adapté aux nouvelles requêtes groupées
            "num": (8 if any(terme in query.lower() for terme in [
                        "habitat inclusif", "habitat partagé"]) and any(org in query.lower() for org in [
                        "apei", "admr", "apf", "udaf", "habitat & humanisme", "ages & vie", "cettefamille", "gurekin"
                    ])  # Stratégie A : 8 résultats
                   else 10 if any(terme in query.lower() for terme in [
                        "habitat inclusif", "habitat partagé", "colocation seniors", 
                        "béguinage", "maison partagée", "logement intergénérationnel"
                    ]) and " OR " in query  # Stratégie B : 10 résultats
                   else 4)  # Défaut pour autres requêtes
        }
        
        headers = {
            "X-API-KEY": self.serper_api_key,
            "Content-Type": "application/json"
        }
        
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        
        return response.json()

    def _extract_candidates_from_results(self, search_results: Dict, query: str) -> List[EstablishmentCandidate]:
        """Extrait les candidats depuis les résultats Serper"""
        
        candidates = []
        
        # Résultats organiques
        organic_results = search_results.get("organic", [])
        
        for result in organic_results:
            title = result.get("title", "")
            link = result.get("link", "")
            snippet = result.get("snippet", "")
            
            # Filtrage préliminaire
            if self._should_skip_result(link, title, snippet):
                continue
            
            # Extraire commune si possible du snippet
            commune = self._extract_commune_from_snippet(snippet)
            
            candidate = EstablishmentCandidate(
                nom=title,
                url=link,
                snippet=snippet,
                commune=commune,
                departement=self.department_name if hasattr(self, 'department_name') else ""
            )
            
            candidates.append(candidate)
        
        return candidates

    def _should_skip_result(self, url: str, title: str, snippet: str) -> bool:
        """Vérifie si un résultat doit être ignoré"""
        
        # Domaines exclus
        for domain in self.excluded_domains:
            if domain in url.lower():
                return True
        
        # URLs déjà traitées
        if url in self.processed_urls:
            return True
        
        # Contenu manifestement hors-scope
        text_to_check = (title + " " + snippet).lower()
        
        # Exclusions génériques
        excluded_patterns = [
            "emploi", "recrutement", "formation", "actualité", "article",
            "vente", "achat", "immobilier commercial", "bénévolat",
            "appel à candidature", "projet futur", "en construction"
        ]
        
        if any(pattern in text_to_check for pattern in excluded_patterns):
            return True
        
        # Exclusions géographiques (hors département)
        if any(indicator in text_to_check for indicator in self.out_of_area_indicators):
            return True
        
        # Exclusions établissements déjà couverts
        if any(etab_type in text_to_check for etab_type in self.excluded_establishment_types):
            return True
        
        return False

    def _extract_commune_from_snippet(self, snippet: str) -> str:
        """Extrait la commune depuis le snippet si possible"""
        
        # Patterns courants pour les communes
        commune_patterns = [
            r'à ([A-Z][a-z-]+(?:\s+[A-Z][a-z-]+)*)',  # "à Marmande"
            r'([A-Z][a-z-]+(?:\s+[A-Z][a-z-]+)*)\s+\(\d{5}\)',  # "Agen (47000)"
            r'(\d{5})\s+([A-Z][a-z-]+(?:\s+[A-Z][a-z-]+)*)',  # "47000 Agen"
        ]
        
        for pattern in commune_patterns:
            matches = re.findall(pattern, snippet)
            if matches:
                if isinstance(matches[0], tuple):
                    return matches[0][-1]  # Dernier élément du tuple
                else:
                    return matches[0]
        
        return ""

    def _deduplicate_candidates(self, candidates: List[EstablishmentCandidate]) -> List[EstablishmentCandidate]:
        """Élimine les doublons"""
        
        unique_candidates = []
        seen_urls = set()
        seen_names = set()
        
        for candidate in candidates:
            # Déduplication par URL
            if candidate.url in seen_urls:
                continue
            
            # Déduplication par nom (approximative)
            name_normalized = re.sub(r'[^\w\s]', '', candidate.nom.lower()).strip()
            if name_normalized in seen_names:
                continue
            
            seen_urls.add(candidate.url)
            seen_names.add(name_normalized)
            unique_candidates.append(candidate)
        
        return unique_candidates

    def _validate_candidates(self, candidates: List[EstablishmentCandidate]) -> List[EstablishmentCandidate]:
        """Valide les candidats via IA"""
        
        validated = []
        
        for i, candidate in enumerate(candidates, 1):
            print(f"   🔍 Validation {i}/{len(candidates)}: {candidate.nom}")
            
            try:
                # PRÉ-FILTRE LÉGER : Exclure seulement les documents très lourds
                text_check = f"{candidate.nom} {candidate.snippet}".lower()
                heavy_docs = ["rapport d'activité", "rapport d'activités", "schéma", "carte", "cartographie"]
                
                if any(doc in text_check for doc in heavy_docs):
                    candidate.validation_status = "rejected"
                    print(f"      ❌ Rejeté: Document lourd détecté")
                    continue
                
                # Récupérer le contenu de la page
                page_content = self._get_page_content(candidate.url)
                if not page_content:
                    candidate.validation_status = "rejected"
                    continue
                
                # NOUVEAU: Sauvegarder le contenu pour transmission au module 4
                candidate.page_content = page_content
                
                # Validation IA
                validation_result = self._validate_with_ai(candidate, page_content)
                
                if validation_result["is_valid"]:
                    candidate.validation_status = "validated"
                    candidate.confidence_score = validation_result["confidence"]
                    
                    # Enrichir les données
                    candidate.commune = validation_result.get("commune", candidate.commune)
                    candidate.departement = validation_result.get("departement", "")
                    
                    validated.append(candidate)
                    print(f"      ✅ Validé ({validation_result['confidence']:.1%})")
                else:
                    candidate.validation_status = "rejected"
                    print(f"      ❌ Rejeté: {validation_result.get('reason', 'Hors scope')}")
                
            except Exception as e:
                print(f"      ❌ Erreur validation: {e}")
                candidate.validation_status = "rejected"
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
        
        return validated

    def _get_page_content(self, url: str) -> Optional[str]:
        """Récupère le contenu d'une page via ScrapingBee"""
        
        try:
            response = requests.get(
                'https://app.scrapingbee.com/api/v1/',
                params={
                    'api_key': self.scrapingbee_api_key,
                    'url': url,
                    'render_js': 'false',  # Plus rapide pour validation
                    'wait': '1000'
                },
                timeout=20  # Timeout augmenté pour éviter blocages
            )
            
            if response.status_code == 200:
                # Limiter à 3000 caractères pour accélérer validation
                return response.text[:3000]
            
        except Exception as e:
            print(f"Erreur ScrapingBee {url}: {e}")
        
        return None
    
    def _quick_exclusion_check(self, candidate: EstablishmentCandidate) -> bool:
        """
        Exclusions rapides sans appel ScrapingBee pour éviter lenteur
        """
        text = f"{candidate.nom} {candidate.snippet} {candidate.url}".lower()
        
        # Exclusions AMI et appels à projets
        ami_exclusions = [
            "appel à projet", "appel à manifestation",
            "candidature", "appel à candidature"
        ]
        
        # Exclusions résidences autonomie (déjà Module 1)
        module1_exclusions = [
            "résidence autonomie", "résidences autonomie",
            "logement-foyer", "foyer logement",
            "pour-les-personnes-agees.gouv.fr/annuaire-residences"
        ]
        
        # Exclusions documents administratifs longs
        long_docs_exclusions = [
            "rapport d'activité", "rapport annuel", "bilan d'activité",
            "document de travail", "note de synthèse"
        ]
        
        all_exclusions = ami_exclusions + module1_exclusions + long_docs_exclusions
        
        return any(exclusion in text for exclusion in all_exclusions)

    def _validate_with_ai(self, candidate: EstablishmentCandidate, page_content: str) -> Dict:
        """Validation désactivée - Tous les candidats sont acceptés pour traitement LLM final"""
        
        # VALIDATION DÉSACTIVÉE - Tous les candidats passent au LLM final
        return {
            "is_valid": True,
            "confidence": 0.8,
            "reason": "Validation désactivée - passage vers LLM final"
        }


if __name__ == "__main__":
    import sys
    import argparse
    
    # Interface CLI pour industrialisation
    parser = argparse.ArgumentParser(description='Alternative Search Scraper - Module 3')
    parser.add_argument('--dept-code', '-d', type=str, help='Code département (ex: 10, 47)')
    parser.add_argument('--dept-name', '-n', type=str, help='Nom département (ex: Aube, Lot-et-Garonne)')
    
    args = parser.parse_args()
    
    # Mapping départements courants
    dept_info = {
        '10': 'Aube', 
        '47': 'Lot-et-Garonne'
    }
    
    # Déterminer département à traiter
    if args.dept_code:
        dept_code = args.dept_code
        dept_name = args.dept_name or dept_info.get(dept_code, f"Département {dept_code}")
    else:
        # Fallback: premier département de config
        from config_mvp import mvp_config
        dept_code = mvp_config.departments[0]
        dept_name = dept_info.get(dept_code, f"Département {dept_code}")
    
    print(f"📍 Lancement scraping: {dept_name} ({dept_code})")
    
    scraper = AlternativeSearchScraper()
    candidates = scraper.search_establishments(dept_name, dept_code)
    
    print(f"\n📊 Résultats finaux: {len(candidates)} établissements")
    
    for candidate in candidates:
        print(f"  - {candidate.nom} ({candidate.commune})")
        print(f"    URL: {candidate.url}")
        print(f"    Confiance: {candidate.confidence_score:.1f}%")