"""
Architecture MVP - Pipeline Automatisé Habitat Seniors
Structure modulaire pour 80% précision / 95% couverture
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import pandas as pd
from datetime import datetime

class SourceType(Enum):
    """Types de sources de données"""
    OFFICIAL_GOVERNMENT = "official_gov"
    PRIVATE_CHAIN = "private_chain" 
    ALTERNATIVE_SEARCH = "alternative_search"
    ENRICHMENT = "enrichment"

class ExtractionStatus(Enum):
    """Statuts d'extraction"""
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    SKIPPED = "skipped"

@dataclass
class EstablishmentData:
    """Structure de données pour un établissement"""
    # Champs obligatoires
    nom: str
    commune: str 
    departement: str
    source_type: SourceType
    source_urls: List[str]
    extraction_date: datetime
    
    # Champs optionnels avec valeurs par défaut
    code_postal: Optional[str] = None
    adresse_l1: Optional[str] = None
    telephone: Optional[str] = None
    email: Optional[str] = None
    site_web: Optional[str] = None
    gestionnaire: Optional[str] = None
    sous_categories: Optional[str] = None
    habitat_type: Optional[str] = None
    eligibilite_avp: Optional[str] = None
    presentation: Optional[str] = None
    public_cible: Optional[str] = None
    
    # Métadonnées d'extraction
    extraction_status: ExtractionStatus = ExtractionStatus.SUCCESS
    confidence_score: float = 0.0
    raw_data: Optional[Dict] = None

class BaseExtractor(ABC):
    """Classe abstraite pour tous les extracteurs"""
    
    def __init__(self, config):
        self.config = config
        
    @abstractmethod
    def extract_establishments(self, department: str) -> List[EstablishmentData]:
        """Extrait les établissements pour un département donné"""
        pass
    
    @abstractmethod  
    def get_source_type(self) -> SourceType:
        """Retourne le type de source"""
        pass

# === MODULE 1: SCRAPING OFFICIEL ===
class OfficialScraper(BaseExtractor):
    """
    Scraper pour sources officielles (70% des établissements)
    - Annuaire gouvernemental pour-les-personnes-agees.gouv.fr
    - Résidences autonomie + MARPA
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.session = None
        
    def get_source_type(self) -> SourceType:
        return SourceType.OFFICIAL_GOVERNMENT
        
    def extract_establishments(self, department: str) -> List[EstablishmentData]:
        """
        Extrait depuis l'annuaire officiel
        URL: https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie/{dept}
        """
        establishments = []
        
        # 1. Scraper la page principale du département
        main_url = self._get_department_url(department)
        establishment_links = self._extract_establishment_links(main_url)
        
        # 2. Pour chaque établissement, scraper sa fiche détaillée
        for link in establishment_links:
            try:
                establishment = self._extract_establishment_details(link)
                if establishment:
                    establishments.append(establishment)
            except Exception as e:
                print(f"Erreur extraction {link}: {e}")
                continue
                
        return establishments
    
    def _get_department_url(self, department: str) -> str:
        """Construit l'URL de l'annuaire pour le département"""
        dept_names = {
            '47': 'lot-et-garonne-47',
            '10': 'aube-10'
        }
        dept_slug = dept_names.get(department, f'dept-{department}')
        return f"https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie/{dept_slug}"
    
    def _extract_establishment_links(self, url: str) -> List[str]:
        """Extrait les liens vers les fiches établissements"""
        # Implémentation du scraping BeautifulSoup
        pass
    
    def _extract_establishment_details(self, url: str) -> Optional[EstablishmentData]:
        """Extrait les détails d'un établissement depuis sa fiche"""
        # Implémentation de l'extraction des données structurées
        pass

# === MODULE 2: CHAÎNES PRIVÉES ===
class PrivateChainScraper(BaseExtractor):
    """
    Scraper pour chaînes privées (15% des établissements)
    - DOMITYS, Espace & Vie, Senioriales, Happy Senior, API Résidence
    """
    
    def __init__(self, config):
        super().__init__(config)
        self.chains_config = config.PRIVATE_CHAINS
        
    def get_source_type(self) -> SourceType:
        return SourceType.PRIVATE_CHAIN
        
    def extract_establishments(self, department: str) -> List[EstablishmentData]:
        """Extrait depuis les sites des chaînes privées"""
        establishments = []
        
        for chain_name, chain_config in self.chains_config.items():
            try:
                chain_establishments = self._extract_from_chain(chain_name, chain_config, department)
                establishments.extend(chain_establishments)
            except Exception as e:
                print(f"Erreur chaîne {chain_name}: {e}")
                continue
                
        return establishments
    
    def _extract_from_chain(self, chain_name: str, chain_config: Dict, department: str) -> List[EstablishmentData]:
        """Extrait les établissements d'une chaîne spécifique"""
        # Implémentation spécialisée par chaîne
        pass

# === MODULE 3: RECHERCHE ALTERNATIVE ===
class AlternativeSearchExtractor(BaseExtractor):
    """
    Recherche pour structures alternatives (15% des établissements)
    - Habitat inclusif, intergénérationnel, accueil familial
    - Utilise Serper.dev pour recherche web ciblée
    """
    
    def __init__(self, config, ai_extractor):
        super().__init__(config)
        self.ai_extractor = ai_extractor
        self.search_api = None  # Sera initialisé avec Serper
        
    def get_source_type(self) -> SourceType:
        return SourceType.ALTERNATIVE_SEARCH
        
    def extract_establishments(self, department: str) -> List[EstablishmentData]:
        """
        Recherche structures alternatives via search API + extraction IA
        """
        establishments = []
        
        # 1. Recherche par type d'habitat
        for habitat_type, queries in self.config.SEARCH_QUERIES.items():
            search_results = self._search_establishments(queries, department)
            
            # 2. Extraction IA depuis les pages trouvées  
            for result in search_results:
                try:
                    establishment = self._extract_with_ai(result, habitat_type)
                    if establishment:
                        establishments.append(establishment)
                except Exception as e:
                    print(f"Erreur extraction IA {result['url']}: {e}")
                    continue
                    
        return establishments
    
    def _search_establishments(self, queries: List[str], department: str) -> List[Dict]:
        """Recherche web via Serper.dev"""
        # Implémentation recherche Serper
        pass
        
    def _extract_with_ai(self, search_result: Dict, habitat_type: str) -> Optional[EstablishmentData]:
        """Extraction des données via IA depuis contenu web"""
        # Utilise AIExtractor pour parser le contenu
        pass

# === MODULE 4: EXTRACTION IA ===
class AIExtractor:
    """
    Module d'extraction via IA (Groq/OpenAI)
    - Parsing intelligent de contenus web
    - Normalisation des données
    - Classification des types d'habitat
    """
    
    def __init__(self, config):
        self.config = config
        self.ai_client = None  # Sera initialisé avec Groq ou OpenAI
        
    def extract_from_content(self, content: str, source_url: str, context: str = "") -> Optional[EstablishmentData]:
        """
        Extrait les données d'établissement depuis un contenu web
        """
        prompt = self._build_extraction_prompt(content, context)
        
        try:
            response = self._call_ai_api(prompt)
            return self._parse_ai_response(response, source_url)
        except Exception as e:
            print(f"Erreur extraction IA: {e}")
            return None
            
    def _build_extraction_prompt(self, content: str, context: str) -> str:
        """Construit le prompt d'extraction optimisé"""
        return f"""
        OBJECTIF: Extraire les informations d'un établissement senior depuis le contenu web fourni.
        
        CONTEXTE: {context}
        
        CONTENU À ANALYSER:
        {content[:3000]}  # Limite pour éviter les coûts
        
        EXTRAIRE EN JSON:
        {{
            "nom": "nom complet de l'établissement",
            "commune": "ville sans code postal", 
            "code_postal": "5 chiffres",
            "adresse_l1": "numéro et rue",
            "telephone": "numéro principal",
            "email": "email public si disponible",
            "site_web": "URL officielle",
            "gestionnaire": "organisme gestionnaire",
            "sous_categories": "type exact selon liste fournie",
            "presentation": "description courte 200-300 chars",
            "public_cible": "personnes_agees|personnes_handicapees|mixtes",
            "confidence": "score 0-1"
        }}
        
        RÈGLES:
        - Si une info n'est pas trouvée: null
        - Pas d'invention de données
        - sous_categories parmi: "Résidence autonomie", "MARPA", "Résidence services seniors", "Habitat inclusif", "Accueil familial"
        """
        
    def _call_ai_api(self, prompt: str) -> str:
        """Appelle l'API IA (Groq prioritaire pour le coût)"""
        # Implémentation des appels Groq/OpenAI
        pass
        
    def _parse_ai_response(self, response: str, source_url: str) -> Optional[EstablishmentData]:
        """Parse la réponse IA en EstablishmentData"""
        # Implémentation parsing JSON + validation
        pass

# === MODULE 5: PIPELINE & ORCHESTRATION ===
class HabitatPipeline:
    """
    Pipeline principal orchestrant tous les extracteurs
    - Coordination des modules
    - Déduplication 
    - Normalisation finale
    - Export CSV
    """
    
    def __init__(self, config):
        self.config = config
        
        # Initialisation des modules
        self.ai_extractor = AIExtractor(config)
        self.official_scraper = OfficialScraper(config)
        self.private_scraper = PrivateChainScraper(config)
        self.alternative_scraper = AlternativeSearchExtractor(config, self.ai_extractor)
        
    def process_department(self, department: str) -> pd.DataFrame:
        """
        Process complet d'un département
        Retourne DataFrame prêt à exporter
        """
        print(f"\n🏃 Traitement département {department}")
        all_establishments = []
        
        # Phase 1: Sources officielles (priorité max)
        print("📊 Phase 1: Sources officielles...")
        official_data = self.official_scraper.extract_establishments(department)
        all_establishments.extend(official_data)
        print(f"   ✅ {len(official_data)} établissements trouvés")
        
        # Phase 2: Chaînes privées  
        print("🏢 Phase 2: Chaînes privées...")
        private_data = self.private_scraper.extract_establishments(department)
        all_establishments.extend(private_data)
        print(f"   ✅ {len(private_data)} établissements trouvés")
        
        # Phase 3: Recherche alternative
        print("🔍 Phase 3: Structures alternatives...")
        alternative_data = self.alternative_scraper.extract_establishments(department)
        all_establishments.extend(alternative_data)
        print(f"   ✅ {len(alternative_data)} établissements trouvés")
        
        # Phase 4: Déduplication et normalisation
        print("🧹 Phase 4: Déduplication et normalisation...")
        clean_data = self._deduplicate_and_normalize(all_establishments)
        print(f"   ✅ {len(clean_data)} établissements après nettoyage")
        
        # Phase 5: Export DataFrame
        df = self._to_dataframe(clean_data, department)
        
        print(f"🎯 Résultat: {len(df)} établissements pour {department}")
        return df
        
    def _deduplicate_and_normalize(self, establishments: List[EstablishmentData]) -> List[EstablishmentData]:
        """Supprime doublons et normalise les données"""
        # Implémentation déduplication par nom+commune
        # + normalisation types selon mapping config
        pass
        
    def _to_dataframe(self, establishments: List[EstablishmentData], department: str) -> pd.DataFrame:
        """Convertit en DataFrame avec schéma final"""
        # Implémentation conversion + ajout métadonnées
        pass
        
    def export_csv(self, df: pd.DataFrame, department: str) -> str:
        """Exporte le DataFrame en CSV"""
        filename = f"habitat_seniors_{department}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        filepath = f"data/output/{filename}"
        df.to_csv(filepath, index=False, encoding='utf-8')
        return filepath

# === POINT D'ENTRÉE ===
def run_mvp(departments: List[str] = None):
    """
    Point d'entrée principal du MVP
    """
    from config_mvp import mvp_config, validate_config
    
    # Validation configuration
    config_status = validate_config()
    if not all(config_status.values()):
        print("❌ Configuration incomplète:")
        for check, status in config_status.items():
            if not status:
                print(f"   - {check}")
        return
    
    # Initialisation pipeline
    pipeline = HabitatPipeline(mvp_config)
    
    # Traitement des départements
    target_departments = departments or mvp_config.departments
    results = {}
    
    for dept in target_departments:
        try:
            df = pipeline.process_department(dept)
            filepath = pipeline.export_csv(df, dept)
            results[dept] = {
                'success': True,
                'count': len(df),
                'file': filepath
            }
        except Exception as e:
            results[dept] = {
                'success': False, 
                'error': str(e)
            }
            print(f"❌ Erreur département {dept}: {e}")
    
    # Rapport final
    print("\n📊 RAPPORT FINAL:")
    for dept, result in results.items():
        if result['success']:
            print(f"✅ {dept}: {result['count']} établissements → {result['file']}")
        else:
            print(f"❌ {dept}: {result['error']}")

if __name__ == "__main__":
    # Test avec départements du MVP
    run_mvp(['47', '10'])