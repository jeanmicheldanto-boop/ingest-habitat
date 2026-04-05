"""
Configuration MVP - Automatisation Habitat Seniors
Optimisée pour 80% précision / 95% couverture
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv(override=True)

@dataclass
class ScrapingConfig:
    """Configuration pour le scraping web"""
    scrapingbee_api_key: str = os.getenv('SCRAPINGBEE_API_KEY', '')
    serper_api_key: str = os.getenv('SERPER_API_KEY', '')
    timeout: int = int(os.getenv('SCRAPING_TIMEOUT', '15'))
    max_retries: int = int(os.getenv('MAX_RETRIES', '3'))
    rate_limit_delay: float = float(os.getenv('RATE_LIMIT_DELAY', '1.0'))
    
    # Headers pour éviter la détection
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }

@dataclass  
class AIConfig:
    """Configuration pour l'IA d'extraction"""
    openai_api_key: str = os.getenv('OPENAI_API_KEY', '')
    openai_model: str = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
    groq_api_key: str = os.getenv('GROQ_API_KEY', '')
    groq_model: str = os.getenv('GROQ_MODEL', 'llama-3.1-70b-versatile')
    
    # Prompts optimisés pour précision
    extraction_temperature: float = 0.1
    max_tokens: int = 1500

@dataclass
class MVPConfig:
    """Configuration spécifique au MVP"""
    target_precision: int = int(os.getenv('TARGET_PRECISION', '80'))
    target_coverage: int = int(os.getenv('TARGET_COVERAGE', '95'))
    departments: List[str] = field(default_factory=lambda: os.getenv('DEPARTMENTS', '10,47').split(','))
    
    # Schéma CSV de sortie
    output_schema = [
        'nom', 'commune', 'code_postal', 'gestionnaire', 'adresse_l1',
        'telephone', 'email', 'site_web', 'sous_categories', 'habitat_type',
        'eligibilite_avp', 'presentation', 'departement', 'source', 
        'date_extraction', 'public_cible'
    ]

# URLs des sources officielles par département
OFFICIAL_SOURCES = {
    '47': {  # Lot-et-Garonne
        'residences_autonomie': 'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie/lot-et-garonne-47',
        'conseil_departemental': 'https://www.lotetgaronne.fr/solidarites/personnes-agees/',
        'pattern_communes': 'https://www.{commune}.fr',
        'ccas_pattern': 'https://www.{commune}.fr/ccas'
    },
    '10': {  # Aube  
        'residences_autonomie': 'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie/aube-10',
        'conseil_departemental': 'https://www.aube.fr/solidarite-sociale/seniors/',
        'pattern_communes': 'https://www.{commune}.fr',
        'ccas_pattern': 'https://www.{commune}.fr/ccas'
    }
}

# Chaînes privées nationales (résidences services seniors)
PRIVATE_CHAINS = {
    'domitys': {
        'base_url': 'https://www.domitys.fr',
        'search_pattern': '/residence-services-senior/{region}/{departement}/',
        'data_source': 'domitys_api'
    },
    'espaceetvie': {
        'base_url': 'https://www.residences-espaceetvie.fr', 
        'search_pattern': '/residences/residence-services-seniors-{departement}/',
        'data_source': 'espaceetvie_website'
    },
    'senioriales': {
        'base_url': 'https://www.senioriales.com',
        'search_pattern': '/residences/{departement}',
        'data_source': 'senioriales_locator'
    },
    'happysenior': {
        'base_url': 'https://residenceshappysenior.fr',
        'search_pattern': '/residence/{ville}',
        'data_source': 'happysenior_directory'
    },
    'api_residence': {
        'base_url': 'https://www.api-residence.fr',
        'search_pattern': '/residence-services-seniors-{ville}/',
        'data_source': 'api_residence_listings'
    }
}

# Sources spécialisées habitat inclusif/alternatif
ALTERNATIVE_SOURCES = {
    'ages_et_vie': {
        'base_url': 'https://www.agesetvie.com',
        'search_pattern': '/maisons/{region}',
        'type': 'habitat_inclusif'
    },
    'cette_famille': {
        'base_url': 'https://www.cettefamille.com',
        'search_pattern': '/fr/agences/',
        'type': 'accueil_familial'
    },
    'marpa_national': {
        'base_url': 'https://www.marpa.fr',
        'search_pattern': '/residences-retraite/departement-{dept_num}/',
        'type': 'marpa'
    },
    'udaf_network': {
        'base_url': 'https://www.unaf.fr',
        'search_pattern': '/nos-actions/habitat-inclusif',
        'type': 'habitat_inclusif_udaf'
    }
}

# Règles de normalisation et mapping
NORMALIZATION_RULES = {
    'sous_categories_mapping': {
        'résidence autonomie': 'Résidence autonomie',
        'foyer logement': 'Résidence autonomie', 
        'foyer-logement': 'Résidence autonomie',
        'résidence services seniors': 'Résidence services seniors',
        'MARPA (Résidence autonomie)': 'MARPA',
        'habitat inclusif (habitat partagé)': 'Habitat inclusif',
        'accueil familial / réseau habitat': 'Accueil familial',
        'opérateur / association (habitat inclusif)': 'Opérateur habitat inclusif',
        'institution – point d\'entrée projets': 'Institution publique'
    },
    'habitat_type_mapping': {
        'Résidence autonomie': 'residence',
        'MARPA': 'residence', 
        'Résidence services seniors': 'residence',
        'Habitat inclusif': 'habitat_partage',
        'Accueil familial': 'habitat_partage',
        'Opérateur habitat inclusif': 'habitat_partage',
        'Institution publique': 'habitat_partage',
        'Béguinage': 'logement_independant',
        'Village seniors': 'logement_independant'
    },
    'departement_names': {
        '47': 'Lot-et-Garonne (47)',
        '10': 'Aube (10)'
    }
}

# Configuration recherche web pour structures alternatives
SEARCH_QUERIES = {
    'habitat_inclusif': [
        'habitat inclusif seniors {departement}',
        'habitat partagé personnes âgées {ville}',
        'logement accompagné seniors {departement}',
        'aide vie partagée AVP {departement}'
    ],
    'accueil_familial': [
        'accueil familial seniors {departement}',
        'famille accueil personnes âgées {ville}',
        'placement familial seniors {departement}'
    ],
    'intergenerationnel': [
        'habitat intergénérationnel {departement}',
        'logement intergénérationnel seniors {ville}',
        'cohabitation intergénérationnelle {departement}'
    ],
    'village_seniors': [
        'village seniors {departement}',
        'village retraités {ville}',
        'cité seniors {departement}'
    ]
}

# Patterns de validation des données extraites
VALIDATION_PATTERNS = {
    'telephone': r'^(?:(?:\+|00)33|0)\s*[1-9](?:[\s.-]*\d{2}){4}$',
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'code_postal': r'^\d{5}$',
    'url': r'^https?://(?:[-\w.])+(?:\:[0-9]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?)?$',
    'nom_etablissement': r'^[A-Za-zÀ-ÿ0-9\s\-\'\"()&.]{3,100}$'
}

# Configuration des logs et debug
DEBUG_CONFIG = {
    'log_level': 'INFO',
    'log_file': 'logs/mvp_scraping.log',
    'save_raw_data': True,
    'raw_data_folder': 'data/temp/raw_extractions',
    'save_failed_extractions': True
}

# Instances de configuration
scraping_config = ScrapingConfig()
ai_config = AIConfig() 
mvp_config = MVPConfig()

# Fonction de validation de la configuration
def validate_config() -> Dict[str, bool]:
    """Valide que toutes les API keys nécessaires sont présentes"""
    checks = {
        'scrapingbee_key': bool(scraping_config.scrapingbee_api_key),
        'serper_key': bool(scraping_config.serper_api_key),
        'ai_key': bool(ai_config.openai_api_key or ai_config.groq_api_key),
        'departments_set': bool(mvp_config.departments)
    }
    return checks

if __name__ == "__main__":
    # Test de la configuration
    validation = validate_config()
    print("=== Configuration MVP ===")
    for check, status in validation.items():
        print(f"{check}: {'✅ OK' if status else '❌ MANQUANT'}")
    
    print(f"\nDépartements cibles: {mvp_config.departments}")
    print(f"Objectifs: {mvp_config.target_precision}% précision, {mvp_config.target_coverage}% couverture")