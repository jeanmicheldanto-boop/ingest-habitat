import os
from dotenv import load_dotenv

# Charger le fichier .env
load_dotenv(override=True)

# Configuration base de données
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'habitat_db'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'port': int(os.getenv('DB_PORT', 5432))
}

# Configuration APIs
GEOCODING_CONFIG = {
    'service': os.getenv('GEOCODING_SERVICE', 'nominatim'),  # nominatim, google
    'google_api_key': os.getenv('GOOGLE_MAPS_API_KEY', ''),
    'timeout': 10
}

WEB_ENRICHMENT_CONFIG = {
    'timeout': 15,
    'max_retries': 3,
    'user_agent': 'HabitatIngestionBot/1.0',
    'ollama_url': 'http://localhost:11434',
    'ollama_model': 'phi3:mini',  # Modèle léger pour T495s
    'use_ollama': True,
    # API externes gratuites (optionnel)
    'huggingface_token': '',  # Token gratuit Hugging Face
    'openai_compatible_url': 'https://api.groq.com/openai/v1/chat/completions',  # Groq gratuit
    'openai_compatible_key': '',  # Votre clé API gratuite
    'openai_compatible_model': 'llama2-70b-4096'
}

# Configuration application
APP_CONFIG = {
    'upload_folder': 'data/uploads',
    'temp_folder': 'data/temp',
    'images_folder': 'data/images',
    'max_file_size': 50 * 1024 * 1024,  # 50MB
    'allowed_extensions': {
        'csv': ['.csv'],
        'excel': ['.xlsx', '.xls'],
        'images': ['.jpg', '.jpeg', '.png', '.webp']
    }
}

# Mapping des colonnes CSV vers la base
COLUMN_MAPPING = {
    # Colonnes principales établissement
    'nom': ['nom', 'name', 'établissement', 'etablissement'],
    'presentation': ['presentation', 'description', 'présentation'],
    'adresse_l1': ['adresse_l1', 'adresse', 'address', 'rue'],
    'adresse_l2': ['adresse_l2', 'complement_adresse'],
    'code_postal': ['code_postal', 'cp', 'postal_code', 'zipcode'],
    'commune': ['commune', 'ville', 'city'],
    'departement': ['departement', 'département', 'dept'],
    'region': ['region', 'région'],
    'telephone': ['telephone', 'téléphone', 'phone', 'tel'],
    'email': ['email', 'mail', 'e-mail'],
    'site_web': ['site_web', 'site', 'website', 'url'],
    'gestionnaire': ['gestionnaire', 'gestionnaire/opérateur', 'operateur', 'opérateur'],
    'source': ['source'],
    'habitat_type': ['habitat_type', 'type_habitat'],
    
    # Colonnes pour mapping vers sous-catégories
    'type': ['type', 'sous_categories', 'sous_categorie'],
    'sous_categories': ['sous_categories', 'type', 'category']
}

# Mapping des valeurs pour habitat_type
HABITAT_TYPE_MAPPING = {
    'residence': ['residence', 'résidence', 'résidence autonomie', 'résidence services seniors', 'marpa'],
    'habitat_partage': ['habitat_partage', 'habitat partagé', 'habitat inclusif', 'accueil familial'],
    'logement_independant': ['logement_independant', 'logement indépendant', 'logement']
}

# Champs obligatoires pour publication
REQUIRED_FIELDS = [
    'nom',
    'commune',
    'code_postal', 
    'gestionnaire',
    'email',
    'habitat_type'
]

# Champs optionnels mais recommandés
RECOMMENDED_FIELDS = [
    'presentation',
    'telephone',
    'site_web',
    'adresse_l1'
]