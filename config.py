import math
import os
from dotenv import load_dotenv

# Charger le fichier .env (local uniquement)
# IMPORTANT: override=False pour ne PAS écraser les variables Cloud Run Secrets
# En Cloud Run, les secrets sont injectés via env vars et ne doivent pas être écrasés
_dotenv_loaded = load_dotenv(override=False)

# Configuration base de données
DATABASE_CONFIG = {
    'host': os.getenv('DB_HOST', 'db.minwoumfgutampcgrcbr.supabase.co'),
    'database': os.getenv('DB_NAME', 'postgres'),
    'user': os.getenv('DB_USER', 'postgres'),
    # Ne jamais mettre de secret en défaut: doit venir de l'environnement (.env / Cloud Run Secrets)
    'password': os.getenv('DB_PASSWORD', ''),
    'port': int(os.getenv('DB_PORT', 5432))
}


def _is_na(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip().lower() in {"", "nan", "none", "null"}:
        return True
    return False

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
    # Configuration OpenAI (PREMIUM) - Qualité maximale
    'openai_api_key': os.getenv('OPENAI_API_KEY', ''),
    'openai_model': os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo'),
    # Configuration Groq (GRATUIT) - Modèle plus léger pour éviter les rate limits
    'groq_api_key': os.getenv('GROQ_API_KEY', ''),
    'groq_model': os.getenv('GROQ_MODEL', 'llama-3.1-8b-instant'),
    # Configuration Ollama (LOCAL) - Modèles légers optimisés T495s
    'ollama_url': os.getenv('OLLAMA_URL', 'http://localhost:11434'),
    'ollama_model': os.getenv('OLLAMA_MODEL', 'phi3:mini'),  # Recommandé pour T495s
    'ollama_available_models': [
        'phi3:mini',      # 3.8B - Recommandé #1 (Microsoft, excellent JSON)
        'llama3.2:3b',    # 3B - Équilibré (Meta, polyvalent)
        'gemma:2b',       # 2B - Ultra rapide (Google, léger)
        'qwen2.5:3b'      # 3B - Multilingue (Alibaba, bon français)
    ],
    # API externes alternatives
    'huggingface_token': os.getenv('HUGGINGFACE_TOKEN', ''),
    'openai_compatible_url': 'https://api.groq.com/openai/v1/chat/completions',
    'openai_compatible_key': os.getenv('GROQ_API_KEY', ''),
    'openai_compatible_model': os.getenv('GROQ_MODEL', 'llama-3.1-8b-instant')
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
    'gestionnaire': ['gestionnaire', 'gestionnaire/opérateur', 'operateur', 'opérateur', 'porteur'],
    'source': ['source'],
    'habitat_type': ['habitat_type', 'type_habitat'],
    'eligibilite_avp': ['eligibilite_avp', 'avp', 'eligibilite'],
    
    # Colonnes spécifiques supplémentaires
    'code_insee': ['code_insee', 'insee', 'code_commune'],
    'eligibilite_statut': ['eligibilite_statut', 'statut_eligibilite', 'status'],
    'public_cible': ['public_cible', 'public', 'cible', 'target'],
    
    # Colonnes pour les types de logements
    'logements_types': ['logements_types', 'types_logement', 'logements'],
    'surface_min': ['surface_min', 'surface_minimale'],
    'surface_max': ['surface_max', 'surface_maximale'], 
    'pmr_accessible': ['pmr_accessible', 'pmr', 'accessibilite'],
    'meuble': ['meuble', 'meublé', 'furnished'],
    'domotique': ['domotique', 'smart_home'],
    
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
    'habitat_type'
]

# Champs optionnels mais recommandés
RECOMMENDED_FIELDS = [
    'presentation',
    'telephone',
    'email',
    'site_web',
    'adresse_l1'
]

# Valeurs valides pour les sous-catégories (ordre alphabétique)
VALID_SOUS_CATEGORIES = [
    'Accueil familial',
    'Béguinage', 
    'Habitat inclusif',
    'Habitat intergénérationnel',
    'Logement adapté PMR',
    'MARPA',
    'Résidence autonomie',
    'Résidence services seniors',
    'Village seniors'
]

# Mapping de normalisation des sous-catégories (pour compatibilité anciens CSV)
SOUS_CATEGORIES_NORMALISATION = {
    # Variantes résidence services seniors
    'résidence services seniors (indépendante)': 'Résidence services seniors',
    'residence services seniors (independante)': 'Résidence services seniors',
    'résidence services seniors / village seniors': 'Village seniors',  # Village seniors prime
    'residence services seniors / village seniors': 'Village seniors',
    
    # Variantes habitat inclusif
    'habitat partagé accompagné (AVP)': 'Habitat inclusif',
    'habitat partage accompagne (avp)': 'Habitat inclusif',
    'habitat partagé accompagné': 'Habitat inclusif',
    'habitat partage accompagne': 'Habitat inclusif',
    'habitat inclusif (regroupés)': 'Habitat inclusif',
    'habitat inclusif (regroures)': 'Habitat inclusif',
    
    # Variantes habitat partagé / maison relais
    'habitat partagé / maison relais': 'Habitat partagé',
    'habitat partage / maison relais': 'Habitat partagé',
    'maison relais / habitat partagé': 'Habitat partagé',
    'maison relais / habitat partage': 'Habitat partagé',
    
    # Variantes béguinage
    'béguinage / domicile regroupé de seniors': 'Béguinage',
    'beguinage / domicile regroupe de seniors': 'Béguinage',
    'domicile regroupé de seniors': 'Béguinage',
    'domicile regroupe de seniors': 'Béguinage',
    
    # Variantes habitat intergénérationnel
    'habitat intergénérationnel (logements sociaux adaptés)': 'Habitat intergénérationnel',
    'habitat intergenerationnel (logements sociaux adaptes)': 'Habitat intergénérationnel',
    
    # Variantes communes (avec/sans accents, casse)
    'residence autonomie': 'Résidence autonomie',
    'RESIDENCE AUTONOMIE': 'Résidence autonomie',
    'marpa': 'MARPA',
    'Marpa': 'MARPA',
    'accueil familial': 'Accueil familial',
    'ACCUEIL FAMILIAL': 'Accueil familial',
    'habitat inclusif': 'Habitat inclusif',
    'HABITAT INCLUSIF': 'Habitat inclusif',
    'habitat intergénérationnel': 'Habitat intergénérationnel',
    'habitat intergenerationnel': 'Habitat intergénérationnel',
    'HABITAT INTERGENERATIONNEL': 'Habitat intergénérationnel',
    'logement adapté pmr': 'Logement adapté PMR',
    'logement adapte pmr': 'Logement adapté PMR',
    'LOGEMENT ADAPTE PMR': 'Logement adapté PMR',
    'béguinage': 'Béguinage',
    'beguinage': 'Béguinage',
    'BEGUINAGE': 'Béguinage',
    'village seniors': 'Village seniors',
    'village séniors': 'Village seniors',
    'VILLAGE SENIORS': 'Village seniors',
    'résidence services seniors': 'Résidence services seniors',
    'residence services seniors': 'Résidence services seniors',
    'RESIDENCE SERVICES SENIORS': 'Résidence services seniors'
}

def normalize_sous_categorie(categorie: str) -> str:
    """Normalise une sous-catégorie selon le mapping défini"""
    if not categorie:
        return categorie
    
    # Nettoyer la chaîne
    cleaned = categorie.strip()
    
    # Chercher une correspondance directe
    if cleaned in SOUS_CATEGORIES_NORMALISATION:
        return SOUS_CATEGORIES_NORMALISATION[cleaned]
    
    # Chercher une correspondance insensible à la casse
    for variant, normalized in SOUS_CATEGORIES_NORMALISATION.items():
        if cleaned.lower() == variant.lower():
            return normalized
    
    # Si pas de correspondance, retourner la valeur nettoyée
    return cleaned

# Mapping de normalisation pour public_cible
PUBLIC_CIBLE_NORMALISATION = {
    # Variantes personnes âgées
    'seniors': 'personnes_agees',
    'personnes âgées': 'personnes_agees',
    'personnes agees': 'personnes_agees', 
    'personnes_âgées': 'personnes_agees',
    'seniors autonomes': 'personnes_agees',
    'personnes âgées autonomes': 'personnes_agees',
    'personnes âgées dépendantes': 'personnes_agees',
    '60 ans et plus': 'personnes_agees',
    'retraités': 'personnes_agees',
    'retraites': 'personnes_agees',
    
    # Variantes personnes handicapées
    'handicap': 'personnes_handicapees',
    'handicapé': 'personnes_handicapees',
    'handicapés': 'personnes_handicapees',
    'handicapees': 'personnes_handicapees',
    'personnes handicapées': 'personnes_handicapees',
    'personnes handicapees': 'personnes_handicapees',
    'personnes_handicapées': 'personnes_handicapees',
    'personnes en situation de handicap': 'personnes_handicapees',
    'situation handicap': 'personnes_handicapees',
    'déficience': 'personnes_handicapees',
    'déficience intellectuelle': 'personnes_handicapees',
    'handicap mental': 'personnes_handicapees',
    'handicap physique': 'personnes_handicapees',
    
    # Variantes mixtes
    'mixte': 'mixtes',
    'seniors et handicapés': 'mixtes',
    'personnes âgées et handicapées': 'mixtes',
    'tous publics': 'mixtes',
    'intergénérationnel': 'mixtes',
    'intergenerationnel': 'mixtes',
    
    # Variantes Alzheimer
    'alzheimer': 'alzheimer_accessible',
    'maladie d\'alzheimer': 'alzheimer_accessible',
    'maladie d alzheimer': 'alzheimer_accessible',
    'troubles cognitifs': 'alzheimer_accessible',
    'démence': 'alzheimer_accessible',
    'demence': 'alzheimer_accessible',
    'alzheimer accessible': 'alzheimer_accessible',
    'accessible alzheimer': 'alzheimer_accessible',
}

def normalize_public_cible(value):
    """Normalise les valeurs de public_cible selon les standards définis"""
    if _is_na(value):
        return None
    
    # Convertir en string et nettoyer
    value_str = str(value).strip()
    if not value_str:
        return None
    
    # Traiter les valeurs multiples séparées par virgule ou point-virgule
    if ',' in value_str or ';' in value_str:
        # Séparer les valeurs
        separators = [',', ';']
        values = [value_str]
        for sep in separators:
            new_values = []
            for v in values:
                new_values.extend([x.strip() for x in v.split(sep)])
            values = new_values
        
        # Normaliser chaque valeur
        normalized_values = []
        for v in values:
            if v.strip():  # Ignorer les valeurs vides
                normalized = _normalize_single_public_cible(v.strip())
                if normalized and normalized not in normalized_values:
                    normalized_values.append(normalized)
        
        return ','.join(normalized_values) if normalized_values else None
    else:
        return _normalize_single_public_cible(value_str)

def _normalize_single_public_cible(value):
    """Normalise une seule valeur de public_cible"""
    if not value:
        return None
    
    value_lower = value.lower().strip()
    
    # Recherche exacte dans le mapping
    if value_lower in PUBLIC_CIBLE_NORMALISATION:
        return PUBLIC_CIBLE_NORMALISATION[value_lower]
    
    # Détection prioritaire des cas mixtes (plusieurs publics mentionnés)
    has_seniors = any(keyword in value_lower for keyword in ['senior', 'âgé', 'age', 'retraité'])
    has_handicap = any(keyword in value_lower for keyword in ['handicap', 'déficience', 'situation'])
    has_mixte_explicit = any(keyword in value_lower for keyword in ['mixte', 'tous', 'intergénération'])
    
    # Si plusieurs publics ou mention explicite de mixte
    if has_mixte_explicit or (has_seniors and has_handicap):
        return 'mixtes'
    
    # Recherche par mots-clés pour les cas spécifiques
    if any(keyword in value_lower for keyword in ['alzheimer', 'cognitif', 'démence', 'demence']):
        return 'alzheimer_accessible'
    elif has_handicap:
        return 'personnes_handicapees'
    elif has_seniors:
        return 'personnes_agees'
    
    # Si aucune correspondance, retourner la valeur nettoyée
    return value