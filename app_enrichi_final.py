#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Application Streamlit pour l'enrichissement et normalisation des données habitat senior
Conforme aux directives d'enrichissement et au schéma de base PostgreSQL/Supabase
"""

import os
import re
import json
import time
import struct
import binascii
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import numpy as np
import streamlit as st

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    requests = None
    BeautifulSoup = None
    st.error("Modules requests et beautifulsoup4 requis: pip install requests beautifulsoup4")

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    psycopg2 = None
    st.warning("Module psycopg2 requis pour l'import en base: pip install psycopg2-binary")

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

try:
    from groq import Groq as GroqClient
except ImportError:
    GroqClient = None

# Import des nouveaux modules d'enrichissement
try:
    from enrichment.eligibilite_rules import (
        deduce_eligibilite_statut, 
        is_avp_eligible,
        should_enrich_avp_data
    )
    from enrichment.normalizer import DataNormalizer
    ENRICHMENT_MODULES_AVAILABLE = True
except ImportError:
    ENRICHMENT_MODULES_AVAILABLE = False
    st.warning("⚠️ Modules d'enrichissement optimisés non disponibles - utilisation des fonctions legacy")

# Configuration Streamlit
st.set_page_config(page_title="Habitat Senior - Enrichissement", layout="wide")

# CSS pour améliorer l'affichage mobile
st.markdown("""
<style>
    /* Optimisation mobile */
    @media (max-width: 768px) {
        /* Forcer la sidebar à se replier sur mobile */
        .css-1d391kg {
            width: 0 !important;
        }
        
        /* Contenu principal prend toute la largeur sur mobile */
        .main .block-container {
            max-width: 100% !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
        
        /* Ajuster la marge gauche du contenu principal */
        .css-18e3th9 {
            margin-left: 0 !important;
        }
        
        /* Sidebar complètement cachée sur mobile */
        .css-1cypcdb {
            display: none !important;
        }
    }
    
    /* Amélioration générale de l'interface */
    .stTextInput > div > div > input {
        font-size: 14px;
    }
    
    /* Meilleur espacement sur mobile */
    @media (max-width: 768px) {
        .element-container {
            margin-bottom: 0.5rem !important;
        }
        
        /* Réduire l'espacement des métriques sur mobile */
        div[data-testid="metric-container"] {
            margin-bottom: 0.5rem;
        }
    }
</style>
""", unsafe_allow_html=True)

st.title("🏠 Habitat Senior - Normalisation et Enrichissement")

with st.expander("ℹ️ Guide d'utilisation", expanded=False):
    st.markdown("""
    ### 🚀 Démarrage rapide
    Les valeurs par défaut sont **déjà configurées** dans la barre latérale pour une utilisation immédiate !
    
    **Configuration automatique :**
    - ✅ Base de données : Supabase pré-configurée
    - ✅ API OpenAI : Clé pré-remplie  
    - ✅ API Groq : Clé pré-remplie (alternative à OpenAI)
    - ✅ API Tavily : Clé pré-remplie (recherche web)
    
    **Étapes d'utilisation :**
    1. 📁 Chargez votre fichier CSV d'établissements
    2. ⚙️ Choisissez le mode d'enrichissement :
       - **Webscraping** : Enrichit via les sites officiels
       - **IA seule** : Enrichit avec GPT depuis les données CSV
       - **Websearch + IA** : Combine recherche web + IA
    3. 🗺️ Activez le géocodage si nécessaire  
    4. ▶️ Lancez le processus d'enrichissement
    5. ✅ Vérifiez les résultats et exportez ou importez en base
    
    ### 🔧 Personnalisation
    Si besoin, modifiez les paramètres dans la barre latérale :
    - Changez de fournisseur IA (OpenAI/Groq)
    - Modifiez les clés API 
    - Testez la connexion à la base de données
    """)

with st.expander("📋 Format CSV attendu", expanded=False):
    st.markdown("""
    **Colonnes requises :**
    - `nom` : Nom de l'établissement
    - `commune` : Ville
    - `code_postal` : Code postal
    - `gestionnaire` : Organisme gestionnaire
    - `adresse_l1` : Adresse principale
    - `telephone` : Numéro de téléphone
    - `sous_categories` : Type d'habitat
    - `public_cible` : Public visé
    """)

# Configuration sidebar
with st.sidebar:
    st.header("🔧 Configuration")
    
    # Message informatif
    st.info("💡 Les valeurs par défaut sont pré-remplies pour faciliter l'utilisation")
    
    # Base de données
    st.subheader("📊 Base de données")
    db_config = {
        'host': st.text_input("Host", value="db.minwoumfgutampcgrcbr.supabase.co"),
        'port': st.number_input("Port", value=5432, min_value=1, max_value=65535),
        'database': st.text_input("Database", value="postgres"),
        'user': st.text_input("User", value="postgres"),
        'password': st.text_input("Password", value="", type="password"),
        'sslmode': st.selectbox("SSL Mode", ["require", "prefer", "disable"], index=0)
    }
    
    st.divider()
    
    # Enrichissement avec session_state pour persistance
    st.subheader("🧠 Enrichissement")
    
    # Configuration dans session_state pour persistance
    if 'enrich_mode' not in st.session_state:
        st.session_state.enrich_mode = "Aucun"
    if 'ai_provider' not in st.session_state:
        st.session_state.ai_provider = "OpenAI"
    if 'openai_key' not in st.session_state:
        st.session_state.openai_key = ""
    if 'groq_key' not in st.session_state:
        st.session_state.groq_key = ""
    if 'ai_model' not in st.session_state:
        st.session_state.ai_model = "gpt-4o-mini"
    
    enrich_mode = st.selectbox(
        "Mode d'enrichissement", 
        ["Aucun", "Webscraping", "IA seule", "Websearch + IA"], 
        index=["Aucun", "Webscraping", "IA seule", "Websearch + IA"].index(st.session_state.enrich_mode),
        key="enrich_mode_select"
    )
    st.session_state.enrich_mode = enrich_mode
    
    # Mode debug
    debug_mode = st.checkbox("Mode détaillé (afficher les logs de scraping)", value=False)
    
    # IA Configuration
    ai_provider = st.selectbox(
        "Fournisseur IA", 
        ["OpenAI", "Groq"], 
        index=["OpenAI", "Groq"].index(st.session_state.ai_provider),
        key="ai_provider_select"
    )
    st.session_state.ai_provider = ai_provider
    
    # Auto-changement du modèle selon le provider
    if ai_provider == "OpenAI":
        default_model = "gpt-4o-mini"
        available_models = ["gpt-4o-mini", "gpt-4o", "gpt-3.5-turbo"]
    else:  # Groq - Modèles actifs à jour (Oct 2025)
        default_model = "llama-3.1-8b-instant"
        available_models = [
            "llama-3.1-8b-instant",      # Recommandé: rapide et gratuit
            "llama-3.1-70b-versatile",   # Plus puissant
            "llama-3.2-1b-preview",      # Très rapide, moins précis
            "llama-3.2-3b-preview",      # Bon compromis
            "gemma2-9b-it",              # Alternative Google
            "mixtral-8x7b-32768"         # Si encore disponible
        ]
    
    # Si le modèle actuel n'est pas compatible avec le provider, on change automatiquement
    current_model = st.session_state.ai_model
    if current_model not in available_models:
        st.session_state.ai_model = default_model
        st.info(f"🔄 Modèle changé automatiquement vers {default_model} pour {ai_provider}")
    
    ai_model = st.selectbox(
        "Modèle IA", 
        options=available_models,
        index=available_models.index(st.session_state.ai_model) if st.session_state.ai_model in available_models else 0,
        key="ai_model_select"
    )
    st.session_state.ai_model = ai_model
    
    openai_key = st.text_input(
        "OPENAI_API_KEY", 
        value=st.session_state.openai_key, 
        type="password",
        key="openai_key_input"
    )
    st.session_state.openai_key = openai_key
    
    groq_key = st.text_input(
        "GROQ_API_KEY", 
        value=st.session_state.groq_key, 
        type="password",
        key="groq_key_input"
    )
    st.session_state.groq_key = groq_key
    
    # Search Configuration avec session_state
    st.subheader("🔍 Recherche Web")
    
    # Configuration dans session_state pour persistance  
    if 'search_provider' not in st.session_state:
        st.session_state.search_provider = "Tavily"
    if 'tavily_key' not in st.session_state:
        st.session_state.tavily_key = "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"
    if 'serpapi_key' not in st.session_state:
        st.session_state.serpapi_key = ""
    
    search_provider = st.selectbox(
        "Provider", 
        ["Tavily", "SerpAPI"], 
        index=["Tavily", "SerpAPI"].index(st.session_state.search_provider),
        key="search_provider_select"
    )
    st.session_state.search_provider = search_provider
    
    tavily_key = st.text_input(
        "TAVILY_API_KEY", 
        value=st.session_state.tavily_key, 
        type="password",
        key="tavily_key_input"
    )
    st.session_state.tavily_key = tavily_key
    
    serpapi_key = st.text_input(
        "SERPAPI_KEY", 
        value=st.session_state.serpapi_key,
        type="password",
        key="serpapi_key_input"
    )
    st.session_state.serpapi_key = serpapi_key
    
    # Indicateurs d'état des API
    col1, col2, col3 = st.columns(3)
    with col1:
        if openai_key:
            st.success("🤖 OpenAI configuré")
        else:
            st.warning("⚠️ OpenAI manquant")
    
    with col2:
        if groq_key:
            st.success("🚀 Groq configuré")
        else:
            st.warning("⚠️ Groq manquant")
    
    with col3:
        if tavily_key:
            st.success("🔍 Tavily configuré")
        else:
            st.warning("⚠️ Tavily manquant")
    
    # Géocodage
    st.subheader("🗺️ Géocodage")
    geocode_provider = st.selectbox("Provider", ["Nominatim (gratuit)", "Google Maps", "Mapbox"], index=0)
    google_maps_key = st.text_input("Google Maps API Key", type="password")
    mapbox_key = st.text_input("Mapbox Access Token", type="password")
    
    st.divider()
    
    # Bouton de réinitialisation
    if st.button("🔄 Réinitialiser les valeurs par défaut", help="Recharger la page pour remettre les valeurs par défaut"):
        st.rerun()
    
    # Test de connexion
    st.divider()
    if st.button("🔗 Tester la connexion DB"):
        try:
            if psycopg2:
                conn = psycopg2.connect(**db_config)
                conn.close()
                st.success("✅ Connexion à la base de données réussie !")
            else:
                st.error("❌ Module psycopg2 non installé")
        except Exception as e:
            st.error(f"❌ Erreur de connexion : {str(e)}")

# === FONCTIONS UTILITAIRES ===

def create_ewkb_point(longitude: float, latitude: float) -> str:
    """Créer un point EWKB au format PostGIS (SRID 4326)"""
    try:
        # EWKB format: 0x20000001 = Point with SRID
        ewkb = struct.pack('<I', 0x20000001)  # Point type with SRID flag
        ewkb += struct.pack('<I', 4326)       # SRID 4326 (WGS84)
        ewkb += struct.pack('<d', float(longitude))  # X coordinate
        ewkb += struct.pack('<d', float(latitude))   # Y coordinate
        return '01' + binascii.hexlify(ewkb).decode('ascii')
    except Exception as e:
        st.error(f"Erreur création EWKB: {e}")
        return None

def clean_text(value: Any) -> Optional[str]:
    """Nettoyer et normaliser un texte"""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text if text else None

def normalize_phone_fr(phone: str) -> Optional[str]:
    """Normaliser un numéro de téléphone français"""
    if not phone or pd.isna(phone):
        return None
    
    # Extraire les chiffres
    digits = re.sub(r'\D', '', str(phone))
    
    # Traiter le format international
    if digits.startswith('33') and len(digits) == 11:
        digits = '0' + digits[2:]
    
    # Vérifier le format français
    if len(digits) == 10 and digits.startswith('0'):
        return f"{digits[:2]} {digits[2:4]} {digits[4:6]} {digits[6:8]} {digits[8:10]}"
    
    return None

def normalize_email(email: str) -> Optional[str]:
    """Valider et normaliser un email"""
    if not email or pd.isna(email):
        return None
    
    email = str(email).strip().lower()
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    if re.match(pattern, email):
        return email
    return None

# Mapping des départements vers régions
DEPARTEMENT_REGIONS = {
    "01": "Auvergne-Rhône-Alpes", "02": "Hauts-de-France", "03": "Auvergne-Rhône-Alpes",
    "04": "Provence-Alpes-Côte d'Azur", "05": "Provence-Alpes-Côte d'Azur", "06": "Provence-Alpes-Côte d'Azur",
    "07": "Auvergne-Rhône-Alpes", "08": "Grand Est", "09": "Occitanie", "10": "Grand Est",
    "11": "Occitanie", "12": "Occitanie", "13": "Provence-Alpes-Côte d'Azur", "14": "Normandie",
    "15": "Auvergne-Rhône-Alpes", "16": "Nouvelle-Aquitaine", "17": "Nouvelle-Aquitaine", "18": "Centre-Val de Loire",
    "19": "Nouvelle-Aquitaine", "21": "Bourgogne-Franche-Comté", "22": "Bretagne", "23": "Nouvelle-Aquitaine",
    "24": "Nouvelle-Aquitaine", "25": "Bourgogne-Franche-Comté", "26": "Auvergne-Rhône-Alpes", "27": "Normandie",
    "28": "Centre-Val de Loire", "29": "Bretagne", "30": "Occitanie", "31": "Occitanie",
    "32": "Occitanie", "33": "Nouvelle-Aquitaine", "34": "Occitanie", "35": "Bretagne",
    "36": "Centre-Val de Loire", "37": "Centre-Val de Loire", "38": "Auvergne-Rhône-Alpes", "39": "Bourgogne-Franche-Comté",
    "40": "Nouvelle-Aquitaine", "41": "Centre-Val de Loire", "42": "Auvergne-Rhône-Alpes", "43": "Auvergne-Rhône-Alpes",
    "44": "Pays de la Loire", "45": "Centre-Val de Loire", "46": "Occitanie", "47": "Nouvelle-Aquitaine",
    "48": "Occitanie", "49": "Pays de la Loire", "50": "Normandie", "51": "Grand Est",
    "52": "Grand Est", "53": "Pays de la Loire", "54": "Grand Est", "55": "Grand Est",
    "56": "Bretagne", "57": "Grand Est", "58": "Bourgogne-Franche-Comté", "59": "Hauts-de-France",
    "60": "Hauts-de-France", "61": "Normandie", "62": "Hauts-de-France", "63": "Auvergne-Rhône-Alpes",
    "64": "Nouvelle-Aquitaine", "65": "Occitanie", "66": "Occitanie", "67": "Grand Est",
    "68": "Grand Est", "69": "Auvergne-Rhône-Alpes", "70": "Bourgogne-Franche-Comté", "71": "Bourgogne-Franche-Comté",
    "72": "Pays de la Loire", "73": "Auvergne-Rhône-Alpes", "74": "Auvergne-Rhône-Alpes", "75": "Île-de-France",
    "76": "Normandie", "77": "Île-de-France", "78": "Île-de-France", "79": "Nouvelle-Aquitaine",
    "80": "Hauts-de-France", "81": "Occitanie", "82": "Occitanie", "83": "Provence-Alpes-Côte d'Azur",
    "84": "Provence-Alpes-Côte d'Azur", "85": "Pays de la Loire", "86": "Nouvelle-Aquitaine", "87": "Nouvelle-Aquitaine",
    "88": "Grand Est", "89": "Bourgogne-Franche-Comté", "90": "Bourgogne-Franche-Comté", "91": "Île-de-France",
    "92": "Île-de-France", "93": "Île-de-France", "94": "Île-de-France", "95": "Île-de-France"
}

# Mapping sous-catégories normalisées (AVEC BONNE CASSE)
SOUS_CATEGORIES_MAPPING = {
    "residence autonomie": "Résidence autonomie",
    "résidence autonomie": "Résidence autonomie", 
    "residence services seniors": "Résidence services seniors",
    "résidence services seniors": "Résidence services seniors",
    "résidence service seniors": "Résidence services seniors",
    "residence_seniors": "Résidence services seniors",  # Ajout pour les CSV avec underscore
    "residence seniors": "Résidence services seniors",  # Ajout variation
    "marpa": "MARPA",
    "habitat inclusif": "Habitat inclusif",
    "colocation avec services": "Colocation avec services",
    "habitat intergénérationnel": "Habitat intergénérationnel",
    "habitat intergenerationnel": "Habitat intergénérationnel",
    "accueil familial": "Accueil familial",
    "maison d'accueil familial": "Maison d'accueil familial",
    "béguinage": "Béguinage",
    "beguinage": "Béguinage",
    "village seniors": "Village seniors",
    "habitat alternatif": "Habitat alternatif"
}

def detect_sous_categorie_intelligente(nom: str, presentation: str = None, gestionnaire: str = None, site_web: str = None) -> Optional[str]:
    """
    Détection intelligente de la sous-catégorie depuis le nom, présentation, gestionnaire et site_web
    Priorité: nom > site_web > présentation > gestionnaire
    """
    # Combiner toutes les sources de texte
    all_text = f"{nom} {site_web or ''} {presentation or ''} {gestionnaire or ''}".lower()
    
    # Patterns de détection par ordre de spécificité (du plus spécifique au plus général)
    detection_patterns = [
        # MARPA (très spécifique)
        (r'\bmarpa\b', 'MARPA'),
        
        # Béguinage (très spécifique)
        (r'\bb[ée]guinage\b', 'Béguinage'),
        
        # Village seniors (très spécifique)
        (r'\bvillage\s+(?:senior|sénior)', 'Village seniors'),
        
        # Maison d'accueil familial (CetteFamille)
        (r'\bcettefamille\b.*\bmaison', 'Maison d\'accueil familial'),
        (r'\bmaison\s+(?:d\')?accueil\s+familial', 'Maison d\'accueil familial'),
        
        # Colocation avec services (Ages & Vie)
        (r'\bages\s*&\s*vie\b', 'Colocation avec services'),
        (r'\bcolocation\b.*\bservices?\b', 'Colocation avec services'),
        (r'\bcoloc\b', 'Colocation avec services'),
        
        # Habitat intergénérationnel (mot-clé fort)
        (r'\binterg[ée]n[ée]rationn', 'Habitat intergénérationnel'),
        
        # Accueil familial simple
        (r'\baccueil\s+familial\b', 'Accueil familial'),
        
        # Habitat inclusif (mot-clé fort)
        (r'\bhabitat\s+inclusif\b', 'Habitat inclusif'),
        
        # Résidence services seniors (plusieurs variations)
        (r'\br[ée]sidence\s+services?\s+(?:senior|sénior)', 'Résidence services seniors'),
        (r'\bdomitys\b', 'Résidence services seniors'),
        (r'\bespace\s+(?:et\s+)?vie\b', 'Résidence services seniors'),
        (r'\bsenioriales?\b', 'Résidence services seniors'),
        (r'\babc\s+r[ée]sidences?\b', 'Résidence services seniors'),
        
        # Résidence autonomie (le plus commun, donc en dernier)
        (r'\br[ée]sidence\s+autonomie\b', 'Résidence autonomie'),
        (r'\bfoyer[- ]logement\b', 'Résidence autonomie'),
        (r'\blogement[- ]foyer\b', 'Résidence autonomie'),
    ]
    
    # Tester chaque pattern
    for pattern, sous_cat in detection_patterns:
        if re.search(pattern, all_text, re.IGNORECASE):
            return sous_cat
    
    # Si aucun pattern ne correspond, retourner None (pas de fallback automatique)
    return None

def normalize_sous_categorie(value: str) -> Optional[str]:
    """
    Normaliser une sous-catégorie UNIQUEMENT si elle est dans le mapping valide.
    Rejette les valeurs invalides comme 'residence' ou 'habitat_partage' (qui sont des habitat_type).
    """
    if not value:
        return None
    
    # Nettoyer la valeur
    value_clean = str(value).strip()
    normalized_lower = value_clean.lower()
    
    # LISTE DES VALEURS INVALIDES À REJETER (habitat_type mal placés dans sous_categories)
    invalid_values = ['residence', 'habitat_partage', 'logement_independant']
    if normalized_lower in invalid_values:
        return None  # Rejeter, ce ne sont pas des sous-catégories
    
    # ÉTAPE 1: Vérifier dans le mapping (qui contient toutes les variations valides)
    mapped_value = SOUS_CATEGORIES_MAPPING.get(normalized_lower)
    if mapped_value:
        return mapped_value
    
    # ÉTAPE 2: Vérification directe des valeurs valides exactes
    valid_categories_normalized = {
        "résidence autonomie": "Résidence autonomie",
        "résidence services seniors": "Résidence services seniors",
        "marpa": "MARPA",
        "habitat inclusif": "Habitat inclusif",
        "colocation avec services": "Colocation avec services",
        "habitat intergénérationnel": "Habitat intergénérationnel",
        "accueil familial": "Accueil familial",
        "maison d'accueil familial": "Maison d'accueil familial",
        "béguinage": "Béguinage",
        "village seniors": "Village seniors",
        "habitat alternatif": "Habitat alternatif"
    }
    
    direct_match = valid_categories_normalized.get(normalized_lower)
    if direct_match:
        return direct_match
    
    # ÉTAPE 3: Règle spéciale MARPA
    if "marpa" in normalized_lower:
        return "MARPA"
    
    # ÉTAPE 4: Si pas dans le mapping valide, retourner None (sera détecté intelligemment après)
    return None

def deduce_habitat_type(sous_categorie: str) -> Optional[str]:
    """Déduire habitat_type depuis sous_categorie selon les règles"""
    if not sous_categorie:
        return None
        
    if sous_categorie in ["Résidence autonomie", "Résidence services séniors", "MARPA"]:
        return "residence"
    elif sous_categorie in ["Colocation avec services", "Habitat intergénérationnel", 
                           "Habitat inclusif", "Habitat alternatif", "Accueil familial", 
                           "Maison d'accueil familial"]:
        return "habitat_partage"
    elif sous_categorie in ["Béguinage", "Village séniors", "Habitat regroupé"]:
        return "logement_independant"
    
    return None

def deduce_eligibilite(sous_categorie: str, mention_avp: bool, eligibilite_csv: str = None) -> str:
    """
    Déduire eligibilite_statut AVP selon les VRAIES règles métier
    Utilise le nouveau module si disponible, sinon fallback sur l'ancienne implémentation
    """
    # Utiliser le nouveau module si disponible
    if ENRICHMENT_MODULES_AVAILABLE:
        return deduce_eligibilite_statut(
            sous_categorie=sous_categorie,
            mention_avp_explicite=mention_avp,
            eligibilite_csv=eligibilite_csv
        )
    
    # Fallback sur l'ancienne implémentation
    if not sous_categorie:
        return eligibilite_csv if eligibilite_csv else 'a_verifier'
    
    sous_cat_clean = sous_categorie.lower().strip()
    
    # JAMAIS éligibles AVP (liste stricte)
    jamais_eligibles = [
        'résidence services seniors', 'résidence services', 'residence services',
        'résidence autonomie', 'residence autonomie',
        'accueil familial',
        'marpa', 'maison d\'accueil rural',
        'village seniors', 'village séniors', 'village senior',
        'béguinage', 'beguinage',
        'ehpad', 'établissement d\'hébergement'
    ]
    
    for pattern in jamais_eligibles:
        if pattern in sous_cat_clean:
            return 'non_eligible'
    
    # CAS SPÉCIAL : Habitat inclusif
    if 'habitat inclusif' in sous_cat_clean:
        # Si mention AVP détectée → avp_eligible
        if mention_avp:
            return 'avp_eligible'
        # Si déjà marqué avp_eligible dans le CSV, on garde (déjà vérifié)
        if eligibilite_csv == 'avp_eligible':
            return 'avp_eligible'
        # Sinon, à vérifier manuellement
        return 'a_verifier'
    
    # Éligibles SI mention AVP détectée
    eligibles_si_mention = [
        'habitat intergénérationnel', 'habitat intergenerationnel',
        'établissement intergénérationnel', 'etablissement intergenerationnel',
        'habitat alternatif',
        'colocation avec services', 'colocation services',
        'maison d\'accueil familial', 'maison d\'accueil familial'
    ]
    
    for pattern in eligibles_si_mention:
        if pattern in sous_cat_clean:
            return 'avp_eligible' if mention_avp else 'non_eligible'
    
    # Cas par défaut
    return eligibilite_csv if eligibilite_csv else 'a_verifier'

def validate_data_consistency(data: Dict[str, Any]) -> List[str]:
    """Validation de cohérence des données selon vos règles métier"""
    warnings = []
    
    # Vérifier cohérence habitat_type <-> sous_categorie
    habitat_type = data.get('habitat_type')
    sous_categorie = data.get('sous_categorie')
    
    expected_habitat = deduce_habitat_type(sous_categorie)
    if habitat_type and expected_habitat and habitat_type != expected_habitat:
        warnings.append(f"Incohérence: habitat_type='{habitat_type}' mais sous_categorie='{sous_categorie}' devrait donner '{expected_habitat}'")
    
    # Vérifier cohérence eligibilite_statut
    eligibilite = data.get('eligibilite_statut')
    mention_avp = data.get('mention_avp', False)
    expected_eligibilite = deduce_eligibilite(sous_categorie, mention_avp)
    
    if eligibilite and eligibilite != expected_eligibilite:
        warnings.append(f"Incohérence: eligibilite_statut='{eligibilite}' mais règles donnent '{expected_eligibilite}'")
    
    # Vérifier public_cible selon directives
    public_cible = data.get('public_cible', '')
    valid_publics = ["personnes_agees", "personnes_handicapees", "mixtes", "alzheimer_accessible"]
    if public_cible:
        # Gérer le cas où public_cible est déjà une liste (après enrichissement IA)
        if isinstance(public_cible, list):
            publics = public_cible
        else:
            publics = [p.strip() for p in str(public_cible).split(',')]
        invalid_publics = [p for p in publics if p not in valid_publics]
        if invalid_publics:
            warnings.append(f"Public cible invalide: {invalid_publics}. Autorisés: {valid_publics}")
    
    # Vérifier services autorisés
    services = data.get('services', [])
    valid_services = ["activités organisées", "espace_partage", "conciergerie", "personnel de nuit", "commerces à pied", "médecin intervenant"]
    if services:
        invalid_services = [s for s in services if s not in valid_services]
        if invalid_services:
            warnings.append(f"Services invalides: {invalid_services}")
    
    # Vérifier fourchette_prix cohérente
    tarif = data.get('tarification', {})
    prix_min = tarif.get('prix_min')
    fourchette = tarif.get('fourchette_prix')
    
    if prix_min and fourchette:
        expected_fourchette = None
        if prix_min < 750:
            expected_fourchette = "euro"
        elif 750 <= prix_min <= 1500:
            expected_fourchette = "deux_euros"
        else:
            expected_fourchette = "trois_euros"
        
        if fourchette != expected_fourchette:
            warnings.append(f"Fourchette prix incohérente: prix_min={prix_min} devrait donner '{expected_fourchette}', pas '{fourchette}'")
    
    return warnings

def normalize_public_cible(value: str) -> Optional[str]:
    """Normaliser public_cible selon les valeurs autorisées"""
    if not value:
        return None
        
    mapping = {
        "personnes âgées": "personnes_agees",
        "personnes_agees": "personnes_agees",
        "seniors": "personnes_agees", 
        "personnes handicapées": "personnes_handicapees",
        "personnes_handicapees": "personnes_handicapees",
        "alzheimer": "alzheimer_accessible",
        "alzheimer_accessible": "alzheimer_accessible",
        "mixte": "mixtes",
        "mixtes": "mixtes",
        "intergénérationnel": "mixtes"
    }
    
    # Traiter les valeurs multiples séparées par des virgules
    values = [v.strip().lower() for v in str(value).split(',')]
    normalized = []
    
    for v in values:
        if v in mapping:
            mapped = mapping[v]
            if mapped not in normalized:
                normalized.append(mapped)
    
    return ",".join(normalized) if normalized else None

def parse_departement_region(code_postal: str, departement_csv: str = None) -> Tuple[str, str]:
    """Parser département et région depuis code postal et champ département"""
    if not code_postal:
        return None, None
        
    # Extraire code département du code postal
    dept_code = str(code_postal)[:2].zfill(2)
    
    # Si département fourni avec format "Nom (XX)", utiliser ce format
    if departement_csv and '(' in departement_csv:
        dept_label = departement_csv
    else:
        # Sinon utiliser format générique
        dept_label = f"Landes ({dept_code})" if dept_code == "40" else f"Département ({dept_code})"
    
    # Récupérer région depuis le mapping
    region = DEPARTEMENT_REGIONS.get(dept_code)
    
    return dept_label, region

# === FONCTIONS D'ENRICHISSEMENT ===

def format_enrichment_summary(etablissement: Dict[str, Any], enriched_data: Dict[str, Any]) -> str:
    """Créer un résumé concis des données enrichies"""
    summary = []
    
    # Données de base trouvées
    basic_data = []
    if enriched_data.get('email'):
        basic_data.append("📧")
    if enriched_data.get('telephone'):
        basic_data.append("📞") 
    if enriched_data.get('gestionnaire'):
        basic_data.append("👤")
    
    if basic_data:
        summary.append(f"Données: {' '.join(basic_data)}")
    
    # Services trouvés
    services = enriched_data.get('services', [])
    if services:
        summary.append(f"Services: {len(services)}")
    
    # Restauration
    resto = enriched_data.get('restauration', {})
    resto_count = sum(1 for v in resto.values() if v)
    if resto_count:
        summary.append(f"Resto: {resto_count}")
    
    # Tarifs
    tarif = enriched_data.get('tarification', {})
    if tarif.get('prix_min'):
        summary.append(f"Tarifs: {tarif['prix_min']}-{tarif['prix_max']}€")
    
    # Logements
    logements = enriched_data.get('logements_types', [])
    if logements:
        types = [log['libelle'] for log in logements]
        summary.append(f"Logements: {'/'.join(types)}")
    
    # AVP
    if enriched_data.get('mention_avp'):
        summary.append("🏷️ AVP")
    
    return " | ".join(summary) if summary else "Aucune donnée enrichie"


# Fonction pour l'affichage optionnel des détails de scraping
def display_scraping_details(url: str, scraped_data: Dict[str, Any], debug: bool):
    """Afficher les détails de scraping seulement en mode debug"""
    if not debug or not scraped_data:
        return
        
    with st.expander(f"🔍 Détails scraping: {url[:50]}..."):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Données extraites:**")
            if scraped_data.get('email'):
                st.write(f"📧 Email: {scraped_data['email']}")
            if scraped_data.get('telephone'):
                st.write(f"📞 Téléphone: {scraped_data['telephone']}")
            if scraped_data.get('gestionnaire'):
                st.write(f"👤 Gestionnaire: {scraped_data['gestionnaire']}")
            if scraped_data.get('presentation'):
                st.write(f"📝 Présentation: {scraped_data['presentation'][:100]}...")
        
        with col2:
            st.write("**Enrichissements:**")
            services = scraped_data.get('services', [])
            if services:
                st.write(f"🛎️ Services: {', '.join(services)}")
            
            resto = scraped_data.get('restauration', {})
            resto_found = [k for k, v in resto.items() if v]
            if resto_found:
                st.write(f"🍽️ Restauration: {', '.join(resto_found)}")
                
            tarif = scraped_data.get('tarification', {})
            if tarif.get('prix_min'):
                st.write(f"💰 Tarifs: {tarif['prix_min']}-{tarif['prix_max']}€")
                
            logements = scraped_data.get('logements_types', [])
            if logements:
                types = [log['libelle'] for log in logements]
                st.write(f"🏠 Logements: {'/'.join(types)}")
                
            if scraped_data.get('mention_avp'):
                st.write("🏷️ Mention AVP détectée")


def scrape_website_enhanced(url: str) -> Dict[str, Any]:
    """Version améliorée du scraper avec extraction sophistiquée"""
    if not requests or not BeautifulSoup:
        return {}
        
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)  # Réduit de 25s à 10s
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # RÉCUPÉRER LES LIENS AVANT DE NETTOYER LE CONTENU
        all_links = soup.find_all('a', href=True)
        
        # Nettoyer le contenu (supprimer scripts, styles, etc.)
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()
        
        text = soup.get_text(separator=' ', strip=True)
        
        # Initialiser les données
        data = {
            'email': None,
            'telephone': None, 
            'site_web': url,
            'presentation': None,
            'gestionnaire': None,
            'mention_avp': False,
            'restauration': {
                'kitchenette': False,
                'resto_collectif_midi': False,
                'resto_collectif': False,
                'portage_repas': False
            },
            'logements_types': [],
            'tarification': {
                'fourchette_prix': None,
                'prix_min': None,
                'prix_max': None
            },
            'services': []
        }
        
        # === EXTRACTION AMÉLIORÉE ===
        
        # 1. EMAIL - Patterns multiples et validation
        email_patterns = [
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
            r'contact[\s@]*:[\s]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        ]
        for pattern in email_patterns:
            emails = re.findall(pattern, text, re.IGNORECASE)
            if emails:
                email_candidate = emails[0] if isinstance(emails[0], str) else emails[0]
                validated_email = normalize_email(email_candidate)
                if validated_email and '@' in validated_email:
                    data['email'] = validated_email
                    break
        
        # 2. TÉLÉPHONE - Patterns étendus avec contexte
        phone_patterns = [
            r'(?:tél|téléphone|phone|contact)[\s:]*(\+33[\s\.\-]?[1-9](?:[\s\.\-]?\d){8})',
            r'(?:tél|téléphone|phone|contact)[\s:]*(0[1-9](?:[\s\.\-]?\d){8})',
            r'(\+33[\s\.\-]?[1-9](?:[\s\.\-]?\d){8})',
            r'(0[1-9](?:[\s\.\-]?\d){8})',
            r'(\d{2}[\s\.\-]?\d{2}[\s\.\-]?\d{2}[\s\.\-]?\d{2}[\s\.\-]?\d{2})'
        ]
        for pattern in phone_patterns:
            phones = re.findall(pattern, text, re.IGNORECASE)
            if phones:
                phone_candidate = phones[0] if isinstance(phones[0], str) else phones[0]
                validated_phone = normalize_phone_fr(phone_candidate)
                if validated_phone:
                    data['telephone'] = validated_phone
                    break
        
        # 3. PRÉSENTATION - Sources multiples et intelligentes
        presentation_candidates = []
        
        # Title tag
        if soup.title:
            title = soup.title.get_text(strip=True)
            if len(title) > 20 and len(title) < 200:
                presentation_candidates.append(('title', title, len(title)))
        
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            desc = meta_desc['content'].strip()
            if len(desc) > 30:
                presentation_candidates.append(('meta', desc, len(desc)))
        
        # Premiers paragraphes significatifs
        paragraphs = soup.find_all('p')
        for i, p in enumerate(paragraphs[:5]):
            p_text = p.get_text(strip=True)
            if 50 <= len(p_text) <= 500:
                # Bonus pour les premiers paragraphes
                score = len(p_text) + (50 if i == 0 else 20)
                presentation_candidates.append(('paragraph', p_text, score))
        
        # Headers h1, h2
        for tag in ['h1', 'h2']:
            headers = soup.find_all(tag)
            for h in headers[:3]:
                h_text = h.get_text(strip=True)
                if 20 <= len(h_text) <= 300:
                    presentation_candidates.append(('header', h_text, len(h_text) + 30))
        
        # Sélectionner la meilleure présentation
        if presentation_candidates:
            # Trier par score puis prendre le meilleur
            best = max(presentation_candidates, key=lambda x: x[2])
            data['presentation'] = clean_text(best[1])[:500]
        
        # 4. GESTIONNAIRE - Patterns contextuels améliorés
        gestionnaire_patterns = [
            (r'(?:géré|gestion|gestionnaire)[\s:]+([A-Z][a-zA-ZÀ-ÿ\s\-]+(?:CCAS|CIAS|Mairie|Association|Office)[a-zA-ZÀ-ÿ\s\-]*)', 
             lambda m: m.group(1).strip()),
            (r'\b(CCAS|CIAS)\s+(?:de\s+)?([A-Z][a-zA-ZÀ-ÿ\s\-]+)', 
             lambda m: f"{m.group(1)} {m.group(2).strip()}"),
            (r'\b(?:Mairie|Commune)\s+de\s+([A-Z][a-zA-ZÀ-ÿ\s\-]+)', 
             lambda m: f"Mairie de {m.group(1).strip()}"),
            (r'\b(Association\s+[A-Z][a-zA-ZÀ-ÿ\s\-]+)', 
             lambda m: m.group(1).strip()),
            (r'\b(Mutualité[a-zA-ZÀ-ÿ\s\-]*)', 
             lambda m: m.group(1).strip()),
            (r'\b(Office\s+Public[a-zA-ZÀ-ÿ\s\-]*)', 
             lambda m: m.group(1).strip())
        ]
        
        for pattern, extractor in gestionnaire_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    gestionnaire = extractor(match)
                    if len(gestionnaire) > 3:
                        data['gestionnaire'] = clean_text(gestionnaire)[:100]
                        break
                except:
                    continue
        
        # 5. SERVICES - Détection contextuelle étendue
        services_detection = {
            'activités organisées': [
                r'\b(?:activités|animations|ateliers)[\s\w]*(?:organisées|proposées|collectives|quotidienn)',
                r'\bprogramme\s+d.activités\b',
                r'\banimation\s+sociale\b',
                r'\bsortie[s]?\s+organisée[s]?\b'
            ],
            'espace_partage': [
                r'\b(?:espace|salon|salle)[\s\w]*(?:partagé|commun|collective)\b',
                r'\blieu[x]?\s+de\s+(?:vie|convivialité|rencontre)\b',
                r'\bespace[s]?\s+de\s+détente\b'
            ],
            'conciergerie': [
                r'\bconciergerie\b',
                r'\baccueil\s+(?:permanent|de\s+jour|24h)\b',
                r'\bréception\b',
                r'\bservice[s]?\s+d.accueil\b'
            ],
            'personnel de nuit': [
                r'\bpersonnel\s+de\s+nuit\b',
                r'\bveilleur\s+(?:de\s+)?nuit\b',
                r'\bprésence\s+nocturne\b',
                r'\bsurveillance\s+24h\b'
            ],
            'commerces à pied': [
                r'\bcommerce[s]?\s+(?:à\s+pied|proximité|proche[s]?)\b',
                r'\bproximité[\s\w]*commerce[s]?\b',
                r'\baccès\s+piéton\b',
                r'\bcentre[\-\s]ville\s+accessible\b'
            ],
            'médecin intervenant': [
                r'\bmédecin\s+(?:intervenant|sur\s+site|présent)\b',
                r'\bcabinet\s+médical\s+(?:sur\s+place|intégré)\b',
                r'\bsoins?\s+médicaux\s+sur\s+site\b',
                r'\binfirmier[ère]?\s+(?:présent|disponible)\b'
            ]
        }
        
        for service, patterns in services_detection.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    if service not in data['services']:
                        data['services'].append(service)
                    break
        
        # 6. RESTAURATION - Détection fine
        resto_patterns = {
            'kitchenette': [
                r'\bkitchenette[s]?\b',
                r'\bcoin[\s\-]cuisine\s+(?:équipé|aménagé)\b',
                r'\bespace\s+cuisson\b',
                r'\bplaque[s]?\s+(?:de\s+)?cuisson\b',
                r'\bélément[s]?\s+de\s+cuisine\b'
            ],
            'resto_collectif': [
                r'\brestaurant\s+(?:collectif|commun|sur\s+place)\b',
                r'\bsalle\s+(?:à\s+manger|de\s+restaurant)\s+(?:commune|collective)\b',
                r'\brestauration\s+collective\b',
                r'\bservice\s+de\s+restauration\b'
            ],
            'resto_collectif_midi': [
                r'\brepas\s+(?:du\s+)?midi\b',
                r'\bdéjeuner\s+(?:servi|proposé|collectif)\b',
                r'\bservice\s+repas\s+midi\b'
            ],
            'portage_repas': [
                r'\bportage\s+de\s+repas\b',
                r'\blivraison\s+(?:de\s+)?repas\b',
                r'\brepas\s+(?:livrés|à\s+domicile)\b'
            ]
        }
        
        for resto_type, patterns in resto_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    data['restauration'][resto_type] = True
                    break
        
        # 7. TARIFICATION - Extraction avec CONTEXTE OBLIGATOIRE (pas de pattern nu)
        price_patterns = [
            # CONTEXTE FORT : loyer, tarif, redevance + mensuel
            (r'(?:loyer|redevance)[\s:]+(?:mensuel|par\s+mois|/mois)[\s:]*(\d{3,4})\s*(?:€|euros?)', True),
            (r'(?:tarif|prix)[\s:]+(?:mensuel|par\s+mois|/mois)[\s:]*(\d{3,4})\s*(?:€|euros?)', True),
            
            # CONTEXTE FORT : "à partir de" / "dès" + mois explicite
            (r'(?:à\s+partir\s+de|dès)[\s:]*(\d{3,4})\s*(?:€|euros?)[\s/]+(?:par\s+)?mois', True),
            
            # CONTEXTE FORT : prix + indication mensuelle explicite
            (r'(\d{3,4})\s*(?:€|euros?)[\s/]+par\s+mois', True),
            (r'(\d{3,4})\s*(?:€|euros?)[\s/]+mois', True),
        ]
        
        all_prices = []
        
        # Extraction STRICTE avec validation contextuelle obligatoire
        for pattern, high_confidence in price_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    price = int(match) if isinstance(match, str) else int(match[0])
                    
                    # Plage réaliste : 500-2500€ (réduit pour éviter outliers)
                    if 500 <= price <= 2500:
                        # Rejeter TOUS les multiples de 100 suspects
                        if price % 500 == 0:  # 500, 1000, 1500, 2000 = très suspects
                            continue
                        
                        all_prices.append(price)
                except:
                    continue
        
        # Si pas de prix trouvés, explorer les pages tarifs/prix
        if not all_prices:
            try:
                # Utiliser les liens récupérés avant le nettoyage
                explored_links = set()
                
                for link in all_links:
                    href = link.get('href', '')
                    text_link = link.get_text().strip().lower()
                    
                    # Vérifier si c'est un lien vers une page tarifs
                    if any(keyword in href.lower() or keyword in text_link 
                           for keyword in ['tarif', 'prix', 'cout']):
                        
                        # Construire l'URL complète
                        if href.startswith('/'):
                            tarif_url = urljoin(url, href)
                        elif href.startswith('http'):
                            tarif_url = href
                        else:
                            continue
                        
                        # Éviter les doublons
                        if tarif_url in explored_links:
                            continue
                        explored_links.add(tarif_url)
                        
                        # Scraper la page tarifs
                        try:
                            tarif_response = requests.get(tarif_url, headers=headers, timeout=15)
                            if tarif_response.ok:
                                tarif_soup = BeautifulSoup(tarif_response.content, 'html.parser')
                                tarif_text = tarif_soup.get_text(separator=' ', strip=True)
                                
                                # Extraire les prix de cette page
                                for pattern in price_patterns:
                                    matches = re.findall(pattern, tarif_text, re.IGNORECASE)
                                    for match in matches:
                                        try:
                                            if isinstance(match, tuple):
                                                if len(match) == 2 and match[1].isdigit():
                                                    if len(match[1]) == 3:
                                                        price = int(match[0] + match[1])
                                                    else:
                                                        price = int(match[0])
                                                else:
                                                    price = int(match[0])
                                            else:
                                                price = int(match)
                                            
                                            # Filtrer les prix réalistes pour les logements (exclure prestations < 500€)
                                            if 500 <= price <= 3000:
                                                all_prices.append(price)
                                        except:
                                            continue
                                
                                # Si on a trouvé des prix, arrêter l'exploration
                                if all_prices:
                                    break
                        except:
                            continue
                        
                        # Limiter à 1 page pour optimiser performance (réduit de 3 à 1)
                        if len(explored_links) >= 1:
                            break
            except:
                pass
        
        # Traitement final des prix - UNIQUEMENT si des prix réels ont été trouvés
        if all_prices and len(all_prices) > 0:
            # Supprimer les doublons et trier
            unique_prices = sorted(set(all_prices))
            data['tarification']['prix_min'] = min(unique_prices)
            data['tarification']['prix_max'] = max(unique_prices)
            
            # Fourchette selon règles métier
            min_price = min(unique_prices)
            if min_price < 750:
                data['tarification']['fourchette_prix'] = 'euro'
            elif 750 <= min_price <= 1500:
                data['tarification']['fourchette_prix'] = 'deux_euros'
            else:
                data['tarification']['fourchette_prix'] = 'trois_euros'
        else:
            # PAS de prix trouvés → laisser vide
            data['tarification'] = {'fourchette_prix': None, 'prix_min': None, 'prix_max': None}
        
        # 8. LOGEMENTS - Patterns sophistiqués
        logement_patterns = [
            r'\b(?:studio[s]?|T1|F1)\b',
            r'\b(?:T2|F2|deux\s+pièces)\b', 
            r'\b(?:T3|F3|trois\s+pièces)\b',
            r'\bappartement[s]?\s+(T[1-3]|F[1-3]|studio)\b',
            r'\blogement[s]?\s+(T[1-3]|F[1-3]|studio)\b'
        ]
        
        logements_found = set()
        for pattern in logement_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                match_clean = match.lower().strip()
                if match_clean in ['studio', 'f1', 't1']:
                    logements_found.add('T1')
                elif match_clean in ['f2', 't2', 'deux pièces']:
                    logements_found.add('T2')
                elif match_clean in ['f3', 't3', 'trois pièces']:
                    logements_found.add('T3')
        
        for logement in logements_found:
            data['logements_types'].append({'libelle': logement})
        
        # 9. MENTION AVP - Détection exhaustive
        avp_patterns = [
            r'\baide\s+à\s+la\s+vie\s+partagée\b',
            r'\bavp\b',
            r'\bprojet\s+avp\b',
            r'\béligible\s+(?:au\s+)?avp\b',
            r'\bfinancement\s+avp\b',
            r'\bsubvention\s+avp\b'
        ]
        for pattern in avp_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                data['mention_avp'] = True
                break
        
        # 10. DONNÉES AVP SPÉCIFIQUES - Extraction avancée pour établissements AVP
        if data.get('mention_avp'):
            avp_data = {}
            
            # Détection statut projet
            if any(pattern in text.lower() for pattern in ['ouvert depuis', 'fonctionne depuis', 'en activité']):
                avp_data['statut'] = 'ouvert'
            elif any(pattern in text.lower() for pattern in ['en projet', 'prochainement', 'développement']):
                avp_data['statut'] = 'en_projet'
            else:
                avp_data['statut'] = 'intention'
            
            # Extraction PVSP fondamentaux depuis le contenu
            pvsp_patterns = {
                'objectifs': [r'objectif[s]?\s*:?\s*([^.!?]{20,200})', r'but[s]?\s*:?\s*([^.!?]{20,200})'],
                'animation_vie_sociale': [r'animation[s]?\s*:?\s*([^.!?]{20,200})', r'activité[s]?\s*:?\s*([^.!?]{20,200})'],
                'gouvernance_partagee': [r'gouvernance\s*:?\s*([^.!?]{20,200})', r'concertation\s*:?\s*([^.!?]{20,200})'],
                'ouverture_au_quartier': [r'quartier\s*:?\s*([^.!?]{20,200})', r'territoire\s*:?\s*([^.!?]{20,200})'],
                'prevention_isolement': [r'isolement\s*:?\s*([^.!?]{20,200})', r'lien social\s*:?\s*([^.!?]{20,200})'],
                'participation_habitants': [r'participation\s*:?\s*([^.!?]{20,200})', r'implication\s*:?\s*([^.!?]{20,200})']
            }
            
            pvsp_fondamentaux = {}
            for pvsp_key, patterns in pvsp_patterns.items():
                extracted_text = ''
                for pattern in patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        extracted_text = match.group(1).strip()
                        break
                pvsp_fondamentaux[pvsp_key] = extracted_text
            
            avp_data['pvsp_fondamentaux'] = pvsp_fondamentaux
            
            # Extraction partenaires depuis le texte
            partenaires_patterns = [
                (r'\b(ccas|cias)\b', 'collectivite'),
                (r'\b(mutualité[^.]*)', 'association'),
                (r'\b(association\s+[A-Z][a-zA-Z\s]+)', 'association'),
                (r'\b(mairie|commune)\s+de\s+([A-Z][a-zA-Z\s-]+)', 'collectivite'),
                (r'\b(centre\s+communal\s+d\'action\s+sociale)', 'collectivite')
            ]
            
            partenaires = []
            for pattern, type_part in partenaires_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    nom = match.group(1) if match.lastindex == 1 else f"{match.group(1)} {match.group(2)}"
                    partenaires.append({'nom': nom.strip(), 'type': type_part})
                    if len(partenaires) >= 5:  # Limiter
                        break
                if len(partenaires) >= 5:
                    break
            
            avp_data['partenaires_principaux'] = partenaires
            
            # Extraction heures animation si mentionnées
            heures_pattern = r'(\d+)\s*heures?\s*(?:d\'animation|d\'activité|par\s+semaine)'
            match = re.search(heures_pattern, text, re.IGNORECASE)
            if match:
                try:
                    heures = float(match.group(1))
                    if 0 <= heures <= 168:
                        avp_data['heures_animation_semaine'] = heures
                except:
                    pass
            
            # Extraction public accueilli
            public_patterns = [
                r'public\s*:?\s*([^.!?]{20,200})',
                r'accueil[le]?\s*:?\s*([^.!?]{20,200})',
                r'destiné[e]?\s+aux?\s*([^.!?]{20,200})'
            ]
            for pattern in public_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    avp_data['public_accueilli'] = match.group(1).strip()
                    break
            
            # Ajout des données AVP au résultat principal
            data['avp_infos'] = avp_data
        
        return data
        
    except Exception as e:
        # Échouer silencieusement
        return {}


def scrape_website(url: str) -> Dict[str, Any]:
    """Scraper amélioré avec validation stricte et extraction intelligente"""
    if not requests or not BeautifulSoup:
        return {}
        
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=25)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        
        # Extraction des données avec validation stricte
        data = {
            'email': None,
            'telephone': None, 
            'site_web': url,
            'presentation': None,
            'gestionnaire': None,
            'mention_avp': False,
            'restauration': {
                'kitchenette': False,
                'resto_collectif_midi': False,
                'resto_collectif': False,
                'portage_repas': False
            },
            'logements_types': [],
            'tarification': {
                'fourchette_prix': None,
                'prix_min': None,
                'prix_max': None
            },
            'services': []
        }
        
        # Email avec validation améliorée
        email_patterns = [
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
        ]
        for pattern in email_patterns:
            emails = re.findall(pattern, text)
            if emails:
                email_candidate = emails[0] if isinstance(emails[0], str) else emails[0][0]
                validated_email = normalize_email(email_candidate)
                if validated_email:
                    data['email'] = validated_email
                    break
        
        # Téléphone avec patterns multiples
        phone_patterns = [
            r'(?:\+33\s?|0)(?:\d[\s\.\-]?){9}',
            r'Tél\s*:?\s*((?:\+33\s?|0)(?:\d[\s\.\-]?){9})',
            r'Téléphone\s*:?\s*((?:\+33\s?|0)(?:\d[\s\.\-]?){9})'
        ]
        for pattern in phone_patterns:
            phones = re.findall(pattern, text, re.IGNORECASE)
            if phones:
                phone_candidate = phones[0] if isinstance(phones[0], str) else phones[0][0]
                validated_phone = normalize_phone_fr(phone_candidate)
                if validated_phone:
                    data['telephone'] = validated_phone
                    break
        
        # Présentation améliorée
        presentation_sources = []
        if soup.title:
            presentation_sources.append(soup.title.get_text(strip=True))
        
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            presentation_sources.append(meta_desc['content'])
        
        # Premier paragraphe significatif
        paragraphs = soup.find_all('p')
        for p in paragraphs[:3]:
            p_text = p.get_text(strip=True)
            if len(p_text) > 50:
                presentation_sources.append(p_text)
                break
        
        if presentation_sources:
            # Prendre la source la plus informative
            best_presentation = max(presentation_sources, key=len)
            data['presentation'] = clean_text(best_presentation)[:500]
        
        # Gestionnaire avec détection intelligente améliorée
        gestionnaire_patterns = [
            (r'\b(ccas|cias)\b.*?([A-Z][a-zA-Z\s-]+)', lambda m: f"CCAS {m.group(2).strip()}"),
            (r'\bmairie\s+de\s+([A-Z][a-zA-Z\s-]+)', lambda m: f"Mairie de {m.group(1).strip()}"),
            (r'\bcommune\s+de\s+([A-Z][a-zA-Z\s-]+)', lambda m: f"Commune de {m.group(1).strip()}"),
            (r'\b(mutualité[^.]*)', lambda m: m.group(1).strip().title()),
            (r'\b(association\s+[A-Z][a-zA-Z\s]+)', lambda m: m.group(1).strip()),
            (r'\b(ccas|cias)\b', "CCAS"),
            (r'\boffice\s+public', "Office Public"),
            (r'\b(admr|apajh|apei)\b', lambda m: m.group(1).upper())
        ]
        
        for pattern, replacement in gestionnaire_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if callable(replacement):
                    gestionnaire = replacement(match)
                else:
                    gestionnaire = replacement
                data['gestionnaire'] = clean_text(gestionnaire)[:100]
                break
        
        # Détection AVP stricte et exhaustive
        avp_patterns = [
            r'\baide\s+à\s+la\s+vie\s+partagée\b',
            r'\bavp\b',
            r'\bprojet\s+avp\b',
            r'\béligible\s+avp\b',
            r'\bfinancement\s+avp\b'
        ]
        for pattern in avp_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                data['mention_avp'] = True
                break
        
        # Services selon liste strictement autorisée avec patterns améliorés
        services_detection = {
            'activités organisées': [
                r'\bactivités\s+(organisées|collectives)\b',
                r'\banimations?\b.*\b(quotidien|régulière)',
                r'\bateliers?\b.*\b(proposés|organisés)',
                r'\bprogramme\s+d\'activités\b'
            ],
            'espace_partage': [
                r'\bespace\s+(partagé|commun)\b',
                r'\bsalon\s+commun\b',
                r'\bsalle\s+(commune|collective)\b',
                r'\blieu\s+de\s+vie\s+partagé\b'
            ],
            'conciergerie': [
                r'\bconciergerie\b',
                r'\baccueil\s+(permanent|de\s+jour)\b',
                r'\bréception\b',
                r'\bservice\s+d\'accueil\b'
            ],
            'personnel de nuit': [
                r'\bpersonnel\s+de\s+nuit\b',
                r'\bveilleur\s+de\s+nuit\b',
                r'\bprésence\s+nocturne\b',
                r'\bsurveillance\s+24h\b'
            ],
            'commerces à pied': [
                r'\bcommerces?\s+(à\s+pied|proximité)\b',
                r'\bproximité\s+(commerces?|services)\b',
                r'\baccès\s+piéton\s+aux\s+commerces\b'
            ],
            'médecin intervenant': [
                r'\bmédecin\s+(intervenant|sur\s+site)\b',
                r'\bcabinet\s+médical\s+(sur\s+place|intégré)\b',
                r'\bsoins?\s+médicaux\s+sur\s+site\b',
                r'\binfirmier\s+présent\b'
            ]
        }
        
        for service, patterns in services_detection.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    if service not in data['services']:
                        data['services'].append(service)
                    break
        
        # Restauration avec détection précise et contextualisée
        resto_patterns = {
            'kitchenette': [
                r'\bkitchenette\b',
                r'\bcoin\s+cuisine\s+(équipé|aménagé)\b',
                r'\bespace\s+cuisson\b',
                r'\bplaques?\s+de\s+cuisson\b'
            ],
            'resto_collectif': [
                r'\brestaurant\s+(collectif|commun)\b',
                r'\bsalle\s+à\s+manger\s+(commune|collective)\b',
                r'\brestauration\s+collective\b',
                r'\bservice\s+de\s+restauration\b'
            ],
            'resto_collectif_midi': [
                r'\brepas\s+du\s+midi\b',
                r'\bdéjeuner\s+(servi|proposé|collectif)\b',
                r'\bservice\s+repas\s+midi\b'
            ],
            'portage_repas': [
                r'\bportage\s+de\s+repas\b',
                r'\blivraison\s+de\s+repas\b',
                r'\brepas\s+(livrés|à\s+domicile)\b'
            ]
        }
        
        for resto_type, patterns in resto_patterns.items():
            for pattern in patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    data['restauration'][resto_type] = True
                    break
        
        # Tarification avec patterns contextuels et filtrage des faux positifs
        price_patterns = [
            # Patterns avec contexte fort (prioritaires)
            (r'(?:loyer|tarif|redevance)[\s:]+(?:mensuel|par\s+mois|/mois)?[\s:]*(\d{3,5})\s*(?:€|euros?)', True),
            (r'(?:à\s+partir\s+de|dès)[\s:]*(\d{3,5})\s*(?:€|euros?)[\s/]*(?:mois|mensuel)?', True),
            (r'(\d{3,5})\s*(?:€|euros?)\s*(?:par\s+mois|/mois|mensuel)', True),
            # Patterns avec contexte faible (nécessitent validation)
            (r'prix[\s:]+(\d{3,5})\s*(?:€|euros?)', False),
            (r'(\d{3,5})\s*(?:€|euros?)\s*(?:de\s+)?(loyer|redevance)', False)
        ]
        
        all_prices = []
        for pattern, high_confidence in price_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    price = int(match[0] if isinstance(match, tuple) else match)
                    # Filtrer les prix réalistes (500-3000€)
                    if 500 <= price <= 3000:
                        # Filtrage des faux positifs pour patterns à faible confiance
                        if not high_confidence:
                            # Rejeter les multiples de 1000 exacts
                            if price % 1000 == 0:
                                continue
                        
                        all_prices.append(price)
                except (ValueError, IndexError):
                    pass
        
        # Traitement final des prix - SCRAPING UNIQUEMENT
        if all_prices and len(all_prices) > 0:
            data['tarification']['prix_min'] = min(all_prices)
            data['tarification']['prix_max'] = max(all_prices)
            
            # Calculer fourchette selon vos règles
            min_price = min(all_prices)
            if min_price < 750:
                data['tarification']['fourchette_prix'] = 'euro'
            elif 750 <= min_price <= 1500:
                data['tarification']['fourchette_prix'] = 'deux_euros'
            else:
                data['tarification']['fourchette_prix'] = 'trois_euros'
        else:
            # PAS de prix trouvés → laisser vide
            data['tarification'] = {'fourchette_prix': None, 'prix_min': None, 'prix_max': None}
        
        # Logements avec normalisation stricte et patterns améliorés
        logement_patterns = [
            r'\b(T[1-3]|studio|F[1-3]|t1bis|t2bis)\b',
            r'\b(une|deux|trois)\s+pièces?\b',
            r'\bappartements?\s+(T[1-3]|F[1-3])\b',
            r'\blogements?\s+(T[1-3]|F[1-3])\b'
        ]
        
        logements_found = set()
        for pattern in logement_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                match_clean = match.upper().strip()
                # Normalisation selon vos règles
                if match_clean.lower() in ['studio', 'f1', 't1bis']:
                    logements_found.add('T1')
                elif match_clean.lower() in ['f2', 't2bis', 'deux pièces']:
                    logements_found.add('T2')
                elif match_clean.lower() in ['f3', 'trois pièces']:
                    logements_found.add('T3')
                elif match_clean in ['T1', 'T2', 'T3']:
                    logements_found.add(match_clean)
                elif match_clean.lower() == 'une pièce':
                    logements_found.add('T1')
        
        for logement in logements_found:
            data['logements_types'].append({'libelle': logement})
        
        return data
        
    except Exception as e:
        # Échouer silencieusement
        return {}

def search_web(query: str, provider: str, tavily_key: str = None, serpapi_key: str = None, debug: bool = False) -> List[str]:
    """Rechercher des URLs via API de recherche"""
    if not requests:
        if debug:
            st.warning("⚠️ Module requests non disponible pour la recherche web")
        return []
    
    if debug:
        st.write(f"🔍 **Recherche web** : '{query}' via {provider}")
    
    urls = []
    
    try:
        if provider == "Tavily":
            if not tavily_key:
                if debug:
                    st.error("❌ Clé Tavily manquante dans la configuration")
                return []
                
            if debug:
                st.write("📡 Appel API Tavily en cours...")
            
            response = requests.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": tavily_key,
                    "query": query,
                    "include_answer": False,
                    "max_results": 3  # Réduit de 5 à 3 pour accélérer
                },
                timeout=10  # Réduit de 20s à 10s
            )
            
            if debug:
                st.write(f"📊 Statut réponse Tavily: {response.status_code}")
            
            if response.ok:
                data = response.json()
                if debug:
                    st.write(f"📋 Données brutes Tavily: {len(data.get('results', []))} résultats")
                urls = [item.get('url') for item in data.get('results', []) if item.get('url')]
                if debug:
                    st.write(f"🔗 URLs extraites: {len(urls)}")
            elif debug:
                st.error(f"❌ Erreur API Tavily: {response.status_code} - {response.text[:200]}")
        
        elif provider == "SerpAPI":
            if not serpapi_key:
                if debug:
                    st.error("❌ Clé SerpAPI manquante dans la configuration")
                return []
                
            if debug:
                st.write("📡 Appel API SerpAPI en cours...")
            
            params = {
                "q": query,
                "engine": "google", 
                "api_key": serpapi_key,
                "num": 5
            }
            response = requests.get("https://serpapi.com/search.json", params=params, timeout=20)
            
            if debug:
                st.write(f"📊 Statut réponse SerpAPI: {response.status_code}")
            
            if response.ok:
                data = response.json()
                organic_results = data.get('organic_results', [])
                if debug:
                    st.write(f"📋 Données brutes SerpAPI: {len(organic_results)} résultats")
                urls = [item.get('link') for item in organic_results if item.get('link')]
                if debug:
                    st.write(f"🔗 URLs extraites: {len(urls)}")
            elif debug:
                st.error(f"❌ Erreur API SerpAPI: {response.status_code} - {response.text[:200]}")
        elif debug:
            st.warning(f"⚠️ Provider '{provider}' non reconnu ou clé manquante")
    
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, Exception) as e:
        if debug:
            st.error(f"❌ Erreur recherche web {provider}: {str(e)}")
    
    if urls:
        # Filtrer et prioriser les sources officielles
        official_urls = []
        other_urls = []
        
        for url in urls:
            if any(domain in url.lower() for domain in [
                'mairie', 'ccas', 'cias', '.gouv.fr', '.fr/ville', 
                'admr', 'apajh', 'apei', 'office', 'oph'
            ]):
                official_urls.append(url)
            else:
                other_urls.append(url)
        
        final_urls = official_urls + other_urls
        if debug:
            st.write(f"✅ **{len(final_urls)} URLs trouvées** ({len(official_urls)} officielles, {len(other_urls)} autres)")
            # Afficher les premières URLs trouvées
            for i, url in enumerate(final_urls[:3]):
                st.write(f"  🔗 {i+1}. {url[:80]}...")
        
        return final_urls
    else:
        if debug:
            st.warning("⚠️ Aucune URL trouvée par la recherche web")
        return []

def enrich_with_ai_alone(data: Dict[str, Any], provider: str, model: str, 
                         openai_key: str = None, groq_key: str = None) -> Dict[str, Any]:
    """Enrichir via IA seule avec prompts optimisés et validation stricte"""
    if provider == "OpenAI" and openai_key and OpenAI:
        client = OpenAI(api_key=openai_key)
    elif provider == "Groq" and groq_key and GroqClient:
        client = GroqClient(api_key=groq_key)
    else:
        return {}

    # Construire le contexte enrichi
    context = f"Nom: {data.get('nom', '')}\nCommune: {data.get('commune', '')}"
    if data.get('presentation'):
        context += f"\nPrésentation: {data.get('presentation')}"
    if data.get('gestionnaire'):
        context += f"\nGestionnaire: {data.get('gestionnaire')}"
    if data.get('site_web'):
        context += f"\nSite: {data.get('site_web')}"
    if data.get('adresse'):
        context += f"\nAdresse: {data.get('adresse')}"

    prompt = f"""Tu es un expert en habitat senior français. Analyse cet établissement et enrichis STRICTEMENT selon les règles métier.


{context}

RÈGLES MÉTIER STRICTES (OBLIGATOIRES):

1. **SOUS_CATEGORIE** (cohérence absolue, par ordre de priorité):
   
   **RÈGLES DE DÉDUCTION STRICTES (PRIORITAIRES):**
   - "CetteFamille" dans nom/gestionnaire → TOUJOURS "Maison d'accueil familial"
   - "Ages & Vie" dans nom/gestionnaire → TOUJOURS "Colocation avec services"
   - "Intergénérationnel" dans nom/présentation → TOUJOURS "Habitat intergénérationnel"
   - "MARPA" dans nom → TOUJOURS "MARPA"
   - "Béguinage" dans nom → TOUJOURS "Béguinage"
   - "Domitys", "Senioriales", "ABC Résidences" → TOUJOURS "Résidence services seniors"
   - "CCAS", "résidence autonomie" → TOUJOURS "Résidence autonomie"
   
   **SOUS-CATÉGORIES AUTORISÉES:**
   - Résidence autonomie (très fréquent)
   - Résidence services seniors (très fréquent)
   - MARPA (fréquent)
   - Colocation avec services (fréquent)
   - Habitat intergénérationnel (rare)
   - Accueil familial (fréquent)
   - Maison d'accueil familial (fréquent)
   - Béguinage (rare)
   - Village seniors (rare)
   - Habitat alternatif (TRÈS RARE - utiliser uniquement si aucune autre catégorie ne correspond)
   
   ⚠️ **IMPORTANT:** "Habitat alternatif" est une catégorie EXCEPTION à utiliser SEULEMENT si:
   - Aucune des 9 autres catégories ne correspond
   - Le concept est vraiment innovant/atypique
   - Ne PAS l'utiliser par défaut ou par facilité

3. **PUBLIC_CIBLE** (énumération stricte):
   ["personnes_agees", "personnes_handicapees", "mixtes", "alzheimer_accessible"]

4. **SERVICES** (plusieurs items possibles):
   ["activités organisées", "espace_partage", "conciergerie", "personnel de nuit", "commerces à pied", "médecin intervenant"]

5. **RESTAURATION** (booléens selon services détectés):
   - kitchenette: Coin cuisine/kitchenette dans les logements individuels
   - resto_collectif: Restaurant/salle à manger commune pour tous les repas
   - portage_repas: Service de livraison de repas à domicile/en chambre

6. **TARIFICATION**:
   - ⚠️ NE REMPLIS PAS prix_min/prix_max (toujours null, ils seront extraits par scraping uniquement)
   - Déduis UNIQUEMENT fourchette_prix si tu trouves des indices de gamme de prix :
     * 'euro' : si mention "abordable", "social", "accessible", ou <750€
     * 'deux_euros' : si mention "moyen", "modéré", ou 750-1500€  
     * 'trois_euros' : si mention "premium", "haut de gamme", ou >1500€
   - Si aucun indice de gamme → fourchette_prix: null

7. **LOGEMENTS_TYPES**: T1, T2, T3 uniquement avec accessibilité PMR et plain-pied si mentionné

8. **MENTION_AVP**: true uniquement si "aide à la vie partagée" explicitement mentionné

9. **DONNÉES AVP** (pour établissements éligibles AVP uniquement):
   - statut: "intention"|"en_projet"|"ouvert" selon indices dans le contenu


TÂCHES:
1. Déduis intelligemment les champs manquants et complète selon le contexte
2. Assure la cohérence habitat_type/sous_categorie/eligibilite_statut
6. RESPECTE ABSOLUMENT les énumérations autorisées

Réponds UNIQUEMENT en JSON valide avec cette structure exacte:
{{"mention_avp":false,"habitat_type":"","sous_categorie":"","eligibilite_statut":"","public_cible":[],"services":[],"restauration":{{"kitchenette":false,"resto_collectif_midi":false,"resto_collectif":false,"portage_repas":false}},"tarification":{{"fourchette_prix":null}},"logements_types":[{{"type":"","pmr":false,"plain_pied":false}}],"presentation":"","avp_infos":{{"statut":"","pvsp_fondamentaux":{{"objectifs":"","animation_vie_sociale":"","gouvernance_partagee":"","ouverture_au_quartier":"","prevention_isolement":"","participation_habitants":""}},"public_accueilli":"","modalites_admission":"","partenaires_principaux":[],"intervenants":[],"heures_animation_semaine":null,"infos_complementaires":""}}}}"""

    result = _call_ai_api(client, model, prompt, data.get('nom'))

    # Validation stricte post-IA
    validated_result = validate_ai_enrichment(result, data)
    
    # Fallback scraping si données critiques manquantes
    if needs_scraping_fallback(validated_result):
        st.info("🔄 Fallback scraping pour compléter les données")
        if data.get('site_web'):
            scraped = scrape_website(data['site_web'])
            validated_result = merge_scraped_data(validated_result, scraped)
    
    return validated_result

def enrich_with_ai_websearch(data: Dict[str, Any], websearch_content: str, provider: str, model: str,
                            openai_key: str = None, groq_key: str = None) -> Dict[str, Any]:
    """Enrichir via IA + websearch avec validation renforcée"""
    if provider == "OpenAI" and openai_key and OpenAI:
        client = OpenAI(api_key=openai_key)
    elif provider == "Groq" and groq_key and GroqClient:
        client = GroqClient(api_key=groq_key)
    else:
        return {}
        
    # Filtrer et structurer le contenu websearch (optimisé: 2000 chars pour équilibre performance/qualité)
    filtered_content = websearch_content[:2000]  # Optimisé de 3000 à 2000
    
    prompt = f"""Tu es un expert en habitat senior français. Analyse cet établissement et enrichis STRICTEMENT selon les règles métier.

ÉTABLISSEMENT: {data.get('nom', '')} - {data.get('commune', '')}

CONTENU WEB TROUVÉ:
{filtered_content}

RÈGLES MÉTIER STRICTES (OBLIGATOIRES):

1. **SOUS_CATEGORIE** (cohérence absolue):
   - Résidence services séniors
   - Colocation avec services
   - Habitat intergénérationnel
   - Accueil familial
   - Maison d'accueil familial
   - Béguinage
   - Village séniors
   - Habitat alternatif
   - Résidence autonomie
   - MARPA
   ⚠️ INTERDICTION ABSOLUE de créer des sous-catégories non listées.    

   **RÈGLES DE DÉDUCTION GESTIONNAIRE**:
   - "Cette Famille" → TOUJOURS "Maison d'accueil familial"
   - "Ages & Vie" → TOUJOURS "Colocation avec services"
   - Mention "intergénérationnel" → TOUJOURS "Habitat intergénérationnel"

3. **PUBLIC_CIBLE** (énumération stricte):
   ["personnes_agees", "personnes_handicapees", "mixtes", "alzheimer_accessible"]

4. **SERVICES** (plusieurs items possibles):
   ["activités organisées", "espace_partage", "conciergerie", "personnel de nuit", "commerces à pied", "médecin intervenant"]

5. **RESTAURATION** (booléens selon services détectés):
   - kitchenette: Coin cuisine/kitchenette dans les logements individuels
   - resto_collectif: Restaurant/salle à manger commune pour tous les repas
   - portage_repas: Service de livraison de repas à domicile/en chambre

6. **TARIFICATION**:
   - ⚠️ NE REMPLIS PAS prix_min/prix_max (toujours null, scraping uniquement)
   - Déduis UNIQUEMENT fourchette_prix si tu trouves des indices de gamme :
     * 'euro' : abordable/social (<750€)
     * 'deux_euros' : moyen/modéré (750-1500€)
     * 'trois_euros' : premium/haut de gamme (>1500€)
   - Si aucun indice → fourchette_prix: null

7. **LOGEMENTS_TYPES**: T1, T2, T3 uniquement avec accessibilité PMR et plain-pied si mentionné

8. **MENTION_AVP**: true uniquement si "aide à la vie partagée" explicitement mentionné

9. **DONNÉES AVP** (pour établissements éligibles AVP uniquement):
   - statut: "intention"|"en_projet"|"ouvert" selon indices dans le contenu

TÂCHES:
1. Déduis intelligemment les champs manquants et complète selon le contexte
2. Assure la cohérence habitat_type/sous_categorie/eligibilite_statut
6. RESPECTE ABSOLUMENT les énumérations autorisées

Réponds UNIQUEMENT en JSON valide avec cette structure exacte:
{{"mention_avp":false,"habitat_type":"","sous_categorie":"","eligibilite_statut":"","public_cible":[],"services":[],"restauration":{{"kitchenette":false,"resto_collectif_midi":false,"resto_collectif":false,"portage_repas":false}},"tarification":{{"fourchette_prix":null}},"logements_types":[{{"type":"","pmr":false,"plain_pied":false}}],"presentation":"","avp_infos":{{"statut":"","pvsp_fondamentaux":{{"objectifs":"","animation_vie_sociale":"","gouvernance_partagee":"","ouverture_au_quartier":"","prevention_isolement":"","participation_habitants":""}},"public_accueilli":"","modalites_admission":"","partenaires_principaux":[],"intervenants":[],"heures_animation_semaine":null,"infos_complementaires":""}}}}"""

    result = _call_ai_api(client, model, prompt, data.get('nom'))

    # Validation stricte post-IA
    validated_result = validate_ai_enrichment(result, data)
    
    # Fallback scraping si données critiques manquantes
    if needs_scraping_fallback(validated_result):
        st.info("🔄 Fallback scraping pour compléter les données (websearch)")
        if data.get('site_web'):
            scraped = scrape_website(data['site_web'])
            validated_result = merge_scraped_data(validated_result, scraped)
    
    return validated_result

def validate_ai_enrichment(ai_result: Dict[str, Any], original_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validation stricte des résultats d'enrichissement IA"""
    validated = {}
    
    # Validation habitat_type et sous_categorie avec cohérence
    valid_combinations = {
        'residence-services': 'Résidence services séniors',
        'colocation-services': 'Colocation avec services',
        'habitat-intergenerationnel': 'Habitat intergénérationnel',
        'accueil-familial': 'Accueil familial',
        'maison-accueil-familial': 'Maison d\'accueil familial',
        'beguinage': 'Béguinage',
        'village-seniors': 'Village séniors',
        'habitat-alternatif': 'Habitat alternatif',
        'residence-autonomie': 'Résidence autonomie',
        'marpa': 'MARPA'
    }
    
    # Déduction intelligente selon gestionnaire ET présentation
    gestionnaire = original_data.get('gestionnaire', '').lower()
    presentation = original_data.get('presentation', '').lower()
    nom = original_data.get('nom', '').lower()
    sous_categories_csv = original_data.get('sous_categories', '').lower()
    
    # Priorité absolue aux règles gestionnaire (détection améliorée)
    maf_detected = False
    all_text = f"{gestionnaire} {presentation} {nom} {sous_categories_csv}"
    
    if 'cette famille' in all_text:
        maf_detected = True
    elif 'maison d\'accueil familial' in all_text:
        maf_detected = True
    elif 'maison accueil familial' in all_text:
        maf_detected = True
    elif "maison d'accueil familial" in all_text:
        maf_detected = True
    
    if maf_detected:
        validated['habitat_type'] = 'habitat_partage'
        validated['sous_categorie'] = 'Maison d\'accueil familial'
    elif 'ages & vie' in gestionnaire or 'ages et vie' in gestionnaire:
        validated['habitat_type'] = 'habitat_partage'  # Correct selon schéma
        validated['sous_categorie'] = 'Colocation avec services'
    elif 'intergénérationnel' in nom or 'intergénérationnel' in presentation:
        validated['habitat_type'] = 'habitat_partage'  # Correct selon schéma
        validated['sous_categorie'] = 'Habitat intergénérationnel'
    else:
        # Fallback sur résultats IA avec mapping habitat_type corrigé
        habitat_type_ai = ai_result.get('habitat_type')
        sous_categorie = ai_result.get('sous_categorie')
        
        # Mapping vers les 3 habitat_type autorisés selon schéma
        habitat_type_mapping = {
            'residence-services': 'residence',
            'residence-autonomie': 'residence', 
            'marpa': 'residence',
            'colocation-services': 'habitat_partage',
            'habitat-intergenerationnel': 'habitat_partage',
            'maison-accueil-familial': 'habitat_partage',
            'habitat-inclusif': 'habitat_partage',
            'accueil-familial': 'habitat_partage',
            'habitat-alternatif': 'habitat_partage',
            'beguinage': 'logement_independant',
            'village-seniors': 'logement_independant'
        }
        
        if habitat_type_ai in valid_combinations and sous_categorie == valid_combinations[habitat_type_ai]:
            validated['habitat_type'] = habitat_type_mapping.get(habitat_type_ai, habitat_type_ai)
            validated['sous_categorie'] = sous_categorie
        
        # Déduction eligibilite_statut selon nouvelles règles
        if sous_categorie:
            eligibilite = deduce_eligibilite(sous_categorie, ai_result.get('mention_avp', False))
            validated['eligibilite_statut'] = eligibilite
    
    # Validation public_cible
    public_cible_allowed = ["personnes_agees", "personnes_handicapees", "mixtes", "alzheimer_accessible"]
    public_cible = ai_result.get('public_cible', [])
    if isinstance(public_cible, list):
        validated_public = [p for p in public_cible if p in public_cible_allowed]
        if validated_public:
            validated['public_cible'] = validated_public
    
    # Validation services
    services_allowed = ["activités organisées", "espace_partage", "conciergerie", "personnel de nuit", "commerces à pied", "médecin intervenant"]
    services = ai_result.get('services', [])
    if isinstance(services, list):
        validated_services = [s for s in services if s in services_allowed]
        validated['services'] = validated_services
    
    # Validation tarification - UNIQUEMENT fourchette_prix de l'IA, JAMAIS les prix
    tarif = ai_result.get('tarification', {})
    if isinstance(tarif, dict):
        validated_tarif = {}
        
        # UNIQUEMENT fourchette_prix (classification qualitative de l'IA)
        fourchette = tarif.get('fourchette_prix')
        if fourchette in ['euro', 'deux_euros', 'trois_euros']:
            validated_tarif['fourchette_prix'] = fourchette
        
        # NE JAMAIS utiliser prix_min/prix_max de l'IA
        # Ces valeurs viendront UNIQUEMENT du scraping
        
        if validated_tarif:
            validated['tarification'] = validated_tarif
    
    # Validation logements_types avec PMR et plain_pied
    logements = ai_result.get('logements_types', [])
    if isinstance(logements, list):
        validated_logements = []
        for logement in logements:
            if isinstance(logement, dict):
                # Support ancien format (string) et nouveau format (dict)
                if 'type' in logement:
                    type_logement = logement['type']
                elif 'libelle' in logement:
                    type_logement = logement['libelle']
                elif isinstance(logement, str):
                    type_logement = logement
                else:
                    continue
                    
                if type_logement in ['T1', 'T2', 'T3']:
                    validated_logement = {
                        'libelle': type_logement,
                        'pmr': logement.get('pmr', False) if isinstance(logement, dict) else False,
                        'plain_pied': logement.get('plain_pied', False) if isinstance(logement, dict) else False
                    }
                    validated_logements.append(validated_logement)
        validated['logements_types'] = validated_logements
    
    # Validation restauration
    resto = ai_result.get('restauration', {})
    if isinstance(resto, dict):
        validated_resto = {}
        for field in ['kitchenette', 'resto_collectif_midi', 'resto_collectif', 'portage_repas']:
            if field in resto and isinstance(resto[field], bool):
                validated_resto[field] = resto[field]
        validated['restauration'] = validated_resto
    
    # Validation coordonnées
    if 'email' in ai_result:
        email = normalize_email(ai_result['email'])
        if email:
            validated['email'] = email
    
    if 'telephone' in ai_result:
        phone = normalize_phone_fr(ai_result['telephone'])
        if phone:
            validated['telephone'] = phone
    
    # Autres champs texte
    for field in ['presentation', 'gestionnaire']:
        if field in ai_result and isinstance(ai_result[field], str):
            text_value = clean_text(ai_result[field])
            if text_value and len(text_value) > 3:
                validated[field] = text_value
    
    # Mention AVP
    if 'mention_avp' in ai_result and isinstance(ai_result['mention_avp'], bool):
        validated['mention_avp'] = ai_result['mention_avp']
    
    # Validation données AVP
    avp_infos = ai_result.get('avp_infos', {})
    if isinstance(avp_infos, dict) and avp_infos:
        validated_avp = {}
        
        # Validation statut AVP
        statut = avp_infos.get('statut')
        if statut in ['intention', 'en_projet', 'ouvert']:
            validated_avp['statut'] = statut
        
        # Validation PVSP fondamentaux
        pvsp = avp_infos.get('pvsp_fondamentaux', {})
        if isinstance(pvsp, dict):
            validated_pvsp = {}
            required_pvsp_keys = ['objectifs', 'animation_vie_sociale', 'gouvernance_partagee', 
                                'ouverture_au_quartier', 'prevention_isolement', 'participation_habitants']
            for key in required_pvsp_keys:
                if key in pvsp and isinstance(pvsp[key], str):
                    validated_pvsp[key] = pvsp[key][:500]  # Limiter longueur
                else:
                    validated_pvsp[key] = ''  # Valeur par défaut
            validated_avp['pvsp_fondamentaux'] = validated_pvsp
        
        # Validation textes descriptifs
        for field in ['public_accueilli', 'modalites_admission', 'infos_complementaires']:
            if field in avp_infos and isinstance(avp_infos[field], str):
                text_value = clean_text(avp_infos[field])
                if text_value:
                    validated_avp[field] = text_value[:1000]  # Limiter longueur
        
        # Validation partenaires principaux
        partenaires = avp_infos.get('partenaires_principaux', [])
        if isinstance(partenaires, list):
            validated_partenaires = []
            for partenaire in partenaires[:10]:  # Limiter nombre
                if isinstance(partenaire, dict) and 'nom' in partenaire:
                    nom = clean_text(partenaire['nom'])
                    type_part = partenaire.get('type', 'autre')
                    if nom and type_part in ['gestionnaire', 'association', 'collectivite', 'sante', 'autre']:
                        validated_partenaires.append({'nom': nom[:100], 'type': type_part})
            validated_avp['partenaires_principaux'] = validated_partenaires
        
        # Validation intervenants
        intervenants = avp_infos.get('intervenants', [])
        if isinstance(intervenants, list):
            validated_intervenants = []
            for intervenant in intervenants[:20]:  # Limiter nombre
                if isinstance(intervenant, dict) and 'nom' in intervenant:
                    nom = clean_text(intervenant['nom'])
                    specialite = clean_text(intervenant.get('specialite', ''))
                    if nom:
                        validated_intervenants.append({'nom': nom[:100], 'specialite': specialite[:100]})
            validated_avp['intervenants'] = validated_intervenants
        
        # Validation heures animation
        heures = avp_infos.get('heures_animation_semaine')
        if isinstance(heures, (int, float)) and 0 <= heures <= 168:
            validated_avp['heures_animation_semaine'] = float(heures)
        
        if validated_avp:
            validated['avp_infos'] = validated_avp
    
    return validated

def needs_scraping_fallback(data: Dict[str, Any]) -> bool:
    """Détermine si un fallback scraping est nécessaire"""
    # Critères pour fallback
    missing_services = not data.get('services')
    missing_tarif = not data.get('tarification') or (
        data.get('tarification', {}).get('prix_min') is None and 
        data.get('tarification', {}).get('prix_max') is None
    )
    missing_contact = not data.get('email') and not data.get('telephone')
    
    return missing_services or missing_tarif or missing_contact

def merge_scraped_data(ai_data: Dict[str, Any], scraped_data: Dict[str, Any]) -> Dict[str, Any]:
    """Fusion intelligente des données IA et scraping"""
    merged = ai_data.copy()
    
    # Compléter services manquants
    if not merged.get('services') and scraped_data.get('services'):
        merged['services'] = scraped_data['services']
    
    # Compléter tarification manquante
    if not merged.get('tarification') and scraped_data.get('tarification'):
        merged['tarification'] = scraped_data['tarification']
    elif merged.get('tarification') and scraped_data.get('tarification'):
        tarif = merged['tarification']
        scraped_tarif = scraped_data['tarification']
        
        if tarif.get('prix_min') is None and scraped_tarif.get('prix_min'):
            tarif['prix_min'] = scraped_tarif['prix_min']
        if tarif.get('prix_max') is None and scraped_tarif.get('prix_max'):
            tarif['prix_max'] = scraped_tarif['prix_max']
        if not tarif.get('fourchette_prix') and scraped_tarif.get('fourchette_prix'):
            tarif['fourchette_prix'] = scraped_tarif['fourchette_prix']
    
    # Compléter coordonnées manquantes
    if not merged.get('email') and scraped_data.get('email'):
        merged['email'] = scraped_data['email']
    if not merged.get('telephone') and scraped_data.get('telephone'):
        merged['telephone'] = scraped_data['telephone']
    
    # Compléter restauration si manquante
    if not merged.get('restauration') and scraped_data.get('restauration'):
        merged['restauration'] = scraped_data['restauration']
    
    # Compléter logements si manquants
    if not merged.get('logements_types') and scraped_data.get('logements_types'):
        merged['logements_types'] = scraped_data['logements_types']
    
    return merged

def _call_ai_api(client, model: str, prompt: str, etablissement_nom: str) -> Dict[str, Any]:
    """Appeler l'API IA et parser la réponse JSON avec fallback automatique pour Groq"""
    try:
        st.write(f"🤖 Appel IA pour: {etablissement_nom}")
        
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=2500  # Augmenté pour éviter troncature JSON
        )
        
        result = response.choices[0].message.content.strip()
        
        # Nettoyer et parser JSON avec gestion améliorée
        # Supprimer les blocs de code markdown
        if result.startswith('```'):
            result = re.sub(r'^```(?:json)?\n?', '', result)
            result = re.sub(r'\n?```$', '', result)
        
        # Supprimer tout texte avant le premier {
        if '{' in result:
            json_start = result.find('{')
            result = result[json_start:]
        
        # Supprimer tout texte après le dernier }
        if '}' in result:
            json_end = result.rfind('}') + 1
            result = result[:json_end]
        
        # Nettoyer les caractères invisibles
        result = result.strip()
        
        parsed = json.loads(result)
        st.success(f"✅ IA enrichissement réussi pour {etablissement_nom}")
        return parsed
    
    except Exception as e:
        error_msg = str(e)
        
        # Gestion spéciale pour modèles Groq désactivés
        if "decommissioned" in error_msg or "model_not_found" in error_msg:
            st.warning(f"⚠️ Modèle {model} indisponible, tentative avec modèle de fallback...")
            
            # Liste de modèles Groq de fallback (du plus au moins performant)
            fallback_models = ["mixtral-8x7b-32768", "llama3-8b-8192", "gemma-7b-it"]
            
            for fallback_model in fallback_models:
                if fallback_model != model:  # Éviter de retester le même modèle
                    try:
                        st.info(f"🔄 Test du modèle: {fallback_model}")
                        response = client.chat.completions.create(
                            model=fallback_model,
                            messages=[{"role": "user", "content": prompt}],
                            temperature=0,
                            max_tokens=1500
                        )
                        
                        result = response.choices[0].message.content.strip()
                        
                        if result.startswith('```'):
                            result = re.sub(r'^```(?:json)?\n?', '', result)
                            result = re.sub(r'\n?```$', '', result)
                        
                        parsed = json.loads(result)
                        st.success(f"✅ Succès avec modèle de fallback {fallback_model} pour {etablissement_nom}")
                        return parsed
                        
                    except Exception as fallback_error:
                        st.warning(f"❌ Échec avec {fallback_model}: {str(fallback_error)}")
                        continue
            
            # Si tous les fallbacks échouent
            st.error(f"❌ Tous les modèles Groq ont échoué pour {etablissement_nom}")
            return {}
        
        # Autres erreurs (JSON, etc.)
        if "JSONDecodeError" in str(type(e)):
            st.error(f"❌ Erreur JSON IA pour {etablissement_nom}: {str(e)}")
            try:
                st.write(f"Réponse brute: {result[:200]}")
            except:
                pass
        else:
            st.error(f"❌ Erreur IA pour {etablissement_nom}: {str(e)}")
        
        return {}

def search_address_from_web(nom: str, commune: str, code_postal: str, site_web: str = None) -> Optional[str]:
    """
    Rechercher l'adresse manquante sur le web avant géocodage
    Scrape le site web et extrait l'adresse avec patterns intelligents
    """
    if not site_web or not requests or not BeautifulSoup:
        return None
    
    try:
        # Scraper le site web
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(site_web, headers=headers, timeout=10)
        if not response.ok:
            return None
        
        soup = BeautifulSoup(response.content, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        
        # Patterns d'adresse française
        address_patterns = [
            # Numéro + type de voie + nom
            r'(\d+(?:\s+(?:bis|ter|quater))?[\s,]+(?:rue|avenue|boulevard|place|chemin|route|allée|impasse|cours|square|passage)[^,\n]{5,80})',
            # Adresse avec code postal et ville
            r'(\d+[^,\n]{10,80}' + re.escape(code_postal) + r'[^,\n]{0,30}' + re.escape(commune) + r')',
            # Pattern général avec nom établissement comme contexte
            r'(?:Adresse|Située?|Situé|Localisé)[:\s]+([^,\n]{10,100})',
        ]
        
        for pattern in address_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                address_candidate = match.strip()
                
                # Validation: doit contenir un numéro et un type de voie
                if re.search(r'\d+', address_candidate) and \
                   re.search(r'(rue|avenue|boulevard|place|chemin|route|allée|impasse|cours|square|passage)', 
                            address_candidate, re.IGNORECASE):
                    
                    # Nettoyage
                    address_candidate = re.sub(r'\s+', ' ', address_candidate)
                    address_candidate = address_candidate[:150]  # Limiter longueur
                    
                    # Vérifier pertinence (doit mentionner la commune ou le code postal)
                    if commune.lower() in address_candidate.lower() or code_postal in address_candidate:
                        return address_candidate
                    else:
                        # Adresse trouvée mais sans contexte ville -> ajouter ville
                        return f"{address_candidate}, {code_postal} {commune}"
        
        return None
        
    except Exception:
        return None

def geocode_address(address: str, provider: str, google_key: str = None, mapbox_key: str = None) -> Tuple[float, float, str]:
    """Géocoder une adresse"""
    if not requests:
        return None, None, None
        
    try:
        if provider.startswith("Nominatim"):
            params = {
                'q': address,
                'format': 'json',
                'addressdetails': 1,
                'limit': 1
            }
            headers = {'User-Agent': 'HabitatSeniorApp/1.0'}
            response = requests.get(
                "https://nominatim.openstreetmap.org/search", 
                params=params, 
                headers=headers,
                timeout=10
            )
            
            if response.ok:
                data = response.json()
                if data:
                    result = data[0]
                    return float(result['lon']), float(result['lat']), 'street'
        
        elif provider == "Google Maps" and google_key:
            params = {
                'address': address,
                'key': google_key
            }
            response = requests.get(
                "https://maps.googleapis.com/maps/api/geocode/json",
                params=params,
                timeout=10
            )
            
            if response.ok:
                data = response.json()
                if data.get('results'):
                    location = data['results'][0]['geometry']['location']
                    location_type = data['results'][0]['geometry']['location_type']
                    precision = 'rooftop' if 'ROOFTOP' in location_type else 'street'
                    return float(location['lng']), float(location['lat']), precision
        
        elif provider == "Mapbox" and mapbox_key:
            response = requests.get(
                f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json",
                params={'access_token': mapbox_key, 'limit': 1},
                timeout=10
            )
            
            if response.ok:
                data = response.json()
                if data.get('features'):
                    coords = data['features'][0]['center']
                    return float(coords[0]), float(coords[1]), 'street'
        
    except Exception as e:
        st.warning(f"Erreur géocodage: {str(e)}")
    
    return None, None, None

# === FONCTIONS DE BASE DE DONNÉES ===

def get_db_connection(config: Dict[str, Any]):
    """Créer une connexion à la base de données"""
    if not psycopg2:
        st.error("psycopg2 non installé")
        return None
    
    try:
        dsn = f"host={config['host']} port={config['port']} dbname={config['database']} user={config['user']} password={config['password']} sslmode={config['sslmode']}"
        conn = psycopg2.connect(dsn)
        return conn
    except Exception as e:
        st.error(f"Erreur connexion base: {str(e)}")
        return None

def insert_etablissement(cursor, data: Dict[str, Any]) -> str:
    """Insérer un établissement et retourner son ID"""
    
    # Validation des données avant insertion
    st.write(f"🔍 Validation données pour: {data.get('nom')}")
    
    # Convertir geom en format correct si nécessaire
    geom_value = None
    if data.get('geom'):
        geom_value = f"ST_GeomFromEWKB(decode('{data.get('geom')}', 'hex'))"
    
    # Convertir les enums en lowercase pour correspondre au schéma
    eligibilite_statut = data.get('eligibilite_statut')
    if eligibilite_statut and eligibilite_statut not in ['avp_eligible', 'non_eligible', 'a_verifier']:
        eligibilite_statut = 'a_verifier'  # valeur par défaut
    
    habitat_type = data.get('habitat_type')  
    if habitat_type and habitat_type not in ['logement_independant', 'residence', 'habitat_partage']:
        habitat_type = 'residence'  # valeur par défaut
    
    geocode_precision = data.get('geocode_precision')
    if geocode_precision and geocode_precision not in ['rooftop', 'range_interpolated', 'street', 'locality', 'unknown']:
        geocode_precision = 'street'  # valeur par défaut
    
    statut_editorial = data.get('statut_editorial', 'draft')
    if statut_editorial not in ['draft', 'soumis', 'valide', 'publie', 'archive']:
        statut_editorial = 'draft'
    
    st.write(f"📝 Données validées:")
    st.write(f"  - nom: {data.get('nom')}")
    st.write(f"  - eligibilite_statut: {eligibilite_statut}")
    st.write(f"  - habitat_type: {habitat_type}")
    st.write(f"  - statut_editorial: {statut_editorial}")
    st.write(f"  - geom: {'Oui' if geom_value else 'Non'}")
    
    if geom_value:
        query = """
        INSERT INTO public.etablissements 
        (nom, presentation, adresse_l1, code_postal, commune, departement, region, 
         telephone, email, site_web, gestionnaire, public_cible, habitat_type, 
         eligibilite_statut, statut_editorial, pays, geom, geocode_precision, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, {}, %s, %s)
        RETURNING id
        """.format(geom_value)
        
        values = (
            data.get('nom'), data.get('presentation'), data.get('adresse_l1'),
            data.get('code_postal'), data.get('commune'), data.get('departement'),
            data.get('region'), data.get('telephone'), data.get('email'),
            data.get('site_web'), data.get('gestionnaire'), data.get('public_cible'),
            habitat_type, eligibilite_statut, statut_editorial, data.get('pays', 'FR'),
            geocode_precision, data.get('source')
        )
    else:
        query = """
        INSERT INTO public.etablissements 
        (nom, presentation, adresse_l1, code_postal, commune, departement, region, 
         telephone, email, site_web, gestionnaire, public_cible, habitat_type, 
         eligibilite_statut, statut_editorial, pays, geocode_precision, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        
        values = (
            data.get('nom'), data.get('presentation'), data.get('adresse_l1'),
            data.get('code_postal'), data.get('commune'), data.get('departement'),
            data.get('region'), data.get('telephone'), data.get('email'),
            data.get('site_web'), data.get('gestionnaire'), data.get('public_cible'),
            habitat_type, eligibilite_statut, statut_editorial, data.get('pays', 'FR'),
            geocode_precision, data.get('source')
        )
    
    st.write(f"🔄 Exécution de la requête SQL...")
    st.write(f"Query: {query[:200]}...")
    
    try:
        cursor.execute(query, values)
        result = cursor.fetchone()
        if result:
            etablissement_id = result[0]
            st.write(f"✅ Établissement créé avec ID: {etablissement_id}")
            return etablissement_id
        else:
            raise Exception("Aucun ID retourné par la base")
    except Exception as e:
        st.error(f"❌ Erreur SQL détaillée: {str(e)}")
        st.write(f"Values: {values}")
        raise e

def insert_sous_categorie(cursor, etablissement_id: str, sous_categorie: str):
    """Insérer une sous-catégorie pour un établissement"""
    if not sous_categorie:
        return
    
    # Créer la catégorie générique si elle n'existe pas
    cursor.execute("""
        SELECT id FROM public.categories WHERE libelle = 'générique'
    """)
    result = cursor.fetchone()
    if not result:
        cursor.execute("""
            INSERT INTO public.categories (libelle) 
            VALUES ('générique') 
            RETURNING id
        """)
        result = cursor.fetchone()
    categorie_id = result[0]
    
    # Vérifier si la sous-catégorie existe déjà
    cursor.execute("SELECT id FROM public.sous_categories WHERE libelle = %s", (sous_categorie,))
    result = cursor.fetchone()
    
    if not result:
        # Insérer la sous-catégorie
        cursor.execute("""
            INSERT INTO public.sous_categories (libelle, categorie_id)
            VALUES (%s, %s)
            RETURNING id
        """, (sous_categorie, categorie_id))
        result = cursor.fetchone()
    
    if result:
        sous_categorie_id = result[0]
        # Vérifier si la liaison existe déjà
        cursor.execute("""
            SELECT 1 FROM public.etablissement_sous_categorie 
            WHERE etablissement_id = %s AND sous_categorie_id = %s
        """, (etablissement_id, sous_categorie_id))
        
        if not cursor.fetchone():
            # Lier l'établissement à la sous-catégorie
            cursor.execute("""
                INSERT INTO public.etablissement_sous_categorie (etablissement_id, sous_categorie_id)
                VALUES (%s, %s)
            """, (etablissement_id, sous_categorie_id))

def insert_restauration(cursor, etablissement_id: str, restauration_data: Dict[str, bool]):
    """Insérer les données de restauration"""
    if not restauration_data:
        return
    
    # Vérifier si l'enregistrement existe déjà
    cursor.execute("""
        SELECT id FROM public.restaurations WHERE etablissement_id = %s
    """, (etablissement_id,))
    
    result = cursor.fetchone()
    
    if result:
        # Mettre à jour l'enregistrement existant
        cursor.execute("""
            UPDATE public.restaurations SET
                kitchenette = %s,
                resto_collectif_midi = %s,
                resto_collectif = %s,
                portage_repas = %s,
                updated_at = NOW()
            WHERE etablissement_id = %s
        """, (
            restauration_data.get('kitchenette', False),
            restauration_data.get('resto_collectif_midi', False), 
            restauration_data.get('resto_collectif', False),
            restauration_data.get('portage_repas', False),
            etablissement_id
        ))
    else:
        # Insérer nouvel enregistrement
        cursor.execute("""
            INSERT INTO public.restaurations 
            (etablissement_id, kitchenette, resto_collectif_midi, resto_collectif, portage_repas)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            etablissement_id,
            restauration_data.get('kitchenette', False),
            restauration_data.get('resto_collectif_midi', False), 
            restauration_data.get('resto_collectif', False),
            restauration_data.get('portage_repas', False)
        ))

def insert_tarification(cursor, etablissement_id: str, tarif_data: Dict[str, Any]):
    """Insérer une tarification"""
    if not tarif_data or not any(tarif_data.values()):
        return
        
    cursor.execute("""
        INSERT INTO public.tarifications 
        (etablissement_id, fourchette_prix, prix_min, prix_max, date_observation)
        VALUES (%s, %s, %s, %s, CURRENT_DATE)
    """, (
        etablissement_id,
        tarif_data.get('fourchette_prix'),
        tarif_data.get('prix_min'),
        tarif_data.get('prix_max')
    ))

def insert_logements_types(cursor, etablissement_id: str, logements_data: List[Dict[str, Any]]):
    """Insérer les types de logements"""
    if not logements_data:
        return
        
    for logement in logements_data:
        # Valeurs par défaut pour les champs avec contraintes NOT NULL
        cursor.execute("""
            INSERT INTO public.logements_types
            (etablissement_id, libelle, surface_min, surface_max, meuble, pmr, 
             domotique, nb_unites, plain_pied)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            etablissement_id, 
            logement.get('libelle', 'T1'),  # Valeur par défaut
            logement.get('surface_min'),  # Peut être NULL
            logement.get('surface_max'),  # Peut être NULL
            logement.get('meuble', False),  # Valeur par défaut False
            logement.get('pmr', False),  # Valeur par défaut False
            logement.get('domotique', False),  # Valeur par défaut False
            logement.get('nb_unit', 1) if logement.get('nb_unit') is not None else 1,  # Valeur par défaut 1
            logement.get('plain_pied', False)  # Valeur par défaut False
        ))

def insert_services(cursor, etablissement_id: str, services_list: List[str]):
    """Insérer les services d'un établissement"""
    if not services_list:
        return
        
    for service in services_list:
        if service in ["activités organisées", "espace_partage", "conciergerie", "personnel de nuit", "commerces à pied", "médecin intervenant"]:
            # Vérifier si le service existe déjà
            cursor.execute("SELECT id FROM public.services WHERE libelle = %s", (service,))
            result = cursor.fetchone()
            
            if not result:
                # Créer le service s'il n'existe pas
                cursor.execute("""
                    INSERT INTO public.services (libelle) 
                    VALUES (%s) 
                    RETURNING id
                """, (service,))
                result = cursor.fetchone()
            
            if result:
                service_id = result[0]
                # Vérifier si la liaison existe déjà
                cursor.execute("""
                    SELECT 1 FROM public.etablissement_service 
                    WHERE etablissement_id = %s AND service_id = %s
                """, (etablissement_id, service_id))
                
                if not cursor.fetchone():
                    # Lier l'établissement au service
                    cursor.execute("""
                        INSERT INTO public.etablissement_service (etablissement_id, service_id)
                        VALUES (%s, %s)
                    """, (etablissement_id, service_id))

def insert_avp_infos(cursor, etablissement_id: str, avp_data: Dict[str, Any]):
    """Insérer les informations AVP d'un établissement selon la vraie structure de la table"""
    try:
        # Extraction et validation des données AVP selon le schéma réel
        statut = avp_data.get('statut', 'intention')
        
        # Validation du statut AVP selon l'enum défini
        valid_statuts = ['intention', 'en_projet', 'ouvert']
        if statut not in valid_statuts:
            statut = 'intention'
        
        # Dates (peuvent être None)
        date_intention = avp_data.get('date_intention')
        date_en_projet = avp_data.get('date_en_projet') 
        date_ouverture = avp_data.get('date_ouverture')
        
        # PVSP fondamentaux avec structure par défaut
        pvsp_default = {
            'objectifs': '',
            'animation_vie_sociale': '',
            'gouvernance_partagee': '',
            'ouverture_au_quartier': '',
            'prevention_isolement': '',
            'participation_habitants': ''
        }
        
        pvsp_fondamentaux = avp_data.get('pvsp_fondamentaux', pvsp_default)
        if not isinstance(pvsp_fondamentaux, dict):
            pvsp_fondamentaux = pvsp_default
        
        # Textes descriptifs
        public_accueilli = avp_data.get('public_accueilli')
        modalites_admission = avp_data.get('modalites_admission')
        infos_complementaires = avp_data.get('infos_complementaires')
        
        # Arrays JSON
        partenaires_principaux = avp_data.get('partenaires_principaux', [])
        if not isinstance(partenaires_principaux, list):
            partenaires_principaux = []
            
        intervenants = avp_data.get('intervenants', [])
        if not isinstance(intervenants, list):
            intervenants = []
        
        # Heures d'animation
        heures_animation_semaine = avp_data.get('heures_animation_semaine')
        if heures_animation_semaine is not None:
            try:
                heures_animation_semaine = float(heures_animation_semaine)
                # Validation range raisonnable
                if heures_animation_semaine < 0 or heures_animation_semaine > 168:
                    heures_animation_semaine = None
            except (ValueError, TypeError):
                heures_animation_semaine = None
        
        # Insertion avec gestion des conflits selon la vraie structure
        cursor.execute("""
            INSERT INTO avp_infos (
                etablissement_id, statut, date_intention, date_en_projet, date_ouverture,
                pvsp_fondamentaux, public_accueilli, modalites_admission,
                partenaires_principaux, intervenants, heures_animation_semaine,
                infos_complementaires, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (etablissement_id) 
            DO UPDATE SET 
                statut = EXCLUDED.statut,
                date_intention = EXCLUDED.date_intention,
                date_en_projet = EXCLUDED.date_en_projet,
                date_ouverture = EXCLUDED.date_ouverture,
                pvsp_fondamentaux = EXCLUDED.pvsp_fondamentaux,
                public_accueilli = EXCLUDED.public_accueilli,
                modalites_admission = EXCLUDED.modalites_admission,
                partenaires_principaux = EXCLUDED.partenaires_principaux,
                intervenants = EXCLUDED.intervenants,
                heures_animation_semaine = EXCLUDED.heures_animation_semaine,
                infos_complementaires = EXCLUDED.infos_complementaires,
                updated_at = NOW()
        """, (
            etablissement_id, statut, date_intention, date_en_projet, date_ouverture,
            json.dumps(pvsp_fondamentaux), public_accueilli, modalites_admission,
            json.dumps(partenaires_principaux), json.dumps(intervenants), 
            heures_animation_semaine, infos_complementaires
        ))
        
    except Exception as e:
        st.error(f"Erreur insertion avp_infos: {str(e)}")

def extract_avp_data_from_enrichment(etablissement: Dict[str, Any], enrich_data: Dict[str, Any]) -> Dict[str, Any]:
    """Extraire et formater les données AVP depuis l'enrichissement IA ou fallback heuristique"""
    
    # PRIORITÉ 1: Utiliser les données AVP extraites par l'IA si disponibles
    ai_avp_data = enrich_data.get('avp_infos', {})
    if ai_avp_data and isinstance(ai_avp_data, dict) and ai_avp_data.get('statut'):
        st.write("🤖 Utilisation des données AVP extraites par l'IA")
        
        # Valider et compléter les données IA
        validated_avp = ai_avp_data.copy()
        
        # S'assurer que les PVSP fondamentaux sont complets
        pvsp = validated_avp.get('pvsp_fondamentaux', {})
        if not isinstance(pvsp, dict):
            pvsp = {}
        
        # Structure PVSP complète requise
        pvsp_complete = {
            'objectifs': pvsp.get('objectifs', ''),
            'animation_vie_sociale': pvsp.get('animation_vie_sociale', ''),
            'gouvernance_partagee': pvsp.get('gouvernance_partagee', ''),
            'ouverture_au_quartier': pvsp.get('ouverture_au_quartier', ''),
            'prevention_isolement': pvsp.get('prevention_isolement', ''),
            'participation_habitants': pvsp.get('participation_habitants', '')
        }
        validated_avp['pvsp_fondamentaux'] = pvsp_complete
        
        # Compléter partenaires si vide avec gestionnaire
        if not validated_avp.get('partenaires_principaux'):
            gestionnaire = etablissement.get('gestionnaire')
            if gestionnaire:
                validated_avp['partenaires_principaux'] = [{'nom': gestionnaire, 'type': 'gestionnaire'}]
            else:
                validated_avp['partenaires_principaux'] = []
        
        # S'assurer que intervenants existe
        if 'intervenants' not in validated_avp:
            validated_avp['intervenants'] = []
            
        return validated_avp
    
    # FALLBACK : Génération heuristique si pas de données IA
    st.write("🔄 Génération heuristique des données AVP (fallback)")
    avp_data = {}
    
    # Détecter le statut AVP selon les indices disponibles
    mention_avp = etablissement.get('mention_avp', False) or enrich_data.get('mention_avp', False)
    
    if mention_avp:
        # Si mention AVP détectée, supposer au minimum "intention"
        avp_data['statut'] = 'intention'
        
        # Essayer de déduire plus d'infos depuis le contenu
        presentation = etablissement.get('presentation', '')
        if presentation:
            # Recherche d'indices pour un statut plus avancé
            if any(pattern in presentation.lower() for pattern in ['ouvert', 'fonctionne', 'en cours']):
                avp_data['statut'] = 'ouvert'
            elif any(pattern in presentation.lower() for pattern in ['projet', 'prévoit', 'développe']):
                avp_data['statut'] = 'en_projet'
        
        # PVSP fondamentaux basiques si détection AVP
        pvsp_fondamentaux = {
            'objectifs': '',
            'animation_vie_sociale': '',
            'gouvernance_partagee': '',
            'ouverture_au_quartier': '',
            'prevention_isolement': '',
            'participation_habitants': ''
        }
        
        # Enrichir les PVSP selon les services détectés
        services = enrich_data.get('services', [])
        if 'activités organisées' in services:
            pvsp_fondamentaux['animation_vie_sociale'] = 'Activités organisées régulièrement'
        if 'espace_partage' in services:
            pvsp_fondamentaux['ouverture_au_quartier'] = 'Espaces partagés disponibles'
        
        avp_data['pvsp_fondamentaux'] = pvsp_fondamentaux
        
        # Public accueilli depuis public_cible
        public_cible = enrich_data.get('public_cible', [])
        if public_cible:
            descriptions = {
                'personnes_agees': 'Personnes âgées autonomes',
                'personnes_handicapees': 'Personnes en situation de handicap',
                'mixtes': 'Public mixte intergénérationnel',
                'alzheimer_accessible': 'Personnes atteintes de troubles cognitifs'
            }
            public_descriptions = [descriptions.get(p, p) for p in public_cible]
            avp_data['public_accueilli'] = ', '.join(public_descriptions)
        
        # Partenaires basiques (gestionnaire comme partenaire principal)
        gestionnaire = etablissement.get('gestionnaire')
        if gestionnaire:
            avp_data['partenaires_principaux'] = [{'nom': gestionnaire, 'type': 'gestionnaire'}]
        else:
            avp_data['partenaires_principaux'] = []
        
        # Intervenants vides par défaut (à compléter manuellement)
        avp_data['intervenants'] = []
        
        # Heures d'animation estimées selon les services
        if 'activités organisées' in services:
            avp_data['heures_animation_semaine'] = 10.0  # Estimation basique
    
    else:
        # Pas de mention AVP détectée, données minimales
        avp_data = {
            'statut': 'intention',
            'pvsp_fondamentaux': {
                'objectifs': '',
                'animation_vie_sociale': '',
                'gouvernance_partagee': '',
                'ouverture_au_quartier': '',
                'prevention_isolement': '',
                'participation_habitants': ''
            },
            'partenaires_principaux': [],
            'intervenants': []
        }
    
    return avp_data

def insert_public_cible(cursor, etablissement_id: str, public_cible_list: List[str]):
    """Insérer le public cible d'un établissement (fonction pour extension future)"""
    try:
        # Validation du public cible selon les valeurs normalisées
        public_cible_allowed = ["personnes_agees", "personnes_handicapees", "mixtes", "alzheimer_accessible"]
        
        for public in public_cible_list:
            if public in public_cible_allowed:
                # Adapter selon votre schéma de BDD
                cursor.execute("""
                    INSERT INTO public_cible (id, etablissement_id, libelle, created_at)
                    VALUES (gen_random_uuid(), %s, %s, NOW())
                    ON CONFLICT (etablissement_id, libelle) DO NOTHING
                """, (etablissement_id, public))
                
    except Exception as e:
        # Ne pas interrompre le processus si cette table n'existe pas encore
        pass

# === INTERFACE PRINCIPALE ===

st.header("📁 Chargement des données")

uploaded_file = st.file_uploader(
    "Sélectionnez votre fichier CSV d'établissements",
    type=['csv'],
    help="Format attendu: nom, commune, code_postal, gestionnaire, adresse_l1, etc."
)

if uploaded_file is not None:
    try:
        # Lecture du CSV avec gestion robuste d'erreurs de format
        try:
            df = pd.read_csv(uploaded_file)
        except pd.errors.ParserError as e:
            st.warning(f"⚠️ Erreur de format CSV détectée: {str(e)}")
            st.info("🔧 Correction automatique en cours...")
            
            # Reset le curseur du fichier
            uploaded_file.seek(0)
            
            # Méthode de fallback simple : lecture ligne par ligne avec nettoyage
            try:
                # Méthode 1: Lecture avec paramètres flexibles
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, sep=',', quotechar='"', 
                               doublequote=True, skipinitialspace=True, 
                               on_bad_lines='skip', engine='python')
                
                if df.empty:
                    raise ValueError("DataFrame vide après correction")
                    
                st.success(f"✅ Fichier automatiquement corrigé (méthode simple): {len(df)} lignes")
                
            except Exception as e2:
                st.info("🔧 Tentative de correction avancée...")
                
                # Méthode 2: Correction manuelle ligne par ligne
                uploaded_file.seek(0)
                content = uploaded_file.read().decode('utf-8')
                lines = content.split('\n')
                
                if not lines:
                    st.error("❌ Fichier CSV vide")
                    st.stop()
                
                # Analyser l'en-tête
                header = lines[0].strip()
                expected_cols = len(header.split(','))
                st.info(f"📊 Colonnes attendues: {expected_cols}")
                
                # Corriger les lignes problématiques
                corrected_lines = [header]  # Commencer avec l'en-tête
                problems_found = 0
                
                for i, line in enumerate(lines[1:], 1):
                    if line.strip():
                        # Compter les virgules hors guillemets
                        in_quotes = False
                        comma_count = 0
                        for char in line:
                            if char == '"':
                                in_quotes = not in_quotes
                            elif char == ',' and not in_quotes:
                                comma_count += 1
                        
                        actual_cols = comma_count + 1
                        
                        # Si trop de colonnes, corriger
                        if actual_cols > expected_cols:
                            problems_found += 1
                            # Stratégie simple: prendre les N-1 premières colonnes et fusionner le reste
                            parts = line.split(',')
                            if len(parts) > expected_cols:
                                corrected_parts = parts[:expected_cols-1]
                                # Fusionner le reste dans la dernière colonne avec guillemets
                                last_part = ','.join(parts[expected_cols-1:])
                                if not (last_part.startswith('"') and last_part.endswith('"')):
                                    last_part = f'"{last_part}"'
                                corrected_parts.append(last_part)
                                corrected_lines.append(','.join(corrected_parts))
                            else:
                                corrected_lines.append(line.strip())
                        else:
                            corrected_lines.append(line.strip())
                
                if problems_found > 0:
                    st.info(f"🔧 {problems_found} lignes corrigées automatiquement")
                
                # Créer le CSV corrigé
                corrected_content = '\n'.join(corrected_lines)
                from io import StringIO
                df = pd.read_csv(StringIO(corrected_content))
                
                st.success(f"✅ Fichier automatiquement corrigé (méthode avancée): {len(df)} lignes")
        
        if df.empty:
            st.error("❌ Le fichier CSV ne contient aucune donnée")
            st.stop()
        
        st.success(f"✅ Fichier chargé: {len(df)} lignes")
        
        with st.expander("Aperçu des données"):
            st.dataframe(df.head(10), use_container_width=True)
            st.write(f"**Colonnes:** {', '.join(df.columns.tolist())}")
            
            # Vérification de la cohérence des données
            col1, col2, col3 = st.columns(3)
            col1.metric("Lignes", len(df))
            col2.metric("Colonnes", len(df.columns))
            
            # Vérifier les colonnes essentielles
            required_cols = ['nom', 'commune', 'code_postal', 'habitat_type']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                col3.metric("Colonnes manquantes", len(missing_cols), delta=f"-{missing_cols}")
                st.warning(f"⚠️ Colonnes manquantes: {', '.join(missing_cols)}")
            else:
                col3.metric("Colonnes essentielles", "✅", delta="Toutes présentes")
        
        # Configuration enrichissement
        st.header("🔄 Processus d'enrichissement")
        
        enable_geocoding = st.checkbox("Activer le géocodage", value=True)
        
        # Vérifier si les données sont déjà enrichies dans la session
        if 'enriched_data' not in st.session_state:
            st.write("🚀 **DÉBUT DU PROCESSUS D'ENRICHISSEMENT**")
            st.write(f"📊 **{len(df)} lignes à traiter**")
            st.write(f"🔧 **Mode sélectionné:** {st.session_state.enrich_mode}")
            
            # Lancer l'enrichissement
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Initialiser les listes de résultats
            enriched_data = []
            logs = []
            errors = []
            
            total_rows = len(df)
            
            # Vérifier que le DataFrame n'est pas vide
            if total_rows == 0:
                st.error("❌ Le fichier CSV est vide ou ne contient aucune ligne de données.")
                st.stop()
            
            # Interface simplifiée - pas de logs détaillés
            
            for idx, row in df.iterrows():
                try:
                    # Affichage minimal du progrès
                    status_text.text(f"🔄 Traitement {idx+1}/{total_rows}: {row.get('nom', 'Sans nom')[:30]}...")
                    
                    # Données de base normalisées
                    etablissement = {
                        'nom': clean_text(row.get('nom')),
                        'adresse_l1': clean_text(row.get('adresse_l1')),
                        'code_postal': clean_text(str(row.get('code_postal', '')))[:5],
                        'commune': clean_text(row.get('commune', '')),
                        'telephone': normalize_phone_fr(row.get('telephone')),
                        'email': normalize_email(row.get('email')),
                        'site_web': clean_text(row.get('site_web')),
                        'gestionnaire': clean_text(row.get('gestionnaire')),
                        'presentation': clean_text(row.get('presentation')),
                        'source': clean_text(row.get('source')),
                        'statut_editorial': 'draft',  # Statut par défaut, sera changé à l'import
                        'pays': 'FR'
                    }
                    
                    # Normalisation département/région
                    dept_label, region = parse_departement_region(
                        etablissement['code_postal'], 
                        row.get('departement')
                    )
                    etablissement['departement'] = dept_label
                    etablissement['region'] = region
                    
                    # DÉTECTION AVP dans présentation (CORRECTION 2)
                    mention_avp_csv = False
                    presentation_text = (etablissement.get('presentation') or '').lower()
                    nom_text = (etablissement.get('nom') or '').lower()
                    # Combiner nom + présentation pour détecter AVP
                    combined_text = f"{nom_text} {presentation_text}"
                    
                    avp_patterns_in_text = [
                        r'\baide\s+à\s+la\s+vie\s+partagée\b',
                        r'\bavp\b',
                        r'\bprojet\s+avp\b',
                        r'\béligible\s+(?:au\s+)?avp\b'
                    ]
                    for pattern in avp_patterns_in_text:
                        if re.search(pattern, combined_text):
                            mention_avp_csv = True
                            st.success(f"🏷️ Mention AVP détectée dans CSV pour: {etablissement['nom']}")
                            break
                    
                    # SOUS-CATÉGORIE : Enrichir SEULEMENT si vide dans CSV (CORRECTION 3)
                    sous_cat_csv = row.get('sous_categories')
                    
                    if sous_cat_csv and str(sous_cat_csv).strip():
                        # CSV a une valeur → GARDER telle quelle après normalisation
                        sous_cat = normalize_sous_categorie(sous_cat_csv)
                        if not sous_cat:
                            # Normalisation a échoué → détecter intelligemment
                            sous_cat = detect_sous_categorie_intelligente(
                                nom=etablissement.get('nom', ''),
                                site_web=etablissement.get('site_web', ''),
                                presentation=etablissement.get('presentation', ''),
                                gestionnaire=etablissement.get('gestionnaire', '')
                            )
                            if sous_cat:
                                st.info(f"🔍 CSV invalide, détection intelligente: {sous_cat}")
                    else:
                        # CSV vide → détecter intelligemment
                        sous_cat = detect_sous_categorie_intelligente(
                            nom=etablissement.get('nom', ''),
                            site_web=etablissement.get('site_web', ''),
                            presentation=etablissement.get('presentation', ''),
                            gestionnaire=etablissement.get('gestionnaire', '')
                        )
                        if sous_cat:
                            st.info(f"🔍 Sous-catégorie détectée (CSV vide): {sous_cat}")
                    
                    # PRIORITÉ : Préserver habitat_type du CSV s'il existe, sinon le déduire
                    csv_habitat_type = row.get('habitat_type')
                    if csv_habitat_type and csv_habitat_type.strip() and csv_habitat_type in ['logement_independant', 'residence', 'habitat_partage']:
                        habitat_type = csv_habitat_type
                    else:
                        habitat_type = deduce_habitat_type(sous_cat)
                    
                    etablissement['sous_categorie'] = sous_cat
                    etablissement['habitat_type'] = habitat_type
                    
                    # Préserver eligibilite_statut du CSV s'il existe (gérer les 2 noms de colonnes possibles)
                    csv_eligibilite = row.get('eligibilite_statut') or row.get('eligibilite_avp')
                    if csv_eligibilite and csv_eligibilite in ['avp_eligible', 'non_eligible', 'a_verifier']:
                        etablissement['eligibilite_statut'] = csv_eligibilite
                        st.info(f"✅ Éligibilité CSV préservée: {csv_eligibilite} pour {etablissement['nom']}")
                    
                    # Normalisation public_cible  
                    etablissement['public_cible'] = normalize_public_cible(row.get('public_cible'))
                    
                    # Initialiser données enrichissement (mention_avp_csv déjà détectée plus haut)
                    mention_avp = mention_avp_csv
                    enrich_data = {
                        'restauration': {'kitchenette': False, 'resto_collectif_midi': False, 'resto_collectif': False, 'portage_repas': False},
                        'logements_types': [],
                        'tarification': {'fourchette_prix': None, 'prix_min': None, 'prix_max': None},
                        'services': []
                    }
                    
                    # Processus d'enrichissement
                    scraped_data_summary = {}
                    
                    if st.session_state.enrich_mode in ["Webscraping", "Websearch + IA"] and etablissement['site_web']:
                        scraped = scrape_website_enhanced(etablissement['site_web'])
                        scraped_data_summary = scraped.copy()
                        
                        # Afficher détails seulement en mode debug
                        display_scraping_details(etablissement['site_web'], scraped, debug_mode)
                        
                        # Fusionner données scrappées
                        for key in ['email', 'telephone', 'presentation', 'gestionnaire']:
                            if not etablissement.get(key) and scraped.get(key):
                                etablissement[key] = scraped[key]
                        # CORRECTION 1: Accumuler mention_avp (OR logique, jamais écraser)
                        mention_avp = mention_avp or scraped.get('mention_avp', False)
                        if scraped.get('mention_avp'):
                            st.success(f"🏷️ Mention AVP détectée par scraping pour: {etablissement['nom']}")
                        
                        # Détection intergénérationnel dans les données scrappées
                        scraped_text = f"{scraped.get('presentation', '')}".lower()
                        if any(terme in scraped_text for terme in ['intergénérationnel', 'intergenerationnel', 'inter-générationnel', 'intergenerationelle']):
                            etablissement['sous_categorie'] = 'habitat intergénérationnel'
                            etablissement['habitat_type'] = deduce_habitat_type('habitat intergénérationnel')
                        
                        # Fusionner données enrichissement
                        for key in enrich_data:
                            if scraped.get(key):
                                enrich_data[key] = scraped[key]
                    
                    # Gestion des modes d'IA selon votre logique
                    websearch_content = ""
                    
                    if st.session_state.enrich_mode in ["Websearch + IA"]:
                        # 1. Recherche web + scraping pour collecter contenu
                        query = f"{etablissement['nom']} {etablissement['commune']}"
                        urls = search_web(query, st.session_state.search_provider, st.session_state.tavily_key, st.session_state.serpapi_key, debug_mode)
                        
                        # Collecter le contenu des pages trouvées
                        for url in urls[:2]:  # Réduit de 3 à 2 URLs par recherche
                            scraped = scrape_website_enhanced(url)
                            if scraped:
                                # Afficher détails seulement en mode debug
                                display_scraping_details(url, scraped, debug_mode)
                                # Fusionner données de base
                                for key in ['email', 'telephone', 'presentation', 'gestionnaire']:
                                    if not etablissement.get(key) and scraped.get(key):
                                        etablissement[key] = scraped[key]
                                # CORRECTION 1: Accumuler mention_avp (OR logique)
                                mention_avp = mention_avp or scraped.get('mention_avp', False)
                                if scraped.get('mention_avp'):
                                    st.success(f"🏷️ Mention AVP détectée par websearch scraping")
                                
                                # Collecter le contenu textuel pour l'IA
                                if scraped.get('presentation'):
                                    websearch_content += f"\n{scraped['presentation']}"
                        
                        # 2. Enrichissement IA basé sur le contenu websearch
                        if websearch_content.strip():
                            ai_data = enrich_with_ai_websearch(etablissement, websearch_content, st.session_state.ai_provider, st.session_state.ai_model, st.session_state.openai_key, st.session_state.groq_key)
                            if ai_data:
                                # Appliquer les données IA selon votre logique
                                # CORRECTION 1: Accumuler mention_avp (OR logique)
                                mention_avp = mention_avp or ai_data.get('mention_avp', False)
                                if ai_data.get('mention_avp'):
                                    st.success(f"🏷️ Mention AVP détectée par IA (websearch)")
                                if ai_data.get('sous_categorie'):
                                    etablissement['sous_categorie'] = ai_data['sous_categorie']
                                    etablissement['habitat_type'] = deduce_habitat_type(ai_data['sous_categorie'])
                                if ai_data.get('public_cible'):
                                    etablissement['public_cible'] = ai_data['public_cible']
                                if ai_data.get('eligibilite_statut'):
                                    etablissement['eligibilite_statut'] = ai_data['eligibilite_statut']
                                # Fusionner données tables périphériques
                                for key in ['restauration', 'tarification', 'services']:
                                    if ai_data.get(key):
                                        enrich_data[key] = ai_data[key]
                                # Fusionner logements_types spécifiquement
                                if ai_data.get('logements_types'):
                                    enrich_data['logements_types'] = ai_data['logements_types']
                    
                    elif st.session_state.enrich_mode == "IA seule":
                        # Enrichissement IA basé sur données CSV + webscraping existant
                        ai_data = enrich_with_ai_alone(etablissement, st.session_state.ai_provider, st.session_state.ai_model, st.session_state.openai_key, st.session_state.groq_key)
                        if ai_data:
                            # Appliquer les données IA
                            # CORRECTION 1: Accumuler mention_avp (OR logique)
                            mention_avp = mention_avp or ai_data.get('mention_avp', False)
                            if ai_data.get('mention_avp'):
                                st.success(f"🏷️ Mention AVP détectée par IA seule")
                            if ai_data.get('sous_categorie'):
                                etablissement['sous_categorie'] = ai_data['sous_categorie']
                                # Préserver habitat_type du CSV si défini, sinon utiliser IA
                                if not (csv_habitat_type and csv_habitat_type.strip()):
                                    etablissement['habitat_type'] = deduce_habitat_type(ai_data['sous_categorie'])
                            if ai_data.get('public_cible'):
                                etablissement['public_cible'] = ai_data['public_cible']
                            # Fusionner données tables périphériques
                            for key in ['restauration', 'tarification', 'services']:
                                if ai_data.get(key):
                                    enrich_data[key] = ai_data[key]
                            # Fusionner logements_types spécifiquement
                            if ai_data.get('logements_types'):
                                enrich_data['logements_types'] = ai_data['logements_types']
                    
                    # Déduire eligibilite_statut seulement si pas déjà défini par le CSV
                    if 'eligibilite_statut' not in etablissement:
                        etablissement['eligibilite_statut'] = deduce_eligibilite(
                            sous_cat, 
                            mention_avp,
                            csv_eligibilite  # CORRECTION 3: Passer eligibilite_csv
                        )
                    
                    # Stocker mention_avp pour la validation
                    etablissement['mention_avp'] = mention_avp
                    
                    # Géocodage si activé
                    if enable_geocoding:
                        lon, lat, precision = None, None, None
                        address_found_from_scraping = False
                        
                        # NOUVELLE ÉTAPE : Recherche d'adresse si manquante
                        if not etablissement.get('adresse_l1') and etablissement.get('site_web'):
                            st.info(f"🔍 Recherche d'adresse pour: {etablissement['nom']}")
                            found_address = search_address_from_web(
                                etablissement['nom'],
                                etablissement['commune'],
                                etablissement['code_postal'],
                                etablissement['site_web']
                            )
                            if found_address:
                                etablissement['adresse_l1'] = found_address
                                address_found_from_scraping = True
                                st.success(f"✅ Adresse trouvée: {found_address[:60]}...")
                            else:
                                st.warning(f"⚠️ Adresse non trouvée sur le site web")
                        
                        # Tentative 1: Adresse complète si disponible
                        has_address = etablissement.get('adresse_l1') and etablissement['code_postal'] and etablissement['commune']
                        
                        if has_address:
                            address = f"{etablissement['adresse_l1']}, {etablissement['code_postal']} {etablissement['commune']}, France"
                            lon, lat, precision = geocode_address(address, geocode_provider, google_maps_key, mapbox_key)
                            if lon and lat:
                                st.info(f"🎯 Géocodage adresse complète réussi (precision: {precision})")
                        
                        # Fallback: Centre de la commune UNIQUEMENT si pas d'adresse du tout
                        # OU si géocodage a échoué ET que l'adresse n'a PAS été trouvée par scraping
                        if (not lon or not lat) and etablissement['commune']:
                            if not has_address:
                                # Pas d'adresse du tout -> fallback commune justifié
                                commune_address = f"{etablissement['commune']}, France"
                                if etablissement.get('departement'):
                                    commune_address = f"{etablissement['commune']}, {etablissement['departement']}, France"
                                lon, lat, precision = geocode_address(commune_address, geocode_provider, google_maps_key, mapbox_key)
                                if lon and lat:
                                    precision = 'locality'
                                    st.warning(f"📍 Fallback géocodage commune (pas d'adresse): {commune_address}")
                            elif not address_found_from_scraping:
                                # Adresse existe (du CSV) mais géocodage échoue -> essayer fallback
                                st.warning(f"⚠️ Géocodage de l'adresse échoué, tentative fallback commune")
                                commune_address = f"{etablissement['commune']}, France"
                                if etablissement.get('departement'):
                                    commune_address = f"{etablissement['commune']}, {etablissement['departement']}, France"
                                lon, lat, precision = geocode_address(commune_address, geocode_provider, google_maps_key, mapbox_key)
                                if lon and lat:
                                    precision = 'locality'
                                    st.warning(f"📍 Fallback géocodage commune: {commune_address}")
                            else:
                                # Adresse trouvée par scraping mais géocodage échoue -> NE PAS faire fallback
                                st.error(f"❌ Géocodage de l'adresse scrapée a échoué, pas de fallback appliqué")
                        
                        # Enregistrement des coordonnées si trouvées
                        if lon and lat:
                            etablissement['longitude'] = lon
                            etablissement['latitude'] = lat
                            etablissement['geocode_precision'] = precision
                            etablissement['geom'] = create_ewkb_point(lon, lat)
                    
                    # Ajouter données enrichissement à l'établissement
                    etablissement['enrichment_data'] = enrich_data
                    enriched_data.append(etablissement)
                    
                    # Affichage concis du résumé d'enrichissement
                    if st.session_state.enrich_mode != "Aucun":
                        summary = format_enrichment_summary(etablissement, scraped_data_summary)
                        if summary != "Aucune donnée enrichie":
                            status_text.text(f"✅ {etablissement['nom'][:30]}: {summary}")
                    
                    logs.append({
                        'nom': etablissement['nom'], 
                        'action': st.session_state.enrich_mode, 
                        'success': True,
                        'mention_avp': mention_avp
                    })
                
                except Exception as e:
                    errors.append({
                        'nom': row.get('nom', 'Inconnu'), 
                        'error': str(e)
                    })
                
                # Mise à jour progress
                progress_bar.progress((idx + 1) / total_rows)
            
            status_text.text("✅ Enrichissement terminé!")
            
            # Stocker dans la session pour éviter le re-traitement
            st.session_state['enriched_data'] = enriched_data
            st.session_state['logs'] = logs
            st.session_state['errors'] = errors
        else:
            # Utiliser les données de la session
            st.write("✅ **DONNÉES DÉJÀ ENRICHIES** - Utilisation du cache de session")
            enriched_data = st.session_state['enriched_data']
            logs = st.session_state['logs'] 
            errors = st.session_state['errors']
        
        # Affichage résultats avec métriques qualité détaillées
        st.header("📊 Résultats d'enrichissement")
        
        # Protection contre datasets vides
        if not enriched_data:
            st.warning("⚠️ Aucun établissement n'a pu être traité avec succès.")
            if errors:
                st.subheader("⚠️ Erreurs de traitement")
                st.dataframe(pd.DataFrame(errors))
            st.stop()
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Établissements traités", len(enriched_data))
        col2.metric("Erreurs", len(errors))
        col3.metric("Avec géocodage", len([e for e in enriched_data if e.get('geom')]))
        
        # Métriques de qualité d'enrichissement
        with_services = len([e for e in enriched_data if e.get('enrichment_data', {}).get('services')])
        with_tarifs = len([e for e in enriched_data if e.get('enrichment_data', {}).get('tarification')])
        with_avp = len([e for e in enriched_data if e.get('mention_avp')])
        with_presentation = len([e for e in enriched_data if e.get('presentation') and len(e['presentation']) > 50])
        
        col4.metric("Avec mention AVP", with_avp)
        
        st.subheader("🎯 Qualité de l'enrichissement")
        qual_col1, qual_col2, qual_col3, qual_col4 = st.columns(4)
        
        # Protection contre division par zéro
        total_enriched = len(enriched_data)
        if total_enriched > 0:
            qual_col1.metric("Services identifiés", with_services, f"{with_services/total_enriched*100:.1f}%")
            qual_col2.metric("Tarifs trouvés", with_tarifs, f"{with_tarifs/total_enriched*100:.1f}%")
            qual_col3.metric("Descriptions enrichies", with_presentation, f"{with_presentation/total_enriched*100:.1f}%")
        else:
            qual_col1.metric("Services identifiés", with_services, "0%")
            qual_col2.metric("Tarifs trouvés", with_tarifs, "0%")
            qual_col3.metric("Descriptions enrichies", with_presentation, "0%")
        
        # Validation selon règles métier
        warnings = []
        for etablissement in enriched_data:
            validation_warnings = validate_data_consistency(etablissement)
            if validation_warnings:
                warnings.extend([f"{etablissement.get('nom', 'Inconnu')}: {w}" for w in validation_warnings])
        
        qual_col4.metric("Alertes cohérence", len(warnings), 
                        f"{len(warnings)/total_enriched*100:.1f}% des établissements" if total_enriched > 0 else "0%")
        
        # Affichage des alertes de cohérence
        if warnings:
            with st.expander(f"⚠️ Alertes de cohérence ({len(warnings)} détectées)", expanded=False):
                for warning in warnings[:20]:  # Limiter l'affichage
                    st.warning(warning)
                if len(warnings) > 20:
                    st.info(f"... et {len(warnings)-20} autres alertes")
        
        # Analyse par mode d'enrichissement
        if logs:
            mode_stats = {}
            for log in logs:
                mode = log.get('action', 'Inconnu')
                if mode not in mode_stats:
                    mode_stats[mode] = {'total': 0, 'avec_avp': 0}
                mode_stats[mode]['total'] += 1
                if log.get('mention_avp'):
                    mode_stats[mode]['avec_avp'] += 1
            
            if mode_stats:
                st.subheader("📈 Performance par mode d'enrichissement")
                for mode, stats in mode_stats.items():
                    avp_rate = stats['avec_avp'] / stats['total'] * 100 if stats['total'] > 0 else 0
                    st.info(f"**{mode}**: {stats['total']} établissements - {stats['avec_avp']} avec mention AVP ({avp_rate:.1f}%)")
        
        # Résumé des types d'établissements détectés
        habitat_types = {}
        sous_categories = {}
        for etablissement in enriched_data:
            habitat_type = etablissement.get('habitat_type', 'Non défini')
            sous_cat = etablissement.get('sous_categorie', 'Non définie')
            
            habitat_types[habitat_type] = habitat_types.get(habitat_type, 0) + 1
            sous_categories[sous_cat] = sous_categories.get(sous_cat, 0) + 1
        
        st.subheader("🏠 Répartition des types d'habitat")
        type_col1, type_col2 = st.columns(2)
        
        with type_col1:
            st.write("**Types d'habitat:**")
            for habitat_type, count in sorted(habitat_types.items(), key=lambda x: x[1], reverse=True):
                st.write(f"- {habitat_type}: {count}")
        
        with type_col2:
            st.write("**Sous-catégories:**")
            for sous_cat, count in sorted(sous_categories.items(), key=lambda x: x[1], reverse=True):
                st.write(f"- {sous_cat}: {count}")
        
        # Services les plus fréquents
        services_count = {}
        for etablissement in enriched_data:
            services = etablissement.get('enrichment_data', {}).get('services', [])
            for service in services:
                services_count[service] = services_count.get(service, 0) + 1
        
        if services_count:
            st.subheader("🛠️ Services les plus fréquents")
            total_enriched = len(enriched_data)
            if total_enriched > 0:
                for service, count in sorted(services_count.items(), key=lambda x: x[1], reverse=True):
                    percentage = count / total_enriched * 100
                    st.write(f"- **{service}**: {count} établissements ({percentage:.1f}%)")
            else:
                st.write("Aucune donnée enrichie disponible pour les statistiques.")
        
        # Préparation des DataFrames étendus pour export/import
        df_etablissements_complet = []
        df_restaurations = []
        df_tarifications = []
        df_logements = []
        df_services = []
        df_sous_categories = []
        
        for e in enriched_data:
            # DataFrame établissements principal
            etab_row = {
                'nom': e['nom'],
                'presentation': e['presentation'], 
                'adresse_l1': e['adresse_l1'],
                'code_postal': e['code_postal'],
                'commune': e['commune'],
                'departement': e['departement'],
                'region': e['region'],
                'telephone': e['telephone'],
                'email': e['email'],
                'site_web': e['site_web'],
                'gestionnaire': e['gestionnaire'],
                'public_cible': e['public_cible'],
                'habitat_type': e['habitat_type'],
                'eligibilite_statut': e['eligibilite_statut'],
                'statut_editorial': e['statut_editorial'],
                'source': e['source'],
                'pays': e['pays'],
                'geom': e.get('geom'),
                'geocode_precision': e.get('geocode_precision')
            }
            
            # Normaliser public_cible pour éviter mix liste/string
            public_cible = e.get('public_cible')
            if isinstance(public_cible, list):
                etab_row['public_cible'] = ','.join(public_cible)
            elif public_cible:
                etab_row['public_cible'] = str(public_cible)
            else:
                etab_row['public_cible'] = ''
            
            # Ajouter les données enrichies aux colonnes principales
            enrich_data = e.get('enrichment_data', {})
            
            # Restauration
            resto = enrich_data.get('restauration', {})
            etab_row.update({
                'kitchenette': resto.get('kitchenette', False),
                'resto_collectif_midi': resto.get('resto_collectif_midi', False),
                'resto_collectif': resto.get('resto_collectif', False),
                'portage_repas': resto.get('portage_repas', False)
            })
            df_restaurations.append({
                'nom': e['nom'],
                'kitchenette': resto.get('kitchenette', False),
                'resto_collectif_midi': resto.get('resto_collectif_midi', False),
                'resto_collectif': resto.get('resto_collectif', False),
                'portage_repas': resto.get('portage_repas', False)
            })
            
            # Tarification
            tarif = enrich_data.get('tarification', {})
            etab_row.update({
                'fourchette_prix': tarif.get('fourchette_prix'),
                'prix_min': tarif.get('prix_min'),
                'prix_max': tarif.get('prix_max')
            })
            if tarif.get('fourchette_prix') or tarif.get('prix_min') or tarif.get('prix_max'):
                df_tarifications.append({
                    'nom': e['nom'],
                    'fourchette_prix': tarif.get('fourchette_prix'),
                    'prix_min': tarif.get('prix_min'),
                    'prix_max': tarif.get('prix_max')
                })
            
            # Services
            services = enrich_data.get('services', [])
            etab_row['services'] = ','.join(services) if services else ''
            for service in services:
                df_services.append({
                    'nom': e['nom'],
                    'service': service
                })
            
            # Logements types
            logements = enrich_data.get('logements_types', [])
            etab_row['nb_types_logements'] = len(logements)
            for i, logement in enumerate(logements):
                df_logements.append({
                    'nom': e['nom'],
                    'libelle': logement.get('libelle'),
                    'surface_min': logement.get('surface_min'),
                    'surface_max': logement.get('surface_max'),
                    'meuble': logement.get('meuble'),
                    'pmr': logement.get('pmr'),
                    'domotique': logement.get('domotique'),
                    'nb_unit': logement.get('nb_unit'),
                    'plain_pied': logement.get('plain_pied')
                })
            
            # Sous-catégorie
            if e.get('sous_categorie'):
                etab_row['sous_categorie'] = e['sous_categorie']
                df_sous_categories.append({
                    'nom': e['nom'],
                    'sous_categorie': e['sous_categorie']
                })
            else:
                etab_row['sous_categorie'] = ''
            
            df_etablissements_complet.append(etab_row)
        
        df_etablissements = pd.DataFrame(df_etablissements_complet)
        df_resto_export = pd.DataFrame(df_restaurations)
        df_tarif_export = pd.DataFrame(df_tarifications)
        df_logement_export = pd.DataFrame(df_logements)
        df_service_export = pd.DataFrame(df_services)
        df_souscat_export = pd.DataFrame(df_sous_categories)
        
        # Aperçu des données enrichies avec toutes les colonnes
        st.subheader("Aperçu des données enrichies")
        st.dataframe(df_etablissements.head(), use_container_width=True)
        
        # Statistiques d'enrichissement
        st.subheader("📈 Statistiques d'enrichissement")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Services enrichis", len(df_service_export))
        col2.metric("Tarifications enrichies", len(df_tarif_export))
        col3.metric("Logements enrichis", len(df_logement_export))
        col4.metric("Sous-catégories", len(df_souscat_export))
        
        # Aperçus des tables enrichies
        with st.expander("Détail des enrichissements"):
                if len(df_service_export) > 0:
                    st.write("**Services enrichis:**")
                    st.dataframe(df_service_export, use_container_width=True)
                    
                    # Affichage par établissement
                    st.write("**Services par établissement:**")
                    services_par_etab = df_service_export.groupby('nom')['service'].apply(list).reset_index()
                    services_par_etab['services_count'] = services_par_etab['service'].apply(len)
                    services_par_etab['services_list'] = services_par_etab['service'].apply(lambda x: ', '.join(x))
                    st.dataframe(services_par_etab[['nom', 'services_count', 'services_list']], use_container_width=True)
                
                if len(df_tarif_export) > 0:
                    st.write("**Tarifications enrichies:**")
                    st.dataframe(df_tarif_export, use_container_width=True)
                
                if len(df_logement_export) > 0:
                    st.write("**Types de logements enrichis:**")
                    st.dataframe(df_logement_export, use_container_width=True)
                    
                    # Affichage par établissement  
                    st.write("**Logements par établissement:**")
                    logements_par_etab = df_logement_export.groupby('nom').agg({
                        'libelle': ['count', lambda x: ', '.join(x.unique())]
                    }).reset_index()
                    logements_par_etab.columns = ['nom', 'nb_types_logements', 'types_list']
                    st.dataframe(logements_par_etab, use_container_width=True)
        
        # Export CSV complet
        st.subheader("⬇️ Export")
        csv_data = df_etablissements.to_csv(index=False).encode('utf-8')
        st.download_button(
            "📥 Télécharger CSV enrichi complet",
            csv_data,
            f"habitat_senior_enrichi_complet_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            "text/csv"
        )
        
        # Import en base avec choix du statut éditorial
        st.subheader("📤 Import en base de données")
        
        import_status = st.selectbox(
            "Statut éditorial pour l'import", 
            ["draft", "publie"], 
            index=0,
            help="draft = brouillon | publie = publié directement"
        )
        
        if st.button(f"📤 Importer en base (statut: {import_status})"):
            if psycopg2:
                conn = get_db_connection(db_config)
                if conn:
                    try:
                        # Configuration pour éviter les erreurs de transaction
                        conn.autocommit = False
                        cursor = conn.cursor()
                        
                        imported_etablissements = 0
                        imported_services = 0
                        imported_tarifs = 0
                        imported_logements = 0
                        imported_sous_categories = 0
                        import_errors = []
                        
                        st.write("🔄 Début de l'import en base...")
                        
                        for etablissement in enriched_data:
                            try:
                                st.write(f"📝 Import: {etablissement['nom']}")
                                
                                # Appliquer le statut éditorial choisi
                                etablissement['statut_editorial'] = import_status
                                
                                # Validation préalable des données obligatoires
                                if not etablissement.get('nom'):
                                    raise Exception("Nom de l'établissement manquant")
                                
                                # Corriger habitat_type manquant (problème détecté dans XL Adapt')
                                if not etablissement.get('habitat_type'):
                                    # Déduire depuis la sous-catégorie si possible
                                    sous_cat = etablissement.get('sous_categorie')
                                    if sous_cat:
                                        etablissement['habitat_type'] = deduce_habitat_type(sous_cat)
                                    else:
                                        etablissement['habitat_type'] = 'residence'  # Par défaut
                                    st.write(f"⚠️ habitat_type manquant, déduit: {etablissement['habitat_type']}")
                                
                                # 1. Insert établissement
                                etab_id = insert_etablissement(cursor, etablissement)
                                imported_etablissements += 1
                                st.write(f"✅ Établissement créé avec ID: {etab_id}")
                                
                                # 2. Insert sous-catégorie
                                if etablissement.get('sous_categorie'):
                                    insert_sous_categorie(cursor, etab_id, etablissement['sous_categorie'])
                                    imported_sous_categories += 1
                                    st.write(f"✅ Sous-catégorie: {etablissement['sous_categorie']}")
                                
                                # 3. Insert données enrichissement
                                enrich_data = etablissement.get('enrichment_data', {})
                                
                                # Restauration
                                if enrich_data.get('restauration'):
                                    insert_restauration(cursor, etab_id, enrich_data['restauration'])
                                    st.write("✅ Restauration insérée")
                                
                                # Tarification
                                if enrich_data.get('tarification'):
                                    insert_tarification(cursor, etab_id, enrich_data['tarification'])
                                    imported_tarifs += 1
                                    st.write("✅ Tarification insérée")
                                
                                # Logements types
                                if enrich_data.get('logements_types'):
                                    insert_logements_types(cursor, etab_id, enrich_data['logements_types'])
                                    imported_logements += len(enrich_data['logements_types'])
                                    st.write(f"✅ {len(enrich_data['logements_types'])} type(s) de logement")
                                
                                # Services
                                if enrich_data.get('services'):
                                    insert_services(cursor, etab_id, enrich_data['services'])
                                    imported_services += len(enrich_data['services'])
                                    st.write(f"✅ {len(enrich_data['services'])} service(s)")
                                
                                # AVP Infos - SEULEMENT si établissement éligible AVP
                                eligibilite = etablissement.get('eligibilite_statut', 'a_verifier')
                                mention_avp = etablissement.get('mention_avp', False) or enrich_data.get('mention_avp', False)
                                
                                if eligibilite == 'avp_eligible':
                                    # Enrichir la table avp_infos seulement pour les éligibles
                                    avp_info = extract_avp_data_from_enrichment(etablissement, enrich_data)
                                    insert_avp_infos(cursor, etab_id, avp_info)
                                    st.write(f"✅ AVP Info enrichie - Statut: {avp_info.get('statut', 'intention')}")
                                else:
                                    # Pour les non-éligibles, juste indiquer l'éligibilité
                                    st.write(f"ℹ️ Éligibilité AVP: {eligibilite} (pas d'enrichissement avp_infos)")
                                
                                # Public cible (si disponible)
                                if enrich_data.get('public_cible'):
                                    insert_public_cible(cursor, etab_id, enrich_data['public_cible'])
                                    st.write(f"✅ Public cible: {', '.join(enrich_data['public_cible'])}")
                                
                            except Exception as e:
                                # NE PAS faire de rollback individuel, juste logger l'erreur
                                import_errors.append({
                                    'nom': etablissement.get('nom', 'Inconnu'),
                                    'error': str(e)
                                })
                                st.error(f"❌ Erreur pour {etablissement.get('nom')}: {str(e)}")
                                st.write(f"🔍 Détails de l'erreur: {repr(e)}")
                        
                        conn.commit()
                        conn.close()
                        
                        # Résumé final
                        st.success("🎉 Import terminé avec succès!")
                        
                        col1, col2, col3, col4, col5 = st.columns(5)
                        col1.metric("Établissements", imported_etablissements)
                        col2.metric("Sous-catégories", imported_sous_categories)
                        col3.metric("Services", imported_services)
                        col4.metric("Tarifications", imported_tarifs)
                        col5.metric("Logements", imported_logements)
                        
                        if import_errors:
                            st.error(f"❌ {len(import_errors)} erreurs d'import")
                            st.dataframe(pd.DataFrame(import_errors))
                    
                    except Exception as e:
                        st.error(f"Erreur globale lors de l'import: {str(e)}")
                        if conn:
                            conn.rollback()
                            conn.close()
            else:
                st.error("psycopg2 non installé - impossible d'importer en base")
        
        # Affichage des erreurs s'il y en a
        if errors:
            st.subheader("⚠️ Erreurs de traitement")
            st.dataframe(pd.DataFrame(errors))
    
    except Exception as e:
        st.error(f"Erreur lors du chargement du fichier: {str(e)}")

else:
    st.info("📁 Veuillez charger un fichier CSV pour commencer l'enrichissement.")
    
    # Exemple de structure attendue
    st.subheader("Format CSV attendu")
    exemple_data = {
        'nom': ['Résidence A Nouste', 'Résidence La Martinère'],
        'commune': ['Saint-Sever', 'Saint-Martin-de-Seignanx'], 
        'code_postal': ['40500', '40390'],
        'gestionnaire': ['CCAS de Saint-Sever', 'CCAS de Saint‑Martin‑de‑Seignanx'],
        'adresse_l1': ['4 rue Michel de Montaigne', '424 route de l\'Adour'],
        'telephone': ['05 58 76 41 80', '05 59 52 55 55'],
        'sous_categories': ['Résidence autonomie', 'Résidence autonomie'],
        'public_cible': ['personnes_agees', 'personnes_agees']
    }
    st.dataframe(pd.DataFrame(exemple_data))
