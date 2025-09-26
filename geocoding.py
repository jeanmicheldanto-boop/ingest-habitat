import requests
import time
from geopy.geocoders import Nominatim, GoogleV3
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
import streamlit as st
from typing import Tuple, Optional, Dict
from config import GEOCODING_CONFIG

class GeocodingService:
    """Service de géolocalisation utilisant différents providers"""
    
    def __init__(self):
        self.config = GEOCODING_CONFIG
        self.service = self.config['service']
        
        # Initialiser le géocodeur
        if self.service == 'google' and self.config['google_api_key']:
            self.geocoder = GoogleV3(api_key=self.config['google_api_key'])
        else:
            # Par défaut, utiliser Nominatim (gratuit)
            self.geocoder = Nominatim(user_agent="HabitatIngestionApp/1.0")
            self.service = 'nominatim'
    
    def set_google_api_key(self, api_key: str):
        """Configure la clé API Google Maps"""
        if api_key:
            self.geocoder = GoogleV3(api_key=api_key)
            self.service = 'google'
    
    def geocode_address(self, address: str, commune: str = None, code_postal: str = None) -> Tuple[Optional[float], Optional[float], str]:
        """
        Géolocalise une adresse
        
        Returns:
            Tuple[latitude, longitude, precision_info]
        """
        if not address and not commune:
            return None, None, "Adresse manquante"
        
        # Construire l'adresse complète
        full_address = self._build_full_address(address, commune, code_postal)
        
        try:
            if self.service == 'nominatim':
                # Ajouter un délai pour respecter les limites de Nominatim
                time.sleep(1)
            
            location = self.geocoder.geocode(
                full_address,
                timeout=self.config['timeout']
            )
            
            if location:
                # Déterminer la précision basée sur le service et la réponse
                precision = self._determine_precision(location, full_address)
                return location.latitude, location.longitude, precision
            else:
                return None, None, "Adresse non trouvée"
                
        except GeocoderTimedOut:
            return None, None, "Timeout - service surchargé"
        except GeocoderServiceError as e:
            return None, None, f"Erreur service : {str(e)}"
        except Exception as e:
            return None, None, f"Erreur géocodage : {str(e)}"
    
    def _build_full_address(self, address: str, commune: str, code_postal: str) -> str:
        """Construit une adresse complète pour le géocodage"""
        parts = []
        
        if address and address.strip():
            parts.append(address.strip())
        
        if code_postal and code_postal.strip():
            parts.append(code_postal.strip())
            
        if commune and commune.strip():
            parts.append(commune.strip())
        
        # Ajouter "France" pour améliorer la précision
        parts.append("France")
        
        return ", ".join(parts)
    
    def _determine_precision(self, location, query: str) -> str:
        """Détermine la précision du géocodage"""
        if self.service == 'google':
            # Google fournit des types de résultats précis
            return "street"  # Approximation, Google a des types plus précis
        else:
            # Pour Nominatim, on estime basé sur la présence d'un numéro de rue
            import re
            if re.search(r'\d+', query.split(',')[0] if ',' in query else query):
                return "rooftop"  # Adresse avec numéro
            else:
                return "locality"  # Ville/commune seulement
    
    def geocode_batch(self, addresses: list, progress_callback=None) -> list:
        """
        Géolocalise une liste d'adresses
        
        Args:
            addresses: Liste de dictionnaires avec 'address', 'commune', 'code_postal'
            progress_callback: Fonction appelée avec (current, total) pour le suivi
        
        Returns:
            Liste de dictionnaires avec les résultats
        """
        results = []
        total = len(addresses)
        
        for i, addr_data in enumerate(addresses):
            if progress_callback:
                progress_callback(i, total)
            
            lat, lon, precision = self.geocode_address(
                addr_data.get('address', ''),
                addr_data.get('commune', ''),
                addr_data.get('code_postal', '')
            )
            
            results.append({
                'index': addr_data.get('index', i),
                'latitude': lat,
                'longitude': lon,
                'geocode_precision': precision,
                'geocoded_address': self._build_full_address(
                    addr_data.get('address', ''),
                    addr_data.get('commune', ''),
                    addr_data.get('code_postal', '')
                )
            })
        
        return results
    
    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[Dict]:
        """Géocodage inverse pour obtenir l'adresse depuis les coordonnées"""
        try:
            location = self.geocoder.reverse(
                (latitude, longitude),
                timeout=self.config['timeout']
            )
            
            if location:
                return {
                    'address': location.address,
                    'raw': location.raw
                }
            return None
            
        except Exception as e:
            st.error(f"Erreur géocodage inverse : {e}")
            return None

class AddressValidator:
    """Validateur d'adresses françaises"""
    
    def __init__(self):
        self.base_url = "https://api-adresse.data.gouv.fr"
    
    def validate_address(self, address: str, commune: str = None, code_postal: str = None) -> Dict:
        """
        Valide une adresse via l'API Adresse du gouvernement français
        
        Returns:
            Dict avec les informations de validation
        """
        if not address and not commune:
            return {'valid': False, 'error': 'Adresse manquante'}
        
        # Construire la requête
        query = address or ""
        if commune:
            query += f" {commune}"
        if code_postal:
            query += f" {code_postal}"
        
        try:
            response = requests.get(
                f"{self.base_url}/search/",
                params={
                    'q': query,
                    'limit': 1
                },
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('features'):
                    feature = data['features'][0]
                    properties = feature['properties']
                    coords = feature['geometry']['coordinates']
                    
                    return {
                        'valid': True,
                        'score': properties.get('score', 0),
                        'label': properties.get('label', ''),
                        'postcode': properties.get('postcode', ''),
                        'city': properties.get('city', ''),
                        'latitude': coords[1],
                        'longitude': coords[0],
                        'type': properties.get('type', 'unknown')
                    }
                else:
                    return {'valid': False, 'error': 'Adresse non trouvée'}
            else:
                return {'valid': False, 'error': f'Erreur API : {response.status_code}'}
                
        except Exception as e:
            return {'valid': False, 'error': f'Erreur validation : {str(e)}'}

# Instances globales
geocoding_service = GeocodingService()
address_validator = AddressValidator()