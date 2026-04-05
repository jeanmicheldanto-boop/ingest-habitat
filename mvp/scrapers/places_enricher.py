"""
Module 3.5 - Google Places Enricher
Enrichissement des adresses manquantes via Google Places API
"""

import requests
import os
from typing import Dict, Optional, List
from dataclasses import dataclass


@dataclass
class PlacesEnrichment:
    """Données enrichies depuis Google Places"""
    adresse: str = ""
    telephone: str = ""
    site_web: str = ""
    latitude: float = 0.0
    longitude: float = 0.0
    confidence: str = "none"  # none, low, medium, high


class PlacesEnricher:
    """
    Enrichisseur via Google Places API
    
    Enrichit les adresses manquantes en recherchant l'établissement
    sur Google Maps avec nom + commune + code postal
    """
    
    def __init__(self, google_api_key: Optional[str] = None):
        """
        Args:
            google_api_key: Clé API Google Maps Platform
        """
        self.google_api_key = google_api_key or os.getenv('GOOGLE_PLACES_API_KEY')
        
        # Statistiques
        self.stats = {
            'enrichments_attempted': 0,
            'enrichments_successful': 0,
            'addresses_added': 0,
            'phones_added': 0,
            'websites_added': 0
        }
    
    def enrich_establishments(self, establishments: List) -> List:
        """
        Enrichit une liste d'établissements
        
        Args:
            establishments: Liste d'ExtractedEstablishment
            
        Returns:
            Liste enrichie
        """
        if not self.google_api_key:
            print("   ⚠️ Pas de clé Google Places API, enrichissement ignoré")
            return establishments
        
        print(f"\n🗺️  === MODULE 3.5 - GOOGLE PLACES ENRICHER ===")
        print(f"📊 {len(establishments)} établissements à enrichir")
        
        enriched = []
        
        for i, est in enumerate(establishments, 1):
            print(f"\n   [{i}/{len(establishments)}] {est.nom}")
            
            # Enrichir seulement si adresse manquante
            needs_enrichment = not est.adresse_l1 or len(est.adresse_l1) < 5
            
            if not needs_enrichment:
                print(f"      ✅ Adresse déjà présente")
                enriched.append(est)
                continue
            
            self.stats['enrichments_attempted'] += 1
            
            # Rechercher sur Google Places
            places_data = self._search_place(est.nom, est.commune, est.code_postal)
            
            if places_data.confidence in ['medium', 'high']:
                self.stats['enrichments_successful'] += 1
                
                # Enrichir adresse
                if places_data.adresse and not est.adresse_l1:
                    est.adresse_l1 = places_data.adresse
                    self.stats['addresses_added'] += 1
                    print(f"      ✅ Adresse ajoutée: {places_data.adresse}")
                
                # Enrichir téléphone si manquant
                if places_data.telephone and not est.telephone:
                    est.telephone = places_data.telephone
                    self.stats['phones_added'] += 1
                    print(f"      ✅ Téléphone ajouté: {places_data.telephone}")
                
                # Enrichir site web si manquant
                if places_data.site_web and not est.site_web:
                    est.site_web = places_data.site_web
                    self.stats['websites_added'] += 1
                    print(f"      ✅ Site web ajouté: {places_data.site_web}")
            else:
                print(f"      ⚠️ Aucun résultat Google Places fiable")
            
            enriched.append(est)
        
        # Afficher stats
        self._print_stats()
        
        return enriched
    
    def _search_place(self, nom: str, commune: str, code_postal: str) -> PlacesEnrichment:
        """
        Recherche un établissement sur Google Places API
        
        Returns:
            PlacesEnrichment avec données trouvées
        """
        # Construire la requête de recherche
        query_parts = [nom]
        if commune:
            query_parts.append(commune)
        if code_postal:
            query_parts.append(code_postal)
        
        query = " ".join(query_parts)
        
        try:
            # Appel à la nouvelle API Places (New) - Text Search
            response = requests.post(
                'https://places.googleapis.com/v1/places:searchText',
                headers={
                    'Content-Type': 'application/json',
                    'X-Goog-Api-Key': self.google_api_key,
                    'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.internationalPhoneNumber,places.websiteUri,places.location,places.types'
                },
                json={
                    'textQuery': query,
                    'languageCode': 'fr',
                    'regionCode': 'FR'
                },
                timeout=10
            )
            
            if response.status_code != 200:
                print(f"      ❌ Erreur Google Places API: {response.status_code}")
                return PlacesEnrichment()
            
            data = response.json()
            
            # Debug - nouvelle API n'a pas de status, juste places[]
            places = data.get('places', [])
            print(f"      🔍 Places API (New): {len(places)} résultats")
            
            if not places:
                print(f"      ⚠️ Aucun résultat pour: {query}")
                return PlacesEnrichment()
            
            # Prendre le premier résultat (meilleur match)
            place = places[0]
            
            # Construire enrichissement directement (nouvelle API retourne tout)
            enrichment = PlacesEnrichment()
            
            # Adresse formatée
            enrichment.adresse = place.get('formattedAddress', '')
            
            # Téléphone international formaté
            enrichment.telephone = place.get('internationalPhoneNumber', '')
            
            # Site web
            enrichment.site_web = place.get('websiteUri', '')
            
            # Coordonnées GPS
            location = place.get('location', {})
            enrichment.latitude = location.get('latitude', 0.0)
            enrichment.longitude = location.get('longitude', 0.0)
            
            # Évaluer confiance
            place_name = place.get('displayName', {}).get('text', '')
            place_types = place.get('types', [])
            enrichment.confidence = self._evaluate_confidence_v2(place_name, place_types, nom)
            
            # Debug
            print(f"      📍 Trouvé: {place_name} - Confiance: {enrichment.confidence}")
            
            return enrichment
            
        except Exception as e:
            print(f"      ❌ Erreur recherche Places: {e}")
            return PlacesEnrichment()
    
    def _get_place_details(self, place_id: str) -> Optional[Dict]:
        """
        Récupère les détails complets d'un lieu via Place Details API
        
        Returns:
            Dict avec détails ou None
        """
        try:
            response = requests.get(
                'https://maps.googleapis.com/maps/api/place/details/json',
                params={
                    'place_id': place_id,
                    'fields': 'name,formatted_address,formatted_phone_number,website,geometry,types',
                    'key': self.google_api_key,
                    'language': 'fr'
                },
                timeout=10
            )
            
            if response.status_code != 200:
                return None
            
            data = response.json()
            
            if data.get('status') != 'OK':
                return None
            
            return data.get('result')
            
        except Exception as e:
            return None
    
    def _evaluate_confidence_v2(self, place_name: str, place_types: list, original_name: str) -> str:
        """
        Évalue la confiance du résultat Google Places (nouvelle API)
        
        Returns:
            "none", "low", "medium", "high"
        """
        name_from_google = place_name.lower()
        original_lower = original_name.lower()
        
        # Vérifier similarité nom
        words_google = set(name_from_google.split())
        words_original = set(original_lower.split())
        
        # Intersection des mots
        common_words = words_google & words_original
        
        if not common_words:
            return "low"
        
        # Ratio mots communs
        ratio = len(common_words) / max(len(words_original), len(words_google))
        
        # Types de lieu (doit être un établissement)
        # Nouvelle API utilise des types différents
        relevant_types = [
            'lodging', 'health', 'senior_living', 'assisted_living_facility',
            'nursing_home', 'retirement_home', 'point_of_interest', 'establishment'
        ]
        is_relevant = any(t in place_types for t in relevant_types)
        
        # Si très bon match de nom et type pertinent
        if ratio >= 0.7 and is_relevant:
            return "high"
        elif ratio >= 0.5 and is_relevant:
            return "medium"
        # Accepter même sans type si bon match de nom
        elif ratio >= 0.6:
            return "medium"
        elif ratio >= 0.3:
            return "low"
        else:
            return "none"
    
    def _evaluate_confidence(self, details: Dict, original_name: str) -> str:
        """
        Évalue la confiance du résultat Google Places (ancienne API - legacy)
        Gardée pour compatibilité
        
        Returns:
            "none", "low", "medium", "high"
        """
        name_from_google = details.get('name', '').lower()
        original_lower = original_name.lower()
        
        # Vérifier similarité nom
        words_google = set(name_from_google.split())
        words_original = set(original_lower.split())
        
        # Intersection des mots
        common_words = words_google & words_original
        
        if not common_words:
            return "low"
        
        # Ratio mots communs
        ratio = len(common_words) / max(len(words_original), len(words_google))
        
        # Types de lieu (doit être un établissement)
        types = details.get('types', [])
        is_establishment = any(t in types for t in ['point_of_interest', 'establishment', 'health', 'lodging'])
        
        if ratio >= 0.7 and is_establishment:
            return "high"
        elif ratio >= 0.5 and is_establishment:
            return "medium"
        elif ratio >= 0.3:
            return "low"
        else:
            return "none"
    
    def _print_stats(self):
        """Affiche les statistiques d'enrichissement"""
        print(f"\n📊 === STATISTIQUES PLACES ENRICHER ===")
        print(f"   Tentatives d'enrichissement: {self.stats['enrichments_attempted']}")
        print(f"   Enrichissements réussis: {self.stats['enrichments_successful']}")
        print(f"   Adresses ajoutées: {self.stats['addresses_added']}")
        print(f"   Téléphones ajoutés: {self.stats['phones_added']}")
        print(f"   Sites web ajoutés: {self.stats['websites_added']}")
        print("=" * 50)
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques"""
        return self.stats.copy()
