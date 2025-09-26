import requests
import time
from bs4 import BeautifulSoup
import re
from typing import Dict, Optional, List
import streamlit as st
from config import WEB_ENRICHMENT_CONFIG

class WebEnrichmentService:
    """Service d'enrichissement des données via web scraping et APIs"""
    
    def __init__(self):
        self.config = WEB_ENRICHMENT_CONFIG
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['user_agent']
        })
    
    def enrich_from_website(self, url: str) -> Dict:
        """
        Enrichit les données depuis le site web de l'établissement
        
        Returns:
            Dict avec les informations extraites pour la base habitat
        """
        if not url or not url.startswith(('http://', 'https://')):
            return {'error': 'URL invalide'}

        try:
            response = self.session.get(
                url,
                timeout=self.config['timeout']
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            enrichment_data = {
                'url': url,
                'type_public': self._extract_type_public(soup),
                'restauration': self._extract_restauration_services(soup),
                'tarifs': self._extract_tarifs(soup),
                'services': self._extract_services_habitat(soup),
                'eligibilite_statut': self._extract_eligibilite_avp(soup)
            }
            
            return enrichment_data
            
        except requests.exceptions.Timeout:
            return {'error': 'Timeout lors de la récupération'}
        except requests.exceptions.RequestException as e:
            return {'error': f'Erreur réseau : {str(e)}'}
        except Exception as e:
            return {'error': f'Erreur parsing : {str(e)}'}
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrait le titre de la page"""
        title_tag = soup.find('title')
        if title_tag:
            return title_tag.get_text().strip()
        
        # Essayer h1 comme fallback
        h1_tag = soup.find('h1')
        if h1_tag:
            return h1_tag.get_text().strip()
        
        return None
    
    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrait une description depuis les meta tags ou le contenu"""
        # Essayer meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content'].strip()
        
        # Essayer Open Graph description
        og_desc = soup.find('meta', attrs={'property': 'og:description'})
        if og_desc and og_desc.get('content'):
            return og_desc['content'].strip()
        
        # Essayer de trouver le premier paragraphe significatif
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            text = p.get_text().strip()
            if len(text) > 100:  # Paragraphe assez long
                return text[:500] + '...' if len(text) > 500 else text
        
        return None
    
    def _extract_contact_info(self, soup: BeautifulSoup) -> Dict:
        """Extrait les informations de contact"""
        contact_info = {}
        
        # Rechercher les emails
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        text_content = soup.get_text()
        emails = re.findall(email_pattern, text_content)
        if emails:
            contact_info['emails'] = list(set(emails))  # Supprimer les doublons
        
        # Rechercher les téléphones français
        phone_pattern = r'(?:(?:\+33\s?|0)[1-9](?:[\s.-]?\d{2}){4})'
        phones = re.findall(phone_pattern, text_content)
        if phones:
            contact_info['phones'] = list(set(phones))
        
        # Rechercher les adresses (basique)
        address_indicators = ['adresse', 'address', 'rue', 'avenue', 'boulevard', 'place']
        for indicator in address_indicators:
            pattern = f'{indicator}[^\\n]*'
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                contact_info['addresses'] = matches[:3]  # Limiter à 3 résultats
                break
        
        return contact_info
    
    def _extract_type_public(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrait le type de public ciblé"""
        text_content = soup.get_text().lower()
        
        # Mots-clés pour chaque type de public
        type_indicators = {
            'personnes_agees': [
                'personnes âgées', 'seniors', 'retraités', 'âge', 'vieillissement',
                'résidence senior', 'ehpad', 'maison de retraite', '60 ans', '65 ans',
                'troisième âge', 'ainés', 'aînés'
            ],
            'personnes_handicapees': [
                'handicap', 'handicapé', 'pmr', 'mobilité réduite', 'accessibilité',
                'fauteuil roulant', 'malvoyant', 'malentendant', 'déficient',
                'accompagnement spécialisé'
            ],
            'alzheimer_accessible': [
                'alzheimer', 'démence', 'troubles cognitifs', 'mémoire',
                'unité protégée', 'cantou', 'pasa', 'troubles neurocognitifs'
            ]
        }
        
        scores = {}
        for type_public, keywords in type_indicators.items():
            score = sum(1 for keyword in keywords if keyword in text_content)
            if score > 0:
                scores[type_public] = score
        
        if not scores:
            return None
            
        # Déterminer le type principal
        max_score = max(scores.values())
        main_types = [type_pub for type_pub, score in scores.items() if score == max_score]
        
        # Si plusieurs types détectés avec même score = mixte
        if len(main_types) > 1:
            return 'mixtes'
        
        return main_types[0]

    def _extract_restauration_services(self, soup: BeautifulSoup) -> Dict:
        """Extrait les services de restauration"""
        text_content = soup.get_text().lower()
        
        restauration = {
            'kitchenette': False,
            'resto_collectif_midi': False,
            'resto_collectif': False,
            'portage_repas': False
        }
        
        # Détection kitchenette
        kitchenette_keywords = [
            'kitchenette', 'cuisine équipée', 'coin cuisine', 'kitchenettes',
            'cuisinette', 'cuisine dans', 'plaque de cuisson', 'micro-onde'
        ]
        if any(keyword in text_content for keyword in kitchenette_keywords):
            restauration['kitchenette'] = True
        
        # Détection restaurant collectif
        resto_collectif_keywords = [
            'restaurant', 'salle à manger', 'restaurant collectif', 'repas collectif',
            'salle de restaurant', 'service restaurant', 'restauration collective'
        ]
        if any(keyword in text_content for keyword in resto_collectif_keywords):
            restauration['resto_collectif'] = True
            
            # Vérifier si spécifiquement midi
            midi_keywords = ['midi', 'déjeuner', 'repas de midi']
            if any(keyword in text_content for keyword in midi_keywords):
                restauration['resto_collectif_midi'] = True
        
        # Détection portage de repas
        portage_keywords = [
            'portage repas', 'portage de repas', 'livraison repas', 'repas livrés',
            'plateaux repas', 'service traiteur', 'repas à domicile'
        ]
        if any(keyword in text_content for keyword in portage_keywords):
            restauration['portage_repas'] = True
        
        return restauration

    def _extract_tarifs(self, soup: BeautifulSoup) -> Dict:
        """Extrait les informations tarifaires"""
        from datetime import datetime
        
        text_content = soup.get_text()
        
        tarifs = {
            'prix_min': None,
            'prix_max': None,
            'fourchette_prix': None,
            'date_observation': datetime.now().strftime('%Y-%m-%d')
        }
        
        # Patterns de prix améliorés
        price_patterns = [
            r'(\d{3,4})\s*€.*?(?:par mois|/mois|mensuel)',
            r'(?:à partir de|dès|depuis)\s*(\d{3,4})\s*€',
            r'(?:entre|de)\s*(\d{3,4})\s*€.*?(?:et|à)\s*(\d{3,4})\s*€',
            r'tarif.*?(\d{3,4})\s*€',
            r'(\d{3,4})\s*euros?.*?(?:mois|mensuel)',
            r'loyer.*?(\d{3,4})\s*€'
        ]
        
        prix_trouves = []
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    # Pattern avec fourchette (min-max)
                    prix_trouves.extend([int(p) for p in match if p.isdigit()])
                else:
                    if match.isdigit():
                        prix_trouves.append(int(match))
        
        if prix_trouves:
            tarifs['prix_min'] = min(prix_trouves)
            tarifs['prix_max'] = max(prix_trouves)
            
            # Déterminer la fourchette de prix
            prix_moyen = (tarifs['prix_min'] + tarifs['prix_max']) / 2
            if prix_moyen < 750:
                tarifs['fourchette_prix'] = 'euro'
            elif prix_moyen <= 1500:
                tarifs['fourchette_prix'] = 'deux_euros'
            else:
                tarifs['fourchette_prix'] = 'trois_euros'
        
        return tarifs

    def _extract_services_habitat(self, soup: BeautifulSoup) -> Dict:
        """Extrait les services spécifiques à l'habitat"""
        text_content = soup.get_text().lower()
        
        services = {
            'activites_organisees': False,
            'espace_partage': False,
            'personnel_de_nuit': False,
            'commerces_a_pied': False,
            'medecin_intervenant': False
        }
        
        # Activités organisées
        activites_keywords = [
            'activités', 'animations', 'ateliers', 'sorties', 'loisirs',
            'programme d\'activités', 'activités organisées', 'planning d\'animations'
        ]
        if any(keyword in text_content for keyword in activites_keywords):
            services['activites_organisees'] = True
        
        # Espace partagé
        espace_keywords = [
            'espace commun', 'salon commun', 'salle commune', 'espace partagé',
            'espace de vie', 'lieu de rencontre', 'espace collectif', 'salon de détente'
        ]
        if any(keyword in text_content for keyword in espace_keywords):
            services['espace_partage'] = True
        
        # Personnel de nuit
        nuit_keywords = [
            'personnel de nuit', 'garde de nuit', 'veille de nuit', 'surveillance nocturne',
            'présence nocturne', 'astreinte de nuit', '24h/24', '24 heures'
        ]
        if any(keyword in text_content for keyword in nuit_keywords):
            services['personnel_de_nuit'] = True
        
        # Commerces à pied
        commerces_keywords = [
            'commerces à proximité', 'commerces proches', 'centre ville',
            'proximité commerces', 'à pied', 'marchés', 'boutiques',
            'centre commercial proche', 'services de proximité'
        ]
        if any(keyword in text_content for keyword in commerces_keywords):
            services['commerces_a_pied'] = True
        
        # Médecin intervenant
        medecin_keywords = [
            'médecin', 'médecin traitant', 'consultation médicale',
            'suivi médical', 'médecin intervenant', 'soins médicaux',
            'cabinet médical', 'visite médicale'
        ]
        if any(keyword in text_content for keyword in medecin_keywords):
            services['medecin_intervenant'] = True
        
        return services

    def _extract_eligibilite_avp(self, soup: BeautifulSoup) -> str:
        """Extrait l'éligibilité à l'Aide à la Vie Partagée (AVP)"""
        text_content = soup.get_text().lower()
        
        # Mots-clés pour l'éligibilité AVP
        avp_eligible_keywords = [
            'avp', 'aide à la vie partagée', 'aide vie partagée',
            'éligible avp', 'bénéficie avp', 'conventionné'
        ]
        
        avp_non_eligible_keywords = [
            'non éligible avp', 'pas d\'avp', 'sans avp',
            'non conventionné', 'privé non conventionné'
        ]
        
        # Vérifier l'éligibilité
        if any(keyword in text_content for keyword in avp_eligible_keywords):
            return 'avp_eligible'
        elif any(keyword in text_content for keyword in avp_non_eligible_keywords):
            return 'non_eligible'
        else:
            return 'a_verifier'
    
    def _extract_images(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extrait les URLs des images pertinentes"""
        images = []
        
        # Rechercher les images avec des mots-clés pertinents dans alt ou src
        relevant_keywords = ['residence', 'logement', 'chambre', 'appartement', 'batiment', 'facade']
        
        img_tags = soup.find_all('img')
        for img in img_tags:
            src = img.get('src')
            alt = img.get('alt', '').lower()
            
            if src and any(keyword in alt for keyword in relevant_keywords):
                # Convertir en URL absolue si nécessaire
                if src.startswith('http'):
                    images.append(src)
                elif src.startswith('/'):
                    from urllib.parse import urljoin
                    images.append(urljoin(base_url, src))
        
        return images[:5]  # Limiter à 5 images
    
    def _extract_opening_hours(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrait les horaires d'ouverture"""
        # Rechercher des patterns d'horaires
        hour_patterns = [
            r'(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche).*?\d{1,2}[h:]\d{2}',
            r'\d{1,2}[h:]\d{2}.*?\d{1,2}[h:]\d{2}',
            r'ouvert.*?\d{1,2}[h:]',
            r'horaires?.*?\d{1,2}[h:]'
        ]
        
        text_content = soup.get_text()
        
        for pattern in hour_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                return '; '.join(matches[:3])  # Retourner les premiers matches
        
        return None
    
    def _extract_price_info(self, soup: BeautifulSoup) -> Dict:
        """Extrait les informations de prix"""
        price_info = {}
        
        # Rechercher des mentions de prix
        price_patterns = [
            r'(\d+)\s*€.*?(?:mois|mensuel)',
            r'(?:à partir de|dès)\s*(\d+)\s*€',
            r'tarif.*?(\d+)\s*€'
        ]
        
        text_content = soup.get_text()
        
        for pattern in price_patterns:
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                prices = [int(match) for match in matches if match.isdigit()]
                if prices:
                    price_info['prix_min'] = min(prices)
                    price_info['prix_max'] = max(prices)
                    break
        
        return price_info
    
    def enrich_with_huggingface(self, nom: str, commune: str, site_web: str = None) -> Dict:
        """
        Enrichissement via Hugging Face Transformers (gratuit)
        """
        try:
            # Prompt optimisé pour extraction d'informations
            prompt = f"""
Analysez l'établissement "{nom}" situé à {commune} et extrayez les informations suivantes au format JSON :

{{
  "type_public": "personnes_agees ou personnes_handicapees ou mixtes ou alzheimer_accessible",
  "restauration": {{
    "kitchenette": true/false,
    "resto_collectif": true/false,
    "portage_repas": true/false
  }},
  "tarifs": {{
    "prix_min": nombre_ou_null,
    "prix_max": nombre_ou_null,
    "fourchette_prix": "euro ou deux_euros ou trois_euros"
  }},
  "services": {{
    "activites_organisees": true/false,
    "espace_partage": true/false,
    "personnel_de_nuit": true/false,
    "commerces_a_pied": true/false,
    "medecin_intervenant": true/false
  }},
  "eligibilite_statut": "non_eligible ou a_verifier ou avp_eligible"
}}

Contexte : euro<750€/mois, deux_euros=750-1500€/mois, trois_euros>1500€/mois
"""
            
            # Utiliser une API Hugging Face gratuite
            import requests
            import json
            
            API_URL = "https://api-inference.huggingface.co/models/microsoft/DialoGPT-medium"
            headers = {"Authorization": f"Bearer {self.config.get('huggingface_token', 'hf_demo')}"}
            
            payload = {
                "inputs": prompt,
                "parameters": {
                    "max_new_tokens": 200,
                    "temperature": 0.1
                }
            }
            
            response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                # Parser la réponse
                generated_text = result[0].get('generated_text', '') if result else ''
                
                # Extraction JSON basique (à améliorer)
                return {
                    'source': 'huggingface',
                    'raw_response': generated_text,
                    'success': True,
                    'note': 'Analyse basique - résultats à vérifier'
                }
            else:
                return {'error': f'Erreur API Hugging Face: {response.status_code}', 'source': 'huggingface'}
                
        except Exception as e:
            return {'error': f'Erreur Hugging Face: {str(e)}', 'source': 'huggingface'}

    def enrich_with_openai_compatible(self, nom: str, commune: str, site_web: str = None) -> Dict:
        """
        Enrichissement via API compatible OpenAI (diverses options gratuites)
        """
        try:
            import json
            
            # Prompt structuré
            messages = [
                {
                    "role": "system", 
                    "content": "Tu es un expert en analyse d'établissements pour seniors. Réponds uniquement en JSON valide."
                },
                {
                    "role": "user",
                    "content": f"""
Analyse l'établissement "{nom}" à {commune} et retourne UNIQUEMENT ce JSON :
{{
  "type_public": "personnes_agees|personnes_handicapees|mixtes|alzheimer_accessible",
  "restauration": {{"kitchenette": false, "resto_collectif": false, "portage_repas": false}},
  "tarifs": {{"prix_min": null, "prix_max": null, "fourchette_prix": null}},
  "services": {{"activites_organisees": false, "espace_partage": false, "personnel_de_nuit": false, "commerces_a_pied": false, "medecin_intervenant": false}},
  "eligibilite_statut": "a_verifier"
}}
Règles tarifs: euro<750€, deux_euros=750-1500€, trois_euros>1500€
"""
                }
            ]
            
            # API gratuite compatible OpenAI (ex: Together.ai, Groq, etc.)
            api_url = self.config.get('openai_compatible_url', 'https://api.groq.com/openai/v1/chat/completions')
            api_key = self.config.get('openai_compatible_key', '')
            
            if not api_key:
                return {'error': 'Clé API requise pour le service externe', 'source': 'api'}
            
            headers = {
                'Authorization': f'Bearer {api_key}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'model': self.config.get('openai_compatible_model', 'llama2-70b-4096'),
                'messages': messages,
                'temperature': 0.1,
                'max_tokens': 300
            }
            
            response = requests.post(api_url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content']
                
                # Parser le JSON
                try:
                    json_start = content.find('{')
                    json_end = content.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        json_data = json.loads(content[json_start:json_end])
                        return {
                            'source': 'api_externe',
                            'data': json_data,
                            'success': True
                        }
                except json.JSONDecodeError:
                    pass
                
                return {
                    'source': 'api_externe',
                    'raw_response': content,
                    'success': False,
                    'note': 'Réponse non-JSON'
                }
            else:
                return {'error': f'Erreur API: {response.status_code}', 'source': 'api_externe'}
                
        except Exception as e:
            return {'error': f'Erreur API externe: {str(e)}', 'source': 'api_externe'}
        """
        Enrichissement intelligent via l'API Ollama
        """
        try:
            import json
            
            # Préparer le prompt pour Ollama
            prompt = f"""
Analysez les informations sur l'établissement "{nom}" à {commune} et extraire les données suivantes au format JSON strict :

{{
  "type_public": "personnes_agees|personnes_handicapees|mixtes|alzheimer_accessible|null",
  "restauration": {{
    "kitchenette": true/false,
    "resto_collectif_midi": true/false,
    "resto_collectif": true/false,
    "portage_repas": true/false
  }},
  "tarifs": {{
    "prix_min": number_or_null,
    "prix_max": number_or_null,
    "fourchette_prix": "euro|deux_euros|trois_euros|null"
  }},
  "services": {{
    "activites_organisees": true/false,
    "espace_partage": true/false,
    "personnel_de_nuit": true/false,
    "commerces_a_pied": true/false,
    "medecin_intervenant": true/false
  }},
  "eligibilite_statut": "non_eligible|a_verifier|avp_eligible"
}}

Contexte :
- euro = moins de 750€/mois
- deux_euros = 750-1500€/mois  
- trois_euros = plus de 1500€/mois

Établissement : {nom}, {commune}
{f"Site web : {site_web}" if site_web else ""}

Répondez uniquement avec le JSON, sans texte supplémentaire.
"""

            # Appel à l'API Ollama
            response = self.session.post(
                'http://localhost:11434/api/generate',
                json={
                    'model': 'llama2',  # ou le modèle disponible
                    'prompt': prompt,
                    'stream': False,
                    'options': {
                        'temperature': 0.1,  # Réponse plus déterministe
                        'max_tokens': 500
                    }
                },
                timeout=30
            )
            
            if response.status_code == 200:
                ollama_response = response.json()
                try:
                    # Extraire et parser la réponse JSON
                    generated_text = ollama_response.get('response', '')
                    # Nettoyer la réponse pour extraire le JSON
                    json_start = generated_text.find('{')
                    json_end = generated_text.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        json_text = generated_text[json_start:json_end]
                        enriched_data = json.loads(json_text)
                        return {
                            'source': 'ollama',
                            'data': enriched_data,
                            'success': True
                        }
                except (json.JSONDecodeError, KeyError):
                    return {'error': 'Réponse Ollama invalide', 'source': 'ollama'}
            else:
                return {'error': f'Erreur API Ollama: {response.status_code}', 'source': 'ollama'}
                
        except requests.exceptions.ConnectionError:
            return {'error': 'Ollama non accessible (vérifiez que le service est démarré)', 'source': 'ollama'}
        except Exception as e:
            return {'error': f'Erreur Ollama: {str(e)}', 'source': 'ollama'}

    def enrich_establishment_complete(self, nom: str, commune: str, site_web: str = None) -> Dict:
        """
        Enrichissement complet combinant web scraping et Ollama
        """
        results = {
            'nom': nom,
            'commune': commune,
            'site_web': site_web,
            'web_scraping': {},
            'ollama_analysis': {},
            'combined_data': {}
        }
        
        # 1. Web scraping si URL disponible
        if site_web:
            web_data = self.enrich_from_website(site_web)
            results['web_scraping'] = web_data
        
        # 2. Analyse Ollama
        ollama_data = self.enrich_with_ollama(nom, commune, site_web)
        results['ollama_analysis'] = ollama_data
        
        # 3. Combiner les résultats (Ollama prioritaire car plus intelligent)
        combined = {}
        
        # Utiliser Ollama en priorité si disponible
        if ollama_data.get('success') and 'data' in ollama_data:
            combined = ollama_data['data'].copy()
            
        # Compléter avec web scraping si des données manquent
        if site_web and not ollama_data.get('error'):
            web_results = results['web_scraping']
            
            # Fusionner intelligemment
            for key in ['type_public', 'restauration', 'tarifs', 'services', 'eligibilite_statut']:
                if key not in combined or not combined[key]:
                    if key in web_results:
                        combined[key] = web_results[key]
        
        results['combined_data'] = combined
        return results

# Instance globale
web_enrichment_service = WebEnrichmentService()