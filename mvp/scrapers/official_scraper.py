"""
OfficialScraper - Module d'extraction depuis l'annuaire officiel gouv.fr
Cible : 70% des établissements (Résidences autonomie + MARPA)
Source : https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie/
"""

import requests
import time
import re
from bs4 import BeautifulSoup
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
from urllib.parse import urljoin, urlparse
import pandas as pd
import sys
import os

# Ajouter le chemin vers config_mvp
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config_mvp import scraping_config


@dataclass
class EstablishmentData:
    """Structure de données pour un établissement (format prompt original)"""
    nom: str
    commune: str
    code_postal: str = ""
    gestionnaire: str = ""
    adresse_l1: str = ""
    telephone: str = ""
    email: str = ""
    site_web: str = ""
    sous_categories: str = ""
    habitat_type: str = ""
    eligibilite_avp: str = "a_verifier"
    presentation: str = ""
    departement: str = ""
    source: str = ""
    date_extraction: str = ""
    public_cible: str = "personnes_agees"


class OfficialScraper:
    """
    Scraper pour l'annuaire officiel des résidences autonomie
    """
    
    def __init__(self, rate_limit_delay: float = 1.0, timeout: int = 15):
        self.rate_limit_delay = rate_limit_delay
        self.timeout = timeout
        self.scrapingbee_api_key = scraping_config.scrapingbee_api_key
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })
        
        # Mapping départements vers URLs - Support complet France métropolitaine + DOM-TOM
        self.dept_urls = self._generate_dept_urls()
        
        # Mapping pour normalisation
        self.sous_categories_mapping = {
            'résidence autonomie': 'Résidence autonomie',
            'residence autonomie': 'Résidence autonomie', 
            'résidence autonomie marpa': 'MARPA',
            'marpa': 'MARPA',
            'MARPA': 'MARPA',
            'résidence services seniors': 'Résidence services seniors',
            'résidence services': 'Résidence services seniors',
            'residence services seniors': 'Résidence services seniors',
            'résidence avec services': 'Résidence services seniors'
        }
        
        self.habitat_type_mapping = {
            'Résidence autonomie': 'residence',
            'MARPA': 'residence',
            'Résidence services seniors': 'residence'
        }
        
        # Patterns de validation
        self.patterns = {
            'telephone': re.compile(r'(\+33|0)[1-9](?:[\s\.-]?\d{2}){4}'),
            'email': re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'),
            'code_postal': re.compile(r'\b\d{5}\b'),
            'finess': re.compile(r'Finess n° (\d{9})')
        }

    def _generate_dept_urls(self) -> dict:
        """Génère automatiquement les URLs pour tous les départements français"""
        
        # Mapping code département → nom URL (format normalisé gouv.fr)
        dept_mapping = {
            '01': 'ain-01',
            '02': 'aisne-02', 
            '03': 'allier-03',
            '04': 'alpes-de-haute-provence-04',
            '05': 'hautes-alpes-05',
            '06': 'alpes-maritimes-06',
            '07': 'ardeche-07',
            '08': 'ardennes-08',
            '09': 'ariege-09',
            '10': 'aube-10',
            '11': 'aude-11',
            '12': 'aveyron-12',
            '13': 'bouches-du-rhone-13',
            '14': 'calvados-14',
            '15': 'cantal-15',
            '16': 'charente-16',
            '17': 'charente-maritime-17',
            '18': 'cher-18',
            '19': 'correze-19',
            '21': 'cote-dor-21',
            '22': 'cotes-d-armor-22',
            '23': 'creuse-23',
            '24': 'dordogne-24',
            '25': 'doubs-25',
            '26': 'drome-26',
            '27': 'eure-27',
            '28': 'eure-et-loir-28',
            '29': 'finistere-29',
            '2A': 'corse-du-sud-2a',
            '2B': 'haute-corse-2b',
            '30': 'gard-30',
            '31': 'haute-garonne-31',
            '32': 'gers-32',
            '33': 'gironde-33',
            '34': 'herault-34',
            '35': 'ille-et-vilaine-35',
            '36': 'indre-36',
            '37': 'indre-et-loire-37',
            '38': 'isere-38',
            '39': 'jura-39',
            '40': 'landes-40',
            '41': 'loir-et-cher-41',
            '42': 'loire-42',
            '43': 'haute-loire-43',
            '44': 'loire-atlantique-44',
            '45': 'loiret-45',
            '46': 'lot-46',
            '47': 'lot-et-garonne-47',
            '48': 'lozere-48',
            '49': 'maine-et-loire-49',
            '50': 'manche-50',
            '51': 'marne-51',
            '52': 'haute-marne-52',
            '53': 'mayenne-53',
            '54': 'meurthe-et-moselle-54',
            '55': 'meuse-55',
            '56': 'morbihan-56',
            '57': 'moselle-57',
            '58': 'nievre-58',
            '59': 'nord-59',
            '60': 'oise-60',
            '61': 'orne-61',
            '62': 'pas-de-calais-62',
            '63': 'puy-de-dome-63',
            '64': 'pyrenees-atlantiques-64',
            '65': 'hautes-pyrenees-65',
            '66': 'pyrenees-orientales-66',
            '67': 'bas-rhin-67',
            '68': 'haut-rhin-68',
            '69': 'rhone-69',
            '70': 'haute-saone-70',
            '71': 'saone-et-loire-71',
            '72': 'sarthe-72',
            '73': 'savoie-73',
            '74': 'haute-savoie-74',
            '75': 'paris-75',
            '76': 'seine-maritime-76',
            '77': 'seine-et-marne-77',
            '78': 'yvelines-78',
            '79': 'deux-sevres-79',
            '80': 'somme-80',
            '81': 'tarn-81',
            '82': 'tarn-et-garonne-82',
            '83': 'var-83',
            '84': 'vaucluse-84',
            '85': 'vendee-85',
            '86': 'vienne-86',
            '87': 'haute-vienne-87',
            '88': 'vosges-88',
            '89': 'yonne-89',
            '90': 'territoire-de-belfort-90',
            '91': 'essonne-91',
            '92': 'hauts-de-seine-92',
            '93': 'seine-saint-denis-93',
            '94': 'val-de-marne-94',
            '95': 'val-doise-95',
            # DOM-TOM
            '971': 'guadeloupe-971',
            '972': 'martinique-972',
            '973': 'guyane-973',
            '974': 'la-reunion-974',
            '976': 'mayotte-976'
        }
        
        urls = {}
        base_url_autonomie = 'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie'
        base_url_services = 'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-service'
        
        for code_dept, dept_slug in dept_mapping.items():
            urls[code_dept] = {
                'residences_autonomie': f'{base_url_autonomie}/{dept_slug}',
                'residences_services': f'{base_url_services}/{dept_slug}'
            }

        # Exception explicite pour la Côte d'Or (problèmes éventuels liés aux apostrophes
        # ou variations de slug). On force les URLs officielles connues pour ce département.
        urls['21'] = {
            'residences_autonomie': 'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie/cote-dor-21',
            'residences_services': 'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-service/cote-dor-21'
        }

        # Spécificité pour le Rhône (69): utiliser des endpoints alternatifs/paramétrés
        # Résidences autonomie : endpoint avec paramètre département
        # Résidences services : plusieurs pages (métropole de Lyon + listing départemental)
        urls['69'] = {
            'residences_autonomie': 'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie?departement=RHONE-69',
            'residences_services': [
                'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-service/metropole-de-lyon-69m',
                'https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-service?departement=rhone-69d#container-result-query'
            ]
        }
        
        return urls
    
    def extract_establishments(self, department: str) -> List[EstablishmentData]:
        """
        Extrait tous les établissements d'un département depuis les deux annuaires officiels
        """
        print(f"🔍 Extraction département {department}")
        
        if department not in self.dept_urls:
            print(f"❌ Département {department} non supporté")
            return []
        
        establishments = []
        
        # Extraire depuis les annuaires (supporte cas où la valeur est une list de URLs)
        for annuaire_type, main_url in self.dept_urls[department].items():
            urls_to_process = main_url if isinstance(main_url, list) else [main_url]

            for url_idx, url_to_use in enumerate(urls_to_process, 1):
                display_url = url_to_use if len(urls_to_process) == 1 else f"{url_to_use} (part {url_idx}/{len(urls_to_process)})"
                print(f"\n📄 Scraping {annuaire_type}: {display_url}")

                try:
                    # Les résidences services sont affichées directement sur la page de listing
                    # Les résidences autonomie ont des fiches individuelles
                    if annuaire_type == 'residences_services':
                        # Extraction directe depuis la page de listing
                        direct_establishments = self._extract_services_from_listing(url_to_use, department)
                        establishments.extend(direct_establishments)
                        print(f"🏢 {len(direct_establishments)} résidences services extraites directement")

                    else:  # residences_autonomie
                        # Extraction via fiches individuelles
                        main_content = self._get_page_content(url_to_use)
                        if not main_content:
                            print(f"❌ Impossible de récupérer {annuaire_type}")
                            continue

                        establishment_links = self._extract_establishment_links(main_content, url_to_use, '/residence-autonomie/')
                        print(f"🏢 {len(establishment_links)} fiches établissements trouvées dans {annuaire_type}")

                        # Scraper chaque fiche établissement
                        for i, link in enumerate(establishment_links, 1):
                            print(f"   🔍 Traitement {i}/{len(establishment_links)}: {link}")

                            try:
                                establishment = self._extract_establishment_details(link, department, annuaire_type)
                                if establishment:
                                    establishments.append(establishment)
                                    print(f"      ✅ {establishment.nom}")
                                else:
                                    print(f"      ❌ Échec extraction")

                            except Exception as e:
                                print(f"      ❌ Erreur: {e}")

                            # Rate limiting
                            time.sleep(self.rate_limit_delay)

                except Exception as e:
                    print(f"❌ Erreur extraction {annuaire_type}: {e}")
        
        print(f"✅ {len(establishments)} établissements extraits au total")
        return establishments
    
    def _extract_services_from_listing(self, url: str, department: str) -> List[EstablishmentData]:
        """
        Extrait les résidences services directement depuis la page de listing
        (elles ne sont pas dans des fiches individuelles)
        """
        try:
            # Utiliser ScrapingBee pour le JavaScript rendering
            response = requests.get(
                'https://app.scrapingbee.com/api/v1/',
                params={
                    'api_key': self.scrapingbee_api_key,
                    'url': url,
                    'render_js': 'true',
                    'wait': '5000'
                },
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                print(f"❌ Erreur ScrapingBee: {response.status_code}")
                return []
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extraire toutes les résidences depuis les éléments h-card
            residences = soup.find_all('section', class_='result-entry h-card')
            print(f"   📋 {len(residences)} résidences trouvées sur la page")
            
            establishments = []
            
            for i, residence in enumerate(residences, 1):
                try:
                    # Nom
                    nom_element = residence.find('span', class_='p-name')
                    if not nom_element:
                        continue
                    nom = nom_element.get_text().strip()
                    
                    # Adresse complète depuis le texte de la section
                    all_text = residence.get_text()
                    
                    # Extraire code postal et commune
                    commune = ""
                    code_postal = ""
                    address_match = re.search(r'(\d{5})\s*-\s*(.+?)(?=\s+\d{2}\s+\d{2}|$)', all_text)
                    if address_match:
                        code_postal = address_match.group(1)
                        commune_raw = address_match.group(2).strip()
                        
                        # Nettoyer la commune : enlever le département si présent
                        # Ex: "Neuville-Saint-Vaast, PAS-DE-CALAIS" → "Neuville-Saint-Vaast"
                        commune = re.sub(r',\s*[A-Z\-\s]+$', '', commune_raw).strip()
                        
                        # NETTOYAGE DU NOM: Le nom contient parfois la commune et le département
                        # Ex: "Résidence autonomie MARPA Nova Villa Neuville-Saint-Vaast, PAS-DE-CALAIS"
                        # → "Résidence autonomie MARPA Nova Villa"
                        # ATTENTION: Ne pas nettoyer si la commune est un article seul (Le, La, Les)
                        # car c'est probablement une erreur de parsing (ex: "Le Portel" parsé comme "Le")
                        if commune and len(commune) > 3:  # Éviter les articles courts
                            # Patterns à nettoyer du nom
                            patterns_to_remove = [
                                rf'\s+{re.escape(commune)}[,\s]+[A-Z\-\s]+$',  # "Commune, DEPARTEMENT"
                                rf'\s+{re.escape(commune)}$',  # "Commune" seule
                                r',?\s+PAS-DE-CALAIS$',  # ", PAS-DE-CALAIS" résiduel
                            ]
                            for pattern in patterns_to_remove:
                                nom = re.sub(pattern, '', nom, flags=re.IGNORECASE).strip()
                    
                    # Adresse ligne 1
                    adresse_l1 = ""
                    address_full_match = re.search(r'Adresse\s+([^\n]+?)(?=\s+\d{5})', all_text)
                    if address_full_match:
                        adresse_l1 = address_full_match.group(1).strip()
                    
                    # Téléphone
                    telephone = ""
                    phone_match = re.search(r'(\d{2}\s+\d{2}\s+\d{2}\s+\d{2}\s+\d{2})', all_text)
                    if phone_match:
                        telephone = phone_match.group(1).replace(' ', ' ')
                    
                    # Site web (chercher les liens)
                    site_web = ""
                    links = residence.find_all('a', href=True)
                    for link in links:
                        href = link['href']
                        if href.startswith('http') and 'pour-les-personnes-agees.gouv.fr' not in href:
                            site_web = href
                            break
                    
                    # Gestionnaire depuis le site web
                    gestionnaire = self._extract_gestionnaire_from_url(site_web)
                    
                    # Créer l'établissement
                    establishment = EstablishmentData(
                        nom=nom,
                        adresse_l1=adresse_l1,
                        commune=commune,
                        code_postal=code_postal,
                        telephone=telephone,
                        site_web=site_web,
                        gestionnaire=gestionnaire,
                        sous_categories="Résidence services seniors",
                        habitat_type="residence",
                        departement=department,
                        email="",
                        source="pour-les-personnes-agees.gouv.fr"
                    )
                    
                    establishments.append(establishment)
                    print(f"   ✅ {i}/{len(residences)}: {nom}")
                    
                except Exception as e:
                    print(f"   ❌ Erreur extraction résidence {i}: {e}")
                    continue
                    
                # Rate limiting
                time.sleep(self.rate_limit_delay)
            
            return establishments
            
        except Exception as e:
            print(f"❌ Erreur extraction listing services: {e}")
            return []

    def _extract_gestionnaire_from_url(self, url: str) -> str:
        """Extrait le gestionnaire depuis l'URL du site web"""
        if not url:
            return ""
        
        url_lower = url.lower()
        
        if 'senioriales' in url_lower:
            return "Senioriales"
        elif 'domitys' in url_lower:
            return "Domitys"
        elif 'espaceetvie' in url_lower or 'espace-et-vie' in url_lower:
            return "Espace & Vie"
        elif 'api-residence' in url_lower:
            return "API Résidence"
        elif 'happysenior' in url_lower:
            return "Happy Senior"
        elif 'cettefamille' in url_lower:
            return "Cette Famille"
        elif 'ecrins-alienor' in url_lower:
            return "Les Écrins d'Aliénor"
        else:
            # Extraire le domaine
            try:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc.replace('www.', '')
                return domain.split('.')[0].title() if domain else ""
            except:
                return ""

    def _get_page_content(self, url: str) -> Optional[str]:
        """Récupère le contenu d'une page avec gestion d'erreurs"""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Erreur récupération {url}: {e}")
            return None
    
    def _extract_establishment_links(self, content: str, base_url: str, link_pattern: str = '/residence-autonomie/') -> List[str]:
        """Extrait les liens vers les fiches établissements"""
        soup = BeautifulSoup(content, 'html.parser')
        links = []
        
        # Rechercher les liens vers les fiches établissements
        # Pattern: /annuaire-ehpad-et-maisons-de-retraite/residence-autonomie/...
        # ou: /annuaire-ehpad-et-maisons-de-retraite/residence-service/...
        for link in soup.find_all('a', href=True):
            href = link['href']
            
            # Filtrer les liens vers les fiches établissements
            if link_pattern in href and '/annuaire-ehpad-et-maisons-de-retraite/' in href:
                full_url = urljoin(base_url, href)
                if full_url not in links:
                    links.append(full_url)
        
        return links
    
    def _extract_establishment_details(self, url: str, department: str, annuaire_type: str = 'residences_autonomie') -> Optional[EstablishmentData]:
        """Extrait les détails d'un établissement depuis sa fiche"""
        
        content = self._get_page_content(url)
        if not content:
            return None
        
        soup = BeautifulSoup(content, 'html.parser')
        
        try:
            # Extraction des informations de base
            nom = self._extract_name(soup)
            if not nom:
                return None
            
            commune, code_postal, adresse_l1 = self._parse_address_from_url_and_content(url, soup)
            
            telephone = self._extract_telephone(soup)
            email = self._extract_email(soup)
            site_web = self._extract_website(soup)
            gestionnaire = self._extract_gestionnaire(soup, site_web)
            
            # Classification selon le type d'annuaire
            if annuaire_type == 'residences_services':
                sous_categories = 'Résidence services seniors'
            else:
                sous_categories = self._classify_establishment_type(nom, soup)
            
            habitat_type = self.habitat_type_mapping.get(sous_categories, 'residence')
            
            # Présentation
            presentation = self._generate_presentation(nom, commune, sous_categories)
            
            # Métadonnées
            departement_nom = self._get_department_name(department)
            date_extraction = datetime.now().strftime('%Y-%m-%d')
            
            return EstablishmentData(
                nom=nom,
                commune=commune,
                code_postal=code_postal,
                gestionnaire=gestionnaire,
                adresse_l1=adresse_l1,
                telephone=telephone,
                email=email,
                site_web=site_web,
                sous_categories=sous_categories,
                habitat_type=habitat_type,
                eligibilite_avp="a_verifier",  # Nécessitera recherche spécifique AVP
                presentation=presentation,
                departement=departement_nom,
                source=url,
                date_extraction=date_extraction,
                public_cible="personnes_agees"
            )
            
        except Exception as e:
            print(f"Erreur extraction détails {url}: {e}")
            return None
    
    def _extract_name(self, soup: BeautifulSoup) -> str:
        """Extrait le nom de l'établissement"""
        # Chercher dans le titre H1 principal
        for tag in soup.find_all(['h1']):
            text = tag.get_text().strip()
            if any(keyword in text.lower() for keyword in ['résidence', 'marpa', 'foyer']):
                # Nettoyer le nom en retirant la ville et le département
                name = text.split(' Marmande,')[0] if ' Marmande,' in text else text
                name = name.split(' LOT-ET-GARONNE')[0] if ' LOT-ET-GARONNE' in name else name
                # Pattern plus générique
                import re
                name = re.sub(r'\s+[A-Z][a-z-]+,\s*[A-Z-]+$', '', name)
                return name.strip()
        
        return ""
    
    def _extract_address(self, soup: BeautifulSoup) -> str:
        """Extrait l'adresse complète"""
        # Rechercher l'adresse dans les éléments structurés
        address_selectors = [
            'div.address',
            'div.adresse', 
            'p.address',
            'span.address',
            '.etablissement-adresse'
        ]
        
        for selector in address_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()
        
        # Recherche par pattern dans le texte
        text_content = soup.get_text()
        lines = text_content.split('\n')
        
        for line in lines:
            line = line.strip()
            # Rechercher ligne contenant adresse (avec code postal)
            if self.patterns['code_postal'].search(line) and any(keyword in line.lower() for keyword in ['rue', 'avenue', 'place', 'boulevard', 'chemin']):
                return line
        
        return ""
    
    def _parse_address_from_url_and_content(self, url: str, soup: BeautifulSoup) -> tuple:
        """Extrait commune, code postal et adresse depuis l'URL et le contenu"""
        import re
        from urllib.parse import urlparse
        
        # Extraire depuis l'URL : .../marmande-47200/...
        url_pattern = r'/([a-z-]+)-(\d{5})/'
        url_match = re.search(url_pattern, url)
        
        commune = ""
        code_postal = ""
        
        if url_match:
            commune_slug = url_match.group(1)
            code_postal = url_match.group(2)
            
            # Convertir slug en nom propre
            commune = commune_slug.replace('-', ' ')
            # Capitaliser chaque mot
            commune = ' '.join(word.capitalize() for word in commune.split())
            
            # Corrections spécifiques
            commune = commune.replace('De ', 'de ').replace('Du ', 'du ').replace('Sur ', 'sur-')
            
        # Chercher l'adresse dans les coordonnées
        adresse_l1 = self._extract_address_from_coordinates(soup)
        
        return commune, code_postal, adresse_l1
    
    def _extract_address_from_coordinates(self, soup: BeautifulSoup) -> str:
        """Extrait l'adresse depuis la section Coordonnées"""
        import re
        
        # Méthode 1: Chercher dans le texte avec pattern "Coordonnées" suivi de l'adresse
        text = soup.get_text()
        
        # Pattern: "Coordonnées" puis adresse puis code postal-commune
        # Exemple: "Coordonnées4 rue de l'Herminette10320 - Bouilly"
        coord_pattern = r'Coordonnées(\d+[^0-9]+?)(\d{5}\s*-\s*[A-Za-zÀ-ÿ\s-]+)'
        match = re.search(coord_pattern, text)
        
        if match:
            adresse = match.group(1).strip()
            # Nettoyer l'adresse des mots parasites
            adresse = re.sub(r'Voir itinéraire.*$', '', adresse).strip()
            adresse = re.sub(r'Téléphone.*$', '', adresse).strip()
            if adresse and len(adresse) > 5:
                return adresse
        
        # Méthode 2: Chercher ligne AVANT pattern code postal dans les lignes séparées
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        
        for i, line in enumerate(lines):
            # Détecter ligne code postal (5 chiffres suivi de tiret et commune)
            if re.match(r'^\d{5}\s*-\s*[A-Za-zÀ-ÿ\s-]+$', line):
                # La ligne AVANT contient potentiellement l'adresse
                if i > 0:
                    prev_line = lines[i-1]
                    # Extraire juste la partie adresse (peut contenir "Coordonnées" collé)
                    # Pattern: numéro + texte de rue
                    addr_match = re.search(r'(\d+\s+[^\d]+?)(?=\d{5}|$)', prev_line)
                    if addr_match:
                        adresse = addr_match.group(1).strip()
                        # Nettoyer
                        adresse = adresse.replace('Coordonnées', '').strip()
                        adresse = re.sub(r'Voir itinéraire.*$', '', adresse).strip()
                        if adresse and len(adresse) > 5:
                            return adresse
                    
                    # Vérifier que c'est une adresse (commence par numéro ou contient mots-clés)
                    if (re.match(r'^\d+', prev_line) or 
                        any(kw in prev_line.lower() for kw in ['rue', 'avenue', 'place', 'boulevard', 'allée', 'chemin', 'impasse', 'route'])):
                        return prev_line
        
        return ""
    
    def _extract_telephone(self, soup: BeautifulSoup) -> str:
        """Extrait le numéro de téléphone"""
        # Rechercher liens tel: ou patterns de téléphone
        for link in soup.find_all('a', href=True):
            if link['href'].startswith('tel:'):
                phone = link['href'][4:].strip()
                if self.patterns['telephone'].match(phone):
                    return phone
        
        # Rechercher dans le texte
        text_content = soup.get_text()
        phone_match = self.patterns['telephone'].search(text_content)
        if phone_match:
            return phone_match.group(0)
        
        return ""
    
    def _extract_email(self, soup: BeautifulSoup) -> str:
        """Extrait l'adresse email"""
        # Rechercher liens mailto:
        for link in soup.find_all('a', href=True):
            if link['href'].startswith('mailto:'):
                email = link['href'][7:].strip()
                if self.patterns['email'].match(email):
                    return email
        
        # Rechercher dans le texte
        text_content = soup.get_text()
        email_match = self.patterns['email'].search(text_content)
        if email_match:
            return email_match.group(0)
        
        return ""
    
    def _extract_website(self, soup: BeautifulSoup) -> str:
        """Extrait l'URL du site web officiel"""
        # Rechercher liens externes (hors gouv.fr et Google)
        for link in soup.find_all('a', href=True):
            href = link['href']
            if (href.startswith('http') and 
                'pour-les-personnes-agees.gouv.fr' not in href and
                'google.com' not in href and
                'maps.google' not in href and
                'facebook.com' not in href and
                'youtube.com' not in href):
                
                # Priorité aux sites institutionnels
                if any(domain in href for domain in ['.gouv.fr', '.fr']):
                    return href
        
        return ""
    
    def _extract_gestionnaire(self, soup: BeautifulSoup, site_web: str) -> str:
        """Extrait le gestionnaire depuis la section Coordonnées"""
        import re
        
        # Rechercher "Gestionnaire :" dans le contenu
        text_content = soup.get_text()
        gestionnaire_match = re.search(r'Gestionnaire\s*:\s*([^\n]+)', text_content)
        
        if gestionnaire_match:
            gestionnaire = gestionnaire_match.group(1).strip()
            
            # IMPORTANT: S'arrêter exactement avant "N° FINESS"
            if 'N° FINESS' in gestionnaire or 'N° Finess' in gestionnaire or 'Finess' in gestionnaire:
                # Couper juste avant "N° FINESS" (avec variations de casse)
                gestionnaire = re.split(r'N°\s*FINESS|N°\s*Finess|Finess', gestionnaire, flags=re.IGNORECASE)[0].strip()
            
            # Limiter la longueur pour éviter les textes parasites
            if len(gestionnaire) > 100:
                gestionnaire = gestionnaire[:100].strip()
            
            # Normaliser les gestionnaires courants
            if 'centre communal' in gestionnaire.lower() or 'ccas' in gestionnaire.lower():
                return 'CCAS'
            elif 'cias' in gestionnaire.lower():
                return 'CIAS'
            elif 'mutualité' in gestionnaire.lower():
                return 'Mutualité'
            elif 'commune' in gestionnaire.lower() or 'mairie' in gestionnaire.lower():
                return 'Commune'
            elif 'centre municipal' in gestionnaire.lower():
                return 'Centre municipal d\'action sociale'
            elif 'syndicat intercommunal' in gestionnaire.lower():
                # Extraire le nom complet du syndicat
                syndicat_match = re.match(r'(Syndicat [^N]+)', gestionnaire, re.IGNORECASE)
                if syndicat_match:
                    return syndicat_match.group(1).strip()
                return 'Syndicat intercommunal'
            elif 'association' in gestionnaire.lower():
                # Extraire nom association court
                parts = gestionnaire.split()
                return ' '.join(parts[:4])  # Premiers 4 mots seulement
            else:
                return gestionnaire
        
        # Fallback sur detection dans le texte
        text_lower = text_content.lower()
        if 'ccas' in text_lower:
            return 'CCAS'
        elif 'cias' in text_lower:
            return 'CIAS'
        elif 'mutualité' in text_lower:
            return 'Mutualité'
        
        return ""
    
    def _classify_establishment_type(self, nom: str, soup: BeautifulSoup) -> str:
        """Classifie le type d'établissement"""
        nom_lower = nom.lower()
        
        if 'marpa' in nom_lower:
            return 'MARPA'
        elif any(keyword in nom_lower for keyword in ['résidence autonomie', 'residence autonomie']):
            return 'Résidence autonomie'
        else:
            # Par défaut pour l'annuaire résidences autonomie
            return 'Résidence autonomie'
    
    def _generate_presentation(self, nom: str, commune: str, sous_categories: str) -> str:
        """Génère une présentation courte de l'établissement"""
        if not nom or not commune:
            return ""
        
        return f"{sous_categories} située à {commune}, proposant un logement adapté aux seniors en perte d'autonomie légère avec services et animation collective."
    
    def _get_department_name(self, department: str) -> str:
        """Retourne le nom complet du département"""
        names = {
            '47': 'Lot-et-Garonne (47)',
            '10': 'Aube (10)'
        }
        return names.get(department, f'Département ({department})')
    
    def to_dataframe(self, establishments: List[EstablishmentData]) -> pd.DataFrame:
        """Convertit la liste d'établissements en DataFrame"""
        data = [asdict(est) for est in establishments]
        df = pd.DataFrame(data)
        
        # Réorganiser les colonnes selon le schéma du prompt
        columns_order = [
            'nom', 'commune', 'code_postal', 'gestionnaire', 'adresse_l1',
            'telephone', 'email', 'site_web', 'sous_categories', 'habitat_type',
            'eligibilite_avp', 'presentation', 'departement', 'source', 
            'date_extraction', 'public_cible'
        ]
        
        return df.reindex(columns=columns_order)
    
    def export_csv(self, establishments: List[EstablishmentData], department: str, output_dir: str = "../mvp/output") -> str:
        """Exporte les établissements en CSV"""
        import os
        
        # Créer le dossier de sortie s'il n'existe pas
        os.makedirs(output_dir, exist_ok=True)
        
        df = self.to_dataframe(establishments)
        
        filename = f"habitat_seniors_officiel_{department}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        filepath = f"{output_dir}/{filename}"
        
        df.to_csv(filepath, index=False, encoding='utf-8')
        print(f"📄 Export CSV: {filepath}")
        
        return filepath


if __name__ == "__main__":
    # Test sur Lot-et-Garonne
    scraper = OfficialScraper(rate_limit_delay=1.0)
    
    establishments = scraper.extract_establishments('47')
    
    if establishments:
        filepath = scraper.export_csv(establishments, '47')
        print(f"\n✅ {len(establishments)} établissements extraits")
        print(f"📄 Fichier: {filepath}")
        
        # Affichage échantillon
        df = scraper.to_dataframe(establishments)
        print(f"\n📊 Aperçu des données:")
        print(df[['nom', 'commune', 'telephone', 'sous_categories']].head())
    else:
        print("❌ Aucun établissement extrait")
