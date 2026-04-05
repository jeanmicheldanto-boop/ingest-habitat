"""
Web scraping asynchrone pour l'enrichissement
"""
import asyncio
import aiohttp
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import logging
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)


class AsyncWebScraper:
    """Scraper asynchrone pour performance optimale"""
    
    def __init__(self, timeout: int = 10, max_concurrent: int = 10):
        """
        Args:
            timeout: Timeout en secondes par requête
            max_concurrent: Nombre max de connexions simultanées
        """
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        
    async def scrape_batch(self, urls: List[str]) -> List[Dict]:
        """
        Scraper plusieurs URLs en parallèle
        
        Args:
            urls: Liste d'URLs à scraper
            
        Returns:
            Liste de dictionnaires avec les données extraites
        """
        async with aiohttp.ClientSession(
            timeout=self.timeout,
            headers={'User-Agent': self.user_agent}
        ) as session:
            tasks = [self._scrape_one(session, url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filtrer les erreurs
            return [r for r in results if isinstance(r, dict)]
    
    async def _scrape_one(self, session: aiohttp.ClientSession, url: str) -> Dict:
        """
        Scraper une URL (async)
        
        Args:
            session: Session aiohttp
            url: URL à scraper
            
        Returns:
            Dictionnaire avec les données extraites
        """
        async with self.semaphore:
            try:
                async with session.get(url, ssl=False) as response:
                    if response.status != 200:
                        logger.warning(f"Status {response.status} pour {url}")
                        return {}
                    
                    html = await response.text()
                    return self._extract_data(html, url)
                    
            except asyncio.TimeoutError:
                logger.warning(f"Timeout pour {url}")
                return {}
            except Exception as e:
                logger.error(f"Erreur scraping {url}: {e}")
                return {}
    
    def _extract_data(self, html: str, url: str) -> Dict:
        """
        Extraire données du HTML
        
        Args:
            html: Contenu HTML
            url: URL source
            
        Returns:
            Dictionnaire avec données extraites
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            data = {
                'site_web': url,
                'source': url
            }
            
            # Extraction téléphone
            phone = self._extract_phone(soup)
            if phone:
                data['telephone'] = phone
            
            # Extraction email
            email = self._extract_email(soup)
            if email:
                data['email'] = email
            
            # Extraction adresse
            address = self._extract_address(soup)
            if address:
                data.update(address)
            
            # Extraction prix/tarifs
            prix = self._extract_prix(soup)
            if prix:
                data.update(prix)
            
            # Extraction services
            services = self._extract_services(soup)
            if services:
                data['services'] = services
            
            # Extraction texte descriptif
            description = self._extract_description(soup)
            if description:
                data['description_scrapee'] = description
            
            return data
            
        except Exception as e:
            logger.error(f"Erreur extraction données: {e}")
            return {}
    
    def _extract_phone(self, soup: BeautifulSoup) -> Optional[str]:
        """Extraire numéro de téléphone"""
        import re
        
        # Patterns de téléphone français
        patterns = [
            r'0[1-9](?:[\s\.]?\d{2}){4}',
            r'\+33[1-9](?:[\s\.]?\d{2}){4}',
            r'(?:0033|33)[1-9](?:[\s\.]?\d{2}){4}'
        ]
        
        text = soup.get_text()
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(0)
        
        return None
    
    def _extract_email(self, soup: BeautifulSoup) -> Optional[str]:
        """Extraire email"""
        import re
        
        # Chercher dans les liens mailto
        for link in soup.find_all('a', href=True):
            if link['href'].startswith('mailto:'):
                email = link['href'].replace('mailto:', '')
                if '@' in email:
                    return email.split('?')[0]  # Enlever les paramètres
        
        # Chercher dans le texte
        pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        text = soup.get_text()
        match = re.search(pattern, text)
        if match:
            return match.group(0)
        
        return None
    
    def _extract_address(self, soup: BeautifulSoup) -> Dict:
        """Extraire adresse"""
        address_data = {}
        
        # Chercher dans les balises avec classes communes
        address_classes = ['address', 'adresse', 'location', 'contact']
        for cls in address_classes:
            elem = soup.find(class_=lambda x: x and cls in x.lower())
            if elem:
                text = elem.get_text(strip=True)
                if len(text) > 10:  # Filtrer les faux positifs
                    address_data['adresse_scrapee'] = text
                    break
        
        return address_data
    
    def _extract_prix(self, soup: BeautifulSoup) -> Dict:
        """Extraire informations de prix"""
        import re
        prix_data = {}
        
        text = soup.get_text()
        
        # Patterns de prix
        patterns = [
            r'(\d+)\s*€?\s*(?:par|/)\s*mois',
            r'à\s*partir\s*de\s*(\d+)\s*€',
            r'loyer[:\s]+(\d+)\s*€',
            r'tarif[:\s]+(\d+)\s*€'
        ]
        
        prix_trouves = []
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            prix_trouves.extend([int(m) for m in matches])
        
        if prix_trouves:
            prix_data['prix_min'] = min(prix_trouves)
            if len(prix_trouves) > 1:
                prix_data['prix_max'] = max(prix_trouves)
        
        return prix_data
    
    def _extract_services(self, soup: BeautifulSoup) -> List[str]:
        """Extraire liste de services"""
        services = []
        
        # Mots-clés de services à détecter
        services_keywords = {
            'activités organisées': ['activités', 'animations', 'ateliers'],
            'espace_partage': ['espace commun', 'salle commune', 'partage'],
            'conciergerie': ['conciergerie', 'accueil'],
            'personnel de nuit': ['personnel de nuit', 'garde de nuit', 'veilleur'],
            'commerces à pied': ['commerces', 'proximité', 'centre-ville'],
            'médecin intervenant': ['médecin', 'médical', 'infirmier']
        }
        
        text = soup.get_text().lower()
        
        for service, keywords in services_keywords.items():
            if any(kw in text for kw in keywords):
                services.append(service)
        
        return services
    
    def _extract_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extraire description principale"""
        # Chercher dans les meta description
        meta_desc = soup.find('meta', {'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            return meta_desc['content']
        
        # Chercher dans les paragraphes principaux
        main_content = soup.find(['main', 'article', 'div'], class_=lambda x: x and 'content' in str(x).lower())
        if main_content:
            paragraphs = main_content.find_all('p')
            if paragraphs:
                desc = ' '.join([p.get_text(strip=True) for p in paragraphs[:3]])
                if len(desc) > 50:
                    return desc[:500]  # Limiter la longueur
        
        return None


def scrape_url_sync(url: str, timeout: int = 10) -> Dict:
    """
    Version synchrone pour compatibilité
    
    Args:
        url: URL à scraper
        timeout: Timeout en secondes
        
    Returns:
        Dictionnaire avec les données extraites
    """
    scraper = AsyncWebScraper(timeout=timeout)
    
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(scraper._scrape_one(
        aiohttp.ClientSession(), url
    ))
