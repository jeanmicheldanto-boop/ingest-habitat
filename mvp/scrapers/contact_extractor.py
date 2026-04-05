"""
Module d'extraction robuste des contacts
Extrait : adresse, téléphone, email, site_web
Avec politique stricte selon type de source
Version: 1.0
"""

import re
import requests
import os
from typing import Dict, Optional, List
from dataclasses import dataclass
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from mvp.scrapers.source_classifier import SourceClassifier, SourceClassification

# Charger .env
load_dotenv()


@dataclass
class ContactData:
    """Données de contact extraites"""
    adresse: str = ""
    code_postal: str = ""  # Code postal extrait de l'adresse
    telephone: str = ""
    email: str = ""
    site_web: str = ""
    
    # Métadonnées
    contact_source_type: str = ""  # Type de source utilisée
    contact_confidence: float = 0.0  # Confiance 0-100
    extraction_method: str = ""  # regex, llm, manual


class ContactExtractor:
    """
    Extracteur de contacts avec politique stricte
    
    Politique par type de source:
    - officiel_etablissement: ✅ tout accepté (haute confiance)
    - site_gestionnaire: ✅ accepté mais confiance moyenne
    - annuaire: ⚠️ selon mode (strict=jamais, pragmatique=téléphone seulement)
    - article/autre: ❌ jamais
    """
    
    def __init__(self, groq_api_key: Optional[str] = None, 
                 scrapingbee_api_key: Optional[str] = None,
                 serper_api_key: Optional[str] = None,
                 mistral_api_key: Optional[str] = None,
                 mode: str = "strict"):
        """
        Initialise l'extracteur
        
        Args:
            groq_api_key: Clé API Groq
            scrapingbee_api_key: Clé API ScrapingBee
            serper_api_key: Clé API Serper
            mistral_api_key: Clé API Mistral (pour fallback LLM)
            mode: "strict" (jamais annuaire) ou "pragmatique" (téléphone annuaire OK)
        """
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        self.scrapingbee_api_key = scrapingbee_api_key or os.getenv('SCRAPINGBEE_API_KEY')
        self.serper_api_key = serper_api_key or os.getenv('SERPER_API_KEY')
        self.mistral_api_key = mistral_api_key or os.getenv('MISTRAL_API_KEY')
        self.mode = mode
        
        # Classifier de sources
        self.source_classifier = SourceClassifier(groq_api_key)
        
        # Patterns regex
        self.phone_pattern = re.compile(r'(\+33|0)[1-9](?:[\s\.\-]?\d{2}){4}')
        self.email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        
        # Emails suspects (ne jamais utiliser)
        self.suspicious_emails = [
            '@essentiel-autonomie.com',
            '@papyhappy.com',
            '@retraiteplus.',
            'contact@pour-les-personnes-agees'
        ]
        
        # Statistiques
        self.stats = {
            'extractions_attempted': 0,
            'extractions_successful': 0,
            'sources_official': 0,
            'sources_gestionnaire': 0,
            'sources_annuaire': 0,
            'sources_rejected': 0
        }
    
    def extract_contacts_for_establishment(self, nom: str, commune: str, 
                                          code_postal: str, 
                                          gestionnaire: Optional[str] = None) -> ContactData:
        """
        Extrait les contacts d'un établissement avec recherche ciblée
        
        Args:
            nom: Nom de l'établissement
            commune: Commune
            code_postal: Code postal
            gestionnaire: Gestionnaire optionnel
            
        Returns:
            ContactData avec adresse, téléphone, email, site_web
        """
        self.stats['extractions_attempted'] += 1
        
        print(f"      📞 Extraction contacts pour: {nom}")
        
        # Étape 1: Recherche ciblée Serper pour trouver site officiel
        print(f"         🔍 Recherche site officiel...")
        official_sources = self._search_official_sources(nom, commune, code_postal, gestionnaire)
        
        if not official_sources:
            print(f"         ⚠️ Aucune source officielle trouvée")
            return ContactData()
        
        # Étape 2: Extraire contacts depuis les meilleures sources
        print(f"         📊 {len(official_sources)} source(s) à traiter")
        
        best_contact = ContactData()
        best_confidence = 0.0
        
        for source in official_sources[:2]:  # Max 2 sources pour limiter coûts
            contact = self._extract_from_source(source, nom, commune)
            
            if contact and contact.contact_confidence > best_confidence:
                best_contact = contact
                best_confidence = contact.contact_confidence
        
        if best_confidence > 0:
            self.stats['extractions_successful'] += 1
            print(f"         ✅ Contacts extraits (confiance: {best_confidence:.0f}%)")
        else:
            print(f"         ⚠️ Aucun contact valide extrait")
        
        return best_contact
    
    def _search_official_sources(self, nom: str, commune: str, code_postal: str,
                                gestionnaire: Optional[str]) -> List[Dict]:
        """
        Recherche Serper pour sources officielles uniquement
        
        Returns:
            Liste de sources classifiées et validées
        """
        if not self.serper_api_key:
            print(f"         ⚠️ SERPER_API_KEY non configurée")
            return []
        
        # Construction requête - SANS guillemets pour meilleure compatibilité Serper
        query_parts = []
        
        # Stratégie : gestionnaire + commune en priorité (plus fiable)
        if gestionnaire and len(gestionnaire) > 3:
            query_parts.append(gestionnaire)
        
        query_parts.append(commune)
        
        if code_postal:
            query_parts.append(code_postal)
        
        # Ajouter des mots-clés du nom (sans guillemets)
        # Ex: "Habitat inclusif LADAPT" → habitat inclusif (LADAPT déjà dans gestionnaire)
        nom_keywords = nom.lower().replace(gestionnaire.lower() if gestionnaire else '', '').strip()
        if nom_keywords and len(nom_keywords) > 5:
            # Prendre premiers mots significatifs
            keywords = [w for w in nom_keywords.split()[:3] if len(w) > 3]
            if keywords:
                query_parts.extend(keywords)
        
        query = ' '.join(query_parts)
        
        # Exclusion annuaires dans la requête (Serper supporte les exclusions simples)
        exclusions = (
            '-site:essentiel-autonomie.com '
            '-site:papyhappy.com '
            '-site:pour-les-personnes-agees.gouv.fr '
            '-site:retraiteplus.fr '
            '-site:logement-seniors.com '
            '-site:capresidencesseniors.com '
            '-site:capgeris.com '
            '-site:france-maison-de-retraite.org '
            '-site:lesmaisonsderetraite.fr '
            '-site:ascellianceresidence.fr '
            '-site:ascelliance-retraite.fr '
            '-site:conseildependance.fr '
            '-site:tarif-senior.com'
        )
        
        query_with_exclusions = f"{query} {exclusions}"
        
        print(f"         📝 Query Serper: {query_with_exclusions[:100]}...")
        
        try:
            response = requests.post(
                "https://google.serper.dev/search",
                headers={
                    "X-API-KEY": self.serper_api_key,
                    "Content-Type": "application/json"
                },
                json={
                    "q": query_with_exclusions,
                    "num": 5,  # 5 résultats max
                    "gl": "fr",
                    "hl": "fr"
                },
                timeout=10
            )
            
            if response.status_code != 200:
                print(f"         ❌ Erreur Serper: {response.status_code}")
                if response.status_code == 400:
                    print(f"         💡 Détails: {response.text[:200]}")
                return []
            
            data = response.json()
            results = data.get('organic', [])
            
            print(f"         📊 {len(results)} résultat(s) Serper trouvé(s)")
            
            # Classifier chaque résultat
            validated_sources = []
            
            for result in results:
                url = result.get('link', '')
                title = result.get('title', '')
                snippet = result.get('snippet', '')
                
                # Classification
                classification = self.source_classifier.classify_source(url, title, snippet)
                
                # Politique d'acceptation
                if self._is_source_acceptable(classification):
                    validated_sources.append({
                        'url': url,
                        'title': title,
                        'snippet': snippet,
                        'classification': classification,
                        'raw_result': result  # Garder tout le résultat pour extraction
                    })
                    
                    # Log
                    if classification.type_source == 'officiel_etablissement':
                        self.stats['sources_official'] += 1
                        print(f"         ✅ Site officiel trouvé: {url[:50]}...")
                    elif classification.type_source == 'site_gestionnaire':
                        self.stats['sources_gestionnaire'] += 1
                        print(f"         ✅ Site gestionnaire trouvé: {url[:50]}...")
                else:
                    self.stats['sources_rejected'] += 1
                    print(f"         ❌ Source rejetée ({classification.type_source}): {url[:50]}...")
            
            return validated_sources
            
        except Exception as e:
            print(f"         ❌ Erreur recherche: {e}")
            return []
    
    def _is_source_acceptable(self, classification: SourceClassification) -> bool:
        """
        Détermine si une source est acceptable selon la politique
        
        Returns:
            True si acceptable
        """
        type_source = classification.type_source
        
        # Sites officiels et gestionnaires: toujours OK
        if type_source in ['officiel_etablissement', 'site_gestionnaire']:
            return classification.confidence >= 60.0
        
        # Annuaires: selon mode
        if type_source == 'annuaire':
            if self.mode == 'strict':
                return False  # Jamais en mode strict
            elif self.mode == 'pragmatique':
                # OK mais seulement pour téléphone
                return classification.confidence >= 70.0
        
        # Articles et autres: non
        return False
    
    def _extract_from_source(self, source: Dict, nom: str, commune: str) -> Optional[ContactData]:
        """
        Extrait contacts depuis une source validée
        
        Returns:
            ContactData ou None
        """
        url = source['url']
        classification = source['classification']
        snippet = source.get('snippet', '')
        
        print(f"         🌐 Extraction depuis: {url[:60]}...")
        
        # PRIORITÉ 1: Extraire depuis le snippet Google (souvent les infos sont là)
        contact = ContactData()
        contact.contact_source_type = classification.type_source
        contact.extraction_method = "snippet"
        
        # Site web: uniquement si officiel ou gestionnaire
        if classification.type_source in ['officiel_etablissement', 'site_gestionnaire']:
            contact.site_web = url
        
        # Extraction depuis snippet d'abord
        print(f"         📝 Extraction depuis snippet...")
        telephone_snippet = self._extract_telephone(snippet, nom, commune)
        email_snippet = self._extract_email(snippet, nom, commune)
        adresse_snippet, cp_snippet = self._extract_adresse(snippet, nom, commune)
        
        if telephone_snippet and not self._is_suspicious_phone(telephone_snippet):
            contact.telephone = telephone_snippet
            print(f"         ✅ Téléphone trouvé dans snippet: {telephone_snippet}")
        
        if email_snippet and not self._is_suspicious_email(email_snippet):
            contact.email = email_snippet
            print(f"         ✅ Email trouvé dans snippet: {email_snippet}")
        
        if adresse_snippet and not self._is_suspicious_adresse(adresse_snippet):
            contact.adresse = adresse_snippet
            if cp_snippet:
                contact.code_postal = cp_snippet
            print(f"         ✅ Adresse trouvée dans snippet: {adresse_snippet}")
        
        # PRIORITÉ 2: Scraper la page UNIQUEMENT si nécessaire (évite coûts inutiles)
        needs_more = (not contact.telephone) or (not contact.email) or (not contact.adresse)
        can_scrape = classification.type_source in ['officiel_etablissement', 'site_gestionnaire']

        if needs_more and can_scrape:
            print(f"         🌐 Scraping page pour compléter...")
            page_content = self._scrape_page(url)

            if page_content:
                contact.extraction_method = "mixed"

                # Compléter téléphone si manquant
                if not contact.telephone:
                    telephone = self._extract_telephone(page_content, nom, commune)
                    if telephone and not self._is_suspicious_phone(telephone):
                        contact.telephone = telephone
                        print(f"         ✅ Téléphone trouvé sur page: {telephone}")

                # Compléter email si manquant
                if not contact.email:
                    email = self._extract_email(page_content, nom, commune)
                    if email and not self._is_suspicious_email(email):
                        contact.email = email
                        print(f"         ✅ Email trouvé sur page: {email}")

                # Compléter adresse si manquante (PRIORITAIRE)
                if not contact.adresse:
                    adresse, code_postal = self._extract_adresse(page_content, nom, commune)
                    if adresse and not self._is_suspicious_adresse(adresse):
                        contact.adresse = adresse
                        if code_postal:
                            contact.code_postal = code_postal
                        print(f"         ✅ Adresse trouvée sur page: {adresse}")
        else:
            # Pas de scraping si inutile ou source non fiable
            if not can_scrape:
                print(f"         ℹ️ Scraping ignoré (source={classification.type_source})")
            elif not needs_more:
                print(f"         ℹ️ Scraping ignoré (infos déjà trouvées dans snippet)")

        # Nettoyage final de l'adresse (anti téléphone / tokens parasites)
        if contact.adresse:
            contact.adresse = self._clean_address_l1(contact.adresse)
        if contact.code_postal:
            contact.code_postal = self._clean_postal_code(contact.code_postal)
        
        # Calcul confiance selon type de source
        if classification.type_source == 'officiel_etablissement':
            contact.contact_confidence = 90.0
        elif classification.type_source == 'site_gestionnaire':
            contact.contact_confidence = 70.0
        elif classification.type_source == 'annuaire' and self.mode == 'pragmatique':
            contact.contact_confidence = 40.0
            # En mode pragmatique annuaire: garder SEULEMENT téléphone
            contact.email = ""
            contact.adresse = ""
            contact.site_web = ""
        else:
            contact.contact_confidence = 0.0
        
        return contact
    
    def _scrape_page(self, url: str) -> Optional[str]:
        """Scrape une page avec ScrapingBee"""
        
        if not self.scrapingbee_api_key:
            return None
        
        try:
            response = requests.get(
                'https://app.scrapingbee.com/api/v1/',
                params={
                    'api_key': self.scrapingbee_api_key,
                    'url': url,
                    'render_js': 'false',
                    'wait': '1000'
                },
                timeout=20
            )
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Supprimer scripts et styles
                for script in soup(["script", "style"]):
                    script.decompose()
                
                text = soup.get_text()
                lines = (line.strip() for line in text.splitlines())
                text = ' '.join(line for line in lines if line)
                
                return text[:3000]
            else:
                print(f"         ❌ ScrapingBee erreur: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"         ❌ Erreur scraping: {e}")
            return None
    
    def _extract_telephone(self, content: str, nom: str, commune: str) -> str:
        """Extrait téléphone par regex - plusieurs patterns"""
        
        # Pattern 1: Format classique 0X XX XX XX XX ou +33 X XX XX XX XX
        match = self.phone_pattern.search(content)
        if match:
            phone = match.group(0)
            # Normaliser les espaces
            phone = re.sub(r'[\.\-]', ' ', phone)
            phone = re.sub(r'\s+', ' ', phone)
            return phone.strip()
        
        # Pattern 2: Format condensé sans espaces (ex: 0757408679)
        condensed_pattern = re.compile(r'\b(0[1-9]\d{8})\b')
        match = condensed_pattern.search(content)
        if match:
            phone = match.group(0)
            # Formater avec espaces: 07 57 40 86 79
            return f"{phone[0:2]} {phone[2:4]} {phone[4:6]} {phone[6:8]} {phone[8:10]}"
        
        return ""
    
    def _extract_email(self, content: str, nom: str, commune: str) -> str:
        """Extrait email par regex"""
        match = self.email_pattern.search(content)
        if match:
            return match.group(0)
        return ""
    
    def _extract_contacts_with_llm(self, html_content: str, nom: str, commune: str) -> Dict:
        """
        Utilise Mistral LLM pour extraire contacts depuis HTML (fallback robuste)
        
        Returns:
            Dict avec adresse, code_postal, telephone, email, site_web
        """
        if not self.mistral_api_key:
            return {}
        
        # Limiter le contenu à 3000 caractères pour l'API
        content_truncated = html_content[:3000]
        
        prompt = f"""Extrait les coordonnées de cet établissement depuis le HTML ci-dessous.

ETABLISSEMENT: {nom}
COMMUNE: {commune}

HTML:
{content_truncated}

REGLES:
- Adresse: Numéro + rue uniquement (PAS de code postal ni ville dans l'adresse)
- Code postal: EXACTEMENT 5 chiffres
- Téléphone: Format 10 chiffres français (commence par 0)
- Email: Adresse email valide
- Site web: URL complète
- Si une information est absente ou incertaine, laisser vide

Reponds UNIQUEMENT en JSON valide:
{{"adresse": "12 rue Example", "code_postal": "12345", "telephone": "0123456789", "email": "contact@example.com", "site_web": "https://example.com"}}"""
        
        try:
            response = requests.post(
                'https://api.mistral.ai/v1/chat/completions',
                headers={
                    'Authorization': f'Bearer {self.mistral_api_key}',
                    'Content-Type': 'application/json'
                },
                json={
                    'model': 'mistral-large-latest',
                    'messages': [{'role': 'user', 'content': prompt}],
                    'temperature': 0.0,
                    'max_tokens': 300
                },
                timeout=90
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content'].strip()
                
                # Parser la réponse JSON
                import json
                # Nettoyer le contenu (supprimer ```json si présent)
                content = content.replace('```json', '').replace('```', '').strip()
                data = json.loads(content)
                
                return data
        except Exception as e:
            print(f"         ⚠️ Erreur LLM extraction: {e}")
        
        return {}
    
    def _extract_adresse(self, content: str, nom: str, commune: str) -> tuple[str, str]:
        """
        Extrait adresse et code postal - plusieurs patterns
        
        Returns:
            tuple (adresse_l1, code_postal)
        """
        
        # Pattern 1: Adresse complète avec CP (ex: "12 rue de la Paix, 10000 Troyes")
        adresse_pattern1 = re.compile(
            r'\d+[\s,]+(?:rue|avenue|boulevard|place|chemin|allée|impasse|route)[^,\.]{5,50}[,\s]+\d{5}[\s,]+[A-ZÀ-Ÿ][a-zà-ÿ\-\s]{2,30}',
            re.IGNORECASE
        )
        
        match = adresse_pattern1.search(content[:1500])
        if match:
            adresse = match.group(0).strip()
            # Vérifier que la commune correspond
            if commune.lower() in adresse.lower():
                # Extraire le code postal
                cp_match = re.search(r'\b(\d{5})\b', adresse)
                code_postal = cp_match.group(1) if cp_match else ""
                # IMPORTANT: Supprimer le code postal et la ville de l'adresse
                # On ne garde que la rue (adresse_l1 doit contenir uniquement le numéro et la rue)
                adresse_clean = re.sub(r'[,\s]+\d{5}.*$', '', adresse).strip()
                return adresse_clean, code_postal
        
        # Pattern 2: Adresse sans CP (ex: "12 rue de la Paix")
        adresse_pattern2 = re.compile(
            r'\d+[\s,]+(?:rue|avenue|boulevard|place|chemin|allée|impasse|route)\s+[^,\.\n]{5,50}',
            re.IGNORECASE
        )
        
        match = adresse_pattern2.search(content[:1500])
        if match:
            adresse = match.group(0).strip()
            # Nettoyer et retourner sans CP ni ville
            adresse = re.sub(r',.*', '', adresse)  # Supprimer tout après virgule
            adresse = re.sub(r'<[^>]+>', '', adresse)  # Supprimer les balises HTML
            adresse = adresse.strip()
            if len(adresse) > 10:
                # NOUVEAU: Chercher le code postal séparément dans le contenu
                # Pattern: 5 chiffres suivi optionnellement de la commune
                code_postal = ""
                cp_pattern = re.compile(
                    r'\b(\d{5})\s+' + re.escape(commune.upper()),
                    re.IGNORECASE
                )
                cp_match = cp_pattern.search(content[:1500])
                if cp_match:
                    code_postal = cp_match.group(1)
                    print(f"         ✅ Code postal extrait séparément: {code_postal}")
                else:
                    # Fallback: chercher 5 chiffres proches de l'adresse (dans les 200 caractères suivants)
                    addr_pos = content.find(adresse)
                    if addr_pos > 0:
                        vicinity = content[addr_pos:addr_pos+200]
                        cp_match = re.search(r'\b(\d{5})\b', vicinity)
                        if cp_match:
                            code_postal = cp_match.group(1)
                            print(f"         ✅ Code postal trouvé à proximité: {code_postal}")
                
                # Si toujours pas de code postal, essayer le LLM (fallback robuste)
                if not code_postal:
                    print(f"         🤖 Tentative extraction LLM (regex échouée)...")
                    llm_data = self._extract_contacts_with_llm(content, nom, commune)
                    if llm_data.get('code_postal'):
                        code_postal = llm_data['code_postal']
                        print(f"         ✅ Code postal extrait par LLM: {code_postal}")
                        # Si le LLM a aussi trouvé une meilleure adresse, l'utiliser
                        if llm_data.get('adresse') and len(llm_data['adresse']) > 10:
                            adresse = llm_data['adresse']
                            print(f"         ✅ Adresse améliorée par LLM: {adresse}")
                
                return adresse, code_postal
        
        # Si aucun pattern ne matche, essayer directement le LLM
        print(f"         🤖 Aucun pattern regex - tentative extraction LLM...")
        llm_data = self._extract_contacts_with_llm(content, nom, commune)
        if llm_data.get('adresse'):
            return llm_data['adresse'], llm_data.get('code_postal', '')
        
        return "", ""
    
    def _is_suspicious_phone(self, phone: str) -> bool:
        """Vérifie si le téléphone est suspect"""
        # Téléphones génériques à éviter
        suspicious = [
            '0800',  # Numéros verts génériques
            '08 00',
        ]
        
        phone_clean = phone.replace(' ', '').replace('.', '').replace('-', '')
        
        return any(s in phone_clean for s in suspicious)
    
    def _is_suspicious_email(self, email: str) -> bool:
        """Vérifie si l'email est suspect"""
        email_lower = email.lower()
        return any(suspect in email_lower for suspect in self.suspicious_emails)
    
    def _is_suspicious_adresse(self, adresse: str) -> bool:
        """Vérifie si l'adresse est suspecte"""
        # Adresses parisiennes des agrégateurs
        suspicious_patterns = [
            '75009 paris',
            'rue laffitte',
            '21 rue laffitte'
        ]

        adresse_lower = adresse.lower()
        return any(pattern in adresse_lower for pattern in suspicious_patterns)

    def _clean_postal_code(self, code_postal: str) -> str:
        """Garde uniquement 5 chiffres, sinon vide."""
        if not code_postal:
            return ""
        m = re.search(r'\b(\d{5})\b', str(code_postal))
        return m.group(1) if m else ""

    def _clean_address_l1(self, adresse: str) -> str:
        """Nettoie l'adresse ligne 1.

        But: supprimer téléphone, email, CP/ville, mentions parasites (Tel, Tél, Fax, Sec...).
        """
        if not adresse:
            return ""

        s = str(adresse)

        # Supprimer email
        s = re.sub(self.email_pattern, ' ', s)

        # Supprimer téléphone (formats divers)
        s = re.sub(self.phone_pattern, ' ', s)
        s = re.sub(r'\b0[1-9]\d{8}\b', ' ', s)

        # Supprimer tokens parasites
        s = re.sub(r'\b(TEL|TÉL|TELEPHONE|TÉLÉPHONE|FAX|SEC|STANDARD)\b\s*:?',' ', s, flags=re.IGNORECASE)

        # Couper dès qu'un CP apparait (adresse_l1 ne doit pas contenir CP)
        s = re.split(r'\b\d{5}\b', s)[0]

        # Couper sur tirets longs fréquents avant ville
        s = s.split('–')[0]
        s = s.split('-')[0] if ' - ' in s else s

        # Normaliser espaces
        s = re.sub(r'\s+', ' ', s).strip(' ,;:-')

        return s
    
    def get_stats(self) -> Dict:
        """Retourne les statistiques"""
        return self.stats.copy()


if __name__ == "__main__":
    """Test du module"""
    
    extractor = ContactExtractor(mode="strict")
    
    # Test extraction
    contact = extractor.extract_contacts_for_establishment(
        nom="Habitat inclusif LADAPT",
        commune="Troyes",
        code_postal="10000",
        gestionnaire="LADAPT"
    )
    
    print(f"\n✅ Résultat extraction:")
    print(f"   Site web: {contact.site_web}")
    print(f"   Téléphone: {contact.telephone}")
    print(f"   Email: {contact.email}")
    print(f"   Adresse: {contact.adresse}")
    print(f"   Type source: {contact.contact_source_type}")
    print(f"   Confiance: {contact.contact_confidence:.0f}%")
    
    print(f"\nStatistiques: {extractor.get_stats()}")
