"""
Module 3 - Mixtral Extractor v3.1
Extraction multipasse stricte avec Mixtral-8x7B-32768
Objectif : 0% hallucination via prompts dédiés par champ
+ Extraction contacts robuste avec classification sources
"""

import requests
import json
import re
import time
import os
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from contact_extractor import ContactExtractor


@dataclass
class ExtractedEstablishment:
    """Établissement extrait avec scores de confiance"""
    nom: str
    commune: str
    code_postal: str
    gestionnaire: str = ""
    adresse_l1: str = ""
    telephone: str = ""
    email: str = ""
    site_web: str = ""
    sous_categories: str = "Habitat inclusif"
    habitat_type: str = "habitat_partage"
    eligibilite_avp: str = "eligible"
    presentation: str = ""
    departement: str = ""
    source: str = ""
    date_extraction: str = ""
    public_cible: str = "seniors"
    
    # Scores de confiance
    confidence_nom: float = 0.0
    confidence_geo: float = 0.0
    confidence_gestionnaire: float = 0.0
    confidence_global: float = 0.0


class MixtralExtractor:
    """
    Extracteur intelligent avec Mixtral-8x7B-32768
    Extraction multipasse : UN prompt par champ
    """
    
    def __init__(self, groq_api_key: Optional[str] = None, serper_api_key: Optional[str] = None, 
                 scrapingbee_api_key: Optional[str] = None, contact_extraction_mode: str = "strict"):
        """
        Initialise l'extracteur
        
        Args:
            groq_api_key: Clé API Groq (ou depuis .env)
            serper_api_key: Clé API Serper (ou depuis .env)
            scrapingbee_api_key: Clé API ScrapingBee (ou depuis .env)
            contact_extraction_mode: "strict" (jamais annuaire) ou "pragmatique"
        """
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        self.serper_api_key = serper_api_key or os.getenv('SERPER_API_KEY')
        self.scrapingbee_api_key = scrapingbee_api_key or os.getenv('SCRAPINGBEE_API_KEY')
        
        # Extracteur de contacts robuste
        self.contact_extractor = ContactExtractor(
            groq_api_key=self.groq_api_key,
            serper_api_key=self.serper_api_key,
            scrapingbee_api_key=self.scrapingbee_api_key,
            mode=contact_extraction_mode
        )
        
        # Modèle Mixtral pour extraction précise
        self.model = "mixtral-8x7b-32768"
        
        # Pricing Groq Mixtral
        self.pricing = {
            "input": 0.24,   # $/1M tokens
            "output": 0.24   # $/1M tokens
        }
        
        # Sites à exclure
        self.excluded_sites = [
            'essentiel-autonomie.com',
            'papyhappy.com',
            'pour-les-personnes-agees.gouv.fr'
        ]
        
        # Gestionnaires suspects
        self.suspicious_gestionnaires = [
            'essentiel autonomie',
            'papyhappy',
            'malakoff humanis',
            'pour-les-personnes-agees',
            'non renseigné',
            'non précisé'
        ]
        
        # Statistiques
        self.stats = {
            'candidates_processed': 0,
            'establishments_identified': 0,
            'establishments_extracted': 0,
            'establishments_validated': 0,
            'extraction_cost': 0.0
        }
    
    def extract_from_candidates(self, candidates: List, department_name: str, department_code: str) -> List[ExtractedEstablishment]:
        """
        Extrait les établissements depuis les candidats classifiés
        
        Args:
            candidates: Liste de SearchResult classifiés pertinents
            department_name: Nom département (ex: "Aube")
            department_code: Code département (ex: "10")
            
        Returns:
            Liste d'établissements extraits et validés
        """
        print(f"\n🔍 === MODULE 3 - MIXTRAL EXTRACTOR V3.0 ===")
        print(f"📍 Département: {department_name} ({department_code})")
        print(f"📊 {len(candidates)} candidats à traiter")
        
        all_establishments = []
        
        for i, candidate in enumerate(candidates, 1):
            print(f"\n{'='*60}")
            print(f"🔄 CANDIDAT [{i}/{len(candidates)}]: {candidate.title[:50]}...")
            print(f"   URL: {candidate.url}")
            
            self.stats['candidates_processed'] += 1
            
            try:
                # Scraping du contenu avec ScrapingBee
                page_content = self._scrape_page_content(candidate.url)
                
                if not page_content:
                    print(f"   ❌ Échec scraping, passage au suivant")
                    continue
                
                # Pipeline 5 étapes
                establishments = self._extract_pipeline(candidate, page_content, department_name, department_code)
                
                if establishments:
                    all_establishments.extend(establishments)
                    print(f"   ✅ {len(establishments)} établissement(s) extrait(s)")
                else:
                    print(f"   ❌ Aucun établissement valide extrait")
                    
            except Exception as e:
                print(f"   ❌ Erreur traitement candidat: {e}")
                continue
        
        # Statistiques finales
        self._print_stats()
        
        return all_establishments
    
    def _extract_pipeline(self, candidate, page_content: str, department_name: str, 
                         department_code: str) -> List[ExtractedEstablishment]:
        """
        Pipeline d'extraction 5 étapes
        
        Returns:
            Liste d'établissements extraits
        """
        # ÉTAPE 1: Identification des établissements
        print(f"\n   📋 ÉTAPE 1: Identification...")
        establishments_info = self._stage1_identify(candidate, page_content)
        
        if not establishments_info:
            print(f"      ❌ Aucun établissement identifié")
            return []
        
        print(f"      ✅ {len(establishments_info)} établissement(s) identifié(s)")
        self.stats['establishments_identified'] += len(establishments_info)
        
        results = []
        
        for j, est_info in enumerate(establishments_info, 1):
            print(f"\n   🔄 Traitement {j}/{len(establishments_info)}: {est_info['nom']}")
            
            try:
                # ÉTAPE 2: Recherche ciblée (optionnel si infos manquantes)
                context = page_content  # Par défaut, utiliser le contenu déjà scraped
                
                # ÉTAPE 3: Extraction multipasse
                print(f"      📊 ÉTAPE 3: Extraction multipasse...")
                extracted = self._stage3_multipass_extraction(est_info, context, candidate.url)
                
                if not extracted:
                    print(f"      ❌ Extraction échouée")
                    continue
                
                self.stats['establishments_extracted'] += 1
                
                # ÉTAPE 4: Validation
                print(f"      ✅ ÉTAPE 4: Validation...")
                if not self._stage4_validate(extracted, department_name, department_code):
                    print(f"      ❌ Validation échouée")
                    continue
                
                # ÉTAPE 5: Normalisation
                print(f"      🔧 ÉTAPE 5: Normalisation...")
                normalized = self._stage5_normalize(extracted, department_name, department_code, candidate.url)
                
                results.append(normalized)
                self.stats['establishments_validated'] += 1
                print(f"      ✅ Validé: {normalized.nom} - {normalized.commune}")
                
            except Exception as e:
                print(f"      ❌ Erreur traitement: {e}")
                continue
        
        return results
    
    def _stage1_identify(self, candidate, page_content: str) -> List[Dict]:
        """
        ÉTAPE 1: Identifie les établissements dans le contenu
        
        Returns:
            Liste de dicts avec nom, commune, gestionnaire
        """
        # Nettoyer et limiter le contenu
        clean_content = self._clean_text_for_llm(page_content[:1500])
        
        prompt = f"""Identifie les etablissements d'habitat alternatif pour seniors.

TITRE: {candidate.title}

TEXTE: {clean_content}

TYPES RECHERCHES:
- Habitat inclusif/partage
- Beguinage
- Maisons partagees pour seniors
- Village seniors
- Colocations seniors
- Habitat intergenerationnel
- Maisons d'accueil familial

EXTRAIT pour chaque etablissement:
- nom: OBLIGATOIRE (nom exact)
- commune: Si mentionnee
- gestionnaire: Si mentionne EXPLICITEMENT

NE PAS extraire EHPAD ou articles generaux.

Reponds en JSON strict:
{{"etablissements": [{{"nom": "Nom Etablissement", "commune": "Ville", "gestionnaire": "Gestionnaire"}}]}}

Si aucun etablissement trouve:
{{"etablissements": []}}"""

        try:
            response = self._call_mixtral_api(prompt, max_tokens=500)
            
            if not response:
                return []
            
            # Parser JSON
            content = response['content']
            establishments = self._parse_json_establishments(content)
            
            return establishments
            
        except Exception as e:
            print(f"         ❌ Erreur identification: {e}")
            return []
    
    def _stage3_multipass_extraction(self, est_info: Dict, context: str, source_url: str) -> Optional[Dict]:
        """
        ÉTAPE 3: Extraction multipasse - UN prompt par champ
        
        Returns:
            Dict avec tous les champs extraits
        """
        # Extraction NOM (déjà dans est_info)
        nom = est_info['nom']
        confidence_nom = 90.0  # Déjà identifié à l'étape 1
        
        # Extraction GÉO (code postal + commune)
        print(f"         🌍 Extraction géo...")
        geo_data = self._extract_geo(nom, context)
        commune = geo_data.get('commune', est_info.get('commune', ''))
        code_postal = geo_data.get('code_postal', '')
        confidence_geo = geo_data.get('confidence', 0.0)
        
        # Extraction GESTIONNAIRE (si >50% confidence)
        gestionnaire = ""
        confidence_gest = 0.0
        if confidence_geo > 50:
            print(f"         👔 Extraction gestionnaire...")
            gest_data = self._extract_gestionnaire(nom, commune, context)
            gestionnaire = gest_data.get('gestionnaire', est_info.get('gestionnaire', ''))
            confidence_gest = gest_data.get('confidence', 0.0)
            
            # Nettoyage gestionnaires suspects
            if self._is_suspicious_gestionnaire(gestionnaire):
                print(f"         ⚠️ Gestionnaire suspect '{gestionnaire}' → supprimé")
                gestionnaire = ""
                confidence_gest = 0.0
        
        # Extraction CONTACT (adresse, téléphone, email, site web)
        # Utilisation du ContactExtractor robuste avec recherche ciblée
        print(f"         📞 Extraction contact...")
        contact_data = self._extract_contact_robust(nom, commune, code_postal, gestionnaire)
        
        # Score global
        confidence_global = (confidence_nom + confidence_geo + confidence_gest) / 3
        
        return {
            'nom': nom,
            'commune': commune,
            'code_postal': code_postal,
            'gestionnaire': gestionnaire,
            'adresse_l1': contact_data.get('adresse', ''),
            'telephone': contact_data.get('telephone', ''),
            'email': contact_data.get('email', ''),
            'site_web': contact_data.get('site_web', ''),
            'confidence_nom': confidence_nom,
            'confidence_geo': confidence_geo,
            'confidence_gestionnaire': confidence_gest,
            'confidence_global': confidence_global
        }
    
    def _extract_geo(self, nom: str, context: str) -> Dict:
        """Extrait code postal et commune"""
        
        # Tentative 1: Regex sur le contexte
        postal_match = re.search(r'\b(\d{5})\s+([A-ZÀ-Ÿ][a-zà-ÿ\-\s\']+)', context)
        if postal_match:
            code_postal = postal_match.group(1)
            commune = postal_match.group(2).strip()
            return {
                'code_postal': code_postal,
                'commune': commune,
                'confidence': 85.0
            }
        
        # Tentative 2: LLM Mixtral
        prompt = f"""Extrait le code postal et la commune de cet établissement.

ÉTABLISSEMENT: {nom}

CONTENU:
{context[:1500]}

RÈGLES:
- Code postal: EXACTEMENT 5 chiffres
- Commune: Nom exact tel qu'écrit
- Si incertain → laisser vide

Réponds en JSON:
{{"code_postal": "12345", "commune": "VilleExacte", "confidence": 0-100}}"""

        try:
            response = self._call_mixtral_api(prompt, max_tokens=150)
            if response:
                data = self._parse_json_response(response['content'])
                return data
        except:
            pass
        
        return {'code_postal': '', 'commune': '', 'confidence': 0.0}
    
    def _extract_gestionnaire(self, nom: str, commune: str, context: str) -> Dict:
        """Extrait le gestionnaire"""
        
        prompt = f"""Extrait le gestionnaire de cet établissement.

ÉTABLISSEMENT: {nom}
COMMUNE: {commune}

CONTENU:
{context[:1500]}

RÈGLES STRICTES:
- Gestionnaire = Opérateur/Organisme qui GÈRE l'établissement
- NE PAS confondre avec:
  * Nom du site web
  * Auteur de l'article
  * Propriétaire du site
- "Essentiel Autonomie" = SITE WEB, pas gestionnaire
- Si pas mentionné EXPLICITEMENT → laisser vide

GESTIONNAIRES VALIDES:
- Ages & Vie
- CetteFamille
- CCAS de [ville]
- Habitat & Humanisme
- APEI, ADMR, APF, UDAF
- Commune de [ville]

Réponds en JSON:
{{"gestionnaire": "...", "confidence": 0-100}}

Si incertain → {{"gestionnaire": "", "confidence": 0}}"""

        try:
            response = self._call_mixtral_api(prompt, max_tokens=150)
            if response:
                data = self._parse_json_response(response['content'])
                return data
        except:
            pass
        
        return {'gestionnaire': '', 'confidence': 0.0}
    
    def _extract_contact_robust(self, nom: str, commune: str, code_postal: str, 
                               gestionnaire: Optional[str]) -> Dict:
        """
        Extrait coordonnées de contact avec ContactExtractor robuste
        
        Returns:
            Dict avec adresse, telephone, email, site_web
        """
        try:
            contact_data = self.contact_extractor.extract_contacts_for_establishment(
                nom=nom,
                commune=commune,
                code_postal=code_postal,
                gestionnaire=gestionnaire
            )
            
            return {
                'adresse': contact_data.adresse,
                'telephone': contact_data.telephone,
                'email': contact_data.email,
                'site_web': contact_data.site_web
            }
        except Exception as e:
            print(f"         ⚠️ Erreur extraction contacts: {e}")
            # Fallback: extraction basique depuis contexte déjà scraped (ancienne méthode)
            return {
                'adresse': '',
                'telephone': '',
                'email': '',
                'site_web': ''
            }
    
    def _stage4_validate(self, extracted: Dict, department_name: str, department_code: str) -> bool:
        """
        ÉTAPE 4: Validation de l'établissement extrait
        
        Returns:
            True si valide, False sinon
        """
        # Validation champs obligatoires
        if not extracted.get('nom'):
            print(f"         ❌ Nom manquant")
            return False
        
        if not extracted.get('commune') and not extracted.get('code_postal'):
            print(f"         ❌ Commune ET code postal manquants")
            return False
        
        # Validation code postal si présent
        code_postal = extracted.get('code_postal', '')
        if code_postal and len(code_postal) == 5:
            expected_prefix = department_code
            if not code_postal.startswith(expected_prefix):
                print(f"         ⚠️ Code postal {code_postal} incohérent avec département {department_code}")
                # Ne pas rejeter totalement, juste avertir
        
        # Validation confidence minimale
        if extracted.get('confidence_global', 0) < 50:
            print(f"         ❌ Confidence trop faible: {extracted['confidence_global']:.1f}%")
            return False
        
        return True
    
    def _stage5_normalize(self, extracted: Dict, department_name: str, department_code: str, 
                         source_url: str) -> ExtractedEstablishment:
        """
        ÉTAPE 5: Normalisation et création objet final
        """
        # Normalisation sous-catégories et habitat_type
        sous_categories, habitat_type = self._normalize_categories(
            extracted['nom'],
            extracted.get('gestionnaire', ''),
            ""  # presentation vide pour l'instant
        )
        
        return ExtractedEstablishment(
            nom=extracted['nom'],
            commune=extracted['commune'],
            code_postal=extracted.get('code_postal', ''),
            gestionnaire=extracted.get('gestionnaire', ''),
            adresse_l1=extracted.get('adresse_l1', ''),
            telephone=extracted.get('telephone', ''),
            email=extracted.get('email', ''),
            site_web=extracted.get('site_web', ''),
            sous_categories=sous_categories,
            habitat_type=habitat_type,
            eligibilite_avp="eligible",
            presentation="",  # Sera complété par enricher
            departement=f"{department_name} ({department_code})",
            source=source_url,
            date_extraction=datetime.now().strftime("%Y-%m-%d"),
            public_cible="seniors",
            confidence_nom=extracted.get('confidence_nom', 0.0),
            confidence_geo=extracted.get('confidence_geo', 0.0),
            confidence_gestionnaire=extracted.get('confidence_gestionnaire', 0.0),
            confidence_global=extracted.get('confidence_global', 0.0)
        )
    
    def _normalize_categories(self, nom: str, gestionnaire: str, presentation: str) -> Tuple[str, str]:
        """Normalise sous-catégories et habitat_type selon règles métier"""
        
        nom_lower = nom.lower()
        gest_lower = gestionnaire.lower() if gestionnaire else ""
        
        # Détection par gestionnaire (prioritaire)
        if "ages & vie" in gest_lower or "ages et vie" in gest_lower:
            return "Colocation avec services", "logement_independant"
        
        if "cettefamille" in gest_lower or "cette famille" in gest_lower:
            return "Maison d'accueil familial", "habitat_partage"
        
        # Détection par nom
        if "béguinage" in nom_lower:
            return "Béguinage", "logement_independant"
        
        if "village seniors" in nom_lower:
            return "Village seniors", "logement_independant"
        
        if "intergénérationnel" in nom_lower:
            return "Habitat intergénérationnel", "habitat_partage"
        
        if "accueil familial" in nom_lower or "famille accueil" in nom_lower:
            return "Maison d'accueil familial", "habitat_partage"
        
        # Par défaut
        return "Habitat inclusif", "habitat_partage"
    
    def _scrape_page_content(self, url: str) -> Optional[str]:
        """Scrape le contenu de la page avec ScrapingBee"""
        
        if not self.scrapingbee_api_key:
            print(f"      ⚠️ SCRAPINGBEE_API_KEY non configurée")
            return None
        
        try:
            print(f"      🌐 Scraping avec ScrapingBee...")
            
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
                # Nettoyer le HTML
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Supprimer scripts et styles
                for script in soup(["script", "style"]):
                    script.decompose()
                
                text = soup.get_text()
                # Nettoyer espaces
                lines = (line.strip() for line in text.splitlines())
                text = ' '.join(line for line in lines if line)
                
                print(f"      ✅ {len(text)} caractères récupérés")
                return text[:3000]  # Limiter pour éviter erreurs API
            else:
                print(f"      ❌ ScrapingBee erreur: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"      ❌ Erreur scraping: {e}")
            return None
    
    def _is_suspicious_gestionnaire(self, gestionnaire: str) -> bool:
        """Vérifie si le gestionnaire est suspect"""
        if not gestionnaire or len(gestionnaire.strip()) < 3:
            return False
        
        gest_lower = gestionnaire.lower().strip()
        return any(suspect in gest_lower for suspect in self.suspicious_gestionnaires)
    
    def _clean_text_for_llm(self, text: str) -> str:
        """Nettoie le texte pour éviter erreurs API"""
        # Supprimer caractères problématiques
        text = text.replace('\x00', '')
        text = text.replace('\r\n', ' ')
        text = text.replace('\n', ' ')
        text = text.replace('\t', ' ')
        
        # Réduire espaces multiples
        while '  ' in text:
            text = text.replace('  ', ' ')
        
        return text.strip()
    
    def _call_mixtral_api(self, prompt: str, max_tokens: int = 500) -> Optional[Dict]:
        """Appel API Groq Mixtral avec retry"""
        
        if not self.groq_api_key:
            return None
        
        # Nettoyer le prompt
        prompt = self._clean_text_for_llm(prompt)
        
        headers = {
            "Authorization": f"Bearer {self.groq_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "model": self.model,
            "temperature": 0.1,
            "max_tokens": max_tokens
        }
        
        # Retry logic
        max_retries = 2
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=60
                )
                
                if response.status_code == 200:
                    data = response.json()
                    content = data["choices"][0]["message"]["content"]
                    usage = data.get("usage", {})
                    
                    # Calcul coût
                    input_tokens = usage.get("prompt_tokens", 0)
                    output_tokens = usage.get("completion_tokens", 0)
                    cost = (input_tokens * self.pricing["input"] + 
                           output_tokens * self.pricing["output"]) / 1_000_000
                    
                    self.stats['extraction_cost'] += cost
                    
                    return {
                        "content": content,
                        "input_tokens": input_tokens,
                        "output_tokens": output_tokens,
                        "cost": cost
                    }
                elif response.status_code == 400:
                    print(f"         ⚠️ Erreur 400 Groq (tentative {attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        try:
                            error_data = response.json()
                            print(f"         Détails: {error_data.get('error', {}).get('message', 'Inconnu')}")
                        except:
                            pass
                        return None
                else:
                    print(f"         ❌ Erreur Groq API: {response.status_code}")
                    return None
                    
            except Exception as e:
                print(f"         ❌ Erreur appel Groq: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None
        
        return None
    
    def _parse_json_establishments(self, content: str) -> List[Dict]:
        """Parse la liste d'établissements depuis JSON"""
        try:
            # Nettoyer le contenu
            if '```json' in content:
                content = content.split('```json')[1].split('```')[0]
            elif '```' in content:
                content = content.split('```')[1].split('```')[0]
            
            # Parser JSON
            data = json.loads(content.strip())
            establishments = data.get('etablissements', [])
            
            # Filtrer établissements valides
            valid = []
            for est in establishments:
                if est.get('nom') and len(est['nom'].strip()) > 2:
                    valid.append({
                        'nom': est['nom'].strip(),
                        'commune': est.get('commune', '').strip(),
                        'gestionnaire': est.get('gestionnaire', '').strip()
                    })
            
            return valid
            
        except Exception as e:
            print(f"         ⚠️ Erreur parsing JSON: {e}")
            return []
    
    def _parse_json_response(self, content: str) -> Dict:
        """Parse une réponse JSON simple"""
        try:
            # Nettoyer
            if '```json' in content:
                content = content.split('```json')[1].split('```')[0]
            elif '```' in content:
                content = content.split('```')[1].split('```')[0]
            
            data = json.loads(content.strip())
            return data
            
        except:
            return {}
    
    def _print_stats(self):
        """Affiche les statistiques d'extraction"""
        
        # Récupérer stats du ContactExtractor
        contact_stats = self.contact_extractor.get_stats()
        
        print(f"\n📊 === STATISTIQUES MIXTRAL EXTRACTOR ===")
        print(f"   Candidats traités: {self.stats['candidates_processed']}")
        print(f"   Établissements identifiés: {self.stats['establishments_identified']}")
        print(f"   Établissements extraits: {self.stats['establishments_extracted']}")
        print(f"   Établissements validés: {self.stats['establishments_validated']}")
        print(f"   Coût extraction: €{self.stats['extraction_cost']:.6f}")
        print(f"\n📞 STATISTIQUES EXTRACTION CONTACTS:")
        print(f"   Tentatives d'extraction: {contact_stats['extractions_attempted']}")
        print(f"   Extractions réussies: {contact_stats['extractions_successful']}")
        print(f"   Sites officiels trouvés: {contact_stats['sources_official']}")
        print(f"   Sites gestionnaires trouvés: {contact_stats['sources_gestionnaire']}")
        print(f"   Sources rejetées: {contact_stats['sources_rejected']}")
        print("=" * 50)


if __name__ == "__main__":
    """Test du module"""
    from snippet_classifier import SnippetClassifier, SearchResult
    
    # Simuler un candidat pour test
    test_candidate = SearchResult(
        title="Habitat inclusif LADAPT dans l'Aube",
        url="https://example.com/test",
        snippet="LADAPT propose un habitat inclusif à Troyes pour personnes en situation de handicap",
        is_relevant=True,
        classification_confidence=85.0
    )
    
    extractor = MixtralExtractor()
    results = extractor.extract_from_candidates([test_candidate], "Aube", "10")
    
    print(f"\n✅ {len(results)} établissement(s) extrait(s)")
    for est in results:
        print(f"\n   {est.nom}")
        print(f"   Commune: {est.commune} ({est.code_postal})")
        print(f"   Gestionnaire: {est.gestionnaire}")
        print(f"   Confidence: {est.confidence_global:.1f}%")
