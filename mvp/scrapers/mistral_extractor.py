"""
Module 3 - Mistral Extractor v3.1
Extraction multipasse stricte avec Mistral Large
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
from mvp.scrapers.contact_extractor import ContactExtractor


def _contains_any(text: str, keywords: List[str]) -> bool:
    t = (text or "").lower()
    return any(k in t for k in keywords)


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


class MistralExtractor:
    """
    Extracteur intelligent avec Mistral Large
    Extraction multipasse : UN prompt par champ
    """
    
    def __init__(self, mistral_api_key: Optional[str] = None, serper_api_key: Optional[str] = None, 
                 scrapingbee_api_key: Optional[str] = None, contact_extraction_mode: str = "strict"):
        """
        Initialise l'extracteur
        
        Args:
            mistral_api_key: Clé API Mistral (ou depuis .env)
            serper_api_key: Clé API Serper (ou depuis .env)
            scrapingbee_api_key: Clé API ScrapingBee (ou depuis .env)
            contact_extraction_mode: "strict" (jamais annuaire) ou "pragmatique"
        """
        self.mistral_api_key = mistral_api_key or os.getenv('MISTRAL_API_KEY')
        self.serper_api_key = serper_api_key or os.getenv('SERPER_API_KEY')
        self.scrapingbee_api_key = scrapingbee_api_key or os.getenv('SCRAPINGBEE_API_KEY')
        
        # Extracteur de contacts robuste
        self.contact_extractor = ContactExtractor(
            groq_api_key=os.getenv('GROQ_API_KEY'),  # Contact extractor utilise Groq
            serper_api_key=self.serper_api_key,
            scrapingbee_api_key=self.scrapingbee_api_key,
            mistral_api_key=self.mistral_api_key,  # Pour fallback LLM
            mode=contact_extraction_mode
        )
        
        # Modèle Mistral pour extraction précise
        self.model = os.getenv('MISTRAL_MODEL', 'mistral-large-latest')
        
        # Pricing Mistral Large (€ not $)
        self.pricing = {
            "input": 2.0,    # €/1M tokens
            "output": 6.0    # €/1M tokens
        }
        
        # Sites à exclure
        self.excluded_sites = [
            'essentiel-autonomie.com',
            'papyhappy.com',
            'pour-les-personnes-agees.gouv.fr',
            'villesetvillagesouilfaitbonvivre.com'
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

        # Garde-fou PH: on rejette le médico-social handicap "classique" sauf si
        # habitat inclusif / AVP explicitement présent dans le contenu.
        self.ph_medicosocial_keywords = [
            'foyer d\'hébergement', 'foyer d\'hebergement', 'foyer hébergement', 'foyer hebergement',
            'foyer de vie',
            'foyer occupationnel',
            'fam', 'foyer d\'accueil médicalisé', 'foyer d\'accueil medicalise',
            'mas', 'maison d\'accueil spécialisée', 'maison d\'accueil specialisee',
            'ime', 'institut médico-éducatif', 'institut medico-educatif',
            'esat',
            'samsah', 'savs',
            'ehpa',  # parfois dans pages mixtes
            'handicap mental', 'polyhandicap',
            'autisme',
            'résidence handicap', 'residence handicap',
            'établissement médico-social', 'etablissement medico-social'
        ]
        self.habitat_inclusif_keywords = [
            'habitat inclusif',
            'habitat partagé', 'habitat partage',
            'aide à la vie partagée', 'aide a la vie partagee',
            'avp',
            'colocation seniors',
            'maison partagée', 'maison partagee',
            'béguinage', 'beguinage',
            'village seniors',
            'habitat intergénérationnel', 'habitat intergenerationnel'
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
        print(f"\n🔍 === MODULE 3 - MISTRAL EXTRACTOR V3.0 ===")
        print(f"📍 Département: {department_name} ({department_code})")
        print(f"📊 {len(candidates)} candidats à traiter")
        
        # Stocker le code département pour filtrage ultérieur
        self.target_department = department_code
        
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
                
                # Garde-fou PH : si le contenu ressemble à du médico-social handicap sans signal habitat inclusif/AVP
                if self._should_reject_medicosocial_ph(candidate, page_content):
                    print("   ❌ Rejet PH médico-social (pas d'habitat inclusif/AVP explicite)")
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
    
    def _should_reject_medicosocial_ph(self, candidate, page_content: str) -> bool:
        """Hard gate PH.

        True => on skip complètement le candidat.

        Règle:
        - Si on détecte fortement médico-social handicap (FAM/MAS/IME/ESAT/foyer...) dans titre+snippet+contenu
          ET qu'on ne détecte pas de signal explicite habitat inclusif/AVP/habitat alternatif
          => rejet.

        NB: on ne veut pas bloquer du mixte PA/PH s'il est clairement de l'habitat inclusif.
        """
        title = getattr(candidate, 'title', '') or ''
        snippet = getattr(candidate, 'snippet', '') or ''
        text = f"{title}\n{snippet}\n{page_content}".lower()

        has_ph_medicosocial = _contains_any(text, self.ph_medicosocial_keywords)
        has_habitat_signal = _contains_any(text, self.habitat_inclusif_keywords)

        # Si on a un signal habitat inclusif explicite, on laisse passer.
        if has_habitat_signal:
            return False

        # Si on a du médico-social handicap, on rejette.
        if has_ph_medicosocial:
            return True

        return False

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
            response = self._call_mistral_api(prompt, max_tokens=500)
            
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
        
        # Extraction GÉO (code postal + commune) avec URL en fallback
        print(f"         🌍 Extraction géo...")
        geo_data = self._extract_geo(nom, context, url=source_url)
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
        
        # Récupération adresse et code postal
        adresse_l1 = contact_data.get('adresse', '')
        cp_from_contact = contact_data.get('code_postal', '')
        
        # FALLBACK: Utiliser le code postal du ContactExtractor si celui de geo est vide
        if not code_postal and cp_from_contact:
            code_postal = cp_from_contact
            print(f"         ℹ️ Code postal récupéré du ContactExtractor: {code_postal}")
        
        # Score global
        confidence_global = (confidence_nom + confidence_geo + confidence_gest) / 3
        
        return {
            'nom': nom,
            'commune': commune,
            'code_postal': code_postal,
            'gestionnaire': gestionnaire,
            'adresse_l1': adresse_l1,
            'telephone': contact_data.get('telephone', ''),
            'email': contact_data.get('email', ''),
            'site_web': contact_data.get('site_web', ''),
            'confidence_nom': confidence_nom,
            'confidence_geo': confidence_geo,
            'confidence_gestionnaire': confidence_gest,
            'confidence_global': confidence_global
        }
    
    def _extract_geo(self, nom: str, context: str, url: Optional[str] = None) -> Dict:
        """Extrait code postal et commune avec fallback URL"""
        
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
        
        # Tentative 2: Parser l'URL si disponible (ex: .../boucau/64340)
        if url:
            # Pattern: code postal 5 chiffres dans URL
            url_postal_match = re.search(r'/(\d{5})(?:/|$|\?)', url)
            if url_postal_match:
                code_postal = url_postal_match.group(1)
                # Chercher commune avant le code postal
                url_commune_match = re.search(r'/([a-z\-]+)/\d{5}', url, re.IGNORECASE)
                if url_commune_match:
                    commune_slug = url_commune_match.group(1)
                    # Normaliser (boucau → Boucau, saint-pierre → Saint Pierre)
                    commune = ' '.join(word.capitalize() for word in commune_slug.replace('-', ' ').split())
                    return {
                        'code_postal': code_postal,
                        'commune': commune,
                        'confidence': 70.0
                    }
        
        # Tentative 3: LLM Mistral
        prompt = f"""Extrait le code postal et la commune de cet etablissement.

ETABLISSEMENT: {nom}

CONTENU:
{context[:1500]}

REGLES:
- Code postal: EXACTEMENT 5 chiffres
- Commune: Nom exact tel qu'ecrit
- Si incertain laisser vide

Reponds en JSON:
{{"code_postal": "12345", "commune": "VilleExacte", "confidence": 85}}"""

        try:
            response = self._call_mistral_api(prompt, max_tokens=150)
            if response:
                data = self._parse_json_response(response['content'])
                return data
        except:
            pass
        
        return {'code_postal': '', 'commune': '', 'confidence': 0.0}
    
    def _extract_gestionnaire(self, nom: str, commune: str, context: str) -> Dict:
        """Extrait le gestionnaire"""
        
        prompt = f"""Extrait le gestionnaire de cet etablissement.

ETABLISSEMENT: {nom}
COMMUNE: {commune}

CONTENU:
{context[:1500]}

REGLES STRICTES:
- Gestionnaire = Operateur/Organisme qui GERE l'etablissement
- NE PAS confondre avec nom du site web
- "Essentiel Autonomie" = SITE WEB, pas gestionnaire
- Si pas mentionne EXPLICITEMENT laisser vide

GESTIONNAIRES VALIDES:
- Ages & Vie, CetteFamille, CCAS, Habitat & Humanisme, UDAF, Domani, APEI, ADMR, APF, Commune

Reponds en JSON:
{{"gestionnaire": "Nom Gestionnaire", "confidence": 85}}

Si incertain:
{{"gestionnaire": "", "confidence": 0}}"""

        try:
            response = self._call_mistral_api(prompt, max_tokens=150)
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
            Dict avec adresse, code_postal, telephone, email, site_web
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
                'code_postal': contact_data.code_postal,
                'telephone': contact_data.telephone,
                'email': contact_data.email,
                'site_web': contact_data.site_web
            }
        except Exception as e:
            print(f"         ⚠️ Erreur extraction contacts: {e}")
            # Fallback: retour vide
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
        nom = extracted.get('nom', '')
        if not nom:
            print(f"         ❌ Nom manquant")
            return False
        
        commune = extracted.get('commune', '')
        if not commune and not extracted.get('code_postal'):
            print(f"         ❌ Commune ET code postal manquants")
            return False
        
        # NOUVEAU: Rejeter si commune == département (erreur d'extraction)
        # Ex: commune="Pas-de-Calais" pour département 62
        if commune and (commune == department_name or commune.lower() == department_name.lower()):
            # Cas particulier: Paris = ville/commune unique -> accepter si on a un indication
            # de localisation plus fine (code postal commençant par 75 ou adresse avec arrondissement)
            if department_name.strip().lower() == 'paris':
                code_postal = extracted.get('code_postal', '') or ''
                adresse = extracted.get('adresse_l1', '') or extracted.get('adresse', '') or ''
                # Accepter si code postal commence par le code département (75)
                if code_postal and code_postal.startswith(department_code):
                    pass
                # Accepter si l'adresse contient un arrondissement (ex: '13e', '13ème', '13e arrondissement')
                elif re.search(r"\b\d{1,2}\s*(e|ème|eème)\b|\b\d{2}0?\d{2}\b", adresse.lower()):
                    pass
                else:
                    print(f"         ❌ REJETÉ: Commune = département ('{commune}') → extraction ratée")
                    return False
            else:
                print(f"         ❌ REJETÉ: Commune = département ('{commune}') → extraction ratée")
                return False
        
        # NOUVEAU: Rejeter noms génériques "Résidence(s) service(s)"
        # Ces noms indiquent souvent des pages d'accueil générales, pas un établissement spécifique
        nom_lower = nom.lower().strip()
        generic_patterns = [
            'résidences services',
            'residence services',
            'résidences service',
            'residence service',
            'résidence services',
            'résidence service'
        ]
        # Rejeter UNIQUEMENT si le nom est exactement un de ces patterns (pas de qualificatif)
        if nom_lower in generic_patterns:
            print(f"         ❌ REJETÉ: Nom générique '{nom}' → page généraliste, pas un établissement")
            return False
        
        # Validation code postal STRICTE
        code_postal = extracted.get('code_postal', '')
        
        if code_postal and len(code_postal) == 5:
            # Codes postaux doivent commencer par le code département
            if not code_postal.startswith(department_code):
                print(f"         ❌ REJETÉ: Code postal {code_postal} hors département {department_code}")
                return False
        elif code_postal:
            # Code postal invalide (pas 5 chiffres)
            print(f"         ❌ Code postal invalide: {code_postal}")
            return False
        
        # Validation confidence minimale - ASSOUPLISSEMENT si adresse complète
        confidence = extracted.get('confidence_global', 0)
        has_complete_address = bool(extracted.get('adresse_l1') and len(extracted.get('adresse_l1', '')) > 10)
        
        if confidence < 50:
            # Exception: accepter si adresse complète présente (indique données fiables)
            if has_complete_address:
                print(f"         ℹ️ Confidence faible ({confidence:.1f}%) mais adresse complète → accepté")
            else:
                print(f"         ❌ Confidence trop faible: {confidence:.1f}%")
                return False
        
        return True
    
    def _stage5_normalize(self, extracted: Dict, department_name: str, department_code: str, 
                         source_url: str) -> ExtractedEstablishment:
        """
        ÉTAPE 5: Normalisation et création objet final avec règles métier strictes
        """
        nom = extracted['nom']
        gestionnaire = extracted.get('gestionnaire', '')
        
        # Normalisation sous-catégories et habitat_type (règles strictes)
        sous_categories, habitat_type = self._normalize_categories(nom, gestionnaire)
        
        # Déterminer eligibilite_avp (règles AVP)
        eligibilite_avp = self._determine_eligibilite_avp(nom, sous_categories, source_url)
        
        # Déterminer public_cible (règles strictes)
        public_cible = self._determine_public_cible(nom, gestionnaire, sous_categories)
        
        return ExtractedEstablishment(
            nom=nom,
            commune=extracted['commune'],
            code_postal=extracted.get('code_postal', ''),
            gestionnaire=gestionnaire,
            adresse_l1=extracted.get('adresse_l1', ''),
            telephone=extracted.get('telephone', ''),
            email=extracted.get('email', ''),
            site_web=extracted.get('site_web', ''),
            sous_categories=sous_categories,
            habitat_type=habitat_type,
            eligibilite_avp=eligibilite_avp,
            presentation="",  # Sera complété par enricher
            departement=f"{department_name} ({department_code})",
            source=source_url,
            date_extraction=datetime.now().strftime("%Y-%m-%d"),
            public_cible=public_cible,
            confidence_nom=extracted.get('confidence_nom', 0.0),
            confidence_geo=extracted.get('confidence_geo', 0.0),
            confidence_gestionnaire=extracted.get('confidence_gestionnaire', 0.0),
            confidence_global=extracted.get('confidence_global', 0.0)
        )
    
    def _normalize_categories(self, nom: str, gestionnaire: str) -> Tuple[str, str]:
        """
        Normalise sous-catégories et habitat_type selon règles métier STRICTES
        
        sous_categories (valeurs strictes):
        - Résidence services seniors
        - Habitat inclusif
        - Maison d'accueil familial
        - Habitat intergénérationnel
        - Colocation avec services
        - Béguinage
        - Village seniors
        
        habitat_type (déduit de sous_categories):
        - residence ← Résidence services seniors
        - habitat_partage ← Habitat inclusif, Maison d'accueil familial, Habitat intergénérationnel, Colocation avec services
        - logement_independant ← Béguinage, Village seniors
        """
        nom_lower = nom.lower()
        gest_lower = gestionnaire.lower() if gestionnaire else ""
        
        # RÈGLE 0: Résidence services seniors (PRIORITAIRE - détection dans nom)
        residence_service_keywords = [
            'résidence service',
            'residence service',
            'résidence services',
            'residence services'
        ]
        if any(kw in nom_lower for kw in residence_service_keywords):
            return "Résidence services seniors", "residence"
        
        # RÈGLE 1: Détection par gestionnaire
        if "ages & vie" in gest_lower or "ages et vie" in gest_lower:
            return "Colocation avec services", "habitat_partage"
        
        if "cettefamille" in gest_lower or "cette famille" in gest_lower:
            return "Maison d'accueil familial", "habitat_partage"
        
        # RÈGLE 2: Détection par nom
        if "béguinage" in nom_lower or "beguinage" in nom_lower:
            return "Béguinage", "logement_independant"
        
        if "village seniors" in nom_lower or "village senior" in nom_lower:
            return "Village seniors", "logement_independant"
        
        if "intergénérationnel" in nom_lower or "intergenerationnel" in nom_lower:
            return "Habitat intergénérationnel", "habitat_partage"
        
        if "accueil familial" in nom_lower or "famille accueil" in nom_lower:
            return "Maison d'accueil familial", "habitat_partage"
        
        if "colocation" in nom_lower:
            return "Colocation avec services", "habitat_partage"
        
        # RÈGLE 3: Par défaut
        return "Habitat inclusif", "habitat_partage"
    
    def _determine_eligibilite_avp(self, nom: str, sous_categories: str, source_url: str) -> str:
        """
        Détermine eligibilite_avp selon règles métier
        
        Valeurs:
        - avp_eligible: Si "aide à la vie partagée" ou "AVP" explicite
        - a_verifier: Si indices forts OU si Habitat inclusif
        - non_eligible: Sinon
        """
        nom_lower = nom.lower()
        source_lower = source_url.lower()
        
        # RÈGLE 1: AVP explicite → avp_eligible
        avp_keywords = ['avp', 'aide à la vie partagée', 'aide a la vie partagee']
        if any(kw in nom_lower or kw in source_lower for kw in avp_keywords):
            return "avp_eligible"
        
        # RÈGLE 2: Habitat inclusif → a_verifier
        if sous_categories == "Habitat inclusif":
            return "a_verifier"
        
        # RÈGLE 3: Indices forts → a_verifier
        indices_forts = [
            'vie sociale',
            'vie partagée',
            'vie partagee',
            'partage',
            'collectif'
        ]
        if any(indice in nom_lower for indice in indices_forts):
            return "a_verifier"
        
        # RÈGLE 4: Par défaut → non_eligible
        return "non_eligible"
    
    def _determine_public_cible(self, nom: str, gestionnaire: str, sous_categories: str) -> str:
        """
        Détermine public_cible selon règles métier
        
        Valeurs strictes:
        - personnes_agees
        - personnes_handicapees
        - mixtes
        - alzheimer_accessible
        """
        nom_lower = nom.lower()
        gest_lower = gestionnaire.lower() if gestionnaire else ""
        
        publics = []
        
        # Détection personnes âgées
        seniors_keywords = ['seniors', 'âgées', 'agees', 'retraités', 'retraites']
        if any(kw in nom_lower for kw in seniors_keywords):
            publics.append('personnes_agees')
        
        # Détection personnes handicapées
        handicap_keywords = ['handicap', 'ladapt', 'inclusif']
        if any(kw in nom_lower or kw in gest_lower for kw in handicap_keywords):
            publics.append('personnes_handicapees')
        
        # Détection Alzheimer
        alzheimer_keywords = ['alzheimer', 'cognitif']
        if any(kw in nom_lower for kw in alzheimer_keywords):
            publics.append('alzheimer_accessible')
        
        # Détection mixte (intergénérationnel)
        if sous_categories == "Habitat intergénérationnel":
            publics.append('mixtes')
        
        # Par défaut: personnes_agees
        if not publics:
            publics.append('personnes_agees')
        
        # Si handicap ET âgées → mixtes
        if 'personnes_agees' in publics and 'personnes_handicapees' in publics:
            return 'mixtes'
        
        return ','.join(publics)
    
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
    
    def _call_mistral_api(self, prompt: str, max_tokens: int = 500) -> Optional[Dict]:
        """Appel API Mistral avec retry et support JSON"""
        
        if not self.mistral_api_key:
            return None
        
        # Nettoyer le prompt
        prompt = self._clean_text_for_llm(prompt)
        
        headers = {
            "Authorization": f"Bearer {self.mistral_api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"}  # Force JSON
        }
        
        # Retry logic
        max_retries = 2
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    "https://api.mistral.ai/v1/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=60
                )
                
                if response.status_code == 200:
                    data = response.json()
                    content = data["choices"][0]["message"]["content"]
                    usage = data.get("usage", {})
                    
                    # Calcul coût (Mistral en €)
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
                    print(f"         ⚠️ Erreur 400 Mistral (tentative {attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        continue
                    else:
                        try:
                            error_data = response.json()
                            print(f"         Détails: {error_data.get('message', 'Inconnu')}")
                        except:
                            pass
                        return None
                else:
                    print(f"         ❌ Erreur Mistral API: {response.status_code}")
                    return None
                    
            except Exception as e:
                print(f"         ❌ Erreur appel Mistral: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                return None
        
        return None
    
    def _parse_json_establishments(self, content: str) -> List[Dict]:
        """Parse la liste d'établissements depuis JSON avec fallback robuste"""
        
        # Vérification content non vide
        if not content or content.strip() == '':
            print(f"         ⚠️ Contenu vide reçu de l'API")
            return []
        
        try:
            # Parser JSON direct (Mistral retourne du JSON propre)
            data = json.loads(content.strip())
            establishments = data.get('etablissements', [])
            
            # Filtrer établissements valides
            valid = []
            for est in establishments:
                if isinstance(est, dict) and est.get('nom'):
                    nom = est['nom'].strip() if isinstance(est['nom'], str) else str(est['nom'])
                    if len(nom) > 2:
                        valid.append({
                            'nom': nom,
                            'commune': str(est.get('commune', '')).strip(),
                            'gestionnaire': str(est.get('gestionnaire', '')).strip()
                        })
            
            return valid
            
        except json.JSONDecodeError as e:
            print(f"         ⚠️ JSON invalide: {e}")
            print(f"         Contenu reçu (premiers 200 chars): {content[:200]}")
            
            # Fallback: extraction manuelle si JSON malformé
            return self._fallback_extract_from_text(content)
            
        except Exception as e:
            print(f"         ⚠️ Erreur parsing: {e}")
            return []
    
    def _fallback_extract_from_text(self, text: str) -> List[Dict]:
        """Extraction manuelle en fallback si JSON échoue"""
        print(f"         🔄 Tentative extraction manuelle...")
        
        valid = []
        
        # Rechercher patterns "nom": "..."
        nom_pattern = r'"nom"\s*:\s*"([^"]+)"'
        commune_pattern = r'"commune"\s*:\s*"([^"]*)"'
        code_postal_pattern = r'"code_postal"\s*:\s*"?(\d{5})"?'
        
        noms = re.findall(nom_pattern, text)
        
        for nom in noms:
            if len(nom.strip()) > 2:
                # Chercher commune et code postal associés
                commune_match = re.search(f'"nom"\\s*:\\s*"{re.escape(nom)}"[^}}]*"commune"\\s*:\\s*"([^"]*)"', text)
                commune = commune_match.group(1) if commune_match else ''
                
                code_postal_match = re.search(f'"nom"\\s*:\\s*"{re.escape(nom)}"[^}}]*"code_postal"\\s*:\\s*"?(\d{{5}})"?', text)
                code_postal = code_postal_match.group(1) if code_postal_match else ''
                
                # FILTRAGE: Vérifier que le code postal correspond au département
                if code_postal:
                    dept_from_code = code_postal[:2]
                    if hasattr(self, 'target_department') and self.target_department:
                        dept_expected = self.target_department.zfill(2)
                        if dept_from_code != dept_expected:
                            print(f"         ⚠️ Établissement hors département ignoré: {nom} (code {code_postal})")
                            continue
                
                valid.append({
                    'nom': nom.strip(),
                    'commune': commune.strip(),
                    'gestionnaire': ''
                })
        
        if valid:
            print(f"         ✅ {len(valid)} établissement(s) extrait(s) manuellement")
        
        return valid
    
    def _parse_json_response(self, content: str) -> Dict:
        """Parse une réponse JSON simple"""
        try:
            data = json.loads(content.strip())
            return data
        except:
            return {}
    
    def _print_stats(self):
        """Affiche les statistiques d'extraction"""
        
        # Récupérer stats du ContactExtractor
        contact_stats = self.contact_extractor.get_stats()
        
        print(f"\n📊 === STATISTIQUES MISTRAL EXTRACTOR ===")
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


# Alias pour compatibilité
MixtralExtractor = MistralExtractor


if __name__ == "__main__":
    """Test du module"""
    from snippet_classifier import SearchResult
    
    # Simuler un candidat pour test
    test_candidate = SearchResult(
        title="Habitat inclusif LADAPT dans l'Aube",
        url="https://example.com/test",
        snippet="LADAPT propose un habitat inclusif à Troyes",
        is_relevant=True,
        classification_confidence=85.0
    )
    
    extractor = MistralExtractor()
    results = extractor.extract_from_candidates([test_candidate], "Aube", "10")
    
    print(f"\n✅ {len(results)} établissement(s) extrait(s)")
    for est in results:
        print(f"\n   {est.nom}")
        print(f"   Commune: {est.commune} ({est.code_postal})")
        print(f"   Gestionnaire: {est.gestionnaire}")
        print(f"   Confidence: {est.confidence_global:.1f}%")
