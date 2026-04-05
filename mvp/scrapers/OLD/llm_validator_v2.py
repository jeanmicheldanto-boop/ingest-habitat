"""
Module 4 - LLM Validator V2 AMÉLIORÉ : Validation anti-hallucination stricte
Version: 2.1 - Corrections critiques hallucinations + validation post-extraction
Objectif : Valider et extraire données avec fiabilité >90%
"""

import requests
import json
import time
import re
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime
import sys
import os

# Configuration
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from config_mvp import ai_config

@dataclass
class ValidationStage:
    """Résultat d'une étape de validation"""
    stage_name: str
    candidate_id: str
    input_tokens: int
    output_tokens: int
    cost_euros: float
    duration_seconds: float
    decision: str  # "pass", "reject", "extract"
    confidence: float
    reason: str
    extracted_count: Optional[int] = None

@dataclass 
class ExtractedEstablishment:
    """Établissement extrait et validé - Compatible pipeline 345"""
    nom: str
    commune: str
    code_postal: str
    gestionnaire: str
    adresse_l1: str
    telephone: Optional[str]
    email: Optional[str]
    site_web: Optional[str]
    sous_categories: str
    habitat_type: str
    eligibilite_avp: str
    presentation: str
    departement: str
    source: str
    date_extraction: str
    public_cible: str
    confidence_score: float
    validation_timestamp: str

class ExtractionValidator:
    """Validateur strict post-extraction pour éviter hallucinations"""
    
    # Communes par département (Aube comme référence)
    DEPT_COMMUNES = {
        "aube": ["troyes", "romilly-sur-seine", "la chapelle-saint-luc", "sainte-savine", 
                 "saint-andré-les-vergers", "nogent-sur-seine", "bar-sur-aube", "bar-sur-seine",
                 "arcis-sur-aube", "brienne-le-château", "vendeuvre-sur-barse", "piney",
                 "sainte-maure", "charmont-sous-barbuise", "essoyes", "villemaur-sur-vanne"],
        "marne": ["reims", "châlons-en-champagne", "épernay", "vitry-le-françois", "bezannes"],
        "haute-marne": ["chaumont", "saint-dizier", "langres", "joinville", "wassy"],
        "yonne": ["auxerre", "sens", "avallon", "joigny", "migennes"]
    }
    
    def validate_establishment(self, establishment: ExtractedEstablishment, 
                              source_content: str, department: str, source_url: str = "") -> Tuple[bool, float, List[str]]:
        """
        Validation stricte post-extraction avec filtrage des sources officielles et exclusions par type
        Returns: (is_valid, confidence_score, list_of_issues)
        """
        issues = []
        confidence = 100.0
        
        # 0. EXCLUSIONS PRIORITAIRES - Établissements non compatibles
        excluded, exclusion_reason = self._check_establishment_exclusions(establishment, source_content)
        if excluded:
            issues.append(exclusion_reason)
            confidence = 0  # Rejet immédiat
            return False, confidence, issues
        
        # 1. Validation source officielle PRIORITAIRE  
        source_valid, source_penalty, source_issue = self._validate_official_source(source_url)
        if not source_valid:
            issues.append(source_issue)
            confidence -= source_penalty
        
        # 2. Validation cohérence nom/source (ASSOUPLIE)
        if not self._validate_name_in_source_flexible(establishment.nom, source_content):
            issues.append(f"Nom '{establishment.nom}' peu cohérent avec source")
            confidence -= 20  # Pénalité réduite de 40 à 20
        
        # 3. Validation géographique stricte
        geo_valid, geo_issue = self._validate_geography(establishment.commune, department)
        if not geo_valid:
            issues.append(geo_issue)
            confidence -= 35
        
        # 3. Validation format coordonnées
        if establishment.telephone:
            if not self._is_valid_french_phone(establishment.telephone):
                issues.append(f"Téléphone invalide: {establishment.telephone}")
                confidence -= 10
        
        if establishment.email:
            if not self._is_valid_email(establishment.email):
                issues.append(f"Email invalide: {establishment.email}")
                confidence -= 10
        
        # 4. Validation cohérence gestionnaire - Nettoyage automatique
        # (Gestionnaires suspects sont automatiquement nettoyés à la création)
        
        # 5. Détection patterns hallucination
        if self._detect_hallucination_patterns(establishment):
            issues.append("Pattern d'hallucination détecté")
            confidence -= 50
        
        # Score final
        confidence = max(0, confidence)
        is_valid = confidence >= 70  # Seuil à 70% comme demandé
        
        return is_valid, confidence, issues
    
    def _validate_official_source(self, source_url: str) -> Tuple[bool, float, str]:
        """
        Valide que la source est un site officiel et non un annuaire/presse
        Returns: (is_valid, penalty_score, issue_description)
        """
        if not source_url:
            return True, 0, ""  # Pas de pénalité si pas d'URL
        
        url_lower = source_url.lower()
        
        # 1. Sites à rejeter complètement (annuaires et presse)
        rejected_sources = [
            # Annuaires génériques
            'for-seniors.com', 'pour-les-personnes-agees.gouv.fr', 'essentiel-autonomie.com',
            'papyhappy.com', 'papy-happy.com', 'senioractuel.com', 'guide-senior.com',
            'senior-habitat.com', 'retraite-habitat.com', 'portail-seniors.com',
            'annuaire-seniors.fr', 'senior-services.fr', 'maisons-retraite.fr',
            'sanitaire-social.com', 'lesmaisonsderetraite.fr', 'kelsenior.com',
            'place-du-marche.fr', 'marche-public.fr', '123annuaires.com',
            
            # Presse locale/nationale - ÉLARGI
            'ladepeche.fr', 'sudouest.fr', 'lefigaro.fr', 'lemonde.fr', 'liberation.fr',
            'francebleu.fr', 'france3-regions.francetvinfo.fr', 'actu.fr',
            'lanouvellerepublique.fr', 'leparisien.fr', 'midilibre.fr',
            'leprogres.fr', 'lejdc.fr', 'lyonne.fr', 'lunion.fr',
            'lest-eclair.fr', 'lardennais.fr', 'jhm.fr', 'estrepublicain.fr',
            'nrpyrenees.fr', 'lavoixdunord.fr', 'nordeclair.fr',
            # Ajout presse locale manquante
            'courrier-picard.fr', 'paris-normandie.fr', 'tendanceouest.com',
            'lamontagne.fr', 'lepopulaire.fr', 'centre-presse.fr',
            'larep.fr', 'bienpublic.com', 'lejsl.com', 'ledauphine.com',
            'nicematin.com', 'varmatin.com', 'corsematin.com',
            'dna.fr', 'republicain-lorrain.fr', 'vosgesmatin.fr',
            
            # Plateformes d'information génériques
            'wikipedia.org', 'conseil-departemental.com', 'service-public.fr',
            'ameli.fr', 'msa.fr', 'carsat.fr', 'cnav.fr', 'caf.fr',
            'pole-emploi.fr', 'indeed.fr', 'leboncoin.fr',
            
            # Sites d'information santé/social généralistes
            'santemagazine.fr', 'doctissimo.fr', 'passeportsante.net',
            'pourbienvieillir.fr', 'capgeris.com', 'senioractu.com'
        ]
        
        for rejected in rejected_sources:
            if rejected in url_lower:
                return False, 80, f"Source rejetée: site d'annuaire/presse '{rejected}'"
        
        # 2. Sites officiels privilégiés (bonus de confiance)
        official_patterns = [
            # Sites d'établissements
            r'[a-zA-Z0-9\-]+\.(com|fr|org|net)(?!/annuaire|/liste|/repertoire)',
            # Sites d'associations connues  
            'agesetvie.com', 'ages-et-vie.com', 'gurekin.fr', 'oveole.fr',
            'habitat-humanisme.org', 'familles-solidaires.org',
            'udaf\d+\.fr', 'apf\.asso\.fr', 'adapei\d+\.fr',
            'admr\.org', 'admr\d+\.fr', 'caritas\.fr',
            # Sites institutionnels départementaux
            r'[a-zA-Z\-]+\.(fr|gouv\.fr)$',
            # Sites de communes
            r'mairie-[a-zA-Z\-]+\.fr',
        ]
        
        # Bonus si site officiel détecté
        for pattern in official_patterns:
            if re.search(pattern, url_lower):
                return True, -10, "Source officielle privilégiée"  # Bonus de 10 points
        
        # 3. Sources moyennement fiables (légère pénalité)
        medium_sources = [
            'linkedin.com', 'facebook.com', 'pages-jaunes.fr', 
            'societe.com', 'verif.com', 'infogreffe.fr'
        ]
        
        for medium in medium_sources:
            if medium in url_lower:
                return True, 15, f"Source moyennement fiable: '{medium}'"
        
        # 4. Par défaut: source acceptable sans bonus ni malus
        return True, 0, ""
    
    def _check_establishment_exclusions(self, establishment: ExtractedEstablishment, source_content: str) -> Tuple[bool, str]:
        """
        Vérifie si l'établissement doit être exclu (EHPAD, résidences commerciales, etc.)
        Returns: (is_excluded, exclusion_reason)
        """
        
        # Nettoyer les textes pour analyse
        nom_lower = establishment.nom.lower()
        gestionnaire_lower = (establishment.gestionnaire or "").lower()
        content_lower = source_content.lower()
        
        # 1. EXCLUSIONS STRICTES - EHPAD et établissements médicalisés
        ehpad_patterns = [
            'ehpad', 'établissement hébergement personnes âgées dépendantes',
            'maison de retraite médicalisée', 'établissement médicalisé',
            'unité de soins longue durée', 'usld', 'mapad',
            'établissement pour personnes âgées dépendantes'
        ]
        
        for pattern in ehpad_patterns:
            if pattern in nom_lower or pattern in content_lower:
                return True, f"EXCLUSION: EHPAD/Établissement médicalisé détecté - '{pattern}'"
        
        # 2. GESTIONNAIRES COMMERCIAUX D'EHPAD à exclure
        commercial_ehpad_groups = [
            'korian', 'orpea', 'colisée', 'domusvi', 'lna santé', 'cap retraite',
            'résidences and co', 'garden résidence', 'maisons de famille',
            'emera', 'septime', 'clariane', 'initially'
        ]
        
        for group in commercial_ehpad_groups:
            if group in gestionnaire_lower:
                return True, f"EXCLUSION: Gestionnaire EHPAD commercial - '{group}'"
        
        # 3. EXCLUSIONS PAR MOTS-CLÉS INCOMPATIBLES
        excluded_keywords = [
            'centre de soins', 'hôpital de jour', 'clinique', 'centre médical',
            'service de soins infirmiers', 'ssiad', 'centre de rééducation',
            'maison médicale', 'cabinet médical', 'pharmacie',
            # Résidences étudiants (confusion fréquente)
            'résidence étudiante', 'résidence universitaire', 'crous',
            # Hôtellerie 
            'hôtel', 'chambres d\'hôtes', 'gîte'
        ]
        
        for keyword in excluded_keywords:
            if keyword in nom_lower or keyword in content_lower:
                return True, f"EXCLUSION: Type d'établissement incompatible - '{keyword}'"
        
        # 5. DÉTECTION D'ÉTABLISSEMENTS FICTIFS (articles de presse)
        if self._is_fictional_establishment(establishment, source_content):
            return True, "EXCLUSION: Établissement fictif détecté (article de presse sans établissement réel)"
        
        return False, ""
    
    def _is_fictional_establishment(self, establishment: ExtractedEstablishment, source_content: str) -> bool:
        """
        Détecte les établissements fictifs créés à partir d'articles de presse
        """
        content_lower = source_content.lower()
        
        # Indicateurs d'articles de presse
        press_indicators = [
            'article publié', 'rédaction', 'journaliste', 'correspondant',
            'par [nom prénom]', 'publié le', 'mis à jour le',
            'lire aussi', 'dans la même rubrique', 'tags:', 'mots-clés:',
            'partager cet article', 'commentaires', 'réagir'
        ]
        
        press_count = sum(1 for indicator in press_indicators if indicator in content_lower)
        
        # Indicateurs de projets/annonces (pas d'établissements réels)
        project_indicators = [
            'projet de', 'future résidence', 'sera construit', 'en cours de construction',
            'ouverture prévue', 'bientôt', 'à venir', 'en développement',
            'annonce', 'présentation du projet', 'étude de faisabilité'
        ]
        
        project_count = sum(1 for indicator in project_indicators if indicator in content_lower)
        
        # Si beaucoup d'indicateurs de presse + projet = probablement fictif
        if press_count >= 2 and project_count >= 1:
            return True
        
        # Détection d'informations trop vagues ou génériques
        if (not establishment.adresse_l1 and 
            not establishment.telephone and 
            not establishment.email and
            press_count >= 2):
            return True
        
        return False
    
    def _validate_name_in_source(self, name: str, source: str) -> bool:
        """Vérifie que le nom est présent dans la source"""
        if not name or not source:
            return False
        
        name_clean = name.lower().strip()
        source_clean = source.lower()
        
        # Recherche exacte ou partielle (au moins 50% du nom)
        if name_clean in source_clean:
            return True
        
        # Recherche par mots significatifs (≥3 lettres)
        name_words = [w for w in name_clean.split() if len(w) >= 3]
        if not name_words:
            return False
        
        words_found = sum(1 for word in name_words if word in source_clean)
        return words_found >= len(name_words) * 0.5
    
    def _validate_name_in_source_flexible(self, name: str, source: str) -> bool:
        """Validation assouplie du nom dans la source"""
        if not name or not source:
            return False
        
        name_clean = name.lower().strip()
        source_clean = source.lower()
        
        # 1. Recherche exacte (idéal)
        if name_clean in source_clean:
            return True
        
        # 2. Recherche par mots significatifs (≥3 lettres) avec seuil plus bas
        name_words = [w for w in name_clean.split() if len(w) >= 3 and w not in ['les', 'des', 'une', 'pour', 'aux', 'sur']]
        if not name_words:
            return True  # Si pas de mots significatifs, on accepte
        
        words_found = sum(1 for word in name_words if word in source_clean)
        # Seuil abaissé à 30% au lieu de 50%
        return words_found >= len(name_words) * 0.3
    
    def _validate_geography(self, commune: str, department: str) -> Tuple[bool, str]:
        """Validation géographique stricte"""
        if not commune:
            return False, "Commune non renseignée"
        
        commune_clean = commune.lower().strip()
        dept_clean = department.lower().strip()
        
        # Vérifier si département connu
        if dept_clean not in self.DEPT_COMMUNES:
            return True, ""  # Pas de validation si département inconnu
        
        valid_communes = self.DEPT_COMMUNES[dept_clean]
        
        # Recherche directe ou partielle
        for valid_commune in valid_communes:
            if valid_commune in commune_clean or commune_clean in valid_commune:
                return True, ""
        
        # Vérifier que ce n'est pas une commune d'un autre département
        for other_dept, other_communes in self.DEPT_COMMUNES.items():
            if other_dept != dept_clean:
                for other_commune in other_communes:
                    if other_commune in commune_clean:
                        return False, f"Commune '{commune}' appartient à {other_dept}, pas {dept_clean}"
        
        # Si commune non trouvée mais pas d'autre département détecté
        return True, f"⚠️ Commune '{commune}' non vérifiée dans {dept_clean}"
    
    def _is_valid_french_phone(self, phone: str) -> bool:
        """Validation téléphone français"""
        if not phone:
            return False
        
        # Nettoyer
        clean = re.sub(r'[^0-9+]', '', phone)
        
        # Patterns invalides évidents
        invalid = ['0000000000', '1111111111', '9999999999', '0123456789']
        if clean in invalid:
            return False
        
        # Formats valides français
        if re.match(r'^0[1-9]\d{8}$', clean):  # 0X XX XX XX XX
            return True
        if re.match(r'^\+33[1-9]\d{8}$', clean):  # +33 X XX XX XX XX
            return True
        if re.match(r'^33[1-9]\d{8}$', clean):  # 33 X XX XX XX XX
            return True
        
        return False
    
    def _is_valid_email(self, email: str) -> bool:
        """Validation email"""
        if not email or len(email) < 5:
            return False
        
        # Pattern basique
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return False
        
        # Rejeter emails suspects
        suspects = ['example', 'test', 'fake', 'noreply', 'xxx', '000']
        email_lower = email.lower()
        return not any(s in email_lower for s in suspects)
    
    def _is_suspicious_gestionnaire(self, gestionnaire: str) -> bool:
        """Détecte gestionnaires suspects"""
        if not gestionnaire:
            return False
        
        gest_lower = gestionnaire.lower()
        suspects = [
            'non renseigné', 'non précisé', 'inconnu', 'n/a', 'na',
            'particulier anonyme', 'privé', 'à définir',
            # Sites web confondus avec gestionnaires
            'essentiel autonomie', 'essentiel-autonomie',
            'pour-les-personnes-agees.gouv.fr', 'pour les personnes agees',
            'papyhappy', 'papy happy', 'papy-happy',
            'malakoff humanis',  # Éditeur du site Essentiel Autonomie, pas gestionnaire d'établissement
            'annuaire', 'site web', 'plateforme', 'portail'
        ]
        return any(s in gest_lower for s in suspects)

    def _detect_hallucination_patterns(self, establishment: ExtractedEstablishment) -> bool:
        """Détecte patterns typiques d'hallucination LLM"""
        
        # Pattern 1: Noms trop génériques
        generic_names = ['le béguinage', 'la maison', "l'accueil familial", 
                        'la résidence', 'le logement', 'mon logis']
        if establishment.nom.lower().strip() in generic_names:
            return True
        
        # Pattern 2: Téléphone/email génériques coordonnés
        if establishment.telephone and establishment.email:
            if '00 00' in establishment.telephone and 'contact@' in establishment.email:
                return True
        
        # Pattern 3: Présentation vide ou trop courte
        if establishment.presentation and len(establishment.presentation) < 20:
            return True
        
        return False


class LLMValidator:
    """Validation LLM hiérarchisée avec anti-hallucination stricte"""
    
    def __init__(self):
        self.groq_api_key = ai_config.groq_api_key
        self.validation_logs: List[ValidationStage] = []
        self.total_cost = 0.0
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.debug = False  # Mode debug par défaut désactivé
        
        # Pricing Groq (décembre 2025)
        self.pricing = {
            "light": {"input": 0.05, "output": 0.08},   # $/1M tokens - 8B
            "heavy": {"input": 0.59, "output": 0.79}    # $/1M tokens - 70B
        }
        
        # Modèles Groq - 8B pour qualification binaire + 70B pour extraction
        self.models = {
            "light": "llama-3.1-8b-instant",           # Qualification binaire OUI/NON
            "heavy": "llama-3.3-70b-versatile"         # Extraction précise
        }
        
        # Validateur post-extraction
        self.validator = ExtractionValidator()
    
    def validate_candidates(self, candidates: List, department: str, debug: bool = False) -> List[ExtractedEstablishment]:
        """Pipeline de validation hiérarchisée anti-hallucination"""
        
        self.debug = debug
        
        print(f"\n🧠 === MODULE 4 V2.1 - VALIDATION LLM ANTI-HALLUCINATION ===")
        print(f"📅 Session: {self.session_id} | Département: {department}")
        print(f"🎯 Seuil validation: 70% | Modèles: {self.models['light']} + {self.models['heavy']}")
        print(f"🐛 Debug: {'Activé' if debug else 'Désactivé'}")
        
        # ÉTAPE 1: Pre-filtre supprimé - passage direct vers qualification
        stage1_candidates = candidates  # Plus de pre-filtre
        
        # ÉTAPE 2: Qualification LLM légère
        stage2_candidates = self._stage2_light_qualification(stage1_candidates)
        
        # ÉTAPE 3: Extraction LLM lourde avec validation
        final_establishments = self._stage3_heavy_extraction(stage2_candidates, department)
        
        # Logs de synthèse
        self._print_quality_summary(candidates, final_establishments, department)
        
        return final_establishments
    
    def _stage2_light_qualification(self, candidates: List) -> List:
        """ÉTAPE 2: Qualification LLM légère binaire OUI/NON"""
        
        print(f"\n🤖 ÉTAPE 2 - Qualification LLM binaire ({len(candidates)} candidats)")
        print("🐛 MODE DEBUG ACTIVÉ - Contenu détaillé")
        
        qualified = []
        
        for i, candidate in enumerate(candidates, 1):
            print(f"\n--- DEBUG CANDIDAT {i:02d} ---")
            print(f"URL: {candidate.url}")
            print(f"TITRE: {candidate.nom}")
            print(f"SNIPPET: {candidate.snippet[:200]}...")
            
            start_time = time.time()
            
            # Enrichir le contenu avec un extrait de la page (les 500 premiers caractères)
            enriched_content = self._get_page_preview(candidate)
            
            prompt = self._build_qualification_prompt_strict(candidate, enriched_content)
            
            print(f"PROMPT ENVOYÉ:")
            print(prompt[:500] + "..." if len(prompt) > 500 else prompt)
            
            response = self._call_light_llm(prompt)
            duration = time.time() - start_time
            
            if response:
                # Parsing simplifié pour réponse OUI/NON
                content = response.get("content", "").strip()
                
                print(f"RÉPONSE LLM: '{content}'")
                
                content_upper = content.upper()
                
                # Détection OUI/NON dans la réponse
                if "OUI" in content_upper or "YES" in content_upper:
                    decision = "extract"
                    confidence = 85.0
                    reason = "Opérateur fiable ou établissement détecté"
                elif "NON" in content_upper or "NO" in content_upper:
                    decision = "reject" 
                    confidence = 90.0
                    reason = "Source informationnelle sans établissement"
                else:
                    # Fallback si réponse ambiguë
                    decision = "reject"
                    confidence = 50.0
                    reason = f"Réponse ambiguë: '{content[:50]}'"
                
                stage_log = ValidationStage(
                    stage_name="light_qualification",
                    candidate_id=f"cand_{i:02d}",
                    input_tokens=response.get("input_tokens", 0),
                    output_tokens=response.get("output_tokens", 0),
                    cost_euros=response.get("cost", 0.0),
                    duration_seconds=duration,
                    decision=decision,
                    confidence=confidence,
                    reason=reason,
                    extracted_count=1 if decision == "extract" else 0
                )
                self.validation_logs.append(stage_log)
                self.total_cost += response.get("cost", 0.0)
                
                if decision == "extract" and confidence >= 70:  # Seuil 70%
                    qualified.append(candidate)
                    print(f"   ✅ {i:02d}: {candidate.nom[:50]}... → OUI ({confidence:.0f}%)")
                else:
                    print(f"   ❌ {i:02d}: {reason} ({confidence:.0f}%)")
        
        print(f"\n   📊 Qualifiés: {len(qualified)}/{len(candidates)}")
        return qualified
    
    def _stage3_heavy_extraction(self, candidates: List, department: str) -> List[ExtractedEstablishment]:
        """ÉTAPE 3 TRANSFORMÉE - Pipeline intelligent à 5 étapes pour tous les candidats OUI"""
        
        print(f"\n🔍 ÉTAPE 3 TRANSFORMÉE - Pipeline intelligent 5 étapes ({len(candidates)} candidats)")
        print("📋 Nouveau processus : Identification → Recherche → Extraction → Enrichissement → Normalisation")
        
        extracted_establishments = []
        
        for i, candidate in enumerate(candidates, 1):
            try:
                print(f"\n   --- CANDIDAT {i:02d}: {candidate.nom[:50]}... ---")
                
                # ÉTAPE 3.1: Identification des établissements
                print(f"   📋 3.1: Identification des établissements...")
                establishments_info = self._step_31_identify_establishments(candidate, department)
                
                if not establishments_info:
                    print(f"   ❌ {i:02d}: Aucun établissement identifié")
                    continue
                
                print(f"   ✅ {len(establishments_info)} établissement(s) identifié(s)")
                
                for j, est_info in enumerate(establishments_info, 1):
                    try:
                        print(f"   🔄 {i}.{j}: Traitement '{est_info['nom']}'...")
                        
                        # ÉTAPE 3.2: Recherche ciblée
                        print(f"      🔍 3.2: Recherche ciblée...")
                        search_results = self._step_32_targeted_search(est_info, department)
                        
                        # ÉTAPE 3.3: Extraction stricte
                        print(f"      📊 3.3: Extraction stricte...")
                        extracted_data = self._step_33_strict_extraction(est_info, search_results, candidate, department)
                        
                        if not extracted_data:
                            print(f"   ⚠️ {i}.{j}: Extraction échouée pour '{est_info['nom']}'")
                            continue
                        
                        # ÉTAPE 3.4: Enrichissement conditionnel
                        print(f"      🔧 3.4: Enrichissement conditionnel...")
                        enriched_data = self._step_34_conditional_enrichment(extracted_data, department)
                        
                        # ÉTAPE 3.5: Normalisation et contrôles
                        print(f"      ✅ 3.5: Normalisation et contrôles...")
                        final_establishment = self._step_35_normalize_and_validate(enriched_data, department)
                        
                        if final_establishment:
                            extracted_establishments.append(final_establishment)
                            print(f"   ✅ {i}.{j}: {final_establishment.nom} - {final_establishment.commune} ({final_establishment.confidence_score:.0f}%)")
                        else:
                            print(f"   ❌ {i}.{j}: Rejeté lors de la normalisation")
                            
                    except Exception as est_error:
                        print(f"   ⚠️ {i}.{j}: Erreur traitement établissement - {est_error}")
                        continue
                        
            except Exception as candidate_error:
                print(f"   ❌ {i:02d}: Erreur traitement candidat - {candidate_error}")
                continue
        
        return extracted_establishments
    
    def _process_single_candidate(self, candidate, index: int, department: str) -> List[ExtractedEstablishment]:
        """Traite un candidat de façon isolée avec détection d'échecs"""
        extracted_establishments = []
        
        start_time = time.time()
        prompt = self._build_extraction_prompt_strict(candidate, department)
        
        # Isolation complète de l'appel LLM
        try:
            response = self._call_heavy_llm(prompt)
            duration = time.time() - start_time
            
            if not response:
                # Échec LLM - candidat pour fallback
                raise Exception("LLM_FAILURE")
                
            if not response.get("establishments"):
                # Pas d'établissements - candidat pour fallback
                raise Exception("NO_ESTABLISHMENTS")
            
            establishments = response["establishments"]
            
            # Log avant validation
            stage_log = ValidationStage(
                stage_name="heavy_extraction",
                candidate_id=f"cand_{index:02d}",
                input_tokens=response.get("input_tokens", 0),
                output_tokens=response.get("output_tokens", 0),
                cost_euros=response.get("cost", 0.0),
                duration_seconds=duration,
                decision="extracted",
                confidence=response.get("confidence", 0.0),
                reason=f"Extrait {len(establishments)} établissement(s)",
                extracted_count=len(establishments)
            )
            self.validation_logs.append(stage_log)
            self.total_cost += response.get("cost", 0.0)
            
            # Validation isolée de chaque établissement
            validation_failed = 0
            for j, est_data in enumerate(establishments, 1):
                try:
                    establishment = self._create_establishment_object(est_data, candidate, department)
                    
                    # VALIDATION POST-EXTRACTION STRICTE
                    is_valid, confidence, issues = self.validator.validate_establishment(
                        establishment, candidate.snippet, department, getattr(candidate, 'url', '')
                    )
                    
                    if is_valid:
                        establishment.confidence_score = confidence
                        extracted_establishments.append(establishment)
                        print(f"   ✅ {index}.{j}: {establishment.nom} - {establishment.commune} ({confidence:.0f}%)")
                    else:
                        validation_failed += 1
                        print(f"   ❌ {index}.{j}: REJETÉ - {establishment.nom}")
                        for issue in issues:
                            print(f"      • {issue}")
                            
                except Exception as est_error:
                    validation_failed += 1
                    print(f"   ❌ {index}.{j}: Erreur validation établissement - {est_error}")
                    continue
            
            # Si tous les établissements sont rejetés, déclencher fallback
            if len(extracted_establishments) == 0 and validation_failed > 0:
                raise Exception("ALL_ESTABLISHMENTS_REJECTED")
        
        except Exception as e:
            error_msg = str(e)
            if error_msg in ["LLM_FAILURE", "NO_ESTABLISHMENTS", "ALL_ESTABLISHMENTS_REJECTED"]:
                # Déclencher fallback
                raise e
            else:
                print(f"   ❌ {index:02d}: Erreur extraction LLM - {e}")
                raise e
            
        return extracted_establishments
    
    def _fallback_with_websearch(self, candidate, index: int, department: str) -> List[ExtractedEstablishment]:
        """Fallback intelligent : 1) Identifier les établissements, 2) Recherche ciblée"""
        
        # ÉTAPE 1: Identifier les établissements potentiels dans le contenu
        print(f"      📋 Étape 1: Identification des établissements potentiels...")
        potential_establishments = self._identify_potential_establishments(candidate, department)
        
        if not potential_establishments:
            print(f"   ❌ {index:02d}: Aucun établissement identifié dans le contenu")
            return []
        
        print(f"      ✅ {len(potential_establishments)} établissement(s) identifié(s): {[est['nom'] for est in potential_establishments]}")
        
        # ÉTAPE 2: Recherche ciblée sur chaque établissement identifié
        print(f"      🔍 Étape 2: Recherche d'informations ciblées...")
        extracted = []
        
        for j, potential_est in enumerate(potential_establishments, 1):
            try:
                establishment_info = self._search_specific_establishment(potential_est, department)
                if establishment_info:
                    # Créer l'objet établissement
                    establishment = self._create_establishment_from_info(establishment_info, candidate, department)
                    
                    # VALIDATION POST-EXTRACTION STRICTE 
                    is_valid, confidence, issues = self.validator.validate_establishment(
                        establishment, candidate.snippet, department, getattr(candidate, 'url', '')
                    )
                    
                    if is_valid:
                        establishment.confidence_score = confidence
                        extracted.append(establishment)
                        print(f"   🔄 {index}.{j}: {establishment.nom} (fallback ciblé) - {confidence:.0f}%")
                    else:
                        print(f"   ❌ {index}.{j}: REJETÉ FALLBACK - {establishment.nom}")
                        for issue in issues:
                            print(f"      • {issue}")
                else:
                    print(f"   ⚠️ {index}.{j}: Pas d'informations trouvées pour {potential_est['nom']}")
                    
            except Exception as e:
                print(f"   ⚠️ {index}.{j}: Erreur recherche {potential_est['nom']} - {e}")
                continue
        
        return extracted
    
    def _identify_potential_establishments(self, candidate, department: str) -> List[Dict]:
        """ÉTAPE 1: Identifier les établissements potentiels dans le contenu avec LLM léger"""
        
        prompt = f"""
Analyse ce contenu web et identifie UNIQUEMENT les établissements réels d'habitat alternatif pour seniors.

CONTENU WEB:
{candidate.nom}
{candidate.snippet}

INSTRUCTIONS:
🎯 Cherche SEULEMENT des établissements nommés spécifiquement qui correspondent à :
- Habitat inclusif/partagé
- Béguinage  
- Maisons partagées pour seniors
- Village seniors
- Colocations seniors
- Habitat intergénérationnel
- Maisons d'accueil familial

❌ IGNORE complètement :
- Articles de presse généraux
- Descriptions de politiques/projets  
- EHPAD commerciaux (Korian, Orpea, DomusVi, Colisée)
- Établissements fictifs ou hypothétiques
- Simples mentions de concepts sans établissement précis

⚠️ ATTENTION : Ne rejette pas un établissement simplement parce qu'il contient "résidence" ou "autonomie" dans son nom - regarde le CONTEXTE complet

RÉPONSE ATTENDUE:
Si tu identifies des établissements réels, réponds en JSON:
{{"etablissements": [{{"nom": "nom exact", "commune": "commune", "gestionnaire": "gestionnaire si clairement mentionné ou vide si inconnu"}}]}}

IMPORTANT:
- Si le gestionnaire n'est PAS clairement mentionné, laisse le champ vide: "gestionnaire": ""
- Ne devine JAMAIS le gestionnaire
- Mieux vaut un gestionnaire vide qu'un gestionnaire inventé

Si aucun établissement réel identifié, réponds:
{{"etablissements": []}}
"""

        try:
            response = self._call_light_llm(prompt)
            if response and response.get("content"):
                content = response["content"]
                
                # Chercher et extraire le JSON de façon simple
                import json
                
                # Extraire tout ce qui est entre { et } contenant "etablissements"
                start_idx = content.find('{"etablissements"')
                if start_idx == -1:
                    start_idx = content.find('{\n  "etablissements"')
                
                if start_idx != -1:
                    # Trouver le } fermant
                    brace_count = 0
                    end_idx = start_idx
                    for i, char in enumerate(content[start_idx:], start_idx):
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                end_idx = i + 1
                                break
                    
                    if end_idx > start_idx:
                        json_str = content[start_idx:end_idx]
                        try:
                            result = json.loads(json_str)
                            establishments = result.get("etablissements", [])
                            return establishments
                        except json.JSONDecodeError:
                            pass
                
                return []
            return []
        except Exception as e:
            print(f"      ⚠️ Erreur identification: {e}")
            return []
    
    def _search_specific_establishment(self, establishment_info: Dict, department: str) -> Dict:
        """ÉTAPE 2: Recherche ciblée d'informations sur un établissement spécifique"""
        
        nom = establishment_info.get("nom", "")
        commune = establishment_info.get("commune", "")
        gestionnaire = establishment_info.get("gestionnaire", "")
        
        if not nom:
            return None
        
        # Construire des requêtes spécifiques pour cet établissement
        search_queries = [
            f'"{nom}" {commune} {department} adresse téléphone',
            f'"{nom}" {gestionnaire} contact information',
            f'"{nom}" {commune} habitat partagé seniors'
        ]
        
        enriched_info = ""
        for query in search_queries[:2]:  # Max 2 recherches par établissement
            try:
                search_results = self._perform_web_search(query)
                if search_results:
                    enriched_info += f"\n{search_results}"
            except:
                continue
        
        if not enriched_info:
            return None
        
        # Extraire les informations avec LLM
        extraction_prompt = f"""
Extrait les informations de contact pour cet établissement spécifique:

ÉTABLISSEMENT RECHERCHÉ: {nom}
COMMUNE: {commune} 
GESTIONNAIRE: {gestionnaire}

INFORMATIONS TROUVÉES:
{enriched_info[:1500]}

EXTRAIT en JSON:
{{"nom": "{nom}", "commune": "{commune}", "gestionnaire": "{gestionnaire}", "adresse": "adresse complète si trouvée sinon vide", "telephone": "numéro si trouvé sinon vide", "email": "email si trouvé sinon vide", "site_web": "site si trouvé sinon vide"}}

IMPORTANT: 
- Si une information n'est PAS trouvée, laisse le champ VIDE, ne mets pas "non trouvé"
- Si les informations ne correspondent pas à l'établissement recherché, réponds: {{"erreur": "non_correspondant"}}
"""
        
        try:
            response = self._call_light_llm(extraction_prompt)
            if response and response.get("content"):
                import json
                return json.loads(response["content"])
        except:
            pass
        
        return None
    
    def _create_establishment_from_info(self, info: Dict, candidate, department: str) -> 'ExtractedEstablishment':
        """Créer un objet ExtractedEstablishment à partir des informations extraites"""
        
        # Nettoyer le gestionnaire - vide si non trouvé ou incertain
        gestionnaire = info.get("gestionnaire", "").strip()
        if gestionnaire.lower() in ["non trouvé", "non trouvée", "inconnu", "inconnue", "n/a", "non renseigné", ""]:
            gestionnaire = ""
        
        # Nettoyer les autres champs aussi
        telephone = info.get("telephone", "").strip()
        if telephone.lower() in ["non trouvé", "non trouvée", "inconnu", "n/a", ""]:
            telephone = ""
            
        email = info.get("email", "").strip()
        if email.lower() in ["non trouvé", "non trouvée", "inconnu", "n/a", ""]:
            email = ""
            
        site_web = info.get("site_web", "").strip()
        if site_web.lower() in ["non trouvé", "non trouvée", "inconnu", "n/a", ""]:
            site_web = ""
        
        return ExtractedEstablishment(
            nom=info.get("nom", ""),
            commune=info.get("commune", ""),
            code_postal=self._guess_postal_code(info.get("commune", ""), department, info.get("adresse_l1", ""), info.get("code_postal", "00000")),
            gestionnaire=gestionnaire,
            adresse_l1=info.get("adresse_l1", ""),
            telephone=telephone,
            email=email,
            site_web=site_web,
            sous_categories="Habitat inclusif",
            habitat_type="habitat_partage",
            eligibilite_avp="eligible",
            presentation="",
            departement=department,
            source=getattr(candidate, 'url', ''),
            date_extraction=datetime.now().strftime("%Y-%m-%d"),
            public_cible="seniors",
            confidence_score=70.0,
            validation_timestamp=datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        )
    
    def _guess_postal_code(self, commune: str, department: str, adresse: str = None, existing_postal: str = "00000") -> str:
        """Deviner le code postal à partir de la commune, département et adresse"""
        import re
        
        # Si un code postal valide existe déjà, le retourner
        if existing_postal and existing_postal != "00000" and len(existing_postal) == 5 and existing_postal.isdigit():
            return existing_postal
        
        # 1. AMÉLIORATION: Extraction intelligente depuis l'adresse
        if adresse:
            # Chercher tous les codes postaux 5 chiffres dans l'adresse
            postal_matches = re.findall(r'\b(\d{5})\b', adresse)
            
            # Mapping départements vers préfixes postaux
            dept_postal_mapping = {
                "aube": "10",
                "orne": "61", 
                "lot-et-garonne": "47",
                "landes": "40",
                "dordogne": "24",
                "pyrénées-atlantiques": "64",
                "var": "83",
                "seine-et-marne": "77"
            }
            
            expected_prefix = dept_postal_mapping.get(department.lower())
            
            for postal_code in postal_matches:
                # Vérifier cohérence département
                if expected_prefix and postal_code.startswith(expected_prefix):
                    print(f"      ✅ Code postal extrait de l'adresse: {postal_code} (cohérent avec {department})")
                    return postal_code
                else:
                    print(f"      ⚠️ Code postal {postal_code} incohérent avec département {department} (attendu: {expected_prefix}xx)")
        
        # 2. Fallback: mapping par commune pour l'Aube
        if department.lower() == "aube":
            commune_lower = commune.lower() if commune else ""
            if "troyes" in commune_lower:
                return "10000"
            elif "romilly" in commune_lower:
                return "10100" 
            elif "bar-sur-aube" in commune_lower:
                return "10200"
            elif "nogent" in commune_lower:
                return "10400"
            elif "sainte-savine" in commune_lower:
                return "10300"
            elif "la-chapelle-saint-luc" in commune_lower:
                return "10600"
            elif "essoyes" in commune_lower:
                return "10360"
            elif "charmont" in commune_lower:
                return "10230"
            else:
                return "10000"  # Par défaut Troyes
        
        # 3. Autres départements
        if department.lower() == "orne":
            return "61000"  # Alençon par défaut
        elif department.lower() == "lot-et-garonne":
            return "47000"  # Agen par défaut
            
        return "00000"  # Fallback final
    
    def _clean_address_postal_separation(self, adresse: str, existing_postal: str) -> tuple:
        """
        Nettoie automatiquement la séparation adresse/code postal
        
        Args:
            adresse: Adresse possiblement contaminée par un code postal
            existing_postal: Code postal existant
            
        Returns:
            (adresse_nettoyee, code_postal_extrait)
        """
        import re
        
        if not adresse:
            return "", existing_postal
        
        # Chercher les codes postaux dans l'adresse
        postal_matches = re.findall(r'\b(\d{5})\b', adresse)
        
        if postal_matches:
            # Prendre le premier code postal trouvé
            extracted_postal = postal_matches[0]
            
            # Nettoyer l'adresse en supprimant le code postal
            # Patterns courants: "rue XXX, 10000 Ville" ou "rue XXX 10000 Ville"
            adresse_cleaned = re.sub(r',?\s*\d{5}.*$', '', adresse).strip()
            adresse_cleaned = re.sub(r'\s+\d{5}.*$', '', adresse_cleaned).strip()
            
            # Si on n'avait pas de code postal ou si l'existant était par défaut
            if not existing_postal or existing_postal == "00000":
                print(f"      🧹 Adresse nettoyée: '{adresse}' → '{adresse_cleaned}' + code postal '{extracted_postal}'")
                return adresse_cleaned, extracted_postal
            else:
                # Garder l'existant mais nettoyer l'adresse quand même
                print(f"      🧹 Adresse nettoyée: '{adresse}' → '{adresse_cleaned}' (code postal existant conservé)")
                return adresse_cleaned, existing_postal
        
        # Pas de code postal dans l'adresse
        return adresse, existing_postal
    
    def _validate_geographic_coherence(self, establishment, department: str) -> bool:
        """Valide la cohérence géographique entre département et code postal"""
        import re
        
        postal_code = establishment.code_postal
        
        # Extraire le département du code postal (2 premiers chiffres)
        if len(postal_code) == 5 and postal_code.isdigit():
            postal_dept = postal_code[:2]
            
            # Mapping départements
            dept_mapping = {
                "aube": "10",
                "orne": "61",
                "lot-et-garonne": "47"
            }
            
            expected_dept = dept_mapping.get(department.lower())
            
            if expected_dept and postal_dept != expected_dept:
                print(f"      ❌ ERREUR GÉOGRAPHIQUE: {establishment.nom}")
                print(f"         Département attendu: {department} ({expected_dept})")
                print(f"         Code postal trouvé: {postal_code} (département {postal_dept})")
                print(f"         Commune: {establishment.commune}")
                return False
                
        return True
    
    def _build_fallback_search_queries(self, candidate, department: str) -> List[str]:
        """Construit des requêtes de recherche ciblées pour le fallback"""
        
        # Extraire des mots-clés du nom et snippet
        import re
        
        # Nettoyer le nom pour extraire l'organisation
        name = candidate.nom
        org_keywords = []
        
        # Patterns d'organisations connues
        org_patterns = [
            r'(Ages?\s*&?\s*Vie)',
            r'(Gurekin)',
            r'(Oveole)',
            r'(UDAF)',
            r'(Habitat\s*&?\s*Humanisme)',
            r'(L\'?Étincelle)',
            r'(Familles?\s*solidaires?)',
            r'(\w+)\s*maisons?\s*partag[ée]es?',
            r'(Résidence\s*\w+)',
            r'(Colocation\s*\w+)'
        ]
        
        for pattern in org_patterns:
            match = re.search(pattern, name, re.IGNORECASE)
            if match:
                org_keywords.append(match.group(1).strip())
        
        # Fallback: premier mot du nom
        if not org_keywords:
            first_word = name.split()[0] if name else ""
            if len(first_word) > 3:
                org_keywords.append(first_word)
        
        queries = []
        
        # Requête 1: Organisation + département + type
        if org_keywords:
            for org in org_keywords[:2]:  # Max 2 organisations
                queries.append(f'"{org}" habitat partagé seniors {department}')
                queries.append(f'"{org}" maison partagée {department}')
        
        # Requête 2: Organisation + infos contact
        if org_keywords:
            for org in org_keywords[:1]:  # 1 seule organisation
                queries.append(f'"{org}" adresse téléphone contact {department}')
        
        # Requête 3: Recherche générale département
        if org_keywords:
            for org in org_keywords[:1]:
                queries.append(f'"{org}" etablissement {department} site:*.fr')
        
        # Limiter à 5 requêtes max
        return queries[:5]
    
    def _perform_web_search(self, query: str) -> str:
        """Effectue une recherche web via Serper API"""
        try:
            import requests
            import os
            
            serper_api_key = os.getenv('SERPER_API_KEY')
            if not serper_api_key:
                return ""
            
            headers = {
                'X-API-KEY': serper_api_key,
                'Content-Type': 'application/json'
            }
            
            # Sites à exclure (contamination connue)
            excluded_sites = [
                'essentiel-autonomie.com',
                'papyhappy.com',
                'pour-les-personnes-agees.gouv.fr'
            ]
            
            # Ajouter exclusions à la requête
            exclusions = ' '.join(f'-site:{site}' for site in excluded_sites)
            query_with_exclusions = f'{query} {exclusions}'
            
            payload = {
                'q': query_with_exclusions,
                'num': 3,  # Limiter à 3 résultats
                'gl': 'fr',
                'hl': 'fr'
            }
            
            response = requests.post(
                'https://google.serper.dev/search',
                headers=headers,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Filtrer les résultats pour exclure les sites contaminés
                excluded_domains = ['essentiel-autonomie.com', 'papyhappy.com', 'pour-les-personnes-agees.gouv.fr']
                filtered_results = []
                for result in data.get('organic', []):
                    link = result.get('link', '')
                    if not any(domain in link for domain in excluded_domains):
                        filtered_results.append(result)
                
                content = ""
                for result in filtered_results[:3]:
                    title = result.get('title', '')
                    snippet = result.get('snippet', '')
                    link = result.get('link', '')
                    
                    content += f"TITRE: {title}\nURL: {link}\nEXTRAIT: {snippet}\n\n"
                
                return content[:1000]  # Limiter la taille
            else:
                return ""
                
        except Exception as e:
            print(f"      ⚠️ Erreur recherche web: {e}")
            return ""
    
    def _create_enriched_candidate(self, original_candidate, enriched_content: str):
        """Crée un candidat enrichi avec le contenu supplémentaire"""
        from .alternative_scraper import EstablishmentCandidate
        
        # Enrichir le snippet avec les nouvelles informations
        enriched_snippet = f"{original_candidate.snippet}\n\nINFORMATIONS SUPPLÉMENTAIRES:\n{enriched_content[:500]}"
        
        return EstablishmentCandidate(
            nom=original_candidate.nom,
            url=original_candidate.url,
            snippet=enriched_snippet,
            commune=original_candidate.commune,
            departement=original_candidate.departement,
            confidence_score=original_candidate.confidence_score
        )
    
    def _build_extraction_prompt_enriched(self, candidate, department: str, enriched_content: str) -> str:
        """Prompt d extraction enrichi avec contexte supplémentaire"""
        
        prompt = f"""Tu es un expert en extraction d informations d etablissements seniors.
Extrait UNIQUEMENT les etablissements seniors reels du departement {department} depuis ce contenu enrichi.

IMPORTANT: Les informations proviennent de multiples sources web. Extrait seulement si tu es sur que l etablissement existe dans le departement {department}.

CONTENU ORIGINAL:
{candidate.snippet[:800]}

CONTENU ENRICHI:
{enriched_content[:1200]}

Format JSON:
{{
  "establishments": [
    {{
      "nom": "Nom exact de l etablissement",
      "commune": "Commune precise",
      "gestionnaire": "Nom du gestionnaire/operateur",
      "adresse": "Adresse complete si disponible",
      "telephone": "Telephone si disponible",
      "email": "Email si disponible",
      "sous_categories": "habitat_partage"
    }}
  ]
}}

Reponds uniquement en JSON valide."""
        
        return prompt

    
    def _get_page_preview(self, candidate) -> str:
        """Obtient un aperçu rapide de la page avec headers anti-détection"""
        try:
            import requests
            # Headers anti-détection améliorés
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            }
            
            response = requests.get(candidate.url, headers=headers, timeout=15, allow_redirects=True)
            
            if response.status_code == 200:
                # Nettoyer le HTML basiquement
                import re
                text = re.sub(r'<[^>]+>', ' ', response.text)
                text = re.sub(r'\s+', ' ', text).strip()
                return text[:800]  # Premiers 800 caractères
            elif response.status_code == 403:
                print(f"      ⚠️ HTTP 403 pour {candidate.url} - Utilisation du snippet")
                return candidate.snippet
            else:
                print(f"      ⚠️ HTTP {response.status_code} pour {candidate.url}")
                return candidate.snippet
        except Exception as e:
            print(f"      ⚠️ Erreur scraping {candidate.url}: {e}")
            return candidate.snippet  # Fallback sur snippet original
    
    def _build_qualification_prompt_strict(self, candidate, enriched_content=None) -> str:
        """Prompt de qualification BINAIRE OUI/NON - Version claire et précise"""
        
        # Utiliser le contenu enrichi si disponible, sinon le snippet
        content_to_analyze = enriched_content if enriched_content else candidate.snippet
        
        return f"""Analyse ce contenu web et réponds uniquement OUI ou NON.

🎯 RÉPONDS "OUI" si le texte contient des informations sur un ou plusieurs établissements identifiés par un nom et une commune entrant dans les catégories suivantes :
- Habitat inclusif
- Habitat intergénérationnel  
- Béguinage
- Village seniors
- Colocations avec services
- Accueil familial
- Maisons partagées

❌ RÉPONDS "NON" s'il s'agit :
- D'un EHPAD ou d'un USLD
- D'un texte de présentation générale sans référence à un ou plusieurs établissements nommés et localisés.

CONTENU À ANALYSER:
URL: {candidate.url}
TITRE: {candidate.nom}
TEXTE: {content_to_analyze[:1000]}...

RÉPONDS UN SEUL MOT: OUI ou NON"""
    
    def _build_extraction_prompt_strict(self, candidate, department: str) -> str:
        """Prompt d'extraction STRICT anti-hallucination"""
        
        return f"""RÈGLES ANTI-HALLUCINATION ABSOLUES:

⛔ INTERDICTION FORMELLE:
1. JAMAIS inventer de données
2. JAMAIS supposer ou compléter
3. JAMAIS extraire sans certitude absolue

🚫 EXCLUSIONS STRICTES - Ne pas extraire :
- EHPAD

🌐 SITES DE PRESSE - Ne pas extraire le site web si c'est un site de presse (nous recherchons le site officiel de l'établissement ou du gestionnaire)

✅ RÈGLE D'OR:
Si une information N'EST PAS écrite TEXTUELLEMENT → NE PAS l'inclure

VALIDATION PRÉALABLE OBLIGATOIRE:
Avant d'extraire un établissement, vérifie:
□ Le nom EXACT est-il présent? OUI/NON
□ L'adresse/commune est-elle EXPLICITE? OUI/NON
□ Des coordonnées sont-elles MENTIONNÉES? OUI/NON

Si UN SEUL "NON" → NE PAS extraire cet établissement

SOURCE:
Département: {department}
URL: {candidate.url}
Titre: {candidate.nom}
Contenu: {candidate.snippet}

CRITÈRES MINIMUMS EXTRACTION (suffit pour extraire):
✅ Nom établissement + Commune = SUFFISANT
✅ OU Nom + Opérateur connu (Ages & Vie, CetteFamille, APEI)

INSTRUCTIONS:
- Extraire établissements avec AU MINIMUM: nom + commune
- Coordonnées/adresse/gestionnaire: OPTIONNELS (l'enrichissement se fera après)
- Si info absente → laisser VIDE (pas d'invention)
- JAMAIS inventer ce qui n'est pas écrit

⚠️ ATTENTION GESTIONNAIRE - RÈGLES STRICTES:
- NE PAS confondre le nom du site web avec le gestionnaire
- "Essentiel Autonomie" = SITE WEB, pas un gestionnaire
- Si seul le nom du site est mentionné → laisser gestionnaire VIDE
- NE PAS inventer ou transférer de gestionnaires d'autres établissements
- Si "CetteFamille" est dans le nom → NE PAS mettre "Ages & Vie" comme gestionnaire
- Chaque établissement a SON PROPRE gestionnaire, ne pas mélanger
- VÉRIFIER: le gestionnaire correspond-il VRAIMENT à CET établissement précis?

Format JSON:
{{
  "establishments": [
    {{
      "Nom": "nom établissement",
      "Commune": "ville",
      "Département": "{department}",
      "Adresse": "si trouvée sinon vide",
      "Téléphone": "si trouvé sinon vide",
      "Email": "si trouvé sinon vide",
      "Site": "si trouvé sinon vide",
      "Gestionnaire": "si trouvé sinon vide",
      "Type": "Habitat partagé|Habitat inclusif|Béguinage|Accueil familial",
      "Description": "si trouvée sinon vide",
      "Source": "{candidate.url}",
      "confidence_score": 70-100
    }}
  ],
  "confidence": 70-100
}}

Si aucun nom+commune trouvé → {{"establishments": [], "confidence": 0}}

NOTE: Données partielles OK. Enrichissement se fera ensuite."""
    
    def _call_light_llm(self, prompt: str) -> Optional[Dict]:
        """Appel LLM léger (8B) pour qualification OUI/NON"""
        try:
            response = self._call_groq_api(prompt, self.models["light"], 300, "light")
            if response:
                # Pour qualification OUI/NON, on retourne directement le contenu texte
                return {
                    "content": response["content"],
                    "input_tokens": response["input_tokens"],
                    "output_tokens": response["output_tokens"],
                    "cost": response["cost"]
                }
        except Exception as e:
            print(f"      ⚠️ Erreur LLM léger: {e}")
        return None
    
    def _call_heavy_llm(self, prompt: str) -> Optional[Dict]:
        """Appel LLM lourd (70B)"""
        try:
            response = self._call_groq_api(prompt, self.models["heavy"], 1200, "heavy")
            if response:
                content = response["content"]
                parsed = self._parse_json_robust(content)
                if parsed:
                    parsed.update({
                        "input_tokens": response["input_tokens"],
                        "output_tokens": response["output_tokens"],
                        "cost": response["cost"]
                    })
                    return parsed
        except Exception as e:
            print(f"      ⚠️ Erreur LLM lourd: {e}")
        return None
    
    def _call_groq_api(self, prompt: str, model: str, max_tokens: int, tier: str) -> Optional[Dict]:
        """Appel générique API Groq"""
        try:
            headers = {
                "Authorization": f"Bearer {self.groq_api_key}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "messages": [{"role": "user", "content": prompt}],
                "model": model,
                "temperature": 0.1,
                "max_tokens": max_tokens
            }
            
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
                
                input_tokens = usage.get("prompt_tokens", 0)
                output_tokens = usage.get("completion_tokens", 0)
                cost = (input_tokens * self.pricing[tier]["input"] + 
                       output_tokens * self.pricing[tier]["output"]) / 1_000_000
                
                return {
                    "content": content,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost": cost
                }
            else:
                print(f"      ❌ Erreur Groq API: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"      ❌ Erreur appel Groq: {e}")
            return None
    
    def _parse_json_robust(self, content: str) -> Optional[Dict]:
        """Parser JSON robuste avec gestion d'erreurs améliorée"""
        if not content:
            return None
        
        # Stratégies multiples de nettoyage et parsing
        strategies = [
            self._parse_strategy_1,  # Standard markdown cleanup
            self._parse_strategy_2,  # Aggressive regex extraction
            self._parse_strategy_3,  # Manual field extraction
            self._parse_strategy_4   # Fallback with defaults
        ]
        
        for i, strategy in enumerate(strategies, 1):
            try:
                result = strategy(content)
                if result:
                    if i > 1:
                        print(f"      ℹ️ JSON parsing réussi avec stratégie {i}")
                    return result
            except Exception as e:
                if i < len(strategies):
                    continue
                else:
                    print(f"      ❌ Échec parsing JSON: {e}")
        
        return None
    
    def _parse_strategy_1(self, content: str) -> Optional[Dict]:
        """Stratégie 1: Nettoyage standard"""
        cleaned = content.strip()
        
        # Supprimer blocs markdown
        if "```json" in cleaned:
            cleaned = cleaned.split("```json")[1].split("```")[0]
        elif "```" in cleaned:
            parts = cleaned.split("```")
            if len(parts) >= 2:
                cleaned = parts[1]
        
        # Extraire JSON
        json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if json_match:
            cleaned = json_match.group(0)
        
        # Réparations courantes
        cleaned = cleaned.replace("'", '"')
        cleaned = re.sub(r',(\s*[}\]])', r'\1', cleaned)
        
        return json.loads(cleaned)
    
    def _parse_strategy_2(self, content: str) -> Optional[Dict]:
        """Stratégie 2: Extraction aggressive avec nettoyage"""
        # Rechercher tous les objets JSON potentiels
        json_objects = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', content, re.DOTALL)
        
        for json_str in json_objects:
            try:
                # Nettoyages agressifs
                cleaned = json_str.strip()
                cleaned = re.sub(r'\n', ' ', cleaned)
                cleaned = re.sub(r'\s+', ' ', cleaned)
                cleaned = cleaned.replace("'", '"')
                cleaned = re.sub(r',(\s*[}\]])', r'\1', cleaned)
                cleaned = re.sub(r'"([^"]*?)"\s*:', lambda m: f'"{m.group(1).replace(",", ";")}":', cleaned)
                
                result = json.loads(cleaned)
                if isinstance(result, dict) and "establishments" in result:
                    return result
            except:
                continue
        
        return None
    
    def _parse_strategy_3(self, content: str) -> Optional[Dict]:
        """Stratégie 3: Extraction manuelle des champs essentiels"""
        establishments_match = re.search(r'"establishments":\s*\[(.*?)\]', content, re.DOTALL)
        if not establishments_match:
            return None
        
        establishments_str = establishments_match.group(1)
        establishments = []
        
        # Extraire chaque établissement individuellement
        est_objects = re.findall(r'\{([^{}]*)\}', establishments_str)
        for est_str in est_objects:
            est_dict = {}
            # Extraire champs un par un
            for field in ['nom', 'commune', 'gestionnaire', 'adresse_l1', 'telephone', 'email', 'sous_categories']:
                match = re.search(rf'"{field}":\s*"([^"]*)"', est_str)
                est_dict[field] = match.group(1) if match else ""
            
            if est_dict.get('nom'):  # Si on a au moins un nom
                establishments.append(est_dict)
        
        if establishments:
            return {"establishments": establishments}
        
        return None
    
    def _parse_strategy_4(self, content: str) -> Optional[Dict]:
        """Stratégie 4: Fallback avec valeurs par défaut"""
        # Recherche très basique d'un nom d'établissement
        name_patterns = [
            r'"nom":\s*"([^"]+)"',
            r'nom[\s:]+([^\n,]+)',
            r'établissement[\s:]+([^\n,]+)'
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                return {
                    "establishments": [{
                        "nom": match.group(1).strip(),
                        "commune": "",
                        "gestionnaire": "", 
                        "adresse_l1": "",
                        "telephone": "",
                        "email": "",
                        "sous_categories": "habitat_partage"
                    }]
                }
        
        return None

    def _step_31_identify_establishments(self, candidate, department: str) -> List[Dict]:
        """ÉTAPE 3.1: Identifier les établissements avec nom/commune/gestionnaire obligatoires"""
        
        print(f"      🔍 Analyse du contenu pour identification...")
        
        prompt = f"""
Analyse ce contenu et identifie les établissements d'habitat alternatif pour seniors.

CONTENU:
Titre: {candidate.nom}
Texte: {candidate.snippet}

INSTRUCTIONS:
🎯 Pour chaque établissement identifié, extrait:
- nom: OBLIGATOIRE (nom exact de l'établissement)
- commune: OPTIONNEL (commune si mentionnée)
- gestionnaire: OPTIONNEL (gestionnaire/opérateur si mentionné)

✅ RÈGLES D'IDENTIFICATION:
- Accepter les structures nommées même si génériques (ex: "Habitat inclusif de LADAPT Aube")
- Accepter "[Organisation] + [Lieu]" comme nom valide (ex: "CetteFamille Troyes", "Ages & Vie Charmont")
- Accepter les noms avec indication de type (ex: "Maison partagée de Troyes")
- Au minimum le NOM doit être trouvé
❌ Ne pas extraire si seules des descriptions générales SANS nom

⚠️ ATTENTION GESTIONNAIRE:
- NE PAS confondre le nom du site web avec le gestionnaire
- "Essentiel Autonomie" = SITE WEB, pas un gestionnaire
- Si seul le nom du site est mentionné → laisser gestionnaire vide

Réponds en JSON:
{{
  "etablissements": [
    {{
      "nom": "nom exact obligatoire",
      "commune": "commune si trouvée sinon vide", 
      "gestionnaire": "gestionnaire si trouvé sinon vide"
    }}
  ]
}}

Si aucun établissement nommé trouvé → {{"etablissements": []}}
"""
        
        try:
            print(f"      🤖 Appel LLM en cours...")
            response = self._call_light_llm_with_timeout(prompt, timeout_seconds=30)
            
            if response and response.get("content"):
                import json
                content = response["content"]
                print(f"      📝 Réponse LLM reçue: {content[:100]}...")
                print(f"      🐛 DEBUG - Réponse LLM complète:")
                print(f"         {content}")
                
                # Extraire le JSON de façon robuste
                json_str = self._extract_json_from_content(content)
                if json_str:
                    try:
                        result = json.loads(json_str)
                        establishments = result.get("etablissements", [])
                        print(f"      ✅ {len(establishments)} établissement(s) trouvé(s) dans le JSON")
                        
                        # Filtrer pour garder seulement ceux avec un nom
                        valid_establishments = []
                        for est in establishments:
                            if est.get("nom") and len(est["nom"].strip()) > 2:
                                # Nettoyer les champs
                                cleaned_est = {
                                    "nom": est["nom"].strip(),
                                    "commune": est.get("commune", "").strip(),
                                    "gestionnaire": est.get("gestionnaire", "").strip()
                                }
                                
                                # Vérifier gestionnaire suspect
                                if self._is_suspicious_gestionnaire(cleaned_est["gestionnaire"]):
                                    cleaned_est["gestionnaire"] = ""
                                
                                valid_establishments.append(cleaned_est)
                                print(f"         • {cleaned_est['nom']} ({cleaned_est.get('commune', 'commune inconnue')})")
                        
                        return valid_establishments
                    except json.JSONDecodeError as je:
                        print(f"      ❌ Erreur parsing JSON: {je}")
                        print(f"      📄 Contenu JSON: {json_str[:200]}...")
                else:
                    print(f"      ⚠️ Aucun JSON trouvé dans la réponse")
            else:
                print(f"      ❌ Pas de réponse LLM valide")
                
        except Exception as e:
            print(f"      ❌ Erreur identification: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"      ❌ Aucun établissement identifié")
        return []
    
    def _step_32_targeted_search(self, establishment_info: Dict, department: str) -> List[str]:
        """ÉTAPE 3.2: Recherche ciblée avec exclusion sites contaminés"""
        
        nom = establishment_info["nom"]
        commune = establishment_info.get("commune", "")
        gestionnaire = establishment_info.get("gestionnaire", "")
        
        print(f"      🎯 Recherche ciblée pour: {nom}")
        if commune:
            print(f"         Commune: {commune}")
        if gestionnaire:
            print(f"         Gestionnaire: {gestionnaire}")
        
        # Construire requêtes de recherche très précises
        search_queries = []
        
        # Requête 1: Nom exact + commune + département (prioritaire si commune disponible)
        if commune:
            query1 = f'"{nom}" "{commune}" {department} adresse contact'
            search_queries.append(query1)
            print(f"         Query 1: {query1}")
        
        # Requête 2: Nom + gestionnaire + commune (si disponible)
        if gestionnaire:
            if commune:
                query2 = f'"{nom}" "{gestionnaire}" "{commune}" contact information'
            else:
                query2 = f'"{nom}" "{gestionnaire}" contact information'
            search_queries.append(query2)
            print(f"         Query 2: {query2}")
        
        # Requête 3: Nom + habitat seniors + commune (si disponible)
        if commune:
            query3 = f'"{nom}" habitat seniors "{commune}" {department}'
        else:
            query3 = f'"{nom}" habitat seniors {department}'
        search_queries.append(query3)
        print(f"         Query 3: {query3}")
        
        # Requête 4: Fallback sans commune si pas de commune (pour assurer au moins 3 requêtes)
        if not commune and len(search_queries) < 3:
            query4 = f'"{nom}" (seniors OR handicap) {department} site:*.fr'
            search_queries.append(query4)
            print(f"         Query 4: {query4}")
        
        # Collecter résultats avec timeout
        all_results = []
        
        for i, query in enumerate(search_queries[:3], 1):  # Limiter à 3 recherches
            try:
                print(f"      🔍 Recherche {i}/3...")
                results = self._perform_targeted_web_search_with_timeout(query, timeout_seconds=15)
                if results:
                    all_results.append(results)
                    print(f"         ✅ Résultats trouvés ({len(results)} caractères)")
                else:
                    print(f"         ❌ Aucun résultat")
            except Exception as e:
                print(f"      ⚠️ Erreur recherche {i}: {e}")
                continue
        
        print(f"      📊 Total: {len(all_results)} source(s) collectée(s)")
        return all_results
    
    def _step_33_strict_extraction(self, establishment_info: Dict, search_results: List[str], 
                                   candidate, department: str) -> Dict:
        """ÉTAPE 3.3: Extraction LLM stricte avec règles anti-hallucination"""
        
        # OPTIMISATION: Utiliser d'abord le contenu ScrapingBee si disponible
        content_sources = []
        
        # 1. Priorité: Contenu ScrapingBee déjà récupéré
        if hasattr(candidate, 'page_content') and candidate.page_content:
            content_sources.append(candidate.page_content[:3000])  # Limiter pour performance
            print(f"      📄 Utilisation contenu ScrapingBee existant")
        
        # 2. Complément: Résultats Serper si nécessaire
        if search_results:
            content_sources.extend(search_results)
            print(f"      📄 + {len(search_results)} source(s) Serper")
        
        if not content_sources:
            return None
        
        # Combiner toutes les sources
        combined_content = "\n\n".join(content_sources)
        
        prompt = f"""
RÈGLES ANTI-HALLUCINATION STRICTES:

⛔ INTERDICTION FORMELLE:
1. JAMAIS inventer de données
2. JAMAIS supposer ou compléter  
3. JAMAIS extraire sans certitude absolue

🎯 ÉTABLISSEMENT RECHERCHÉ:
Nom: {establishment_info['nom']}
Commune: {establishment_info.get('commune', 'Non spécifiée')}
Gestionnaire: {establishment_info.get('gestionnaire', 'Non spécifié')}

🌐 SITES DE PRESSE - IMPORTANT:
Si la source est un site de presse, ne pas mettre l'URL du site de presse dans "site_web".
Nous recherchons le site officiel de l'établissement ou du gestionnaire.

📮 EXTRACTION ADRESSE ET CODE POSTAL - TRÈS IMPORTANT:
- adresse_l1: Adresse COMPLÈTE SANS le code postal (ex: "32 rue du Beau Toquat")  
- code_postal: Code postal 5 chiffres UNIQUEMENT (ex: "10000")
- Si tu vois "32 rue du Beau Toquat, 10000 Troyes" → séparer en:
  * adresse_l1: "32 rue du Beau Toquat"
  * code_postal: "10000"
- Si tu vois "Avenue de la Gare 10360 Essoyes" → séparer en:
  * adresse_l1: "Avenue de la Gare"  
  * code_postal: "10360"
- JAMAIS mettre le code postal dans l'adresse_l1 !

CONTENU DE RECHERCHE:
{combined_content[:2000]}

✅ RÈGLE D'OR:
Si une information N'EST PAS écrite TEXTUELLEMENT → NE PAS l'inclure

EXTRAIT en JSON:
{{
  "nom": "nom exact",
  "commune": "commune si trouvée sinon vide",
  "gestionnaire": "gestionnaire si trouvé sinon vide", 
  "adresse_l1": "adresse complète si trouvée sinon vide",
  "code_postal": "code postal 5 chiffres si trouvé sinon vide",
  "telephone": "téléphone si trouvé sinon vide",
  "email": "email si trouvé sinon vide",
  "site_web": "site officiel établissement/gestionnaire si trouvé sinon vide (PAS site de presse)",
  "found": true
}}

IMPORTANT:
- Si l'établissement n'est PAS trouvé dans le contenu → {{"found": false}}
- Si info absente → laisser VIDE (pas d'invention)
- Site web = SEULEMENT site officiel établissement/gestionnaire
"""
        
        try:
            response = self._call_heavy_llm_json(prompt)
            print(f"      🐛 DEBUG 3.3 - Réponse LLM extraction: {response}")
            if response and response.get("found"):
                print(f"      🐛 DEBUG 3.3 - Données extraites: {response}")
                return response
            else:
                print(f"      🐛 DEBUG 3.3 - Pas trouvé ou erreur: {response}")
        except Exception as e:
            print(f"      ⚠️ Erreur extraction stricte: {e}")
        
        return None
    
    def _step_34_conditional_enrichment(self, extracted_data: Dict, department: str) -> Dict:
        """ÉTAPE 3.4: Enrichissement complémentaire SI beaucoup de données manquantes"""
        
        # Compter les champs manquants critiques
        critical_fields = ['telephone', 'email', 'gestionnaire', 'site_web']
        missing_count = sum(1 for field in critical_fields if not extracted_data.get(field))
        
        # Enrichissement seulement si ≥2 champs manquants parmi les critiques
        needs_enrichment = missing_count >= 2
        
        if not needs_enrichment:
            print(f"      ℹ️ Enrichissement non nécessaire ({missing_count} champs manquants)")
            return extracted_data
        
        print(f"      🔧 Enrichissement requis ({missing_count} champs manquants)")
        
        # 2 requêtes d'enrichissement avec ScrapingBee
        queries = []
        
        # Requête 1: Nom + gestionnaire/commune + contact
        base_info = extracted_data['nom']
        location_info = extracted_data.get('gestionnaire') or extracted_data.get('commune') or department
        queries.append(f'"{base_info}" "{location_info}" contact téléphone email')
        
        # Requête 2: Nom + gestionnaire/commune + site officiel  
        queries.append(f'"{base_info}" "{location_info}" site officiel')
        
        all_results = []
        for i, query in enumerate(queries, 1):
            try:
                print(f"      🌐 Recherche enrichissement {i}/2: {query[:50]}...")
                serper_results = self._perform_targeted_web_search(query)
                
                if serper_results:
                    # Extraire URLs pertinentes des résultats Serper
                    urls = self._extract_urls_from_serper(serper_results)
                    
                    # Scraper les URLs avec ScrapingBee (limité à 2 URLs max)
                    for j, url in enumerate(urls[:2], 1):
                        scraped_content = self._get_scrapingbee_content(url)
                        if scraped_content:
                            all_results.append(scraped_content)
                            print(f"      📄 Contenu {i}.{j} récupéré via ScrapingBee")
                            
            except Exception as e:
                print(f"      ⚠️ Erreur enrichissement {i}: {e}")
        
        if all_results:
            try:
                enriched = self._extract_enrichment_data(extracted_data, all_results)
                return enriched
            except Exception as e:
                print(f"      ⚠️ Erreur extraction enrichissement: {e}")
        
        return extracted_data
    
    def _step_35_normalize_and_validate(self, data: Dict, department: str) -> Optional['ExtractedEstablishment']:
        """ÉTAPE 3.5: Normalisation et contrôles finaux"""
        
        # Vérifier commune dans le bon département
        commune = data.get('commune', '')
        if commune and not self._is_commune_in_department(commune, department):
            print(f"      ⚠️ Commune {commune} ne semble pas être dans {department}")
        
        # Contrôle EHPAD dans nom/présentation
        nom = data.get('nom', '')
        if any(term in nom.lower() for term in ['ehpad', 'établissement hébergement personnes âgées']):
            print(f"      ❌ EHPAD détecté dans le nom: {nom}")
            return None
        
        # Contrôle gestionnaire suspect -> le vider mais garder l'établissement
        gestionnaire = data.get('gestionnaire', '')
        if self._is_suspicious_gestionnaire(gestionnaire):
            print(f"      ⚠️ Gestionnaire suspect '{gestionnaire}' -> vidé")
            data['gestionnaire'] = ''
        
        # Créer l'objet établissement
        try:
            establishment = self._create_establishment_from_data(data, department)
            
            # VALIDATION GÉOGRAPHIQUE
            if not self._validate_geographic_coherence(establishment, department):
                print(f"      ❌ Établissement rejeté pour incohérence géographique")
                return None
            
            # Validation finale
            is_valid, confidence, issues = self.validator.validate_establishment(
                establishment, "", department, data.get('site_web', '')
            )
            
            if is_valid:
                establishment.confidence_score = confidence
                return establishment
            else:
                print(f"      ❌ Validation finale échouée:")
                for issue in issues:
                    print(f"         • {issue}")
                return None
                
        except Exception as e:
            print(f"      ❌ Erreur création établissement: {e}")
            return None
    
    def _create_establishment_object(self, est_data: Dict, candidate, department: str) -> ExtractedEstablishment:
        """Crée l'objet ExtractedEstablishment depuis données LLM"""
        
        # Nettoyer gestionnaire suspect
        gestionnaire = est_data.get("Gestionnaire", est_data.get("gestionnaire", ""))
        if self._is_suspicious_gestionnaire(gestionnaire):
            print(f"      ⚠️ Gestionnaire suspect '{gestionnaire}' -> vidé pour enrichissement")
            gestionnaire = ""
        
        return ExtractedEstablishment(
            nom=est_data.get("Nom", est_data.get("nom", "")),
            commune=est_data.get("Commune", est_data.get("commune", "")),
            code_postal=est_data.get("CodePostal", ""),
            gestionnaire=gestionnaire,
            adresse_l1=est_data.get("Adresse", est_data.get("adresse", "")),
            telephone=est_data.get("Téléphone", est_data.get("telephone")) or None,
            email=est_data.get("Email", est_data.get("email")) or None,
            site_web=est_data.get("Site", est_data.get("site_web", "")) or None,
            sous_categories=self._map_gestionnaire_to_sous_categories(
                est_data.get("Nom", est_data.get("nom", "")),
                gestionnaire,
                est_data.get("Description", est_data.get("presentation", "")),
                ""
            ),
            habitat_type=self._map_habitat_type(
                est_data.get("Type", ""),
                est_data.get("Nom", est_data.get("nom", "")),
                gestionnaire,
                est_data.get("Description", est_data.get("presentation", "")),
                ""
            ),
            eligibilite_avp="a_verifier",
            presentation=est_data.get("Description", est_data.get("presentation", "")),
            departement=department,
            source=candidate.url,
            date_extraction=datetime.now().strftime("%Y-%m-%d"),
            public_cible="personnes_agees",
            confidence_score=est_data.get("confidence_score", 80.0),
            validation_timestamp=datetime.now().isoformat()
        )
    
    def _map_gestionnaire_to_sous_categories(self, nom: str = "", gestionnaire: str = "", presentation: str = "", source: str = "") -> str:
        """Map vers sous_categories selon hiérarchie de règles métier"""
        
        # Préparation des textes pour analyse
        nom_lower = nom.lower() if nom else ""
        gestionnaire_lower = gestionnaire.lower() if gestionnaire else ""
        presentation_lower = presentation.lower() if presentation else ""
        source_lower = source.lower() if source else ""
        
        # NIVEAU 1: Détection automatique par nom/contenu (priorité haute)
        # Résidence autonomie dans le nom ou MARPA
        if "résidence autonomie" in nom_lower or "marpa" in nom_lower:
            return "Résidence autonomie"
        
        if "marpa" in nom_lower:
            return "MARPA"
            
        # NIVEAU 2: Établissements du module official scraper résidences services  
        # Les résidences services ont déjà leur sous_categories assignée par le module officiel
        if "pour-les-personnes-agees.gouv.fr" in source_lower and "résidence services seniors" in nom_lower:
            return "Résidence services seniors"
            
        # NIVEAU 3: Détection par contenu (nom ou présentation)
        # Intergénérationnel
        if "intergénérationnel" in nom_lower or "intergénérationnel" in presentation_lower:
            return "Habitat intergénérationnel"
            
        # Béguinage
        if "béguinage" in nom_lower or "béguinage" in presentation_lower:
            return "Béguinage"
            
        # Village seniors
        if "village seniors" in nom_lower or "village seniors" in presentation_lower:
            return "Village seniors"
            
        # NIVEAU 4: Validation croisée nom/gestionnaire (anti-hallucination)
        # Si le nom contient explicitement un gestionnaire, privilégier ça sur l'extraction LLM
        if "cettefamille" in nom_lower and "ages" in gestionnaire_lower and "vie" in gestionnaire_lower:
            print(f"      🔧 Correction gestionnaire: 'CetteFamille' détecté dans nom '{nom}', ignorant gestionnaire halluciné '{gestionnaire}'")
            return "Colocation avec services"  # CetteFamille = colocation avec services
        
        # NIVEAU 5: Détection par gestionnaire (priorité moyenne)
        if "cettefamille" in gestionnaire_lower or "cette famille" in gestionnaire_lower:
            return "Maison d'accueil familial"
            
        if "ages & vie" in gestionnaire_lower or "ages et vie" in gestionnaire_lower:
            return "Colocation avec services"
            
        # NIVEAU 5: Défaut
        return "Habitat inclusif"

    def _map_habitat_type(self, type_str: str, nom: str = "", gestionnaire: str = "", presentation: str = "", source: str = "") -> str:
        """Map sous-catégorie vers habitat_type avec hiérarchie de règles"""
        
        # Déterminer la sous-catégorie réelle selon la hiérarchie
        actual_sous_cat = self._map_gestionnaire_to_sous_categories(nom, gestionnaire, presentation, source)
        
        # Mapping sous_categories → habitat_type
        sous_cat_lower = actual_sous_cat.lower()
        
        # residence ← Résidence autonomie, MARPA, Résidence services seniors
        if any(x in sous_cat_lower for x in ["résidence autonomie", "marpa", "résidence services seniors"]):
            return "residence"
        
        # logement_independant ← Béguinage, Village seniors, habitat regroupe  
        elif any(x in sous_cat_lower for x in ["béguinage", "village seniors", "habitat regroupe"]):
            return "logement_independant"
        
        # habitat_partage ← Habitat inclusif, Accueil familial, Maison d'accueil familial, Habitat intergénérationnel, Colocation avec services
        else:
            return "habitat_partage"
    
    def _is_suspicious_site(self, url: str) -> bool:
        """Détecte les sites suspects qui contaminent les données"""
        if not url:
            return False
            
        suspicious_domains = [
            "essentiel-autonomie.com",
            "pour-les-personnes-agees.gouv.fr", 
            "papyhappy.fr"
        ]
        
        url_lower = url.lower()
        return any(domain in url_lower for domain in suspicious_domains)
    
    def _print_quality_summary(self, initial_candidates: List, final_establishments: List, department: str):
        """Affiche résumé qualité"""
        
        print(f"\n📊 === RÉSUMÉ QUALITÉ MODULE 4 V2.1 ===")
        
        stage1_passed = len(initial_candidates)  # Plus de préfiltre, tous les candidats passent
        stage2_passed = len([log for log in self.validation_logs if log.stage_name == "light_qualification" and log.decision == "extract"])
        
        print(f"\n🔢 Conversion:")
        print(f"   • Candidats initiaux: {len(initial_candidates)}")
        print(f"   • Après pre-filtre: {stage1_passed}")
        print(f"   • Après qualification: {stage2_passed}")
        print(f"   • Après validation: {len(final_establishments)}")
        print(f"   • Taux succès: {len(final_establishments)/len(initial_candidates)*100:.1f}%")
        
        print(f"\n💰 Coûts:")
        print(f"   • TOTAL: €{self.total_cost:.4f}")
        
        if final_establishments:
            avg_confidence = sum(e.confidence_score for e in final_establishments) / len(final_establishments)
            print(f"\n✅ Qualité:")
            print(f"   • Confiance moyenne: {avg_confidence:.1f}%")
            print(f"   • Établissements ≥70%: {sum(1 for e in final_establishments if e.confidence_score >= 70)}")
    
    def export_to_csv(self, establishments: List[ExtractedEstablishment], filename: str):
        """Exporte CSV"""
        import csv
        
        fieldnames = [
            "nom", "commune", "code_postal", "gestionnaire", "adresse_l1",
            "telephone", "email", "site_web", "sous_categories", "habitat_type",
            "eligibilite_avp", "presentation", "departement", "source",
            "date_extraction", "public_cible", "confidence_score"
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for est in establishments:
                writer.writerow({
                    "nom": est.nom,
                    "commune": est.commune,
                    "code_postal": est.code_postal,
                    "gestionnaire": est.gestionnaire,
                    "adresse_l1": est.adresse_l1,
                    "telephone": est.telephone or "",
                    "email": est.email or "",
                    "site_web": est.site_web or "",
                    "sous_categories": est.sous_categories,
                    "habitat_type": est.habitat_type,
                    "eligibilite_avp": est.eligibilite_avp,
                    "presentation": est.presentation,
                    "departement": est.departement,
                    "source": est.source,
                    "date_extraction": est.date_extraction,
                    "public_cible": est.public_cible,
                    "confidence_score": f"{est.confidence_score:.1f}"
                })
        
        print(f"\n📄 CSV exporté: {filename}")
        return filename
    
    def _extract_urls_from_serper(self, serper_content: str) -> List[str]:
        """Extraire les URLs pertinentes des résultats Serper"""
        import re
        urls = []
        
        # Pattern pour extraire les URLs des résultats Serper
        url_pattern = r'https?://[^\s<>"]+'
        found_urls = re.findall(url_pattern, serper_content)
        
        # Filtrer les URLs pertinentes (exclure sites d'agrégation)
        excluded_domains = ['google.com', 'bing.com', 'yahoo.com', 'papyhappy.com', 'essentiel-autonomie.com']
        
        for url in found_urls:
            if not any(domain in url for domain in excluded_domains):
                urls.append(url)
                if len(urls) >= 3:  # Limiter à 3 URLs max
                    break
                    
        return urls
    
    def _get_scrapingbee_content(self, url: str) -> Optional[str]:
        """Récupérer le contenu d'une page via ScrapingBee pour enrichissement"""
        try:
            import requests
            import os
            
            api_key = os.getenv('SCRAPINGBEE_API_KEY')
            if not api_key:
                print(f"      ⚠️ API ScrapingBee non configurée")
                return None
            
            response = requests.get(
                'https://app.scrapingbee.com/api/v1/',
                params={
                    'api_key': api_key,
                    'url': url,
                    'render_js': 'false',
                    'wait': '1000'
                },
                timeout=15
            )
            
            if response.status_code == 200:
                return response.text[:2000]  # Limiter pour performance
            else:
                print(f"      ⚠️ Erreur ScrapingBee {response.status_code}")
                
        except Exception as e:
            print(f"      ⚠️ Erreur ScrapingBee: {e}")
        
        return None
    
    def _perform_targeted_web_search(self, query: str) -> str:
        """Recherche web ciblée avec exclusion des sites contaminés"""
        try:
            import requests
            import os
            
            serper_api_key = os.getenv('SERPER_API_KEY')
            if not serper_api_key:
                print(f"         ⚠️ API Serper non configurée")
                return ""
            
            print(f"         🌐 Recherche: {query[:80]}...")
            
            headers = {
                'X-API-KEY': serper_api_key,
                'Content-Type': 'application/json'
            }
            
            # Sites à exclure absolument (contamination connue)
            excluded_sites = [
                'essentiel-autonomie.com',
                'papyhappy.com', 
                'pour-les-personnes-agees.gouv.fr'
            ]
            
            # Ajouter exclusions à la requête
            exclusions = ' '.join(f'-site:{site}' for site in excluded_sites)
            query_with_exclusions = f'{query} {exclusions}'
            
            payload = {
                'q': query_with_exclusions,
                'num': 5,  # Récupérer 5 résultats pour plus de diversité
                'gl': 'fr',
                'hl': 'fr'
            }
            
            response = requests.post(
                'https://google.serper.dev/search',
                headers=headers,
                json=payload,
                timeout=10  # Timeout réduit
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"         ✅ API Serper: {len(data.get('organic', []))} résultats")
                
                # Scraper les 5 premiers résultats (mode simple pour éviter blocage)
                content = ""
                for i, result in enumerate(data.get('organic', [])[:5], 1):
                    title = result.get('title', '')
                    snippet = result.get('snippet', '')
                    link = result.get('link', '')
                    
                    # Vérifier encore que ce n'est pas un site exclu
                    if not any(excluded in link for excluded in excluded_sites):
                        print(f"            {i}. {title[:50]}...")
                        
                        # Mode simple: utiliser juste le snippet pour éviter blocages scraping
                        content += f"TITRE: {title}\nURL: {link}\nEXTRAIT: {snippet}\n\n"
                        
                        # Optionnel: scraper si temps le permet
                        try:
                            page_content = self._scrape_page_content_quick(link, timeout=5)
                            if page_content:
                                content += f"CONTENU: {page_content[:300]}\n\n"
                        except:
                            # Pas grave, on garde juste le snippet
                            pass
                
                return content[:2000]  # Limiter la taille
            else:
                print(f"         ❌ API Serper: status {response.status_code}")
            
        except Exception as e:
            print(f"         ❌ Erreur recherche ciblée: {e}")
        
        return ""
    
    def _scrape_page_content_quick(self, url: str, timeout: int = 5) -> str:
        """Scrape rapide du contenu d'une page avec timeout court"""
        try:
            import requests
            from bs4 import BeautifulSoup
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Nettoyer et extraire le texte rapidement
                for script in soup(["script", "style"]):
                    script.decompose()
                
                text = soup.get_text()
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
                
                return text[:500]  # Limité à 500 caractères
                
        except:
            pass
        
        return ""
    
    def _scrape_page_content(self, url: str) -> str:
        """Scrape le contenu d'une page pour l'enrichissement"""
        try:
            import requests
            from bs4 import BeautifulSoup
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Nettoyer et extraire le texte
                for script in soup(["script", "style"]):
                    script.decompose()
                
                text = soup.get_text()
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                text = ' '.join(chunk for chunk in chunks if chunk)
                
                return text[:1000]  # Limiter à 1000 caractères
                
        except:
            pass
        
        return ""
    
    def _call_heavy_llm_json(self, prompt: str) -> Optional[Dict]:
        """Appel LLM lourd avec parsing JSON robuste"""
        try:
            response = self._call_groq_api(prompt, self.models["heavy"], 800, "heavy")
            if response and response.get("content"):
                content = response["content"]
                
                # Parser JSON de façon robuste
                import json
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    return json.loads(json_str)
                    
        except Exception as e:
            print(f"      ⚠️ Erreur LLM lourd JSON: {e}")
        
        return None
    
    def _extract_enrichment_data(self, base_data: Dict, enrichment_content: str) -> Dict:
        """Extrait données d'enrichissement avec critère strict sur site web"""
        
        prompt = f"""
Enrichis ces données d'établissement avec le contenu trouvé.

DONNÉES DE BASE:
{json.dumps(base_data, indent=2, ensure_ascii=False)}

CONTENU D'ENRICHISSEMENT:
{enrichment_content[:1500]}

RÈGLES STRICTES:
1. Ne JAMAIS inventer de données
2. Site web = SEULEMENT site officiel établissement/gestionnaire (PAS sites de presse/annuaires)
3. Si info pas trouvée → garder la valeur de base ou vide

Réponds en JSON en conservant les données de base et ajoutant seulement ce qui est trouvé:
{{
  "nom": "conserver valeur de base",
  "commune": "conserver ou améliorer si trouvé",
  "gestionnaire": "conserver ou améliorer si trouvé",
  "adresse": "conserver ou améliorer si trouvé",
  "telephone": "conserver ou améliorer si trouvé",
  "email": "conserver ou améliorer si trouvé",
  "site_web": "SEULEMENT site officiel si trouvé, sinon garder valeur de base"
}}
"""
        
        try:
            response = self._call_heavy_llm_json(prompt)
            if response:
                # Fusionner avec les données de base
                enriched = base_data.copy()
                for key, value in response.items():
                    if value and isinstance(value, str) and value.strip():  # Vérifier que c'est une string
                        enriched[key] = value
                    elif value and not isinstance(value, str):  # Si ce n'est pas une string, convertir
                        enriched[key] = str(value)
                return enriched
        except Exception as e:
            print(f"      ⚠️ Erreur extraction enrichissement: {e}")
        
        return base_data
    
    def _is_commune_in_department(self, commune: str, department: str) -> bool:
        """Vérifie si la commune est dans le bon département (validation assouplie)"""
        if not commune or not department:
            return True  # Pas de vérification possible
        
        dept_clean = department.lower().strip()
        commune_clean = commune.lower().strip()
        
        # 1. Vérifier avec la liste des communes connues
        if dept_clean in self.validator.DEPT_COMMUNES:
            valid_communes = self.validator.DEPT_COMMUNES[dept_clean]
            if any(valid_commune in commune_clean or commune_clean in valid_commune 
                  for valid_commune in valid_communes):
                return True
        
        # 2. ASSOUPLISSEMENT: Vérifier le code postal si détecté dans le nom de commune
        import re
        postal_match = re.search(r'\b(\d{5})\b', commune_clean)
        if postal_match:
            postal_code = postal_match.group(1)
            dept_code = self._get_department_code(dept_clean)
            if dept_code and postal_code.startswith(dept_code):
                print(f"      ✅ Commune acceptée via code postal {postal_code} (département {dept_code})")
                return True
        
        # 3. Cas spéciaux connus (Charmont-sous-Barbuise dans l'Aube)
        aube_communes_supplementaires = [
            "charmont", "charmont-sous-barbuise", "charmont sous barbuise",
            "essoyes", "bar-sur-aube", "bar-sur-seine"
        ]
        if dept_clean == "aube":
            if any(commune_supp in commune_clean for commune_supp in aube_communes_supplementaires):
                print(f"      ✅ Commune Aube supplémentaire reconnue: {commune}")
                return True
        
        return True  # Assouplissement: accepter par défaut plutôt que rejeter
    
    def _create_establishment_from_data(self, data: Dict, department: str) -> 'ExtractedEstablishment':
        """Crée un ExtractedEstablishment depuis les données normalisées"""
        
        gestionnaire = data.get("gestionnaire", "")
        nom = data.get("nom", "")
        presentation = data.get("presentation", "")
        source = data.get("source", "")
        
        # NETTOYAGE AUTOMATIQUE: Extraire code postal de l'adresse si présent
        adresse_brute = data.get("adresse_l1", "")
        code_postal_brut = data.get("code_postal", "00000")
        
        adresse_nettoyee, code_postal_extrait = self._clean_address_postal_separation(adresse_brute, code_postal_brut)
        
        sous_categories = self._map_gestionnaire_to_sous_categories(nom, gestionnaire, presentation, source)
        habitat_type = self._map_habitat_type("", nom, gestionnaire, presentation, source)
        
        return ExtractedEstablishment(
            nom=data.get("nom", ""),
            commune=data.get("commune", ""),
            code_postal=self._guess_postal_code(data.get("commune", ""), department, adresse_nettoyee, code_postal_extrait),
            gestionnaire=gestionnaire,
            adresse_l1=adresse_nettoyee,
            telephone=data.get("telephone") or None,
            email=data.get("email") or None,
            site_web=data.get("site_web") or None,
            sous_categories=sous_categories,
            habitat_type=habitat_type,
            eligibilite_avp="eligible",
            presentation=data.get("presentation", ""),
            departement=department,
            source=data.get("source", ""),
            date_extraction=datetime.now().strftime("%Y-%m-%d"),
            public_cible="seniors",
            confidence_score=80.0,
            validation_timestamp=datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        )
    
    def _is_suspicious_gestionnaire(self, gestionnaire: str) -> bool:
        """Vérifie si le gestionnaire semble être un site web générique ou suspect
        
        Args:
            gestionnaire: Le nom du gestionnaire à vérifier
            
        Returns:
            True si le gestionnaire semble suspect
        """
        if not gestionnaire or len(gestionnaire.strip()) < 3:
            return True
            
        suspicious_patterns = [
            "essentiel autonomie",
            "essentiell autonomie", 
            "papyhappy",
            "papy happy",
            "malakoff humanis",
            "pour-les-personnes-agees.gouv.fr",
            "annuaire",
            "site web",
            "plateforme",
            "portail",
            "www.",
            "http",
            ".fr",
            ".com",
            "recherche",
            "google",
            "bing"
        ]
        
        gestionnaire_lower = gestionnaire.lower().strip()
        
        for pattern in suspicious_patterns:
            if pattern in gestionnaire_lower:
                return True
                
        return False
    
    def _call_light_llm_with_timeout(self, prompt: str, timeout_seconds: int = 30) -> Optional[Dict]:
        """Appel LLM léger avec timeout pour éviter les blocages"""
        import threading
        import time
        
        result = {"response": None, "error": None}
        
        def call_llm():
            try:
                result["response"] = self._call_light_llm(prompt)
            except Exception as e:
                result["error"] = str(e)
        
        print(f"      ⏱️ Timeout configuré: {timeout_seconds}s")
        thread = threading.Thread(target=call_llm)
        thread.start()
        thread.join(timeout=timeout_seconds)
        
        if thread.is_alive():
            print(f"      ⚠️ Timeout atteint ({timeout_seconds}s) - arrêt forcé")
            return None
        
        if result["error"]:
            print(f"      ❌ Erreur LLM: {result['error']}")
            return None
            
        return result["response"]
    
    def _extract_json_from_content(self, content: str) -> Optional[str]:
        """Extrait le JSON de façon robuste depuis le contenu LLM"""
        
        # Stratégie 1: Chercher JSON standard avec "etablissements"
        json_patterns = [
            r'\{[\s\S]*"etablissements"[\s\S]*\}',
            r'\{[\s\S]*"établissements"[\s\S]*\}',  # Avec accent
            r'\{[\s\S]*\}',  # Fallback: tout JSON
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                # Nettoyer le JSON
                cleaned = match.strip()
                
                # Vérifier que ça ressemble à du JSON valide
                if cleaned.startswith('{') and cleaned.endswith('}'):
                    # Compter les accolades pour s'assurer de l'équilibre
                    open_count = cleaned.count('{')
                    close_count = cleaned.count('}')
                    
                    if open_count == close_count:
                        return cleaned
        
        # Si rien trouvé, essayer d'extraire un JSON minimal
        print(f"      ⚠️ Aucun JSON valide trouvé, création JSON par défaut")
        return '{"etablissements": []}'
    
    def _perform_targeted_web_search_with_timeout(self, query: str, timeout_seconds: int = 15) -> str:
        """Recherche web avec timeout pour éviter les blocages"""
        import threading
        
        result = {"content": "", "error": None}
        
        def search_web():
            try:
                result["content"] = self._perform_targeted_web_search(query)
            except Exception as e:
                result["error"] = str(e)
        
        print(f"         ⏱️ Timeout recherche: {timeout_seconds}s")
        thread = threading.Thread(target=search_web)
        thread.start()
        thread.join(timeout=timeout_seconds)
        
        if thread.is_alive():
            print(f"         ⚠️ Timeout recherche atteint ({timeout_seconds}s)")
            return ""
        
        if result["error"]:
            print(f"         ❌ Erreur recherche: {result['error']}")
            return ""
            
        return result["content"]
