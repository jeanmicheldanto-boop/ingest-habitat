"""
Module 5 - Post-traitement et normalisation stricte
Validation finale et correction des données extraites par Module 4 V2
"""

from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
from datetime import datetime
import re

@dataclass
class NormalizedEstablishment:
    """Établissement après post-traitement Module 5"""
    nom: str
    commune: str
    code_postal: str
    gestionnaire: str
    adresse_l1: str
    telephone: Optional[str]
    email: Optional[str]
    site_web: str
    sous_categories: str
    habitat_type: str
    eligibilite_avp: str
    presentation: str
    departement: str
    source: str
    date_extraction: str
    public_cible: str
    validation_status: str  # "valid", "corrected", "rejected"
    correction_log: str     # Log des corrections appliquées

class PostProcessor:
    """Post-traitement et normalisation stricte des données extraites"""
    
    def __init__(self):
        # Catégories validées EXACTES
        self.sous_categories_valides = [
            "Résidence autonomie", "MARPA", "Résidence services seniors",
            "Béguinage", "Village seniors", "Colocation avec services",
            "Habitat inclusif", "Accueil familial", "Maison d'accueil familial",
            "Habitat intergénérationnel", "Habitat regroupé"
        ]
        
        # Mapping habitat_type EXACT
        self.habitat_type_mapping = {
            "Résidence autonomie": "residence",
            "MARPA": "residence", 
            "Résidence services seniors": "residence",
            "Habitat inclusif": "habitat_partage",
            "Accueil familial": "habitat_partage",
            "Maison d'accueil familial": "habitat_partage",
            "Habitat intergénérationnel": "habitat_partage",
            "Béguinage": "logement_independant",
            "Village seniors": "logement_independant",
            "Habitat regroupé": "logement_independant",
            "Colocation avec services": "logement_independant"
        }
        
        # Règles AVP par défaut strictes
        self.avp_rules = {
            "a_verifier": ["Habitat inclusif", "Habitat intergénérationnel", "Accueil familial", 
                          "Maison d'accueil familial", "Colocation avec services", "Habitat regroupé", "Village seniors"],
            "non_eligible": ["Résidence services seniors", "Résidence autonomie", "MARPA", "Béguinage"]
        }
    
    def process_establishments(self, establishments: List) -> List[NormalizedEstablishment]:
        """
        Post-traitement complet des établissements extraits
        
        Args:
            establishments: Liste des ExtractedEstablishmentV2 du Module 4
            
        Returns:
            Liste des établissements normalisés et validés
        """
        print(f"\n🔧 === MODULE 5 - POST-TRAITEMENT ({len(establishments)} établissements) ===")
        
        normalized_establishments = []
        
        for i, est in enumerate(establishments, 1):
            print(f"\n📋 {i:02d}/{len(establishments)}: {est.nom[:50]}...")
            
            # Validation et correction
            normalized_est, corrections = self._normalize_establishment(est)
            
            if normalized_est:
                normalized_establishments.append(normalized_est)
                status = "✅" if corrections == "aucune" else "🔧"
                print(f"   {status} Validé - Corrections: {corrections}")
            else:
                print(f"   ❌ Rejeté - Données insuffisantes")
        
        print(f"\n📊 Résultats post-traitement:")
        print(f"   • Établissements validés: {len(normalized_establishments)}/{len(establishments)}")
        
        return normalized_establishments
    
    def _normalize_establishment(self, est) -> tuple[Optional[NormalizedEstablishment], str]:
        """Normalise et valide un établissement"""
        
        corrections = []
        
        # 1. Validation champs obligatoires
        if not est.nom or not est.nom.strip():
            return None, "nom manquant"
        
        # 2. Normalisation sous_categories STRICTE
        original_sous_cat = est.sous_categories
        normalized_sous_cat = self._normalize_sous_categories_strict(est.sous_categories)
        
        if not normalized_sous_cat:
            return None, f"sous_categories invalide: '{original_sous_cat}'"
        
        if normalized_sous_cat != original_sous_cat:
            corrections.append(f"sous_categories: '{original_sous_cat}' → '{normalized_sous_cat}'")
        
        # 3. Correction habitat_type
        correct_habitat_type = self.habitat_type_mapping.get(normalized_sous_cat)
        if not correct_habitat_type:
            return None, f"mapping habitat_type introuvable pour '{normalized_sous_cat}'"
        
        if est.habitat_type != correct_habitat_type:
            corrections.append(f"habitat_type: '{est.habitat_type}' → '{correct_habitat_type}'")
        
        # 4. Correction eligibilite_avp avec règles par défaut
        correct_avp = self._correct_eligibilite_avp(est.presentation, normalized_sous_cat)
        if est.eligibilite_avp != correct_avp:
            corrections.append(f"eligibilite_avp: '{est.eligibilite_avp}' → '{correct_avp}'")
        
        # 5. Validation format email
        normalized_email = self._validate_email(est.email)
        if normalized_email != est.email:
            corrections.append(f"email: '{est.email}' → '{normalized_email}'")
        
        # 6. Validation format téléphone
        normalized_phone = self._validate_phone(est.telephone)
        if normalized_phone != est.telephone:
            corrections.append(f"telephone: '{est.telephone}' → '{normalized_phone}'")
        
        # 7. Validation code postal
        normalized_cp = self._validate_code_postal(est.code_postal)
        if normalized_cp != est.code_postal:
            corrections.append(f"code_postal: '{est.code_postal}' → '{normalized_cp}'")
        
        # 8. Validation présentation (longueur)
        normalized_presentation = self._validate_presentation(est.presentation)
        if len(normalized_presentation) != len(est.presentation):
            corrections.append(f"presentation: tronquée à {len(normalized_presentation)} caractères")
        
        # Création établissement normalisé
        normalized_est = NormalizedEstablishment(
            nom=est.nom.strip(),
            commune=est.commune.strip() if est.commune else "",
            code_postal=normalized_cp,
            gestionnaire=est.gestionnaire.strip() if est.gestionnaire else "",
            adresse_l1=est.adresse_l1.strip() if est.adresse_l1 else "",
            telephone=normalized_phone,
            email=normalized_email,
            site_web=est.site_web or "",
            sous_categories=normalized_sous_cat,
            habitat_type=correct_habitat_type,
            eligibilite_avp=correct_avp,
            presentation=normalized_presentation,
            departement=est.departement,
            source=est.source,
            date_extraction=est.date_extraction,
            public_cible=est.public_cible,
            validation_status="corrected" if corrections else "valid",
            correction_log=" | ".join(corrections) if corrections else "aucune"
        )
        
        return normalized_est, normalized_est.correction_log
    
    def _normalize_sous_categories_strict(self, sous_cat: str) -> Optional[str]:
        """Normalisation STRICTE sous-catégorie"""
        
        if not sous_cat:
            return None
        
        sous_cat_clean = sous_cat.strip()
        
        # Vérification exacte d'abord
        if sous_cat_clean in self.sous_categories_valides:
            return sous_cat_clean
        
        # Normalisation synonymes UNIQUEMENT
        sous_cat_lower = sous_cat_clean.lower()
        
        # Mapping strict selon prompt utilisateur
        synonyms_mapping = {
            "foyer logement": "Résidence autonomie",
            "foyer-logement": "Résidence autonomie", 
            "foyer de personnes âgées": "Résidence autonomie",
            "marpa": "MARPA",
            "résidence services": "Résidence services seniors",
            "résidence service": "Résidence services seniors",
            "béguinage": "Béguinage",
            "village seniors": "Village seniors",
            "village de retraités": "Village seniors",
            "colocation": "Colocation avec services",
            "ages & vie": "Colocation avec services",
            "habitat inclusif": "Habitat inclusif",
            "accueil familial": "Accueil familial",
            "placement familial": "Accueil familial",
            "maison d'accueil familial": "Maison d'accueil familial",
            "maf": "Maison d'accueil familial",
            "cette famille": "Maison d'accueil familial",
            "intergénérationnel": "Habitat intergénérationnel",
            "logement adapté": "Habitat regroupé",
            "logement pmr": "Habitat regroupé",
            "résidence autonomie": "Résidence autonomie"
        }
        
        for synonym, standard in synonyms_mapping.items():
            if synonym in sous_cat_lower:
                return standard
        
        # REJET si pas de correspondance
        return None
    
    def _correct_eligibilite_avp(self, presentation: str, sous_categorie: str) -> str:
        """Correction eligibilite_avp avec règles par défaut strictes"""
        
        # 1. Vérification mention explicite AVP
        text_lower = (presentation or "").lower()
        if any(term in text_lower for term in ["aide à la vie partagée", "avp", "aide a la vie partagée"]):
            return "avp_eligible"
        
        # 2. Règles PAR DÉFAUT selon catégorie (prompt utilisateur)
        for status, categories in self.avp_rules.items():
            if sous_categorie in categories:
                return status
        
        # Fallback
        return "a_verifier"
    
    def _validate_email(self, email: str) -> Optional[str]:
        """Validation format email"""
        if not email or not email.strip():
            return None
        
        email_clean = email.strip().lower()
        
        # Regex email simple
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if re.match(email_pattern, email_clean):
            return email_clean
        else:
            return None
    
    def _validate_phone(self, phone: str) -> Optional[str]:
        """Validation format téléphone français"""
        if not phone or not phone.strip():
            return None
        
        phone_clean = re.sub(r'[^\d+]', '', phone.strip())
        
        # Formats acceptés: 0X XX XX XX XX ou +33 X XX XX XX XX
        if re.match(r'^0[1-9]\d{8}$', phone_clean):
            # Format: 0XXXXXXXXX → 0X XX XX XX XX
            return f"{phone_clean[:2]} {phone_clean[2:4]} {phone_clean[4:6]} {phone_clean[6:8]} {phone_clean[8:]}"
        elif re.match(r'^\+33[1-9]\d{8}$', phone_clean):
            # Format international
            return phone_clean
        else:
            return None
    
    def _validate_code_postal(self, cp: str) -> str:
        """Validation code postal 5 chiffres"""
        if not cp:
            return ""
        
        cp_clean = re.sub(r'\D', '', cp.strip())
        
        if len(cp_clean) == 5 and cp_clean.isdigit():
            return cp_clean
        else:
            return ""
    
    def _validate_presentation(self, presentation: str) -> str:
        """Validation présentation (max 500 caractères)"""
        if not presentation:
            return ""
        
        presentation_clean = presentation.strip()
        
        # Tronquer si trop long
        if len(presentation_clean) > 500:
            return presentation_clean[:500].rstrip() + "..."
        
        return presentation_clean
    
    def export_to_csv(self, establishments: List[NormalizedEstablishment], filename: str = None) -> str:
        """Export CSV final normalisé"""
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"habitat_seniors_normalized_{timestamp}.csv"
        
        # Ordre des colonnes EXACT du prompt utilisateur
        fieldnames = [
            "nom", "commune", "code_postal", "gestionnaire", "adresse_l1",
            "telephone", "email", "site_web", "sous_categories", "habitat_type",
            "eligibilite_avp", "presentation", "departement", "source",
            "date_extraction", "public_cible"
        ]
        
        import csv
        
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
                    "site_web": est.site_web,
                    "sous_categories": est.sous_categories,
                    "habitat_type": est.habitat_type,
                    "eligibilite_avp": est.eligibilite_avp,
                    "presentation": est.presentation,
                    "departement": est.departement,
                    "source": est.source,
                    "date_extraction": est.date_extraction,
                    "public_cible": est.public_cible
                })
        
        print(f"📄 CSV normalisé exporté: {filename}")
        return filename