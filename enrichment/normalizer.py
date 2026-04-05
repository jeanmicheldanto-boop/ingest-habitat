"""
Normalisation des données d'établissements
"""
from typing import Optional, List
import re


class DataNormalizer:
    """Classe de normalisation centralisée pour toutes les données"""
    
    # Mappings des sous-catégories
    SOUS_CATEGORIES_MAPPING = {
        "residence autonomie": "résidence autonomie",
        "résidence autonomie": "résidence autonomie",
        "residence services seniors": "résidence services seniors",
        "résidence services seniors": "résidence services seniors",
        "résidence service seniors": "résidence services seniors",
        "residence_seniors": "résidence services seniors",
        "residence seniors": "résidence services seniors",
        "marpa": "MARPA",
        "habitat inclusif": "habitat inclusif",
        "colocation avec services": "colocation avec services",
        "habitat intergénérationnel": "habitat intergénérationnel",
        "habitat intergenerationnel": "habitat intergénérationnel",
        "accueil familial": "accueil familial",
        "maison d'accueil familial": "maison d'accueil familial",
        "maison d'accueil familial": "maison d'accueil familial",
        "béguinage": "béguinage",
        "beguinage": "béguinage",
        "village seniors": "village seniors",
        "village séniors": "village seniors",
        "habitat regroupé": "habitat regroupé",
        "habitat alternatif": "habitat alternatif"
    }
    
    # Mapping public cible
    PUBLIC_CIBLE_MAPPING = {
        "personnes âgées": "personnes_agees",
        "personnes_agees": "personnes_agees",
        "seniors": "personnes_agees",
        "personnes handicapées": "personnes_handicapees",
        "personnes_handicapees": "personnes_handicapees",
        "handicap": "personnes_handicapees",
        "alzheimer": "alzheimer_accessible",
        "alzheimer_accessible": "alzheimer_accessible",
        "mixte": "mixtes",
        "mixtes": "mixtes",
        "intergénérationnel": "mixtes",
        "intergenerationnel": "mixtes"
    }
    
    def normalize_phone(self, phone: str) -> Optional[str]:
        """
        Normaliser un numéro de téléphone français
        Format de sortie: 01 23 45 67 89
        """
        if not phone or pd.isna(phone):
            return None
        
        # Extraire les chiffres
        digits = re.sub(r'\D', '', str(phone))
        
        # Traiter le format international
        if digits.startswith('33') and len(digits) == 11:
            digits = '0' + digits[2:]
        
        # Vérifier le format français
        if len(digits) == 10 and digits.startswith('0'):
            return f"{digits[:2]} {digits[2:4]} {digits[4:6]} {digits[6:8]} {digits[8:10]}"
        
        return None
    
    def normalize_email(self, email: str) -> Optional[str]:
        """
        Valider et normaliser un email
        """
        if not email or pd.isna(email):
            return None
        
        email = str(email).strip().lower()
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if re.match(pattern, email):
            return email
        return None
    
    def normalize_sous_categorie(self, value: str) -> Optional[str]:
        """
        Normaliser une sous-catégorie selon le mapping autorisé
        """
        if not value:
            return None
        
        normalized = str(value).strip().lower()
        result = self.SOUS_CATEGORIES_MAPPING.get(normalized)
        
        # Règle spéciale : MARPA prime sur résidence autonomie
        if "marpa" in normalized and "résidence autonomie" in normalized:
            return "MARPA"
        
        # Si pas trouvé, fallback vers habitat alternatif
        if not result:
            return "habitat alternatif"
        
        return result
    
    def normalize_public_cible(self, value: str) -> List[str]:
        """
        Normaliser public_cible en liste de valeurs autorisées
        """
        if not value:
            return []
        
        # Gérer les valeurs multiples séparées par des virgules
        values = [v.strip().lower() for v in str(value).split(',')]
        normalized = []
        
        for v in values:
            mapped = self.PUBLIC_CIBLE_MAPPING.get(v)
            if mapped and mapped not in normalized:
                normalized.append(mapped)
        
        return normalized
    
    def normalize_code_postal(self, code_postal: str) -> Optional[str]:
        """
        Normaliser un code postal français (5 chiffres)
        """
        if not code_postal:
            return None
        
        # Extraire les chiffres
        digits = re.sub(r'\D', '', str(code_postal))
        
        # Prendre les 5 premiers chiffres
        if len(digits) >= 5:
            return digits[:5]
        
        return None
    
    def clean_text(self, value: any) -> Optional[str]:
        """
        Nettoyer et normaliser un texte
        """
        if value is None or pd.isna(value):
            return None
        text = str(value).strip()
        return text if text else None


# Pour compatibilité
def normalize_phone_fr(phone: str) -> Optional[str]:
    """Fonction helper pour normalisation téléphone"""
    normalizer = DataNormalizer()
    return normalizer.normalize_phone(phone)


def normalize_email(email: str) -> Optional[str]:
    """Fonction helper pour normalisation email"""
    normalizer = DataNormalizer()
    return normalizer.normalize_email(email)


def clean_text(value: any) -> Optional[str]:
    """Fonction helper pour nettoyage texte"""
    normalizer = DataNormalizer()
    return normalizer.clean_text(value)


# Import pandas si disponible
try:
    import pandas as pd
except ImportError:
    # Fallback pour pd.isna
    class pd:
        @staticmethod
        def isna(value):
            return value is None or (isinstance(value, float) and value != value)
