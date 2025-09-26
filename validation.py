import pandas as pd
import re
from typing import Dict, List, Any
import streamlit as st
from config import REQUIRED_FIELDS, RECOMMENDED_FIELDS

class DataValidator:
    """Validateur pour les données d'établissements"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.required_fields = REQUIRED_FIELDS
        self.recommended_fields = RECOMMENDED_FIELDS
    
    def validate_record(self, record: Dict[str, Any], row_index: int) -> Dict[str, Any]:
        """Valide un enregistrement complet"""
        self.errors = []
        self.warnings = []
        
        # Validation des champs obligatoires
        missing_required = []
        for field in self.required_fields:
            if not record.get(field) or (isinstance(record.get(field), str) and not record.get(field).strip()):
                missing_required.append(field)
        
        if missing_required:
            self.errors.append(f"Ligne {row_index + 1}: Champs obligatoires manquants: {', '.join(missing_required)}")
        
        # Validation des champs recommandés
        missing_recommended = []
        for field in self.recommended_fields:
            if not record.get(field) or (isinstance(record.get(field), str) and not record.get(field).strip()):
                missing_recommended.append(field)
        
        if missing_recommended:
            self.warnings.append(f"Ligne {row_index + 1}: Champs recommandés manquants: {', '.join(missing_recommended)}")
        
        # Validations spécifiques
        self._validate_email(record.get('email'), row_index)
        self._validate_phone(record.get('telephone'), row_index)
        self._validate_postal_code(record.get('code_postal'), row_index)
        self._validate_url(record.get('site_web'), row_index)
        
        # Calcul du score de qualité
        total_fields = len(self.required_fields) + len(self.recommended_fields)
        filled_fields = total_fields - len(missing_required) - len(missing_recommended)
        quality_score = (filled_fields / total_fields) * 100 if total_fields > 0 else 0
        
        # Réduire le score en cas d'erreurs
        if self.errors:
            quality_score *= 0.5  # Réduction drastique pour les erreurs
        elif self.warnings:
            quality_score *= 0.8  # Réduction modérée pour les avertissements
        
        return {
            'valid': len(self.errors) == 0,
            'score': quality_score,
            'errors': self.errors.copy(),
            'warnings': self.warnings.copy()
        }
    
    def _validate_email(self, email: str, row_index: int):
        """Valide l'adresse email"""
        if not email:
            return  # Email optionnel
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        if not re.match(email_pattern, email):
            self.errors.append(f"Ligne {row_index + 1}: Format email invalide: {email}")
        
        # Vérifier les domaines suspects
        suspicious_domains = ['test.com', 'example.com', 'temp.com']
        domain = email.split('@')[1] if '@' in email else ''
        if domain.lower() in suspicious_domains:
            self.warnings.append(f"Ligne {row_index + 1}: Domaine email suspect: {domain}")
    
    def _validate_phone(self, phone: str, row_index: int):
        """Valide le format du téléphone"""
        if not phone:
            return  # Téléphone optionnel
        
        # Nettoyer le numéro
        clean_phone = re.sub(r'[^\d+]', '', str(phone))
        
        # Patterns pour les numéros français
        french_patterns = [
            r'^0[1-9](\d{8})$',  # 0X XX XX XX XX
            r'^\+33[1-9](\d{8})$',  # +33 X XX XX XX XX
            r'^(?:0033)[1-9](\d{8})$'  # 0033 X XX XX XX XX
        ]
        
        is_valid = any(re.match(pattern, clean_phone) for pattern in french_patterns)
        
        if not is_valid:
            # Vérifier si c'est un numéro court (numéro vert, etc.)
            if len(clean_phone) == 4 and clean_phone.startswith('0'):
                self.warnings.append(f"Ligne {row_index + 1}: Numéro court détecté: {phone}")
            else:
                self.errors.append(f"Ligne {row_index + 1}: Format de téléphone invalide: {phone}")
    
    def _validate_postal_code(self, postal_code, row_index: int):
        """Valide le code postal français"""
        if not postal_code:
            return
        
        # Convertir en string si nécessaire
        postal_code_str = str(postal_code).strip()
        
        # Code postal français: 5 chiffres
        if not re.match(r'^\d{5}$', postal_code_str):
            self.errors.append(f"Ligne {row_index + 1}: Code postal invalide (doit être 5 chiffres): {postal_code_str}")
            return
        
        # Vérifier les codes postaux Corse et DOM-TOM
        special_codes = ['2A', '2B', '97', '98']
        if postal_code_str[:2] not in [str(i).zfill(2) for i in range(1, 96)] + special_codes:
            self.warnings.append(f"Ligne {row_index + 1}: Code postal inhabituel: {postal_code_str}")
    
    def _validate_url(self, url: str, row_index: int):
        """Valide l'URL du site web"""
        if not url:
            return
        
        url_pattern = r'^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)$'
        
        if not re.match(url_pattern, url):
            self.errors.append(f"Ligne {row_index + 1}: URL invalide: {url}")
        
    def get_validation_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Génère un résumé de validation pour tout le DataFrame"""
        summary = {
            'total_records': len(df),
            'valid_records': 0,
            'records_with_warnings': 0,
            'records_with_errors': 0,
            'common_issues': [],
            'quality_distribution': {'excellent': 0, 'good': 0, 'average': 0, 'poor': 0}
        }
        
        all_errors = []
        all_warnings = []
        
        for idx, row in df.iterrows():
            record_data = row.to_dict()
            validation_result = self.validate_record(record_data, idx)
            
            if validation_result['valid']:
                summary['valid_records'] += 1
            else:
                summary['records_with_errors'] += 1
            
            if validation_result['warnings']:
                summary['records_with_warnings'] += 1
            
            all_errors.extend(validation_result['errors'])
            all_warnings.extend(validation_result['warnings'])
            
            # Distribution qualité
            score = validation_result['score']
            if score >= 90:
                summary['quality_distribution']['excellent'] += 1
            elif score >= 75:
                summary['quality_distribution']['good'] += 1
            elif score >= 50:
                summary['quality_distribution']['average'] += 1
            else:
                summary['quality_distribution']['poor'] += 1
        
        # Analyser les problèmes les plus fréquents
        error_types = {}
        for error in all_errors:
            error_type = error.split(':')[1].strip() if ':' in error else error
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        summary['common_issues'] = sorted(error_types.items(), key=lambda x: x[1], reverse=True)[:5]
        
        return summary