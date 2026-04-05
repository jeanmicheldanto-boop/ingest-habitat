import pandas as pd
import numpy as np
import streamlit as st
import chardet
import re
from typing import Dict, List, Tuple, Any
from config import (COLUMN_MAPPING, HABITAT_TYPE_MAPPING, REQUIRED_FIELDS, 
                   RECOMMENDED_FIELDS, VALID_SOUS_CATEGORIES, normalize_sous_categorie, 
                   normalize_public_cible)

class DataProcessor:
    """Processeur pour l'analyse et le traitement des données CSV"""
    
    def __init__(self):
        self.df = None
        self.original_columns = []
        self.mapped_columns = {}
        self.validation_results = {}
        self.excluded_records = set()  # Enregistrements marqués comme à exclure
    
    def load_csv(self, uploaded_file) -> Tuple[bool, str]:
        """Charge un fichier CSV avec détection automatique de l'encodage"""
        try:
            # Détection de l'encodage
            raw_data = uploaded_file.read()
            encoding_result = chardet.detect(raw_data)
            encoding = encoding_result.get('encoding', 'utf-8')
            
            # Rembobiner le fichier
            uploaded_file.seek(0)
            
            # Lecture du CSV
            self.df = pd.read_csv(uploaded_file, encoding=encoding)
            self.original_columns = list(self.df.columns)
            
            return True, f"✅ Fichier chargé avec succès ({len(self.df)} lignes, encodage: {encoding})"
            
        except Exception as e:
            return False, f"❌ Erreur lors du chargement : {str(e)}"
    
    def detect_column_mapping(self) -> Dict[str, str]:
        """Détecte automatiquement le mapping des colonnes"""
        mapping = {}
        
        for target_col, possible_names in COLUMN_MAPPING.items():
            for original_col in self.original_columns:
                original_lower = original_col.lower().strip()
                for possible_name in possible_names:
                    if possible_name.lower() in original_lower:
                        mapping[target_col] = original_col
                        break
                if target_col in mapping:
                    break
        
        self.mapped_columns = mapping
        return mapping
    
    def validate_data(self) -> Dict[str, Any]:
        """Valide les données et identifie les problèmes"""
        if self.df is None:
            return {'error': 'Aucune donnée chargée'}
        
        results = {
            'total_rows': len(self.df),
            'missing_required': {},
            'missing_recommended': {},
            'invalid_emails': [],
            'invalid_phones': [],
            'duplicate_names': [],
            'geocoding_needed': 0,
            'warnings': []
        }
        
        # Vérifier les champs obligatoires manquants
        for field in REQUIRED_FIELDS:
            if field not in self.mapped_columns:
                results['missing_required'][field] = 'Colonne non mappée'
            else:
                col = self.mapped_columns[field]
                missing_count = self.df[col].isnull().sum() + (self.df[col] == '').sum()
                if missing_count > 0:
                    results['missing_required'][field] = f"{missing_count} valeurs manquantes"
        
        # Vérifier les champs recommandés manquants
        for field in RECOMMENDED_FIELDS:
            if field in self.mapped_columns:
                col = self.mapped_columns[field]
                missing_count = self.df[col].isnull().sum() + (self.df[col] == '').sum()
                if missing_count > 0:
                    results['missing_recommended'][field] = f"{missing_count} valeurs manquantes"
        
        # Validation des emails
        if 'email' in self.mapped_columns:
            email_col = self.mapped_columns['email']
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            invalid_emails = []
            
            for idx, email in self.df[email_col].items():
                if pd.notna(email) and email != '' and not re.match(email_pattern, str(email)):
                    invalid_emails.append((idx, email))
            
            results['invalid_emails'] = invalid_emails
        
        # Validation des téléphones (basique)
        if 'telephone' in self.mapped_columns:
            phone_col = self.mapped_columns['telephone']
            phone_pattern = r'^[\d\s\.\-\+\(\)]{10,}$'
            invalid_phones = []
            
            for idx, phone in self.df[phone_col].items():
                if pd.notna(phone) and phone != '' and not re.match(phone_pattern, str(phone).replace(' ', '')):
                    invalid_phones.append((idx, phone))
            
            results['invalid_phones'] = invalid_phones
        
        # Détection des doublons de noms
        if 'nom' in self.mapped_columns:
            nom_col = self.mapped_columns['nom']
            duplicates = self.df[self.df.duplicated(subset=[nom_col], keep=False)]
            if not duplicates.empty:
                results['duplicate_names'] = list(duplicates[nom_col].values)
        
        # Comptage des adresses nécessitant géocodage
        address_fields = ['adresse_l1', 'commune', 'code_postal']
        has_address = False
        for field in address_fields:
            if field in self.mapped_columns:
                has_address = True
                break
        
        if has_address:
            results['geocoding_needed'] = len(self.df)
        
        self.validation_results = results
        return results
    
    def clean_data(self) -> pd.DataFrame:
        """Nettoie et standardise les données"""
        if self.df is None:
            return None
        
        df_clean = self.df.copy()
        
        # Standardisation des types d'habitat
        if 'habitat_type' in self.mapped_columns:
            type_col = self.mapped_columns['habitat_type']
            df_clean[type_col] = df_clean[type_col].apply(self._map_habitat_type)
        elif 'type' in self.mapped_columns:
            # Utiliser la colonne 'type' pour deviner l'habitat_type
            type_col = self.mapped_columns['type']
            df_clean['habitat_type'] = df_clean[type_col].apply(self._map_habitat_type)
            self.mapped_columns['habitat_type'] = 'habitat_type'
        
        # Nettoyage des emails
        if 'email' in self.mapped_columns:
            email_col = self.mapped_columns['email']
            df_clean[email_col] = df_clean[email_col].str.lower().str.strip()
        
        # Nettoyage des téléphones
        if 'telephone' in self.mapped_columns:
            phone_col = self.mapped_columns['telephone']
            df_clean[phone_col] = df_clean[phone_col].apply(self._clean_phone)
        
        # Nettoyage des codes postaux
        if 'code_postal' in self.mapped_columns:
            cp_col = self.mapped_columns['code_postal']
            df_clean[cp_col] = df_clean[cp_col].apply(self._clean_postal_code)
        
        # Normalisation des sous-catégories pour compatibilité avec les CSV existants
        if 'sous_categorie' in self.mapped_columns:
            sc_col = self.mapped_columns['sous_categorie']
            df_clean[sc_col] = df_clean[sc_col].apply(normalize_sous_categorie)
        
        # Normalisation du public cible
        if 'public_cible' in self.mapped_columns:
            pc_col = self.mapped_columns['public_cible']
            df_clean[pc_col] = df_clean[pc_col].apply(normalize_public_cible)
        
        # Nettoyage des adresses - séparer l'adresse de la commune/code postal
        if 'adresse_l1' in self.mapped_columns:
            addr_col = self.mapped_columns['adresse_l1']
            df_clean[addr_col] = df_clean[addr_col].apply(self._clean_address)
        
        return df_clean
    
    def _map_habitat_type(self, value):
        """Mappe une valeur vers un type d'habitat valide"""
        if pd.isna(value) or value == '':
            return 'residence'  # Valeur par défaut
        
        value_lower = str(value).lower().strip()
        
        for habitat_type, keywords in HABITAT_TYPE_MAPPING.items():
            for keyword in keywords:
                if keyword.lower() in value_lower:
                    return habitat_type
        
        return 'residence'  # Valeur par défaut si aucune correspondance
    
    def _clean_phone(self, phone):
        """Nettoie un numéro de téléphone"""
        if pd.isna(phone) or phone == '':
            return None
        
        # Garder seulement les chiffres, espaces, points, tirets, parenthèses et +
        phone_str = re.sub(r'[^\d\s\.\-\+\(\)]', '', str(phone))
        return phone_str.strip() if phone_str.strip() else None
    
    def _clean_postal_code(self, cp):
        """Nettoie un code postal"""
        if pd.isna(cp) or cp == '':
            return None
        
        # Convertir en string et enlever les décimales (.0 par exemple)
        cp_str = str(cp).strip()
        if cp_str.endswith('.0'):
            cp_str = cp_str[:-2]
        
        return cp_str if cp_str else None
    
    def _clean_address(self, addr):
        """Nettoie une adresse en retirant la commune et le code postal s'ils sont présents"""
        if pd.isna(addr) or addr == '':
            return None
        
        addr_str = str(addr).strip()
        
        # Pattern pour détecter et supprimer "code_postal commune" à la fin
        # Ex: "12 rue de la Paix, 75001 Paris" -> "12 rue de la Paix"
        import re
        
        # Supprimer les patterns du type ", 75001 Paris" ou ", 40160 Parentis-en-Born"
        pattern = r',\s*\d{5}\s+[A-Za-z][A-Za-zÀ-ÿ\s\-\']+$'
        cleaned_addr = re.sub(pattern, '', addr_str)
        
        # Supprimer aussi les patterns sans virgule: "12 rue 75001 Paris"
        pattern2 = r'\s+\d{5}\s+[A-Za-z][A-Za-zÀ-ÿ\s\-\']+$'
        cleaned_addr = re.sub(pattern2, '', cleaned_addr)
        
        return cleaned_addr.strip() if cleaned_addr.strip() else None
    
    def get_preview_data(self, max_rows=10) -> pd.DataFrame:
        """Retourne un aperçu des données nettoyées"""
        if self.df is None:
            return pd.DataFrame()
        
        # Si nous avons un mapping, l'utiliser
        if self.mapped_columns:
            # Créer un DataFrame avec les colonnes mappées
            preview_data = {}
            for target_col, original_col in self.mapped_columns.items():
                if original_col in self.df.columns:
                    preview_data[target_col] = self.df[original_col].head(max_rows)
            return pd.DataFrame(preview_data)
        else:
            # Sinon, retourner les données brutes limitées
            return self.df.head(max_rows)
    
    def get_missing_data_summary(self) -> Dict[str, Any]:
        """Résumé des données manquantes par ligne"""
        if self.df is None or not self.mapped_columns:
            return {}
        
        summary = {}
        for idx, row in self.df.iterrows():
            missing_required = []
            missing_recommended = []
            
            # Vérifier les champs obligatoires
            for field in REQUIRED_FIELDS:
                if field in self.mapped_columns:
                    col = self.mapped_columns[field]
                    value = row[col]
                    if pd.isna(value) or str(value).strip() == '':
                        missing_required.append(field)
                else:
                    # Champ obligatoire non mappé = manquant
                    missing_required.append(field)
            
            # Vérifier les champs recommandés
            for field in RECOMMENDED_FIELDS:
                if field in self.mapped_columns:
                    col = self.mapped_columns[field]
                    value = row[col]
                    if pd.isna(value) or str(value).strip() == '':
                        missing_recommended.append(field)
            
            # Ajouter à la liste si des champs manquent
            if missing_required or missing_recommended:
                # Nom pour affichage
                nom_col = self.mapped_columns.get('nom')
                if nom_col and nom_col in self.df.columns:
                    nom = str(row[nom_col]).strip()
                    if not nom or nom == 'nan':
                        nom = f"Ligne {idx + 1}"
                else:
                    nom = f"Ligne {idx + 1}"
                
                summary[idx] = {
                    'nom': nom,
                    'missing_required': missing_required,
                    'missing_recommended': missing_recommended,
                    'row_data': row.to_dict()  # Ajouter les données de la ligne
                }
        
        return summary
    
    def exclude_record(self, index: int, reason: str = "Données obligatoires introuvables"):
        """Marque un enregistrement comme exclu de l'import"""
        self.excluded_records.add(index)
        
        # Ajouter une colonne pour marquer l'exclusion si elle n'existe pas
        if 'excluded_from_import' not in self.df.columns:
            self.df['excluded_from_import'] = False
        if 'exclusion_reason' not in self.df.columns:
            self.df['exclusion_reason'] = ""
        
        self.df.loc[index, 'excluded_from_import'] = True
        self.df.loc[index, 'exclusion_reason'] = reason
    
    def include_record(self, index: int):
        """Annule l'exclusion d'un enregistrement"""
        self.excluded_records.discard(index)
        
        if 'excluded_from_import' in self.df.columns:
            self.df.loc[index, 'excluded_from_import'] = False
        if 'exclusion_reason' in self.df.columns:
            self.df.loc[index, 'exclusion_reason'] = ""
    
    def is_excluded(self, index: int) -> bool:
        """Vérifie si un enregistrement est exclu"""
        return index in self.excluded_records
    
    def get_excluded_count(self) -> int:
        """Retourne le nombre d'enregistrements exclus"""
        return len(self.excluded_records)
    
    def get_importable_records(self) -> pd.DataFrame:
        """Retourne seulement les enregistrements qui peuvent être importés"""
        if self.df is None:
            return pd.DataFrame()
        
        # Filtrer les enregistrements non exclus
        mask = ~self.df.index.isin(self.excluded_records)
        return self.df[mask]