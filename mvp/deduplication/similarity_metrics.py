"""
Module de calcul de similarité pour la déduplication
Utilise plusieurs métriques pour détecter les doublons
"""
from typing import Dict, Any
import re
from difflib import SequenceMatcher


class SimilarityMetrics:
    """Calcul de scores de similarité entre établissements"""
    
    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalise un texte pour comparaison"""
        if not text:
            return ""
        
        # Minuscules
        text = text.lower()
        
        # Retirer accents
        replacements = {
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'à': 'a', 'â': 'a', 'ä': 'a',
            'ù': 'u', 'û': 'u', 'ü': 'u',
            'ô': 'o', 'ö': 'o',
            'î': 'i', 'ï': 'i',
            'ç': 'c'
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Retirer ponctuation et normaliser espaces
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    @staticmethod
    def levenshtein_distance(s1: str, s2: str) -> int:
        """Calcule la distance de Levenshtein entre deux chaînes"""
        if len(s1) < len(s2):
            return SimilarityMetrics.levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                # Coût d'insertion, suppression, substitution
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    @staticmethod
    def calculate_name_similarity(name1: str, name2: str) -> float:
        """
        Calcule la similarité entre deux noms
        Retourne un score de 0 à 100
        """
        if not name1 or not name2:
            return 0.0
        
        # Normalisation
        norm1 = SimilarityMetrics.normalize_text(name1)
        norm2 = SimilarityMetrics.normalize_text(name2)
        
        # Exact match après normalisation
        if norm1 == norm2:
            return 100.0
        
        # Calcul de similarité avec SequenceMatcher
        seq_ratio = SequenceMatcher(None, norm1, norm2).ratio()
        
        # Distance de Levenshtein
        lev_dist = SimilarityMetrics.levenshtein_distance(norm1, norm2)
        max_len = max(len(norm1), len(norm2))
        lev_ratio = 1 - (lev_dist / max_len) if max_len > 0 else 0
        
        # Moyenne pondérée
        similarity = (seq_ratio * 0.6 + lev_ratio * 0.4) * 100
        
        return round(similarity, 2)
    
    @staticmethod
    def calculate_location_similarity(commune1: str, commune2: str) -> float:
        """
        Calcule la similarité de localisation
        Retourne un score de 0 à 100
        """
        if not commune1 or not commune2:
            return 0.0
        
        norm1 = SimilarityMetrics.normalize_text(commune1)
        norm2 = SimilarityMetrics.normalize_text(commune2)
        
        # Exact match
        if norm1 == norm2:
            return 100.0
        
        # Vérifier si l'une contient l'autre (ex: "Charmont sous Barbuise" vs "Charmont-sous-Barbuise")
        if norm1 in norm2 or norm2 in norm1:
            return 90.0
        
        # Calcul de similarité standard
        ratio = SequenceMatcher(None, norm1, norm2).ratio()
        return round(ratio * 100, 2)
    
    @staticmethod
    def calculate_gestionnaire_similarity(gest1: str, gest2: str) -> float:
        """
        Calcule la similarité de gestionnaire
        Retourne un score de 0 à 100
        """
        if not gest1 or not gest2:
            return 0.0
        
        norm1 = SimilarityMetrics.normalize_text(gest1)
        norm2 = SimilarityMetrics.normalize_text(gest2)
        
        # Exact match
        if norm1 == norm2:
            return 100.0
        
        # Vérifier variantes communes (Ages & Vie, Age & Vie, Ages et Vie)
        if "age" in norm1 and "vie" in norm1 and "age" in norm2 and "vie" in norm2:
            return 95.0
        
        # Vérifier si l'une contient l'autre
        if norm1 in norm2 or norm2 in norm1:
            return 85.0
        
        ratio = SequenceMatcher(None, norm1, norm2).ratio()
        return round(ratio * 100, 2)
    
    @staticmethod
    def calculate_completeness_score(record: Dict[str, Any]) -> float:
        """
        Calcule le score de complétude d'un enregistrement
        Plus l'enregistrement est complet, plus le score est élevé
        """
        important_fields = [
            'nom', 'commune', 'code_postal', 'gestionnaire', 
            'adresse_l1', 'telephone', 'email', 'site_web', 
            'presentation'
        ]
        
        score = 0
        total_weight = 0
        
        # Poids par champ (certains sont plus importants)
        weights = {
            'nom': 10,
            'commune': 10,
            'code_postal': 8,
            'gestionnaire': 9,
            'adresse_l1': 15,
            'telephone': 12,
            'email': 12,
            'site_web': 8,
            'presentation': 10
        }
        
        for field in important_fields:
            weight = weights.get(field, 5)
            total_weight += weight
            
            value = record.get(field, '')
            if value and str(value).strip() and str(value).strip() != '':
                # Bonus si le champ est long (plus d'info)
                length_bonus = min(len(str(value)) / 50, 1.0)
                score += weight * (0.7 + 0.3 * length_bonus)
        
        return round((score / total_weight) * 100, 2) if total_weight > 0 else 0.0
    
    @staticmethod
    def calculate_overall_similarity(record1: Dict[str, Any], record2: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calcule la similarité globale entre deux enregistrements
        Retourne un dictionnaire avec le score et les détails
        """
        # Scores individuels
        name_sim = SimilarityMetrics.calculate_name_similarity(
            record1.get('nom', ''), 
            record2.get('nom', '')
        )
        
        location_sim = SimilarityMetrics.calculate_location_similarity(
            record1.get('commune', ''), 
            record2.get('commune', '')
        )
        
        gestionnaire_sim = SimilarityMetrics.calculate_gestionnaire_similarity(
            record1.get('gestionnaire', ''), 
            record2.get('gestionnaire', '')
        )
        
        # Contact similarity
        contact_sim = 0.0
        if record1.get('telephone') and record2.get('telephone'):
            if record1['telephone'] == record2['telephone']:
                contact_sim = 100.0
        
        if record1.get('email') and record2.get('email'):
            if record1['email'] == record2['email']:
                contact_sim = max(contact_sim, 100.0)
        
        # Calcul du score global pondéré
        # Nom et localisation sont les plus importants
        weights = {
            'name': 0.35,
            'location': 0.30,
            'gestionnaire': 0.20,
            'contact': 0.15
        }
        
        overall_score = (
            name_sim * weights['name'] +
            location_sim * weights['location'] +
            gestionnaire_sim * weights['gestionnaire'] +
            contact_sim * weights['contact']
        )
        
        return {
            'overall_score': round(overall_score, 2),
            'name_similarity': name_sim,
            'location_similarity': location_sim,
            'gestionnaire_similarity': gestionnaire_sim,
            'contact_similarity': contact_sim,
            'details': {
                'name1': record1.get('nom', ''),
                'name2': record2.get('nom', ''),
                'commune1': record1.get('commune', ''),
                'commune2': record2.get('commune', ''),
                'gestionnaire1': record1.get('gestionnaire', ''),
                'gestionnaire2': record2.get('gestionnaire', '')
            }
        }
