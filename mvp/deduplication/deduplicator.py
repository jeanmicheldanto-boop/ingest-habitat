"""
MODULE DE DÉDUPLICATION FINALE
Processus de déduplication en fin de pipeline avec sélection des meilleures données
"""

import pandas as pd
from typing import List, Dict, Set
from dataclasses import dataclass
from collections import defaultdict
import re

@dataclass
class DeduplicationResult:
    """Résultat de déduplication"""
    original_count: int
    deduplicated_count: int
    removed_count: int
    duplicates_groups: List[Dict]
    quality_report: Dict

class HabitatDeduplicator:
    """Déduplicateur intelligent pour établissements habitat seniors"""
    
    def __init__(self, similarity_threshold: float = 0.85):
        self.similarity_threshold = similarity_threshold
        
        # Mots de liaison à ignorer dans la comparaison
        self.stop_words = {
            "de", "du", "des", "le", "la", "les", "et", "ou", "pour", 
            "maison", "residence", "habitat", "village", "colocation"
        }
    
    def deduplicate_establishments(self, csv_file: str) -> DeduplicationResult:
        """Déduplique les établissements d'un fichier CSV"""
        
        print(f"\n🔍 === DÉDUPLICATION FINALE ===")
        print(f"📁 Fichier: {csv_file}")
        
        # Lecture CSV
        df = pd.read_csv(csv_file)
        original_count = len(df)
        
        print(f"📊 Établissements originaux: {original_count}")
        
        # Groupement par similarité
        duplicate_groups = self._find_duplicate_groups(df)
        
        # Sélection des meilleurs par groupe
        deduplicated_df = self._select_best_from_groups(duplicate_groups, df)
        
        # Export résultat
        deduplicated_file = csv_file.replace('.csv', '_deduplicated.csv')
        deduplicated_df.to_csv(deduplicated_file, index=False)
        
        # Rapport de qualité
        quality_report = self._generate_quality_report(df, deduplicated_df, duplicate_groups)
        
        result = DeduplicationResult(
            original_count=original_count,
            deduplicated_count=len(deduplicated_df),
            removed_count=original_count - len(deduplicated_df),
            duplicates_groups=duplicate_groups,
            quality_report=quality_report
        )
        
        self._print_deduplication_summary(result, deduplicated_file)
        
        return result
    
    def _find_duplicate_groups(self, df: pd.DataFrame) -> List[Dict]:
        """Trouve les groupes d'établissements similaires"""
        
        print(f"\n🔍 Recherche de doublons...")
        
        groups = []
        processed = set()
        
        for i, row in df.iterrows():
            if i in processed:
                continue
                
            # Groupe initial
            current_group = {
                "master_index": i,
                "duplicates": [i],
                "similarity_scores": []
            }
            
            # Recherche de similaires
            for j, other_row in df.iterrows():
                if j <= i or j in processed:
                    continue
                    
                similarity = self._calculate_similarity(row, other_row)
                
                if similarity >= self.similarity_threshold:
                    current_group["duplicates"].append(j)
                    current_group["similarity_scores"].append(similarity)
                    processed.add(j)
            
            # Ajouter seulement si doublons détectés
            if len(current_group["duplicates"]) > 1:
                groups.append(current_group)
                print(f"   📍 Groupe {len(groups)}: {len(current_group['duplicates'])} doublons - '{row['nom'][:50]}...'")
            
            processed.add(i)
        
        print(f"✅ {len(groups)} groupes de doublons détectés")
        return groups
    
    def _calculate_similarity(self, row1: pd.Series, row2: pd.Series) -> float:
        """Calcule la similarité entre deux établissements"""
        
        # Détection commune invalide (générique ou département)
        commune1 = str(row1.get('commune', '')).strip()
        commune2 = str(row2.get('commune', '')).strip()
        invalid_communes = ['Indre', 'Résidence partenaire', '', 'nan']
        
        # Si même code postal et au moins une commune invalide -> doublon probable
        cp1 = str(row1.get('code_postal', '')).strip()
        cp2 = str(row2.get('code_postal', '')).strip()
        if cp1 and cp2 and cp1 == cp2:
            if any(c in invalid_communes for c in [commune1, commune2]):
                # Vérifier si noms similaires (même commune mentionnée)
                name1_lower = row1['nom'].lower()
                name2_lower = row2['nom'].lower()
                # Extraire nom de commune du nom si présent
                for name in [name1_lower, name2_lower]:
                    # Chercher "sainte-severe", "montgivray", etc.
                    commune_in_name = None
                    if 'sainte-severe' in name or 'sainte severe' in name:
                        commune_in_name = 'sainte-severe'
                    elif 'montgivray' in name:
                        commune_in_name = 'montgivray'
                    elif 'neuvy-saint-sepulchre' in name or 'neuvy saint sepulchre' in name:
                        commune_in_name = 'neuvy-saint-sepulchre'
                    
                    if commune_in_name:
                        # Si les deux noms mentionnent la même commune -> doublon
                        if commune_in_name in name1_lower and commune_in_name in name2_lower:
                            return 0.90  # Fort doublon probable
        
        # Similarité nom (prioritaire)
        name_sim = self._name_similarity(row1['nom'], row2['nom'])
        
        # Similarité commune
        commune_sim = self._commune_similarity(commune1, commune2)
        
        # Similarité gestionnaire  
        gestionnaire_sim = self._gestionnaire_similarity(
            row1.get('gestionnaire', ''), 
            row2.get('gestionnaire', '')
        )
        
        # Score pondéré
        total_similarity = (
            name_sim * 0.6 +      # Nom = 60% du score
            commune_sim * 0.3 +   # Commune = 30%
            gestionnaire_sim * 0.1 # Gestionnaire = 10%
        )
        
        return total_similarity
    
    def _name_similarity(self, name1: str, name2: str) -> float:
        """Similarité entre noms d'établissements"""
        
        if not name1 or not name2:
            return 0.0
        
        # Normalisation
        name1_norm = self._normalize_name(name1)
        name2_norm = self._normalize_name(name2)
        
        # Comparaison exacte
        if name1_norm == name2_norm:
            return 1.0
        
        # Comparaison des mots significatifs
        words1 = set(name1_norm.split()) - self.stop_words
        words2 = set(name2_norm.split()) - self.stop_words
        
        if not words1 or not words2:
            return 0.0
        
        # Jaccard similarity
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        return intersection / union if union > 0 else 0.0
    
    def _commune_similarity(self, commune1: str, commune2: str) -> float:
        """Similarité entre communes"""
        
        if not commune1 or not commune2:
            return 0.5  # Neutre si info manquante
        
        commune1_norm = commune1.lower().strip()
        commune2_norm = commune2.lower().strip()
        
        return 1.0 if commune1_norm == commune2_norm else 0.0
    
    def _gestionnaire_similarity(self, gest1: str, gest2: str) -> float:
        """Similarité entre gestionnaires"""
        
        # Conversion sécurisée en string
        gest1_str = str(gest1) if pd.notna(gest1) else ''
        gest2_str = str(gest2) if pd.notna(gest2) else ''
        
        if not gest1_str or not gest2_str:
            return 0.5  # Neutre
        
        gest1_norm = gest1_str.lower().strip()
        gest2_norm = gest2_str.lower().strip()
        
        return 1.0 if gest1_norm == gest2_norm else 0.0
    
    def _normalize_name(self, name: str) -> str:
        """Normalise un nom d'établissement"""
        
        # Minuscules
        normalized = name.lower()
        
        # Suppression caractères spéciaux
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Suppression espaces multiples
        normalized = re.sub(r'\s+', ' ', normalized)
        
        return normalized.strip()
    
    def _select_best_from_groups(self, duplicate_groups: List[Dict], df: pd.DataFrame) -> pd.DataFrame:
        """Sélectionne le meilleur établissement par groupe de doublons"""
        
        print(f"\n🎯 Sélection des meilleures données par groupe...")
        
        # Indices à conserver
        keep_indices = set(range(len(df)))
        
        # Filtrer les établissements avec données insuffisantes
        invalid_count = 0
        for idx, row in df.iterrows():
            if self._is_invalid_establishment(row):
                keep_indices.discard(idx)
                invalid_count += 1
                print(f"   ❌ Exclu (données insuffisantes): '{row['nom'][:50]}...'")
        
        if invalid_count > 0:
            print(f"🚫 {invalid_count} établissement(s) exclu(s) pour données insuffisantes")
        
        # Pour chaque groupe de doublons
        for group in duplicate_groups:
            duplicates = group["duplicates"]
            
            # Calculer score de qualité pour chaque doublon
            quality_scores = []
            for idx in duplicates:
                score = self._calculate_quality_score(df.iloc[idx])
                quality_scores.append((idx, score))
            
            # Trier par qualité décroissante
            quality_scores.sort(key=lambda x: x[1], reverse=True)
            
            # Garder le meilleur, supprimer les autres
            best_idx = quality_scores[0][0]
            for idx, score in quality_scores[1:]:
                keep_indices.discard(idx)
                print(f"   🗑️ Supprimé: '{df.iloc[idx]['nom'][:40]}...' (score: {score:.2f}) - gardé: '{df.iloc[best_idx]['nom'][:40]}...' (score: {quality_scores[0][1]:.2f})")
        
        # Dataframe filtré
        deduplicated_df = df.iloc[list(keep_indices)].reset_index(drop=True)
        
        print(f"✅ {len(deduplicated_df)} établissements uniques conservés")
        
        return deduplicated_df
    
    def _is_invalid_establishment(self, row: pd.Series) -> bool:
        """Vérifie si un établissement a des données insuffisantes pour être conservé"""
        
        gestionnaire = str(row.get('gestionnaire', '')).strip()
        telephone = str(row.get('telephone', '')).strip()
        email = str(row.get('email', '')).strip()
        site_web = str(row.get('site_web', '')).strip()
        commune = str(row.get('commune', '')).strip()
        
        # Communes invalides (génériques ou départements)
        invalid_communes = ['Indre', 'Résidence partenaire', 'nan', 'none', '']
        has_invalid_commune = commune.lower() in [c.lower() for c in invalid_communes]
        
        # Exclure si aucun gestionnaire ET aucun moyen de contact
        has_no_manager = not gestionnaire or gestionnaire.lower() in ['nan', 'none', '']
        has_no_phone = not telephone or telephone.lower() in ['nan', 'none', '']
        has_no_email = not email or email.lower() in ['nan', 'none', '']
        has_no_website = not site_web or site_web.lower() in ['nan', 'none', '']
        
        # Vérifier si le site web est une agence immobilière (non-gestionnaire)
        is_real_estate_site = 'immobilier' in site_web.lower() if site_web else False
        
        # Vérifier si le site web est exclu (co-living-et-co-working.com)
        is_excluded_site = 'co-living-et-co-working.com' in site_web.lower() if site_web else False
        
        # Invalide si tous les champs critiques sont vides OU si site immobilier sans autres contacts
        # OU si commune invalide ET pas assez de contacts
        return (has_no_manager and has_no_phone and has_no_email and has_no_website) or \
               (has_no_manager and has_no_phone and has_no_email and is_real_estate_site) or \
               (has_invalid_commune and has_no_phone and has_no_email) or \
               is_excluded_site
    
    def _calculate_quality_score(self, row: pd.Series) -> float:
        """Calcule un score de qualité pour un établissement"""
        
        score = 0.0
        
        # Champs remplis (20 points max)
        fields_to_check = ['commune', 'gestionnaire', 'adresse_l1', 'telephone', 'email']
        filled_fields = sum(1 for field in fields_to_check if pd.notna(row.get(field)) and str(row.get(field)).strip())
        score += filled_fields * 4  # 4 points par champ
        
        # Qualité nom (30 points max)
        nom = str(row.get('nom', ''))
        if len(nom) > 10:  # Nom détaillé
            score += 30
        elif len(nom) > 5:
            score += 15
        
        # Présentation (20 points max)
        presentation = str(row.get('presentation', ''))
        if len(presentation) > 100:
            score += 20
        elif len(presentation) > 50:
            score += 10
        
        # Confidence score (30 points max)
        confidence = row.get('confidence_score', 0)
        if pd.notna(confidence):
            score += confidence * 0.3  # Conversion 0-100 → 0-30
        
        return score
    
    def _generate_quality_report(self, original_df: pd.DataFrame, deduplicated_df: pd.DataFrame, groups: List[Dict]) -> Dict:
        """Génère un rapport de qualité"""
        
        return {
            "compression_rate": len(deduplicated_df) / len(original_df),
            "duplicate_groups_count": len(groups),
            "largest_group_size": max(len(g["duplicates"]) for g in groups) if groups else 0,
            "average_group_size": sum(len(g["duplicates"]) for g in groups) / len(groups) if groups else 0,
            "fields_completion": {
                "original": self._calculate_completion_rate(original_df),
                "deduplicated": self._calculate_completion_rate(deduplicated_df)
            }
        }
    
    def _calculate_completion_rate(self, df: pd.DataFrame) -> Dict:
        """Calcule le taux de complétude des champs"""
        
        completion = {}
        for col in ['commune', 'gestionnaire', 'adresse_l1', 'telephone', 'email']:
            if col in df.columns:
                filled = df[col].notna() & (df[col].astype(str).str.strip() != '')
                completion[col] = filled.sum() / len(df) * 100
        
        return completion
    
    def _print_deduplication_summary(self, result: DeduplicationResult, output_file: str):
        """Affiche un résumé de la déduplication"""
        
        print(f"\n📊 === RÉSUMÉ DÉDUPLICATION ===")
        print(f"📥 Établissements originaux: {result.original_count}")
        print(f"📤 Établissements uniques: {result.deduplicated_count}")
        print(f"🗑️ Doublons supprimés: {result.removed_count}")
        print(f"📉 Taux de compression: {result.quality_report['compression_rate']:.1%}")
        print(f"👥 Groupes de doublons: {result.quality_report['duplicate_groups_count']}")
        print(f"📁 Fichier dédupliqué: {output_file}")
        
        if result.quality_report['duplicate_groups_count'] > 0:
            print(f"📏 Taille moyenne groupe: {result.quality_report['average_group_size']:.1f}")
            print(f"📏 Plus grand groupe: {result.quality_report['largest_group_size']}")


if __name__ == "__main__":
    """Test du déduplicateur"""
    
    dedup = HabitatDeduplicator(similarity_threshold=0.85)
    
    # Test sur fichier d'exemple
    test_file = "pipeline_345_aube_20251202_161647.csv"
    if test_file:
        result = dedup.deduplicate_establishments(test_file)
        print(f"\nDéduplication terminée - {result.removed_count} doublons supprimés")