"""
Module de déduplication intelligente avec validation LLM
Détecte et fusionne les doublons en conservant l'établissement le plus complet
"""
import os
import json
from typing import List, Dict, Any, Tuple, Optional
from groq import Groq
from .similarity_metrics import SimilarityMetrics


class IntelligentDeduplicator:
    """Déduplication intelligente multi-niveaux avec validation LLM"""
    
    def __init__(self, groq_api_key: Optional[str] = None):
        """
        Initialise le déduplicateur
        
        Args:
            groq_api_key: Clé API Groq (ou depuis .env)
        """
        self.groq_api_key = groq_api_key or os.getenv('GROQ_API_KEY')
        self.client = Groq(api_key=self.groq_api_key) if self.groq_api_key else None
        self.metrics = SimilarityMetrics()
        
        # Statistiques
        self.stats = {
            'total_records': 0,
            'duplicate_groups': 0,
            'address_duplicate_groups': 0,
            'similarity_duplicate_groups': 0,
            'automatic_merges': 0,
            'llm_validations': 0,
            'llm_confirmed': 0,
            'llm_rejected': 0,
            'final_records': 0,
            'llm_cost': 0.0
        }
        
        # Cache des comparaisons
        self.comparison_cache = {}
    
    def _normalize_address(self, record: Dict[str, Any]) -> str:
        """
        Normalise une adresse pour la comparaison exacte
        """
        adresse = record.get('adresse_l1', '').strip().lower()
        commune = record.get('commune', '').strip().lower()
        
        if not adresse:
            return ""
        
        # Normalisation basique
        adresse = adresse.replace(',', ' ').replace('.', ' ')
        # Supprimer les espaces multiples
        adresse = ' '.join(adresse.split())
        
        # Inclure la commune pour éviter les faux positifs
        return f"{adresse}|{commune}" if commune else adresse
    
    def _detect_address_duplicates(self, records: List[Dict[str, Any]]) -> List[List[int]]:
        """
        Détecte les établissements ayant exactement la même adresse
        """
        address_groups = {}
        
        for i, record in enumerate(records):
            # SURVEILLANCE SPÉCIALE ADAPT
            is_adapt = 'adapt' in record.get('nom', '').lower() or 'adapt' in record.get('gestionnaire', '').lower()
            
            normalized_addr = self._normalize_address(record)
            
            if is_adapt:
                print(f"🔍 ADAPT DÉTECTÉ [Index {i}]: {record.get('nom', 'N/A')}")
                print(f"    Gestionnaire: {record.get('gestionnaire', 'N/A')}")
                print(f"    Adresse: {record.get('adresse_l1', 'N/A')}")
                print(f"    Adresse normalisée: '{normalized_addr}'")
                print(f"    Complétude: {self.metrics.calculate_completeness_score(record)}%")
            
            # Ignorer les adresses vides ou trop courtes
            if not normalized_addr or len(normalized_addr.replace('|', '')) < 10:
                if is_adapt:
                    print(f"    ⚠ ADAPT: Adresse ignorée (vide ou trop courte)")
                continue
                
            if normalized_addr not in address_groups:
                address_groups[normalized_addr] = []
            address_groups[normalized_addr].append(i)
            
            if is_adapt:
                print(f"    ✓ ADAPT: Ajouté au groupe d'adresse '{normalized_addr}'")
        
        # Retourner seulement les groupes avec plusieurs établissements
        duplicate_groups = [group for group in address_groups.values() if len(group) > 1]
        
        if duplicate_groups:
            print(f"\n📍 Détection d'adresses en doublon:")
            for i, group in enumerate(duplicate_groups, 1):
                sample_record = records[group[0]]
                print(f"   Groupe {i}: {len(group)} établissements à '{sample_record.get('adresse_l1', 'N/A')}, {sample_record.get('commune', 'N/A')}'")
                for idx in group:
                    completeness = self.metrics.calculate_completeness_score(records[idx])
                    record_name = records[idx].get('nom', 'N/A')
                    gestionnaire = records[idx].get('gestionnaire', 'N/A')
                    
                    # 🔍 SURVEILLANCE LADAPT
                    if 'ladapt' in gestionnaire.lower() or 'ladapt' in record_name.lower():
                        print(f"      - 🚨 LADAPT DÉTECTÉ: {record_name} (complétude: {completeness}%) - Gestionnaire: {gestionnaire}")
                    else:
                        print(f"      - {record_name} (complétude: {completeness}%)")
        
        return duplicate_groups

    def detect_duplicates(self, records: List[Dict[str, Any]]) -> List[List[int]]:
        """
        Détecte les groupes de doublons (adresses identiques + similarité)
        
        Args:
            records: Liste des enregistrements à analyser
            
        Returns:
            Liste de groupes d'indices de doublons
            Ex: [[0, 5], [1, 3, 7]] = records 0 et 5 sont doublons, 1, 3 et 7 aussi
        """
        self.stats['total_records'] = len(records)
        n = len(records)
        
        # ÉTAPE 1: Détection des doublons d'adresse exacte
        print("\n🏠 Détection des doublons d'adresse...")
        address_duplicate_groups = self._detect_address_duplicates(records)
        
        # Initialiser la structure union-find
        parent = list(range(n))
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        # Fusionner les groupes d'adresses identiques
        for group in address_duplicate_groups:
            for i in range(1, len(group)):
                union(group[0], group[i])
                self.stats['automatic_merges'] += 1
        
        print(f"   → {len(address_duplicate_groups)} groupe(s) d'adresses identiques détectés")
        self.stats['address_duplicate_groups'] = len(address_duplicate_groups)
        
        # ÉTAPE 2: Détection des doublons par similarité (pour les autres cas)
        print("\n🔍 Détection des doublons par similarité...")
        
        # Types d'établissements à exclure de la déduplication (sources officielles vérifiées)
        excluded_types = ['Résidence autonomie', 'Résidence services seniors', 'MARPA']
        
        def is_excluded(record):
            """Vérifie si l'établissement doit être exclu de la déduplication"""
            sous_cat = record.get('sous_categories', '')
            return sous_cat in excluded_types
        
        # Comparer tous les pairs
        for i in range(n):
            # Exclure les résidences autonomie et résidences services (sources officielles)
            if is_excluded(records[i]):
                continue
                
            for j in range(i + 1, n):
                # Exclure les résidences autonomie et résidences services (sources officielles)
                if is_excluded(records[j]):
                    continue
                similarity = self.metrics.calculate_overall_similarity(
                    records[i], 
                    records[j]
                )
                
                score = similarity['overall_score']
                
                # Score 100%: Doublon certain
                if score >= 100:
                    union(i, j)
                    self.stats['automatic_merges'] += 1
                    print(f"✓ Doublon certain (100%): '{records[i].get('nom')}' ≈ '{records[j].get('nom')}'")
                
                # Score 60-99%: Validation LLM
                elif score >= 60:
                    cache_key = f"{i}_{j}"
                    
                    if cache_key in self.comparison_cache:
                        is_duplicate = self.comparison_cache[cache_key]
                    else:
                        is_duplicate = self._validate_with_llm(
                            records[i], 
                            records[j], 
                            similarity
                        )
                        self.comparison_cache[cache_key] = is_duplicate
                    
                    if is_duplicate:
                        union(i, j)
                        self.stats['llm_confirmed'] += 1
                        print(f"✓ Doublon confirmé LLM ({score}%): '{records[i].get('nom')}' ≈ '{records[j].get('nom')}'")
                    else:
                        self.stats['llm_rejected'] += 1
                        print(f"✗ Établissements distincts ({score}%): '{records[i].get('nom')}' ≠ '{records[j].get('nom')}'")
        
        # Construire les groupes
        groups = {}
        for i in range(n):
            root = find(i)
            if root not in groups:
                groups[root] = []
            groups[root].append(i)
        
        # Filtrer les groupes (garder seulement les vrais doublons)
        duplicate_groups = [group for group in groups.values() if len(group) > 1]
        similarity_groups = len(duplicate_groups) - len(address_duplicate_groups)
        
        self.stats['duplicate_groups'] = len(duplicate_groups)
        self.stats['similarity_duplicate_groups'] = similarity_groups
        
        if similarity_groups > 0:
            print(f"   → {similarity_groups} groupe(s) supplémentaires détectés par similarité")
        
        return duplicate_groups
    
    def _validate_with_llm(
        self, 
        record1: Dict[str, Any], 
        record2: Dict[str, Any],
        similarity: Dict[str, Any]
    ) -> bool:
        """
        Valide si deux établissements sont des doublons via LLM
        
        Args:
            record1: Premier enregistrement
            record2: Deuxième enregistrement
            similarity: Résultat de calculate_overall_similarity
            
        Returns:
            True si doublons, False sinon
        """
        if not self.client:
            print("⚠ Pas de clé API Groq, validation automatique basée sur score")
            return similarity['overall_score'] >= 75
        
        self.stats['llm_validations'] += 1
        
        # Préparer les données pour le LLM
        def format_record(record):
            return {
                'nom': record.get('nom', 'N/A'),
                'commune': record.get('commune', 'N/A'),
                'gestionnaire': record.get('gestionnaire', 'N/A'),
                'adresse': record.get('adresse_l1', 'N/A'),
                'telephone': record.get('telephone', 'N/A'),
                'email': record.get('email', 'N/A')
            }
        
        prompt = f"""Tu es un expert en déduplication de données d'établissements seniors.

ÉTABLISSEMENT 1:
{json.dumps(format_record(record1), ensure_ascii=False, indent=2)}

ÉTABLISSEMENT 2:
{json.dumps(format_record(record2), ensure_ascii=False, indent=2)}

SCORES DE SIMILARITÉ:
- Nom: {similarity['name_similarity']}%
- Localisation: {similarity['location_similarity']}%
- Gestionnaire: {similarity['gestionnaire_similarity']}%
- Score global: {similarity['overall_score']}%

QUESTION: S'agit-il du MÊME établissement (doublon) ou de deux établissements DISTINCTS?

IMPORTANT:
- Deux établissements du même groupe (ex: Ages & Vie) dans des villes DIFFÉRENTES sont DISTINCTS
- Des variations de nom dans la MÊME commune avec le MÊME gestionnaire sont généralement des DOUBLONS
- Réponds UNIQUEMENT avec un JSON valide

Réponds au format JSON:
{{"same": true/false, "confidence": 0-100, "reason": "explication courte"}}"""

        try:
            response = self.client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {
                        "role": "system",
                        "content": "Tu es un assistant de déduplication de données. Réponds uniquement en JSON valide."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,
                max_tokens=200
            )
            
            content = response.choices[0].message.content.strip()
            
            # Extraire JSON du contenu
            if '```json' in content:
                content = content.split('```json')[1].split('```')[0].strip()
            elif '```' in content:
                content = content.split('```')[1].split('```')[0].strip()
            
            result = json.loads(content)
            
            # Estimer le coût (approximatif)
            # llama-3.1-8b-instant: ~$0.05/1M tokens input, ~$0.08/1M tokens output
            input_tokens = len(prompt.split()) * 1.3  # Approximation
            output_tokens = len(content.split()) * 1.3
            cost = (input_tokens * 0.05 / 1_000_000) + (output_tokens * 0.08 / 1_000_000)
            self.stats['llm_cost'] += cost
            
            is_same = result.get('same', False)
            confidence = result.get('confidence', 0)
            reason = result.get('reason', '')
            
            print(f"  LLM: same={is_same}, confidence={confidence}%, reason='{reason}'")
            
            return is_same
            
        except Exception as e:
            print(f"⚠ Erreur validation LLM: {e}")
            # Fallback: validation automatique basée sur score
            return similarity['overall_score'] >= 75
    
    def merge_duplicates(
        self, 
        records: List[Dict[str, Any]], 
        duplicate_groups: List[List[int]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Fusionne les doublons en conservant l'enregistrement le plus complet
        
        Args:
            records: Liste complète des enregistrements
            duplicate_groups: Groupes d'indices de doublons
            
        Returns:
            (records_dedupliques, metadonnees_fusion)
        """
        # Indices à supprimer
        indices_to_remove = set()
        
        # Métadonnées de fusion
        merge_metadata = []
        
        print(f"\n🔧 Fusion de {len(duplicate_groups)} groupe(s) de doublons...")
        
        for group_num, group in enumerate(duplicate_groups, 1):
            if len(group) < 2:
                continue
            
            # SURVEILLANCE SPÉCIALE ADAPT
            adapt_in_group = []
            for idx in group:
                record = records[idx]
                if 'adapt' in record.get('nom', '').lower() or 'adapt' in record.get('gestionnaire', '').lower():
                    adapt_in_group.append((idx, record))
            
            if adapt_in_group:
                print(f"\n🚨 GROUPE {group_num} CONTIENT {len(adapt_in_group)} ADAPT:")
                for idx, record in adapt_in_group:
                    print(f"    [Index {idx}] {record.get('nom', 'N/A')} - {record.get('gestionnaire', 'N/A')}")
            
            # Calculer le score de complétude pour chaque enregistrement du groupe
            completeness_scores = []
            for idx in group:
                score = self.metrics.calculate_completeness_score(records[idx])
                completeness_scores.append((idx, score, records[idx]))
            
            # Trier par score décroissant (plus complet en premier)
            completeness_scores.sort(key=lambda x: x[1], reverse=True)
            
            # Le premier est le plus complet
            kept_idx, kept_score, kept_record = completeness_scores[0]
            
            # SURVEILLANCE SPÉCIALE ADAPT - DÉCISION FINALE
            if adapt_in_group:
                kept_is_adapt = 'adapt' in kept_record.get('nom', '').lower() or 'adapt' in kept_record.get('gestionnaire', '').lower()
                print(f"\n🎯 DÉCISION FINALE ADAPT:")
                print(f"    Enregistrement gardé: {'ADAPT' if kept_is_adapt else 'NON-ADAPT'}")
                print(f"    Nom: {kept_record.get('nom', 'N/A')}")
                print(f"    Gestionnaire: {kept_record.get('gestionnaire', 'N/A')}")
                print(f"    Complétude: {kept_score}%")
                if not kept_is_adapt:
                    print(f"    ⚠ ATTENTION: Un ADAPT a été éliminé !")
            
            # Vérifier si c'est un doublon d'adresse
            sample_addr = self._normalize_address(records[group[0]])
            is_address_duplicate = all(
                self._normalize_address(records[idx]) == sample_addr 
                for idx in group
            ) and sample_addr
            
            merge_reason = "adresse_identique" if is_address_duplicate else "similarité_élevée"
            
            print(f"   Groupe {group_num} ({merge_reason}):")
            print(f"      ✅ Gardé: {kept_record.get('nom')} (complétude: {kept_score}%)")
            
            # Fusionner les informations des autres
            merged_from = []
            for idx, score, record in completeness_scores[1:]:
                is_eliminated_adapt = 'adapt' in record.get('nom', '').lower() or 'adapt' in record.get('gestionnaire', '').lower()
                
                if is_eliminated_adapt:
                    print(f"      🚨 ADAPT ÉLIMINÉ: {record.get('nom')} (complétude: {score}%)")
                    print(f"         Gestionnaire: {record.get('gestionnaire', 'N/A')}")
                    print(f"         Adresse: {record.get('adresse_l1', 'N/A')}")
                else:
                    print(f"      ❌ Fusionné: {record.get('nom')} (complétude: {score}%)")
                    
                merged_from.append({
                    'index': idx,
                    'nom': record.get('nom'),
                    'completeness': score,
                    'adresse': record.get('adresse_l1'),
                    'is_adapt': is_eliminated_adapt
                })
                indices_to_remove.add(idx)
                
                # Compléter avec infos manquantes
                for key, value in record.items():
                    if value and str(value).strip() and (not kept_record.get(key) or not str(kept_record.get(key)).strip()):
                        kept_record[key] = value
            
            # Métadonnées
            merge_metadata.append({
                'kept_record_index': kept_idx,
                'kept_record_name': kept_record.get('nom'),
                'completeness_score': kept_score,
                'merged_from': merged_from,
                'fusion_method': merge_reason,
                'total_merged': len(group),
                'is_address_duplicate': is_address_duplicate,
                'common_address': records[group[0]].get('adresse_l1') if is_address_duplicate else None
            })
            
            print(f"\n📦 Fusion: Conservé '{kept_record.get('nom')}' (complétude: {kept_score}%)")
            print(f"   Fusionné depuis: {[m['nom'] for m in merged_from]}")
        
        # Créer la liste dédupliquée
        deduplicated = [
            record for i, record in enumerate(records) 
            if i not in indices_to_remove
        ]
        
        self.stats['final_records'] = len(deduplicated)
        
        return deduplicated, merge_metadata
    
    def deduplicate(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Processus complet de déduplication
        
        Args:
            records: Liste des enregistrements à dédupliquer
            
        Returns:
            Dictionnaire avec résultats et métadonnées
        """
        print(f"\n🔍 DÉDUPLICATION DE {len(records)} ÉTABLISSEMENTS")
        print("=" * 60)
        
        # Étape 1: Détection des doublons
        print("\n1️⃣ Détection des doublons...")
        duplicate_groups = self.detect_duplicates(records)
        
        print(f"\n   Trouvé {len(duplicate_groups)} groupe(s) de doublons")
        for i, group in enumerate(duplicate_groups, 1):
            print(f"   Groupe {i}: {len(group)} établissements")
            for idx in group:
                print(f"      - {records[idx].get('nom')} ({records[idx].get('commune')})")
        
        # Étape 2: Fusion intelligente
        print("\n2️⃣ Fusion intelligente...")
        deduplicated, merge_metadata = self.merge_duplicates(records, duplicate_groups)
        
        # Résultats
        print("\n" + "=" * 60)
        print(f"✅ DÉDUPLICATION TERMINÉE")
        print(f"   Établissements initiaux: {self.stats['total_records']}")
        print(f"   Établissements finaux: {self.stats['final_records']}")
        print(f"   Groupes de doublons: {self.stats['duplicate_groups']}")
        print(f"      - Adresses identiques: {self.stats['address_duplicate_groups']}")
        print(f"      - Similarité élevée: {self.stats['similarity_duplicate_groups']}")
        print(f"   Fusions automatiques: {self.stats['automatic_merges']}")
        print(f"   Validations LLM: {self.stats['llm_validations']}")
        print(f"   - Confirmées: {self.stats['llm_confirmed']}")
        print(f"   - Rejetées: {self.stats['llm_rejected']}")
        if self.stats['llm_cost'] > 0:
            print(f"   Coût LLM: ${self.stats['llm_cost']:.6f}")
        
        return {
            'deduplicated_records': deduplicated,
            'merge_metadata': merge_metadata,
            'statistics': self.stats.copy()
        }
