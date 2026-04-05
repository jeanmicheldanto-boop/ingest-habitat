"""
Pipeline Complet d'Extraction d'Établissements Seniors
Intègre tous les modules : Official Scraper + Alternative Scraper + LLM Validator + Enricher + Déduplication
CLI avec sélection de département et export par blocs de 30
"""
import os
import sys
import csv
import argparse
from datetime import datetime
from typing import List, Dict, Any
from dotenv import load_dotenv

# Imports des modules du pipeline
from mvp.scrapers.official_scraper import OfficialScraper
from mvp.scrapers.alternative_scraper import AlternativeSearchScraper, EstablishmentCandidate
from mvp.scrapers.llm_validator_v2 import LLMValidator, ExtractedEstablishment
from mvp.scrapers.adaptive_enricher import AdaptiveEnricher
from mvp.deduplication import IntelligentDeduplicator
from departements_france import DEPARTEMENTS_FRANCE, get_region_for_department

# Charger les variables d'environnement
load_dotenv()


class PipelineComplet:
    """Pipeline complet d'extraction avec tous les modules"""
    
    # Utiliser tous les départements français
    DEPARTEMENTS = DEPARTEMENTS_FRANCE
    
    def __init__(self, department: str, output_dir: str = 'data'):
        """
        Initialise le pipeline pour un département
        
        Args:
            department: Code département (ex: '47', '10')
            output_dir: Dossier de sortie pour les CSV
        """
        self.department = department
        self.department_name = self.DEPARTEMENTS.get(department, f'Département {department}')
        self.output_dir = output_dir
        
        # Créer le dossier de sortie
        os.makedirs(output_dir, exist_ok=True)
        
        # Statistiques globales
        self.stats = {
            'module1_official': 0,
            'module2_alternative': 0,
            'module3_candidates': 0,
            'module4_validated': 0,
            'module45_enriched': 0,
            'module6_deduplicated': 0,
            'total_cost': 0.0,
            'duration_seconds': 0.0
        }
        
        print(f"\n{'='*70}")
        print(f"🚀 PIPELINE COMPLET D'EXTRACTION - {self.department_name} ({department})")
        print(f"{'='*70}\n")
    
    def run(self, debug: bool = False) -> Dict[str, Any]:
        """
        Exécute le pipeline complet
        
        Args:
            debug: Active le mode debug avec logs détaillés
            
        Returns:
            Dictionnaire avec résultats et statistiques
        """
        start_time = datetime.now()
        
        if debug:
            print("🐛 Mode debug activé - logs détaillés")
        
        try:
            # MODULE 1: Official Scraper (annuaires officiels gouv.fr)
            print("\n" + "="*70)
            print("📋 MODULE 1: OFFICIAL SCRAPER (Annuaires officiels)")
            print("="*70)
            official_records = self._run_module1_official_scraper()
            
            # MODULE 2-3: Alternative Scraper (sites web alternatifs)
            print("\n" + "="*70)
            print("🌐 MODULES 2-3: ALTERNATIVE SCRAPER (Sites web alternatifs)")
            print("="*70)
            alternative_records = self._run_module23_alternative_scraper()
            
            # MODULE 4: LLM Validator (validation anti-hallucination)
            # Note: On ne valide QUE les candidats alternatifs, pas les résidences officielles
            print("\n" + "="*70)
            print("🤖 MODULE 4: LLM VALIDATOR (Anti-hallucination)")
            print("="*70)
            
            if alternative_records:
                validated_alternative = self._run_module4_validator(alternative_records, debug)
            else:
                validated_alternative = []
            
            # Fusionner résidences officielles (non validées) + alternatives (validées)
            all_validated_records = official_records + validated_alternative
            print(f"\n📊 Total après validation: {len(all_validated_records)} établissements")
            print(f"   - Module 1 (Official): {len(official_records)}")
            print(f"   - Modules 2-3-4 (Alternative validés): {len(validated_alternative)}")
            
            if not all_validated_records:
                print("\n❌ Aucun établissement après validation, arrêt du pipeline")
                return {'success': False, 'records': [], 'stats': self.stats}
            
            # MODULE 4.5: Adaptive Enricher (enrichissement adaptatif)
            print("\n" + "="*70)
            print("✨ MODULE 4.5: ADAPTIVE ENRICHER (Enrichissement)")
            print("="*70)
            enriched_records = self._run_module45_enricher(all_validated_records)
            
            # MODULE 6: Déduplication intelligente
            print("\n" + "="*70)
            print("🔍 MODULE 6: DÉDUPLICATION INTELLIGENTE")
            print("="*70)
            final_records = self._run_module6_deduplication(enriched_records)
            
            # Export final par blocs de 30
            print("\n" + "="*70)
            print("💾 EXPORT FINAL (blocs de 30)")
            print("="*70)
            exported_files = self._export_by_blocks(final_records, block_size=30)
            
            # Statistiques finales
            end_time = datetime.now()
            self.stats['duration_seconds'] = (end_time - start_time).total_seconds()
            
            self._print_final_stats(exported_files)
            
            return {
                'success': True,
                'records': final_records,
                'exported_files': exported_files,
                'stats': self.stats
            }
            
        except Exception as e:
            print(f"\n❌ ERREUR PIPELINE: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e), 'stats': self.stats}
    
    def _run_module1_official_scraper(self) -> List[Dict[str, Any]]:
        """Exécute le Module 1: Official Scraper avec filtrage des gestionnaires commerciaux"""
        try:
            scraper = OfficialScraper(rate_limit_delay=1.0)
            establishments = scraper.extract_establishments(self.department)
            
            # Filtrer les gestionnaires commerciaux d'EHPAD
            commercial_ehpad_groups = [
                'korian', 'orpea', 'colisée', 'domusvi', 'lna santé', 'cap retraite',
                'résidences and co', 'garden résidence', 'maisons de famille',
                'emera', 'septime', 'clariane', 'initially'
            ]
            
            filtered_establishments = []
            excluded_count = 0
            
            for est in establishments:
                gestionnaire_lower = (est.gestionnaire or "").lower()
                is_commercial = any(group in gestionnaire_lower for group in commercial_ehpad_groups)
                
                if is_commercial:
                    excluded_count += 1
                    print(f"   ❌ Exclu: {est.nom} (gestionnaire commercial: {est.gestionnaire})")
                else:
                    filtered_establishments.append(est)
            
            # Convertir en dictionnaires
            records = [vars(est) for est in filtered_establishments]
            
            self.stats['module1_official'] = len(records)
            print(f"✅ {len(records)} établissements extraits (annuaires officiels)")
            if excluded_count > 0:
                print(f"   🚫 {excluded_count} établissements exclus (gestionnaires commerciaux)")
            
            return records
            
        except Exception as e:
            print(f"⚠️ Erreur Module 1: {e}")
            return []
    
    def _run_module23_alternative_scraper(self) -> List[Dict[str, Any]]:
        """Exécute les Modules 2-3: Alternative Scraper"""
        try:
            scraper = AlternativeSearchScraper()
            
            # Recherche multi-sources pour le département
            candidates = scraper.search_establishments(
                department=self.department_name,
                department_num=self.department
            )
            
            # Convertir EstablishmentCandidate en Dict
            all_candidates = []
            for candidate in candidates:
                record = {
                    'nom': candidate.nom,
                    'commune': candidate.commune,
                    'code_postal': '',
                    'gestionnaire': '',
                    'adresse_l1': '',
                    'telephone': '',
                    'email': '',
                    'site_web': candidate.url,
                    'sous_categories': '',
                    'habitat_type': 'habitat_partage',
                    'eligibilite_avp': 'a_verifier',
                    'presentation': candidate.snippet,
                    'page_content': getattr(candidate, 'page_content', ''),  # NOUVEAU: Contenu ScrapingBee
                    'departement': self.department_name,
                    'source': candidate.url,
                    'date_extraction': datetime.now().strftime('%Y-%m-%d'),
                    'public_cible': 'personnes_agees',
                    'confidence_score': candidate.confidence_score * 100
                }
                all_candidates.append(record)
            
            self.stats['module2_alternative'] = len(all_candidates)
            self.stats['module3_candidates'] = len(all_candidates)
            
            print(f"✅ {len(all_candidates)} candidats extraits (sources alternatives)")
            
            return all_candidates
            
        except Exception as e:
            print(f"⚠️ Erreur Modules 2-3: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _run_module4_validator(self, records: List[Dict[str, Any]], debug: bool = False) -> List[Dict[str, Any]]:
        """Exécute le Module 4: LLM Validator"""
        try:
            validator = LLMValidator()
            
            # Convertir dict → EstablishmentCandidate
            candidates = []
            for record in records:
                candidate = EstablishmentCandidate(
                    nom=record.get('nom', ''),
                    url=record.get('site_web', record.get('source', '')),
                    snippet=record.get('presentation', ''),
                    commune=record.get('commune', ''),
                    departement=record.get('departement', self.department_name),
                    confidence_score=record.get('confidence_score', 0.0) / 100.0,  # Convertir en 0-1
                    page_content=record.get('page_content', '')  # NOUVEAU: Contenu ScrapingBee
                )
                candidates.append(candidate)
            
            print(f"🔍 Validation de {len(candidates)} candidats...")
            
            if debug:
                print("🐛 Mode debug activé pour le validateur LLM")
            
            # Appel Module 4 avec les candidats
            establishments = validator.validate_candidates(candidates, self.department_name, debug=debug)
            
            # Convertir ExtractedEstablishment → dict
            validated_records = []
            for est in establishments:
                record = {
                    'nom': est.nom,
                    'commune': est.commune,
                    'code_postal': est.code_postal,
                    'gestionnaire': est.gestionnaire,
                    'adresse_l1': est.adresse_l1,
                    'telephone': est.telephone or '',
                    'email': est.email or '',
                    'site_web': est.site_web or '',
                    'sous_categories': est.sous_categories,
                    'habitat_type': est.habitat_type,
                    'eligibilite_avp': est.eligibilite_avp,
                    'presentation': est.presentation,
                    'departement': est.departement,
                    'source': est.source,
                    'date_extraction': est.date_extraction,
                    'public_cible': est.public_cible,
                    'confidence_score': est.confidence_score
                }
                validated_records.append(record)
            
            self.stats['module4_validated'] = len(validated_records)
            
            # Calculer coût (approximatif basé sur nombre d'appels LLM)
            estimated_cost = len(candidates) * 0.0001  # ~$0.0001 par validation
            self.stats['total_cost'] += estimated_cost
            
            print(f"✅ {len(validated_records)} établissements validés")
            print(f"💰 Coût validation: €{estimated_cost:.4f}")
            
            return validated_records
            
        except Exception as e:
            print(f"⚠️ Erreur Module 4: {e}")
            print(f"   Fallback: conservation de tous les {len(records)} établissements")
            import traceback
            traceback.print_exc()
            self.stats['module4_validated'] = len(records)
            return records
    
    def _run_module45_enricher(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Exécute le Module 4.5: Adaptive Enricher"""
        try:
            enricher = AdaptiveEnricher()
            
            enriched_records = []
            enrichment_count = 0
            
            print(f"✨ Enrichissement de {len(records)} établissements...")
            
            for i, record in enumerate(records, 1):
                # Convertir dict → ExtractedEstablishment
                establishment = ExtractedEstablishment(
                    nom=record.get('nom', ''),
                    commune=record.get('commune', ''),
                    code_postal=record.get('code_postal', ''),
                    gestionnaire=record.get('gestionnaire', ''),
                    adresse_l1=record.get('adresse_l1', ''),
                    telephone=record.get('telephone') or None,
                    email=record.get('email') or None,
                    site_web=record.get('site_web') or None,
                    sous_categories=record.get('sous_categories', ''),
                    habitat_type=record.get('habitat_type', 'habitat_partage'),
                    eligibilite_avp=record.get('eligibilite_avp', 'a_verifier'),
                    presentation=record.get('presentation', ''),
                    departement=record.get('departement', self.department_name),
                    source=record.get('source', ''),
                    date_extraction=record.get('date_extraction', datetime.now().strftime('%Y-%m-%d')),
                    public_cible=record.get('public_cible', 'personnes_agees'),
                    confidence_score=float(record.get('confidence_score', 0.0)),
                    validation_timestamp=datetime.now().isoformat()
                )
                
                # Enrichir si :
                # 1. Pas de présentation OU présentation trop courte (< 100 chars)
                # 2. OU données importantes manquantes
                needs_enrichment = False
                presentation_len = len(establishment.presentation or '')
                
                if presentation_len < 100:
                    needs_enrichment = True
                    reason = f"présentation vide/courte ({presentation_len} chars)"
                else:
                    missing_fields = sum([
                        1 for field in ['gestionnaire', 'telephone', 'adresse_l1']
                        if not getattr(establishment, field, None)
                    ])
                    if missing_fields >= 2:
                        needs_enrichment = True
                        reason = f"{missing_fields} champs manquants"
                
                if needs_enrichment:
                    if i <= 5 or i % 5 == 0:  # Afficher progress tous les 5
                        print(f"   {i}/{len(records)}: {establishment.nom[:40]}... ({reason})")
                    
                    try:
                        enriched = enricher.enrich_establishment(establishment, self.department_name)
                        
                        if enriched and enriched != establishment:
                            establishment = enriched
                            enrichment_count += 1
                    except Exception as e:
                        print(f"      ⚠️ Erreur enrichissement: {e}")
                
                # Convertir ExtractedEstablishment → dict
                enriched_record = {
                    'nom': establishment.nom,
                    'commune': establishment.commune,
                    'code_postal': establishment.code_postal,
                    'gestionnaire': establishment.gestionnaire,
                    'adresse_l1': establishment.adresse_l1,
                    'telephone': establishment.telephone or '',
                    'email': establishment.email or '',
                    'site_web': establishment.site_web or '',
                    'sous_categories': establishment.sous_categories,
                    'habitat_type': establishment.habitat_type,
                    'eligibilite_avp': establishment.eligibilite_avp,
                    'presentation': establishment.presentation,
                    'departement': establishment.departement,
                    'source': establishment.source,
                    'date_extraction': establishment.date_extraction,
                    'public_cible': establishment.public_cible,
                    'confidence_score': establishment.confidence_score
                }
                enriched_records.append(enriched_record)
            
            self.stats['module45_enriched'] = enrichment_count
            
            # Coût approximatif enrichissement
            estimated_cost = enrichment_count * 0.0002  # ~$0.0002 par enrichissement
            self.stats['total_cost'] += estimated_cost
            
            print(f"✅ {enrichment_count} établissements enrichis")
            print(f"💰 Coût enrichissement: €{estimated_cost:.4f}")
            
            return enriched_records
            
        except Exception as e:
            print(f"⚠️ Erreur Module 4.5: {e}")
            print(f"   Fallback: conservation des {len(records)} établissements sans enrichissement")
            import traceback
            traceback.print_exc()
            self.stats['module45_enriched'] = 0
            return records
    
    def _run_module6_deduplication(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Exécute le Module 6: Déduplication intelligente"""
        try:
            deduplicator = IntelligentDeduplicator()
            
            result = deduplicator.deduplicate(records)
            
            deduplicated_records = result['deduplicated_records']
            stats = result['statistics']
            
            self.stats['module6_deduplicated'] = stats['final_records']
            self.stats['total_cost'] += stats.get('llm_cost', 0.0)
            
            return deduplicated_records
            
        except Exception as e:
            print(f"⚠️ Erreur Module 6: {e}")
            return records
    
    def _export_by_blocks(self, records: List[Dict[str, Any]], block_size: int = 30) -> List[str]:
        """
        Exporte les enregistrements par blocs de N établissements
        Format: data_XX_1.csv, data_XX_2.csv, etc.
        """
        exported_files = []
        
        if not records:
            print("⚠️ Aucun enregistrement à exporter")
            return exported_files
        
        # Calculer le nombre de blocs
        total_blocks = (len(records) + block_size - 1) // block_size
        
        print(f"📦 Export de {len(records)} établissements en {total_blocks} bloc(s) de {block_size}")
        
        for block_num in range(total_blocks):
            start_idx = block_num * block_size
            end_idx = min(start_idx + block_size, len(records))
            
            block_records = records[start_idx:end_idx]
            
            # Nom du fichier: data_XX_N.csv
            filename = f"data_{self.department}_{block_num + 1}.csv"
            filepath = os.path.join(self.output_dir, filename)
            
            # Export CSV
            self._save_csv(block_records, filepath)
            
            exported_files.append(filepath)
            print(f"   ✅ Bloc {block_num + 1}/{total_blocks}: {filename} ({len(block_records)} établissements)")
        
        return exported_files
    
    def _save_csv(self, records: List[Dict[str, Any]], filepath: str):
        """Sauvegarde les enregistrements en CSV"""
        if not records:
            return
        
        # Ordre des colonnes
        fieldnames = [
            'nom', 'commune', 'code_postal', 'gestionnaire', 'adresse_l1',
            'telephone', 'email', 'site_web', 'sous_categories', 'habitat_type',
            'eligibilite_avp', 'presentation', 'departement', 'source',
            'date_extraction', 'public_cible', 'confidence_score'
        ]
        
        # Ajouter colonnes manquantes avec valeur par défaut
        for record in records:
            for field in fieldnames:
                if field not in record:
                    record[field] = ''
        
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(records)
    
    def _print_final_stats(self, exported_files: List[str]):
        """Affiche les statistiques finales"""
        print("\n" + "="*70)
        print("📊 STATISTIQUES FINALES DU PIPELINE")
        print("="*70)
        print(f"📍 Département: {self.department_name} ({self.department})")
        print(f"\n🔢 Étapes:")
        print(f"   Module 1 (Official): {self.stats['module1_official']} établissements")
        print(f"   Module 2-3 (Alternative): {self.stats['module2_alternative']} candidats")
        print(f"   Module 4 (Validation): {self.stats['module4_validated']} validés")
        print(f"   Module 4.5 (Enrichissement): {self.stats['module45_enriched']} enrichis")
        print(f"   Module 6 (Déduplication): {self.stats['module6_deduplicated']} uniques")
        print(f"\n💰 Coût total: €{self.stats['total_cost']:.4f}")
        print(f"⏱️ Durée: {self.stats['duration_seconds']:.1f} secondes")
        print(f"\n📁 Fichiers exportés ({len(exported_files)}):")
        for filepath in exported_files:
            print(f"   - {filepath}")
        print("="*70)


def main():
    """Fonction principale avec CLI"""
    parser = argparse.ArgumentParser(
        description='Pipeline Complet d\'Extraction d\'Établissements Seniors',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python pipeline_complet_cli.py --department 47
  python pipeline_complet_cli.py -d 10 -o data/output
  python pipeline_complet_cli.py --list
        """
    )
    
    parser.add_argument(
        '-d', '--department',
        type=str,
        help='Code du département à traiter (ex: 47, 10, 24)'
    )
    
    parser.add_argument(
        '-o', '--output',
        type=str,
        default='data',
        help='Dossier de sortie pour les CSV (défaut: data)'
    )
    
    parser.add_argument(
        '-l', '--list',
        action='store_true',
        help='Liste les départements disponibles'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Active le mode debug avec logs détaillés'
    )
    
    args = parser.parse_args()
    
    # Lister les départements
    if args.list:
        print("\n📍 DÉPARTEMENTS DISPONIBLES:")
        print("="*50)
        for code, name in sorted(PipelineComplet.DEPARTEMENTS.items()):
            print(f"   {code} - {name}")
        print("="*50)
        return
    
    # Vérifier qu'un département est fourni
    if not args.department:
        print("❌ Erreur: Département requis")
        print("Usage: python pipeline_complet_cli.py --department <code>")
        print("       python pipeline_complet_cli.py --list  (pour voir les départements)")
        sys.exit(1)
    
    # Vérifier que le département est supporté
    if args.department not in PipelineComplet.DEPARTEMENTS:
        print(f"❌ Erreur: Département {args.department} non supporté")
        print("Départements disponibles:")
        for code, name in sorted(PipelineComplet.DEPARTEMENTS.items()):
            print(f"   {code} - {name}")
        sys.exit(1)
    
    # Exécuter le pipeline
    pipeline = PipelineComplet(
        department=args.department,
        output_dir=args.output
    )
    
    # Activer le mode debug si demandé
    if args.debug:
        print("🐛 MODE DEBUG ACTIVÉ")
        import logging
        logging.basicConfig(level=logging.DEBUG)
    
    result = pipeline.run(debug=args.debug)
    
    if result['success']:
        print(f"\n✅ Pipeline terminé avec succès !")
        sys.exit(0)
    else:
        print(f"\n❌ Pipeline échoué: {result.get('error', 'Erreur inconnue')}")
        sys.exit(1)


if __name__ == '__main__':
    main()
