"""
Pipeline v3.0 - Interface CLI
Extraction, validation, enrichissement et déduplication avec Mixtral multipasse
"""

import sys
import os
import csv
import argparse
from datetime import datetime
from dataclasses import asdict
from typing import Optional, List


class TeeLogger:
    """Classe pour dupliquer stdout vers console ET fichier"""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w', encoding='utf-8')
    
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    
    def flush(self):
        self.terminal.flush()
        self.log.flush()
    
    def close(self):
        self.log.close()
        sys.stdout = self.terminal

# Ajouter les chemins
sys.path.append(os.path.join(os.path.dirname(__file__), 'mvp'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'mvp', 'scrapers'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'mvp', 'deduplication'))

# Imports
from mvp.scrapers.official_scraper import OfficialScraper
from mvp.scrapers.snippet_classifier import SnippetClassifier
from mvp.scrapers.mistral_extractor import MistralExtractor
from mvp.scrapers.places_enricher import PlacesEnricher
from mvp.scrapers.enricher import Enricher
from mvp.deduplication.intelligent_deduplicator import IntelligentDeduplicator


# Mapping départements
DEPARTEMENTS = {
    '01': 'Ain',
    '02': 'Aisne',
    '03': 'Allier',
    '04': 'Alpes-de-Haute-Provence',
    '05': 'Hautes-Alpes',
    '06': 'Alpes-Maritimes',
    '07': 'Ardèche',
    '08': 'Ardennes',
    '09': 'Ariège',
    '10': 'Aube',
    '11': 'Aude',
    '12': 'Aveyron',
    '13': 'Bouches-du-Rhône',
    '14': 'Calvados',
    '15': 'Cantal',
    '16': 'Charente',
    '17': 'Charente-Maritime',
    '18': 'Cher',
    '19': 'Corrèze',
    '21': 'Côte-d\'Or',
    '22': 'Côtes-d\'Armor',
    '23': 'Creuse',
    '24': 'Dordogne',
    '25': 'Doubs',
    '26': 'Drôme',
    '27': 'Eure',
    '28': 'Eure-et-Loir',
    '29': 'Finistère',
    '2A': 'Corse-du-Sud',
    '2B': 'Haute-Corse',
    '30': 'Gard',
    '31': 'Haute-Garonne',
    '32': 'Gers',
    '33': 'Gironde',
    '34': 'Hérault',
    '35': 'Ille-et-Vilaine',
    '36': 'Indre',
    '37': 'Indre-et-Loire',
    '38': 'Isère',
    '39': 'Jura',
    '40': 'Landes',
    '41': 'Loir-et-Cher',
    '42': 'Loire',
    '43': 'Haute-Loire',
    '44': 'Loire-Atlantique',
    '45': 'Loiret',
    '46': 'Lot',
    '47': 'Lot-et-Garonne',
    '48': 'Lozère',
    '49': 'Maine-et-Loire',
    '50': 'Manche',
    '51': 'Marne',
    '52': 'Haute-Marne',
    '53': 'Mayenne',
    '54': 'Meurthe-et-Moselle',
    '55': 'Meuse',
    '56': 'Morbihan',
    '57': 'Moselle',
    '58': 'Nièvre',
    '59': 'Nord',
    '60': 'Oise',
    '61': 'Orne',
    '62': 'Pas-de-Calais',
    '63': 'Puy-de-Dôme',
    '64': 'Pyrénées-Atlantiques',
    '65': 'Hautes-Pyrénées',
    '66': 'Pyrénées-Orientales',
    '67': 'Bas-Rhin',
    '68': 'Haut-Rhin',
    '69': 'Rhône',
    '70': 'Haute-Saône',
    '71': 'Saône-et-Loire',
    '72': 'Sarthe',
    '73': 'Savoie',
    '74': 'Haute-Savoie',
    '75': 'Paris',
    '76': 'Seine-Maritime',
    '77': 'Seine-et-Marne',
    '78': 'Yvelines',
    '79': 'Deux-Sèvres',
    '80': 'Somme',
    '81': 'Tarn',
    '82': 'Tarn-et-Garonne',
    '83': 'Var',
    '84': 'Vaucluse',
    '85': 'Vendée',
    '86': 'Vienne',
    '87': 'Haute-Vienne',
    '88': 'Vosges',
    '89': 'Yonne',
    '90': 'Territoire de Belfort',
    '91': 'Essonne',
    '92': 'Hauts-de-Seine',
    '93': 'Seine-Saint-Denis',
    '94': 'Val-de-Marne',
    '95': 'Val-d\'Oise',
    '971': 'Guadeloupe',
    '972': 'Martinique',
    '973': 'Guyane',
    '974': 'La Réunion',
    '976': 'Mayotte'
}


class PipelineV3:
    """Pipeline complet v3.0 avec Mixtral multipasse"""
    
    def __init__(
        self,
        department: str,
        output_dir: str = 'data',
        extra_queries: Optional[List[str]] = None,
        max_candidates: Optional[int] = None,
    ):
        """
        Initialise le pipeline
        
        Args:
            department: Code département (ex: '10', '47')
            output_dir: Dossier de sortie pour les CSV
            extra_queries: Recherches supplémentaires spécifiques au département
        """
        self.department_code = department
        self.department_name = DEPARTEMENTS.get(department, f'Département {department}')
        self.output_dir = output_dir
        self.extra_queries = extra_queries
        self.max_candidates = max_candidates
        
        # Créer dossier de sortie
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialiser le logger
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        dept_slug = self.department_name.lower().replace(' ', '_').replace('-', '_').replace("'", '')
        self.log_file = f'logs/pipeline_v3_{dept_slug}_{timestamp}.md'
        os.makedirs('logs', exist_ok=True)
        self.logger = None
        
        # Initialiser les modules
        self.official_scraper = OfficialScraper()
        self.classifier = SnippetClassifier()
        self.extractor = MistralExtractor()
        self.places_enricher = PlacesEnricher(google_api_key=os.getenv('GOOGLE_PLACES_API_KEY'))
        self.enricher = Enricher()
        self.deduplicator = IntelligentDeduplicator()
        
        # Statistiques
        self.stats = {
            'official_count': 0,
            'alternative_count': 0,
            'total_before_dedup': 0,
            'final_count': 0,
            'duration_seconds': 0,
            'total_cost': 0.0
        }
    
    def run(self) -> dict:
        """
        Exécute le pipeline complet
        
        Returns:
            Dict avec résultats et statistiques
        """
        import time
        start_time = time.time()
        
        # Démarrer le logging
        self.logger = TeeLogger(self.log_file)
        sys.stdout = self.logger
        
        print("="*70)
        print(f"🚀 PIPELINE V3.0 - {self.department_name} ({self.department_code})")
        print("="*70)
        print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📝 Logs: {self.log_file}\n")
        
        # MODULE 1: Official Scraper
        print("="*70)
        print("📋 MODULE 1: OFFICIAL SCRAPER")
        print("="*70)
        
        official_establishments = self.official_scraper.extract_establishments(self.department_code)
        self.stats['official_count'] = len(official_establishments)
        
        # Convertir en dicts
        official_dicts = [asdict(est) for est in official_establishments]
        
        print(f"\n✅ {len(official_dicts)} établissements officiels extraits")
        
        # MODULE 2: Snippet Classifier
        print(f"\n{'='*70}")
        print("🔍 MODULE 2: SNIPPET CLASSIFIER V3.0")
        print("="*70)
        
        classified_results = self.classifier.search_and_classify(
            self.department_name, 
            self.department_code,
            extra_queries=self.extra_queries
        )
        
        print(f"\n✅ {len(classified_results)} candidats pertinents classifiés")
        
        # MODULE 3: Mistral Extractor
        print(f"\n{'='*70}")
        print("🤖 MODULE 3: MISTRAL EXTRACTOR V3.0")
        print("="*70)
        
        if self.max_candidates is not None:
            classified_results = classified_results[: self.max_candidates]
            print(f"\n🧪 Mode run court: max_candidates={self.max_candidates} (après tri/filtrage)")

        extracted_establishments = self.extractor.extract_from_candidates(
            classified_results,
            self.department_name,
            self.department_code
        )
        
        print(f"\n✅ {len(extracted_establishments)} établissements extraits")
        
        # MODULE 3.5: Google Places Enricher
        print(f"\n{'='*70}")
        print("🗺️  MODULE 3.5: GOOGLE PLACES ENRICHER")
        print("="*70)
        
        places_enriched = self.places_enricher.enrich_establishments(extracted_establishments)
        
        print(f"\n✅ {len(places_enriched)} établissements traités par Places API")
        
        # MODULE 4: Enricher
        print(f"\n{'='*70}")
        print("✨ MODULE 4: ENRICHER V3.0")
        print("="*70)
        
        enriched_establishments = self.enricher.enrich_establishments(
            places_enriched,
            self.department_name
        )
        
        self.stats['alternative_count'] = len(enriched_establishments)
        
        # Convertir en dicts
        enriched_dicts = [self._convert_to_dict(est) for est in enriched_establishments]
        
        print(f"\n✅ {len(enriched_dicts)} établissements enrichis")
        
        # Fusion
        all_establishments = official_dicts + enriched_dicts
        self.stats['total_before_dedup'] = len(all_establishments)
        
        print(f"\n📊 TOTAL AVANT DÉDUPLICATION: {len(all_establishments)}")
        
        # MODULE 5: Déduplication
        print(f"\n{'='*70}")
        print("🔍 MODULE 5: DÉDUPLICATION INTELLIGENTE")
        print("="*70)
        
        dedup_result = self.deduplicator.deduplicate(all_establishments)
        final_establishments = dedup_result['deduplicated_records']
        
        # Filtre final : exclusion précarité principale
        print(f"\n{'='*70}")
        print("🔍 FILTRE FINAL: EXCLUSION PRÉCARITÉ PRINCIPALE")
        print("="*70)
        
        filtered_establishments = self._filter_precarite(final_establishments)
        rejected_count = len(final_establishments) - len(filtered_establishments)
        
        if rejected_count > 0:
            print(f"⚠️ {rejected_count} établissement(s) rejeté(s) (précarité principale)")
        
        final_establishments = filtered_establishments
        
        # Filtre qualité : exclusion établissements mal renseignés
        print(f"\n{'='*70}")
        print("🎯 FILTRE QUALITÉ: EXCLUSION DONNÉES INSUFFISANTES")
        print("="*70)
        
        quality_filtered = self._filter_quality(final_establishments)
        quality_rejected = len(final_establishments) - len(quality_filtered)
        
        if quality_rejected > 0:
            print(f"⚠️ {quality_rejected} établissement(s) rejeté(s) (données insuffisantes)")
        
        final_establishments = quality_filtered
        self.stats['final_count'] = len(final_establishments)
        
        # Durée
        self.stats['duration_seconds'] = time.time() - start_time
        
        # Coûts
        self.stats['total_cost'] = (
            self.classifier.stats.get('classification_cost', 0) +
            self.extractor.stats.get('extraction_cost', 0) +
            self.enricher.stats.get('enrichment_cost', 0)
        )
        
        # Export CSV
        exported_files = self._export_csv(final_establishments)
        
        # Résumé final
        self._print_summary(exported_files)
        
        # Fermer le logger
        if self.logger:
            self.logger.close()
        
        return {
            'success': True,
            'records': final_establishments,
            'exported_files': exported_files,
            'stats': self.stats
        }
    
    def _filter_quality(self, establishments: list) -> list:
        """
        Filtre les établissements avec données insuffisantes
        
        Rejette si:
        - Aucun gestionnaire ET aucun contact (téléphone, email, site)
        - Site immobilier sans autres contacts
        - PDF sans autres contacts
        """
        filtered = []
        
        for est in establishments:
            # Accès aux données (dictionnaires après déduplication)
            gestionnaire = str(est.get('gestionnaire', '')).strip()
            telephone = str(est.get('telephone', '')).strip()
            email = str(est.get('email', '')).strip()
            site_web = str(est.get('site_web', '')).strip()
            
            # Vérifier champs vides
            has_no_manager = not gestionnaire or gestionnaire.lower() in ['nan', 'none', '']
            has_no_phone = not telephone or telephone.lower() in ['nan', 'none', '']
            has_no_email = not email or email.lower() in ['nan', 'none', '']
            has_no_website = not site_web or site_web.lower() in ['nan', 'none', '']
            
            # Site immobilier ou PDF ?
            is_real_estate_site = 'immobilier' in site_web.lower() if site_web else False
            is_pdf = site_web.lower().endswith('.pdf') if site_web else False
            
            # Invalide si aucun gestionnaire ET aucun contact
            is_invalid = (has_no_manager and has_no_phone and has_no_email and has_no_website) or \
                         (has_no_manager and has_no_phone and has_no_email and is_real_estate_site) or \
                         (has_no_manager and has_no_phone and has_no_email and is_pdf)
            
            if is_invalid:
                nom_short = str(est.get('nom', ''))[:50]
                reason = "PDF seul" if is_pdf else "Données insuffisantes"
                print(f"   ❌ Rejeté: {nom_short} - {reason}")
            else:
                filtered.append(est)
        
        return filtered
    
    def _filter_precarite(self, establishments: list) -> list:
        """
        Filtre les établissements ciblant PRINCIPALEMENT la précarité
        
        Rejette si:
        - Présentation contient "grande précarité" + "insertion sociale"
        - Gestionnaire = Habitat et Humanisme + présentation avec insertion/précarité SANS mention seniors
        
        Accepte:
        - Habitat intergénérationnel (même si précarité mentionnée)
        - Habitat seniors (même si H&H)
        """
        filtered = []
        
        for est in establishments:
            nom_lower = est.get('nom', '').lower()
            presentation = est.get('presentation', '')
            presentation_lower = presentation.lower()
            gestionnaire = est.get('gestionnaire', '')
            gestionnaire_lower = gestionnaire.lower()
            
            # Détection précarité PRINCIPALE
            is_precarite_principale = False
            
            # Critère 1: Grande précarité explicite + insertion sociale
            if 'grande précarité' in presentation_lower or 'grande precarite' in presentation_lower:
                if 'insertion sociale' in presentation_lower or 'insertion' in presentation_lower:
                    # Vérifier si seniors/intergénérationnel mentionné
                    has_senior = any(kw in nom_lower or kw in presentation_lower for kw in 
                                    ['senior', 'seniors', 'âgé', 'agé', 'intergénérationnel', 'intergenerationnel'])
                    
                    if not has_senior:
                        is_precarite_principale = True
                        print(f"   ❌ Rejeté: {est.get('nom')} - Grande précarité + insertion sans seniors")
            
            # Critère 2: Habitat et Humanisme + mots-clés précarité/insertion SANS seniors
            if 'habitat et humanisme' in gestionnaire_lower or 'habitat-humanisme' in gestionnaire_lower:
                has_precarite = any(kw in presentation_lower for kw in 
                                   ['précarité', 'precarite', 'personnes isolées', 'personnes isolees', 'insertion'])
                has_senior = any(kw in nom_lower or kw in presentation_lower for kw in 
                                ['senior', 'seniors', 'âgé', 'agé', 'intergénérationnel', 'intergenerationnel'])
                
                if has_precarite and not has_senior:
                    is_precarite_principale = True
                    print(f"   ❌ Rejeté: {est.get('nom')} - H&H précarité sans seniors")
            
            # Garder si pas précarité principale
            if not is_precarite_principale:
                filtered.append(est)
        
        return filtered
    
    def _convert_to_dict(self, est):
        """Convertit ExtractedEstablishment en dict"""
        return {
            'nom': est.nom,
            'commune': est.commune,
            'code_postal': est.code_postal,
            'gestionnaire': est.gestionnaire,
            'adresse_l1': est.adresse_l1,
            'telephone': est.telephone,
            'email': est.email,
            'site_web': est.site_web,
            'sous_categories': est.sous_categories,
            'habitat_type': est.habitat_type,
            'eligibilite_avp': est.eligibilite_avp,
            'presentation': est.presentation,
            'departement': est.departement,
            'source': est.source,
            'date_extraction': est.date_extraction,
            'public_cible': est.public_cible
        }
    
    def _export_csv(self, establishments: list) -> list:
        """Exporte en CSV par blocs de 30"""
        
        print(f"\n{'='*70}")
        print("📤 EXPORT CSV")
        print("="*70)
        
        if not establishments:
            print("⚠️ Aucun établissement à exporter")
            return []
        
        # Export par blocs de 30
        block_size = 30
        exported_files = []
        
        for i in range(0, len(establishments), block_size):
            block = establishments[i:i + block_size]
            block_num = (i // block_size) + 1
            
            filename = f"{self.output_dir}/data_{self.department_code}_{block_num}.csv"
            
            self._write_csv_file(block, filename)
            exported_files.append(filename)
            
            print(f"   📄 {filename} - {len(block)} établissements")
        
        return exported_files
    
    def _write_csv_file(self, establishments: list, filename: str):
        """Écrit un fichier CSV avec nettoyage présentation"""
        
        fieldnames = [
            'nom', 'commune', 'code_postal', 'gestionnaire', 'adresse_l1',
            'telephone', 'email', 'site_web', 'sous_categories', 'habitat_type',
            'eligibilite_avp', 'presentation', 'departement', 'source',
            'date_extraction', 'public_cible'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for est in establishments:
                # Nettoyer la présentation (retours à la ligne → espaces)
                if est.get('presentation'):
                    est['presentation'] = ' '.join(est['presentation'].split())
                
                writer.writerow(est)
    
    def _print_summary(self, exported_files: list):
        """Affiche le résumé final"""
        
        print(f"\n{'='*70}")
        print("📊 STATISTIQUES FINALES DU PIPELINE")
        print("="*70)
        print(f"📍 Département: {self.department_name} ({self.department_code})\n")
        
        print("🔢 Étapes:")
        print(f"   Module 1 (Official): {self.stats['official_count']} établissements")
        print(f"   Modules 2-3-4 (Alternative): {self.stats['alternative_count']} établissements")
        print(f"   Total avant déduplication: {self.stats['total_before_dedup']}")
        print(f"   Module 5 (Déduplication): {self.stats['final_count']} établissements uniques")
        
        if self.stats['total_before_dedup'] > 0:
            dedup_rate = ((self.stats['total_before_dedup'] - self.stats['final_count']) / 
                         self.stats['total_before_dedup'] * 100)
            print(f"   Taux déduplication: {dedup_rate:.1f}%")
        
        # Stats Google Places
        places_stats = self.places_enricher.get_stats()
        if places_stats['enrichments_attempted'] > 0:
            success_rate = (places_stats['enrichments_successful'] / 
                          places_stats['enrichments_attempted'] * 100)
            print(f"\n🗺️  Google Places Enrichment:")
            print(f"   Tentatives: {places_stats['enrichments_attempted']}")
            print(f"   Succès: {places_stats['enrichments_successful']} ({success_rate:.1f}%)")
            print(f"   Adresses ajoutées: {places_stats['addresses_added']}")
            print(f"   Téléphones ajoutés: {places_stats['phones_added']}")
            print(f"   Sites web ajoutés: {places_stats['websites_added']}")
        
        print(f"\n💰 Coût total: €{self.stats['total_cost']:.6f}")
        print(f"⏱️ Durée: {self.stats['duration_seconds']:.1f} secondes")
        
        print(f"\n📁 Fichiers exportés ({len(exported_files)}):")
        for f in exported_files:
            print(f"   - {f}")
        
        print(f"\n📝 Logs complets disponibles: {self.log_file}")
        print("="*70)


def list_departments():
    """Affiche la liste des départements disponibles"""
    print("\n📍 DÉPARTEMENTS DISPONIBLES:")
    print("="*50)
    
    # Grouper par régions (simplifié)
    for code, name in sorted(DEPARTEMENTS.items()):
        print(f"   {code:>4} - {name}")
    
    print("="*50)


def main():
    """Point d'entrée CLI"""
    
    parser = argparse.ArgumentParser(
        description='Pipeline v3.0 - Extraction habitat seniors avec Mixtral multipasse',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python pipeline_v3_cli.py --list
  python pipeline_v3_cli.py --department 10
  python pipeline_v3_cli.py -d 47 -o data/output
        """
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='Lister les départements disponibles'
    )
    
    parser.add_argument(
        '--department', '-d',
        type=str,
        help='Code du département (ex: 10, 47)'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='data',
        help='Dossier de sortie pour les CSV (défaut: data)'
    )
    
    parser.add_argument(
        '--extra-queries', '-e',
        type=str,
        nargs='*',
        help='Requêtes supplémentaires spécifiques (3 résultats chacune)'
    )

    parser.add_argument(
        '--max-candidates',
        type=int,
        default=None,
        help='Limiter le nombre de candidats (debug run court)'
    )
    
    args = parser.parse_args()
    
    # Afficher la liste
    if args.list:
        list_departments()
        return 0
    
    # Vérifier département
    if not args.department:
        parser.print_help()
        print("\n❌ Erreur: --department requis")
        return 1
    
    if args.department not in DEPARTEMENTS:
        print(f"\n❌ Erreur: Département '{args.department}' non supporté")
        print("Utilisez --list pour voir les départements disponibles")
        return 1
    
    # Configuration spécifique par département
    extra_queries = args.extra_queries
    
    # Pas-de-Calais (62): Recherches supplémentaires pour gros département
    if args.department == '62' and not extra_queries:
        extra_queries = [
            '"habitat inclusif" ("Down up" OR "ferme sénéchal") Pas-de-Calais',
            '"habitat inclusif" (Arras OR Lens OR Boulogne-sur-Mer) Pas-de-Calais',
            '"habitat partagé" (seniors OR handicap) (Saint-Omer OR Béthune OR Calaisis) Pas-de-Calais'
        ]
        print(f"\n🎯 Configuration spécifique département 62: {len(extra_queries)} requêtes supplémentaires")

    # Nord (59): requêtes SERPer spécifiques par sous-territoire (5 résultats chacune)
    if args.department == '59' and not extra_queries:
        territories = [
            'Flandre Cambrésis',
            'métropole de lille',
            'valenciennois',
            'avesnois',
            'douaisis'
        ]

        keywords = [
            '"habitat inclusif"',
            '(colocation seniors OR maison partagée)',
            '(béguinage OR village seniors)',
            'habitat intergénérationnel'
        ]

        extra_queries = []
        for t in territories:
            for k in keywords:
                # On passe un tuple (query, num_results) pour demander 5 résultats
                q = f"{k} Nord {t}"
                extra_queries.append((q, 5))

        print(f"\n🎯 Configuration spécifique département 59: {len(extra_queries)} requêtes spécifiques ({len(territories)} sous-territoires, 5 résultats chacune)")
    
    # Rhône (69): requêtes SERPer spécifiques pour habitat inclusif (non appliquées aux autres départements)
    if args.department == '69' and not extra_queries:
        extra_queries = [
            ('"habitat inclusif" Lyon', 10),
            ('"habitat inclusif" Beaujolais', 5),
            ('"habitat inclusif" Villefranche-sur-Saone', 5),
        ]
        print(f"\n🎯 Configuration spécifique département 69: {len(extra_queries)} requêtes spécifiques (Lyon 10, Beaujolais 5, Villefranche-sur-Saone 5)")
    
    # Exécuter pipeline
    pipeline = None
    try:
        pipeline = PipelineV3(
            department=args.department,
            output_dir=args.output_dir,
            extra_queries=extra_queries,
            max_candidates=args.max_candidates,
        )
        
        result = pipeline.run()
        
        if result['success']:
            print(f"\n✅ Pipeline terminé avec succès")
            return 0
        else:
            print(f"\n❌ Pipeline échoué")
            return 1
            
    except KeyboardInterrupt:
        print("\n\n⚠️ Interruption utilisateur")
        if pipeline and pipeline.logger:
            pipeline.logger.close()
        return 130
    except Exception as e:
        print(f"\n\n❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        if pipeline and pipeline.logger:
            pipeline.logger.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())
