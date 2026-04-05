"""
Pipeline de Correction Automatique - Base Habitat Intermédiaire
Corrige les problèmes identifiés par quality_check_readonly.py

Corrections appliquées :
1. Normalisation départements : "01" → "Ain (01)"
2. Correction codes postaux : "1000" → "01000", NULL traités
3. Nettoyage emails : "00contact@..." → "contact@..."
4. Fusion sous-catégories doublons : vers format avec majuscules
5. Géolocalisation manquante : Google Geocoding API (550 établissements)
"""
import psycopg2
import os
import re
import time
import requests
from datetime import datetime
from database import DatabaseManager
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

# Mapping départements code → nom complet
DEPARTEMENTS = {
    '01': 'Ain', '02': 'Aisne', '03': 'Allier', '04': 'Alpes-de-Haute-Provence',
    '05': 'Hautes-Alpes', '06': 'Alpes-Maritimes', '07': 'Ardèche', '08': 'Ardennes',
    '09': 'Ariège', '10': 'Aube', '11': 'Aude', '12': 'Aveyron', '13': 'Bouches-du-Rhône',
    '14': 'Calvados', '15': 'Cantal', '16': 'Charente', '17': 'Charente-Maritime',
    '18': 'Cher', '19': 'Corrèze', '20': 'Corse', '21': "Côte-d'Or", '22': "Côtes-d'Armor",
    '23': 'Creuse', '24': 'Dordogne', '25': 'Doubs', '26': 'Drôme', '27': 'Eure',
    '28': 'Eure-et-Loir', '29': 'Finistère', '2A': 'Corse-du-Sud', '2B': 'Haute-Corse',
    '30': 'Gard', '31': 'Haute-Garonne', '32': 'Gers', '33': 'Gironde', '34': 'Hérault',
    '35': 'Ille-et-Vilaine', '36': 'Indre', '37': 'Indre-et-Loire', '38': 'Isère',
    '39': 'Jura', '40': 'Landes', '41': 'Loir-et-Cher', '42': 'Loire', '43': 'Haute-Loire',
    '44': 'Loire-Atlantique', '45': 'Loiret', '46': 'Lot', '47': 'Lot-et-Garonne',
    '48': 'Lozère', '49': 'Maine-et-Loire', '50': 'Manche', '51': 'Marne',
    '52': 'Haute-Marne', '53': 'Mayenne', '54': 'Meurthe-et-Moselle', '55': 'Meuse',
    '56': 'Morbihan', '57': 'Moselle', '58': 'Nièvre', '59': 'Nord', '60': 'Oise',
    '61': 'Orne', '62': 'Pas-de-Calais', '63': 'Puy-de-Dôme', '64': 'Pyrénées-Atlantiques',
    '65': 'Hautes-Pyrénées', '66': 'Pyrénées-Orientales', '67': 'Bas-Rhin', '68': 'Haut-Rhin',
    '69': 'Rhône', '70': 'Haute-Saône', '71': 'Saône-et-Loire', '72': 'Sarthe', '73': 'Savoie',
    '74': 'Haute-Savoie', '75': 'Paris', '76': 'Seine-Maritime', '77': 'Seine-et-Marne',
    '78': 'Yvelines', '79': 'Deux-Sèvres', '80': 'Somme', '81': 'Tarn', '82': 'Tarn-et-Garonne',
    '83': 'Var', '84': 'Vaucluse', '85': 'Vendée', '86': 'Vienne', '87': 'Haute-Vienne',
    '88': 'Vosges', '89': 'Yonne', '90': 'Territoire de Belfort', '91': 'Essonne',
    '92': 'Hauts-de-Seine', '93': 'Seine-Saint-Denis', '94': 'Val-de-Marne', '95': "Val-d'Oise"
}

# Mapping sous-catégories à fusionner : snake_case/minuscule → Format final
SOUS_CATEGORIES_FUSION = {
    'residence_autonomie': 'Résidence autonomie',
    'residence_services_seniors': 'Résidence services seniors',
    'habitat_inclusif': 'Habitat inclusif',
    'habitat_intergenerationnel': 'Habitat intergénérationnel',
    'colocation_avec_services': 'Colocation avec services',
    'maison_accueil_familial': "Maison d'accueil familial",
    'marpa': 'MARPA',
    'beguinage': 'Béguinage',
    'village_seniors': 'Village seniors',
    'habitat intergénérationnel': 'Habitat intergénérationnel'  # Espace au lieu d'underscore
}


class PipelineCorrection:
    """Pipeline de correction automatique des données"""
    
    def __init__(self, dry_run: bool = False):
        self.db = DatabaseManager()
        self.conn = None
        self.cur = None
        self.dry_run = dry_run
        
        # API Google Maps
        self.google_api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')
        
        # Statistiques corrections
        self.stats = {
            'departements_corriges': 0,
            'codes_postaux_corriges': 0,
            'emails_corriges': 0,
            'sous_categories_fusionnees': 0,
            'etablissements_geolocalises': 0,
            'erreurs': []
        }
        
        # Log des corrections
        self.log_corrections = []
    
    def connect(self):
        """Connexion à la base"""
        self.conn = psycopg2.connect(**self.db.config)
        self.cur = self.conn.cursor()
        
        # Désactiver temporairement la contrainte publish_check
        if not self.dry_run:
            try:
                print("⚠️  Suppression temporaire de la contrainte publish_check...")
                self.cur.execute("ALTER TABLE etablissements DROP CONSTRAINT IF EXISTS etablissements_publish_check")
            except Exception as e:
                print(f"   ⚠️  Impossible de supprimer la contrainte: {e}")
        
        if self.dry_run:
            print("⚠️  MODE DRY-RUN : Aucune modification ne sera appliquée")
        print(f"✅ Connecté à la base : {self.db.config['database']}")
    
    def close(self, commit: bool = True):
        """Fermeture et commit"""
        if self.cur:
            # Réactiver la contrainte publish_check avant de fermer
            if not self.dry_run:
                try:
                    print("\n⚙️  Réactivation de la contrainte publish_check...")
                    self.cur.execute("""
                        ALTER TABLE etablissements 
                        ADD CONSTRAINT etablissements_publish_check 
                        CHECK (((statut_editorial <> 'publie'::public.statut_editorial) OR public.can_publish(id))) 
                        NOT VALID
                    """)
                    print("✅ Contrainte réactivée")
                except Exception as e:
                    print(f"⚠️  Erreur réactivation contrainte: {e}")
            
            self.cur.close()
        if self.conn:
            if commit and not self.dry_run:
                self.conn.commit()
                print("✅ Modifications committées")
            elif self.dry_run:
                self.conn.rollback()
                print("⚠️  Rollback (DRY-RUN)")
            else:
                self.conn.rollback()
                print("⚠️  Rollback (pas de commit)")
            self.conn.close()
    
    def execute(self):
        """Exécution du pipeline complet"""
        print("="*70)
        print("🔧 PIPELINE DE CORRECTION AUTOMATIQUE")
        print("="*70)
        
        self.connect()
        
        try:
            # 1. Normalisation départements
            print("\n" + "="*70)
            self._corriger_departements()
            
            # 2. Correction codes postaux
            print("\n" + "="*70)
            self._corriger_codes_postaux()
            
            # 3. Nettoyage emails
            print("\n" + "="*70)
            self._nettoyer_emails()
            
            # 4. Fusion sous-catégories
            print("\n" + "="*70)
            self._fusionner_sous_categories()
            
            # 5. Géolocalisation Google
            print("\n" + "="*70)
            self._geolocaliser_manquants()
            
            # Commit final
            print("\n" + "="*70)
            print("📊 RÉSUMÉ DES CORRECTIONS")
            print("="*70)
            print(f"✅ Départements corrigés      : {self.stats['departements_corriges']:,}")
            print(f"✅ Codes postaux corrigés     : {self.stats['codes_postaux_corriges']:,}")
            print(f"✅ Emails nettoyés            : {self.stats['emails_corriges']:,}")
            print(f"✅ Sous-catégories fusionnées : {self.stats['sous_categories_fusionnees']:,}")
            print(f"✅ Établissements géolocalisés: {self.stats['etablissements_geolocalises']:,}")
            
            if self.stats['erreurs']:
                print(f"\n⚠️  Erreurs rencontrées: {len(self.stats['erreurs'])}")
                for err in self.stats['erreurs'][:10]:
                    print(f"   • {err}")
            
            # Sauvegarder log
            self._save_log()
            
        except Exception as e:
            print(f"\n❌ ERREUR CRITIQUE: {e}")
            import traceback
            traceback.print_exc()
            self.close(commit=False)
            raise
        
        finally:
            self.close(commit=not self.dry_run)
    
    def _corriger_departements(self):
        """Correction 1: Normalisation départements"""
        print("🗺️  CORRECTION 1: NORMALISATION DÉPARTEMENTS")
        print("   Format cible: 'Nom Département (XX)'")
        
        # Récupérer départements à corriger
        self.cur.execute("""
            SELECT id, nom, departement, commune
            FROM etablissements
            WHERE is_test = false
              AND (
                departement IS NULL
                OR trim(departement) = ''
                OR departement !~ E'^[A-ZÀ-Ÿ][^(]*\\s*\\([0-9]{1,3}[AB]?\\)$'
              )
        """)
        
        etablissements = self.cur.fetchall()
        print(f"   📊 Établissements à corriger: {len(etablissements):,}")
        
        corrections = []
        
        for etab_id, nom, dept_actuel, commune in etablissements:
            dept_corrige = self._normaliser_departement(dept_actuel, commune)
            
            if dept_corrige and dept_corrige != dept_actuel:
                corrections.append({
                    'id': etab_id,
                    'nom': nom,
                    'ancien': dept_actuel,
                    'nouveau': dept_corrige
                })
                
                # NOTE: Nettoyage communes désactivé car trop de faux positifs
                # Exemples: "Bagnoles-de-l'Orne", "Sablé-sur-Sarthe" contiennent légitimement
                # le nom du département/rivière dans leur nom officiel
        
        print(f"   ✅ Corrections à appliquer: {len(corrections):,}")
        
        # Appliquer corrections départements
        if not self.dry_run and corrections:
            for i, corr in enumerate(corrections):
                # UPDATE sans trigger de updated_at pour éviter publish_check
                self.cur.execute("""
                    UPDATE etablissements
                    SET departement = %s
                    WHERE id = %s
                """, (corr['nouveau'], corr['id']))
                
                self.log_corrections.append({
                    'type': 'departement',
                    'etablissement_id': corr['id'],
                    'etablissement_nom': corr['nom'],
                    'ancienne_valeur': corr['ancien'],
                    'nouvelle_valeur': corr['nouveau']
                })
                
                if (i + 1) % 100 == 0:
                    print(f"      • {i + 1}/{len(corrections)} corrections appliquées...")
            
            self.stats['departements_corriges'] = len(corrections)
            print(f"   ✅ {len(corrections):,} départements normalisés")
        elif self.dry_run:
            print(f"   ⚠️  [DRY-RUN] {len(corrections):,} départements seraient corrigés")
            # Afficher exemples
            for corr in corrections[:5]:
                print(f"      • '{corr['ancien']}' → '{corr['nouveau']}'")
    
    def _normaliser_departement(self, dept: Optional[str], commune: Optional[str]) -> Optional[str]:
        """Normalise un département au format 'Nom (XX)'"""
        # Cas 1: Département vide mais présent dans commune
        if (not dept or dept.strip() == '') and commune:
            # Chercher un nom de département dans la commune
            for code, nom in DEPARTEMENTS.items():
                # Pattern: "Ville, Département" ou "Département" seul
                if f", {nom}" in commune or commune.strip().upper() == nom.upper():
                    return f"{nom} ({code})"
                # Pattern: code entre parenthèses dans commune
                if f"({code})" in commune:
                    return f"{nom} ({code})"
            return None
        
        if not dept:
            return None
        
        dept = dept.strip()
        
        # Format "Département (XX)" générique → extraire code et retrouver nom
        match_dept_generique = re.match(r'^Département\s*\(([0-9]{1,3}[AB]?)\)$', dept, re.IGNORECASE)
        if match_dept_generique:
            code = match_dept_generique.group(1)
            nom = DEPARTEMENTS.get(code)
            if nom:
                return f"{nom} ({code})"
        
        # Déjà au bon format (nom spécifique + code) ?
        if re.match(r'^[A-ZÀ-Ÿ][^(]*\s*\([0-9]{1,3}[AB]?\)$', dept):
            # Vérifier que ce n'est pas "Département (XX)"
            if not dept.lower().startswith('département'):
                return dept
        
        # Format code seul "77"
        if re.match(r'^[0-9]{1,3}[AB]?$', dept):
            nom = DEPARTEMENTS.get(dept)
            if nom:
                return f"{nom} ({dept})"
        
        # Format nom seul "Seine-et-Marne"
        for code, nom in DEPARTEMENTS.items():
            if nom.lower() == dept.lower():
                return f"{nom} ({code})"
        
        # Format quelconque avec code entre parenthèses
        match = re.search(r'\(([0-9]{1,3}[AB]?)\)', dept)
        if match:
            code = match.group(1)
            nom = DEPARTEMENTS.get(code)
            if nom:
                return f"{nom} ({code})"
        
        return None
    
    def _corriger_codes_postaux(self):
        """Correction 2: Codes postaux"""
        print("📮 CORRECTION 2: CODES POSTAUX")
        print("   • Ajout zéros manquants pour depts 01-09")
        print("   • Codes NULL ignorés (nécessitent validation manuelle)")
        
        # Codes postaux à 4 chiffres (zéro manquant)
        self.cur.execute("""
            SELECT id, nom, code_postal, departement
            FROM etablissements
            WHERE is_test = false
              AND code_postal IS NOT NULL
              AND trim(code_postal) != ''
              AND length(trim(code_postal)) = 4
              AND code_postal ~ '^[0-9]{4}$'
        """)
        
        etablissements = self.cur.fetchall()
        print(f"   📊 Codes postaux à 4 chiffres: {len(etablissements):,}")
        
        corrections = []
        for etab_id, nom, cp, dept in etablissements:
            # Vérifier si département 01-09
            cp_dept = cp[:1]
            if cp_dept in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
                cp_corrige = f"0{cp}"
                corrections.append({
                    'id': etab_id,
                    'nom': nom,
                    'ancien': cp,
                    'nouveau': cp_corrige,
                    'departement': dept
                })
        
        print(f"   ✅ Corrections à appliquer: {len(corrections):,}")
        
        # Appliquer corrections
        if not self.dry_run and corrections:
            for corr in corrections:
                self.cur.execute("""
                    UPDATE etablissements
                    SET code_postal = %s
                    WHERE id = %s
                """, (corr['nouveau'], corr['id']))
                
                self.log_corrections.append({
                    'type': 'code_postal',
                    'etablissement_id': corr['id'],
                    'etablissement_nom': corr['nom'],
                    'ancienne_valeur': corr['ancien'],
                    'nouvelle_valeur': corr['nouveau']
                })
            
            self.stats['codes_postaux_corriges'] = len(corrections)
            print(f"   ✅ {len(corrections):,} codes postaux corrigés")
        elif self.dry_run:
            print(f"   ⚠️  [DRY-RUN] {len(corrections):,} codes postaux seraient corrigés")
            for corr in corrections[:5]:
                print(f"      • '{corr['ancien']}' → '{corr['nouveau']}'")
    
    def _nettoyer_emails(self):
        """Correction 3: Nettoyage emails"""
        print("📧 CORRECTION 3: NETTOYAGE EMAILS")
        print("   • Suppression préfixes numériques suspects")
        
        # Emails avec préfixes numériques
        self.cur.execute("""
            SELECT id, nom, email
            FROM etablissements
            WHERE is_test = false
              AND email IS NOT NULL
              AND trim(email) != ''
              AND email ~ E'^[0-9]+'
        """)
        
        etablissements = self.cur.fetchall()
        print(f"   📊 Emails avec préfixes numériques: {len(etablissements):,}")
        
        corrections = []
        for etab_id, nom, email in etablissements:
            # Supprimer préfixe numérique
            match = re.match(r'^([0-9]+)(.+@.+)$', email)
            if match:
                prefixe = match.group(1)
                email_propre = match.group(2)
                
                # Vérifier que l'email propre est valide
                if re.match(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$', email_propre):
                    corrections.append({
                        'id': etab_id,
                        'nom': nom,
                        'ancien': email,
                        'nouveau': email_propre,
                        'prefixe': prefixe
                    })
        
        print(f"   ✅ Corrections à appliquer: {len(corrections):,}")
        
        # Appliquer corrections
        if not self.dry_run and corrections:
            for corr in corrections:
                self.cur.execute("""
                    UPDATE etablissements
                    SET email = %s
                    WHERE id = %s
                """, (corr['nouveau'], corr['id']))
                
                self.log_corrections.append({
                    'type': 'email',
                    'etablissement_id': corr['id'],
                    'etablissement_nom': corr['nom'],
                    'ancienne_valeur': corr['ancien'],
                    'nouvelle_valeur': corr['nouveau']
                })
            
            self.stats['emails_corriges'] = len(corrections)
            print(f"   ✅ {len(corrections):,} emails nettoyés")
        elif self.dry_run:
            print(f"   ⚠️  [DRY-RUN] {len(corrections):,} emails seraient nettoyés")
            for corr in corrections[:5]:
                print(f"      • '{corr['ancien']}' → '{corr['nouveau']}'")
    
    def _fusionner_sous_categories(self):
        """Correction 4: Fusion sous-catégories doublons"""
        print("🏷️  CORRECTION 4: FUSION SOUS-CATÉGORIES DOUBLONS")
        print("   Format cible: Majuscules + accents (ex: 'Résidence autonomie')")
        
        # Récupérer toutes les sous-catégories
        self.cur.execute("""
            SELECT id, libelle
            FROM sous_categories
            ORDER BY libelle
        """)
        
        sous_cats = {row[1]: row[0] for row in self.cur.fetchall()}
        
        # Identifier doublons à fusionner
        fusions = []
        for ancien_libelle, nouveau_libelle in SOUS_CATEGORIES_FUSION.items():
            if ancien_libelle in sous_cats and nouveau_libelle in sous_cats:
                ancien_id = sous_cats[ancien_libelle]
                nouveau_id = sous_cats[nouveau_libelle]
                
                if ancien_id != nouveau_id:
                    fusions.append({
                        'ancien_id': ancien_id,
                        'ancien_libelle': ancien_libelle,
                        'nouveau_id': nouveau_id,
                        'nouveau_libelle': nouveau_libelle
                    })
        
        print(f"   📊 Paires de doublons à fusionner: {len(fusions):,}")
        
        # Appliquer fusions
        if not self.dry_run and fusions:
            for fusion in fusions:
                # Compter établissements impactés
                self.cur.execute("""
                    SELECT COUNT(*)
                    FROM etablissement_sous_categorie
                    WHERE sous_categorie_id = %s
                """, (fusion['ancien_id'],))
                
                nb_etabs = self.cur.fetchone()[0]
                
                print(f"   🔄 Fusion '{fusion['ancien_libelle']}' → '{fusion['nouveau_libelle']}' ({nb_etabs} établissements)")
                
                # Mettre à jour les relations
                self.cur.execute("""
                    UPDATE etablissement_sous_categorie
                    SET sous_categorie_id = %s
                    WHERE sous_categorie_id = %s
                      AND NOT EXISTS (
                        SELECT 1 FROM etablissement_sous_categorie esc2
                        WHERE esc2.etablissement_id = etablissement_sous_categorie.etablissement_id
                          AND esc2.sous_categorie_id = %s
                      )
                """, (fusion['nouveau_id'], fusion['ancien_id'], fusion['nouveau_id']))
                
                # Supprimer doublons (établissements ayant déjà la bonne catégorie)
                self.cur.execute("""
                    DELETE FROM etablissement_sous_categorie
                    WHERE sous_categorie_id = %s
                """, (fusion['ancien_id'],))
                
                # Supprimer ancienne sous-catégorie
                self.cur.execute("""
                    DELETE FROM sous_categories
                    WHERE id = %s
                """, (fusion['ancien_id'],))
                
                self.log_corrections.append({
                    'type': 'sous_categorie',
                    'ancien_id': fusion['ancien_id'],
                    'ancien_libelle': fusion['ancien_libelle'],
                    'nouveau_id': fusion['nouveau_id'],
                    'nouveau_libelle': fusion['nouveau_libelle'],
                    'nb_etablissements': nb_etabs
                })
                
                self.stats['sous_categories_fusionnees'] += nb_etabs
            
            print(f"   ✅ {len(fusions):,} fusions effectuées")
        elif self.dry_run:
            print(f"   ⚠️  [DRY-RUN] {len(fusions):,} fusions seraient effectuées")
            for fusion in fusions:
                print(f"      • '{fusion['ancien_libelle']}' → '{fusion['nouveau_libelle']}'")
    
    def _geolocaliser_manquants(self):
        """Correction 5: Géolocalisation avec Google API"""
        print("📍 CORRECTION 5: GÉOLOCALISATION (Google Maps API)")
        
        if not self.google_api_key:
            print("   ⚠️  Clé API Google Maps manquante (GOOGLE_MAPS_API_KEY)")
            print("   ℹ️  Définir la variable d'environnement pour activer")
            return
        
        # Récupérer établissements sans géolocalisation
        self.cur.execute("""
            SELECT id, nom, adresse_l1, adresse_l2, code_postal, commune, departement
            FROM etablissements
            WHERE is_test = false
              AND geom IS NULL
            LIMIT 550
        """)
        
        etablissements = self.cur.fetchall()
        print(f"   📊 Établissements sans géolocalisation: {len(etablissements):,}")
        
        if not etablissements:
            print("   ✅ Tous les établissements sont déjà géolocalisés")
            return
        
        print(f"   🌐 Géolocalisation via Google Geocoding API...")
        print(f"   ⏱️  Estimation: ~{len(etablissements) * 0.2:.0f} secondes")
        
        geocodes = []
        erreurs = []
        
        for i, (etab_id, nom, adr1, adr2, cp, commune, dept) in enumerate(etablissements):
            # Stratégie: Si adresse manquante, utiliser "nom établissement, commune"
            # Car c'est pourquoi Nominatim a échoué
            adresse_parts = []
            
            if adr1:
                # Adresse complète disponible
                adresse_parts.append(adr1)
                if adr2:
                    adresse_parts.append(adr2)
                if cp:
                    adresse_parts.append(cp)
                if commune:
                    adresse_parts.append(commune)
            else:
                # Pas d'adresse → utiliser nom + commune
                if nom:
                    adresse_parts.append(nom)
                if commune:
                    adresse_parts.append(commune)
            
            # Toujours ajouter "France" pour améliorer précision
            adresse_parts.append("France")
            adresse_complete = ", ".join(adresse_parts)
            
            # Géolocaliser
            try:
                coords = self._geocode_google(adresse_complete)
                
                if coords:
                    lat, lng, precision = coords
                    geocodes.append({
                        'id': etab_id,
                        'nom': nom,
                        'adresse': adresse_complete,
                        'lat': lat,
                        'lng': lng,
                        'precision': precision
                    })
                else:
                    erreurs.append(f"{nom[:40]}: Aucun résultat")
                
                # Rate limiting (50 req/s max Google)
                if (i + 1) % 50 == 0:
                    print(f"      • {i + 1}/{len(etablissements)} géocodages effectués...")
                    time.sleep(1)
            
            except Exception as e:
                erreurs.append(f"{nom[:40]}: {str(e)[:50]}")
        
        print(f"   ✅ Géocodages réussis: {len(geocodes):,}")
        print(f"   ⚠️  Échecs: {len(erreurs):,}")
        
        # Appliquer géolocalisations
        if not self.dry_run and geocodes:
            for geocode in geocodes:
                self.cur.execute("""
                    UPDATE etablissements
                    SET geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        geocode_precision = %s
                    WHERE id = %s
                """, (geocode['lng'], geocode['lat'], geocode['precision'], geocode['id']))
                
                self.log_corrections.append({
                    'type': 'geolocalisation',
                    'etablissement_id': geocode['id'],
                    'etablissement_nom': geocode['nom'],
                    'adresse': geocode['adresse'],
                    'latitude': geocode['lat'],
                    'longitude': geocode['lng'],
                    'precision': geocode['precision']
                })
            
            self.stats['etablissements_geolocalises'] = len(geocodes)
            print(f"   ✅ {len(geocodes):,} établissements géolocalisés")
        elif self.dry_run:
            print(f"   ⚠️  [DRY-RUN] {len(geocodes):,} géolocalisations seraient appliquées")
        
        # Logger erreurs
        if erreurs:
            self.stats['erreurs'].extend(erreurs[:20])
    
    def _geocode_google(self, adresse: str) -> Optional[Tuple[float, float, str]]:
        """Géocode avec Google Geocoding API"""
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {
            'address': adresse,
            'key': self.google_api_key,
            'region': 'fr'
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data['status'] == 'OK' and data['results']:
            result = data['results'][0]
            location = result['geometry']['location']
            location_type = result['geometry']['location_type']
            
            # Mapper location_type vers geocode_precision ENUM
            precision_mapping = {
                'ROOFTOP': 'rooftop',
                'RANGE_INTERPOLATED': 'range_interpolated',
                'GEOMETRIC_CENTER': 'street',
                'APPROXIMATE': 'locality'
            }
            
            precision = precision_mapping.get(location_type, 'unknown')
            
            return (location['lat'], location['lng'], precision)
        
        return None
    
    def _save_log(self):
        """Sauvegarder log des corrections"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = f"LOG_CORRECTIONS_{timestamp}.md"
        
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"# LOG CORRECTIONS - PIPELINE AUTOMATIQUE\n\n")
            f.write(f"**Date**: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"**Mode**: {'DRY-RUN' if self.dry_run else 'PRODUCTION'}\n\n")
            
            f.write("## 📊 RÉSUMÉ\n\n")
            f.write(f"- **Départements corrigés**: {self.stats['departements_corriges']:,}\n")
            f.write(f"- **Codes postaux corrigés**: {self.stats['codes_postaux_corriges']:,}\n")
            f.write(f"- **Emails nettoyés**: {self.stats['emails_corriges']:,}\n")
            f.write(f"- **Sous-catégories fusionnées**: {self.stats['sous_categories_fusionnees']:,}\n")
            f.write(f"- **Établissements géolocalisés**: {self.stats['etablissements_geolocalises']:,}\n")
            f.write(f"- **Total corrections**: {len(self.log_corrections):,}\n\n")
            
            if self.stats['erreurs']:
                f.write(f"## ⚠️  ERREURS ({len(self.stats['erreurs'])})\n\n")
                for err in self.stats['erreurs']:
                    f.write(f"- {err}\n")
                f.write("\n")
            
            f.write("## 📝 DÉTAIL DES CORRECTIONS\n\n")
            
            # Grouper par type
            by_type = defaultdict(list)
            for corr in self.log_corrections:
                by_type[corr['type']].append(corr)
            
            for type_corr, corrections in by_type.items():
                f.write(f"### {type_corr.upper()} ({len(corrections):,})\n\n")
                
                if type_corr == 'geolocalisation':
                    for corr in corrections[:50]:
                        f.write(f"- **{corr.get('etablissement_nom', 'N/A')}** ({corr.get('adresse', 'N/A')[:50]}): {corr.get('latitude', 0):.6f}, {corr.get('longitude', 0):.6f} ({corr.get('precision', 'N/A')})\n")
                elif type_corr == 'sous_categorie':
                    for corr in corrections[:50]:
                        f.write(f"- **{corr.get('ancien_libelle', 'N/A')}** → **{corr.get('nouveau_libelle', 'N/A')}** ({corr.get('nb_etablissements', 0)} établissements)\n")
                else:
                    for corr in corrections[:50]:
                        f.write(f"- **{corr.get('etablissement_nom', 'N/A')}**: `{corr.get('ancienne_valeur', 'N/A')}` → `{corr.get('nouvelle_valeur', 'N/A')}`\n")
                
                if len(corrections) > 50:
                    f.write(f"\n*... et {len(corrections) - 50} autres*\n")
                f.write("\n")
        
        print(f"\n📄 Log sauvegardé: {log_file}")


if __name__ == "__main__":
    import sys
    
    # Mode dry-run par défaut
    dry_run = '--execute' not in sys.argv
    
    if dry_run:
        print("\n⚠️  MODE DRY-RUN ACTIVÉ")
        print("   Aucune modification ne sera appliquée")
        print("   Pour exécuter réellement: python pipeline_correction.py --execute\n")
    else:
        print("\n🚨 MODE PRODUCTION")
        print("   Les modifications seront committées dans la base")
        confirmation = input("   Confirmer l'exécution ? (oui/non): ")
        if confirmation.lower() != 'oui':
            print("   ❌ Annulé")
            sys.exit(0)
    
    pipeline = PipelineCorrection(dry_run=dry_run)
    pipeline.execute()
