# 🚀 PIPELINE COMPLET - QUALITY CHECK & ENRICHISSEMENT AVANCÉ

## 📊 ÉTAT ACTUEL DE LA BASE (30/01/2026)

### Vue d'ensemble
- **Total**: 2,795 établissements réels
- **Types**: 2,095 résidences | 567 habitat_partage | 97 logement_independant | 36 non défini
- **Statut**: 2,743 publiés | 52 draft
- **Éligibilité**: 1,742 à_verifier | 1,031 non_eligible | 22 avp_eligible

### Données de base (table `etablissements`)
| Champ | Couverture | Observations |
|-------|-----------|--------------|
| Présentation | 99.5% | ✅ Très bon |
| Téléphone | 93.6% | ✅ Bon |
| Site web | 95.4% | ✅ Bon |
| Géolocalisation | 80.6% | ⚠️ À améliorer (19% manquants) |
| Email | 60.1% | ⚠️ Moyen (40% manquants) |
| Département | Variable | ⚠️ Normalisation nécessaire |

### 🔴 Tables d'enrichissement - DONNÉES CRITIQUES À COMPLÉTER

#### 💰 TARIFICATIONS - **9.3% seulement** (259/2,795)
```
Données présentes:
  • Fourchette prix: 99.6% des tarifs entrés
  • Prix min/max: 71.0% des tarifs entrés
  
Données ABSENTES (0%):
  • loyer_base
  • charges
  • periode
  • source
  
❌ PRIORITÉ MAXIMALE: 90.7% des établissements SANS tarifs
```

#### 🏠 LOGEMENTS_TYPES - 31.6% (882/2,795)
```
Données présentes:
  • Libellé: 100% des logements entrés
  • PMR accessible: 8.2%
  • Plain-pied: 3.7%
  
Données QUASI ABSENTES:
  • Surface min/max: 0.1% ❌
  • Domotique: 0% ❌
  • Meublé: 0.05% ❌
  
❌ 68.4% des établissements SANS types de logements
```

#### 🛎️ SERVICES - 34.1% (952/2,795)
```
Seulement 6 types de services:
  1. conciergerie (577)
  2. activités organisées (541)
  3. espace_partage (505)
  4. commerces à pied (275)
  5. personnel de nuit (53)
  6. médecin intervenant (10)
  
❌ 65.9% des établissements SANS services
❌ Palette de services TRÈS limitée (devrait être 20-30 types)
```

#### 🍽️ RESTAURATIONS - 100% mais données booléennes basiques
```
Données présentes (booléens):
  • Kitchenette: 254
  • Resto collectif: 147
  • Resto midi: 24
  • Portage repas: 263
  
⚠️ Manque: détails menus, prix repas, types cuisine, horaires
```

---

## 🎯 OBJECTIFS DU PIPELINE

### 1. QUALITY CHECK (Normalisation & Validation)
- ✅ Normaliser les départements (format cohérent)
- ✅ Valider et corriger les sites web officiels
  - Priorité: résultats hors annuaires gouvernementaux
  - Vérifier authenticité vs sites agrégateurs
- ✅ Compléter géolocalisations manquantes (19%)
- ✅ Valider emails (format + domaine)
- ✅ Détecter et nettoyer doublons potentiels
- ✅ Vérifier cohérence habitat_type vs sous_categories

### 2. ENRICHISSEMENT AVANCÉ - Tables Complémentaires

#### 🔴 PRIORITÉ 1: TARIFICATIONS (90% manquants!)
**Objectif**: Passer de 9% à 60-70% de couverture

**Sources d'extraction**:
1. **ScrapingBee**: Scraping pages tarifs des sites web
   - Extraction HTML/JS des tableaux de prix
   - Navigation multi-pages (jusqu'à 3 pages de tarifs)
   - Pattern recognition: "à partir de XXX€", "loyer XXX€/mois"

2. **Groq Vision** (multimodal): Analyse des visuels de tarifs
   - Screenshots de pages tarifs
   - PDFs de grilles tarifaires
   - Images de brochures

3. **Groq LLM** (llama-3.3-70b-versatile): Extraction structurée
   - Parsing HTML → JSON structuré
   - Extraction loyer, charges, services inclus
   - Détection période (mois/jour/semaine)

**Données cibles**:
```sql
- fourchette_prix (ENUM: euro, deux_euros, trois_euros)
- prix_min / prix_max (numeric)
- loyer_base (numeric) ← NOUVEAU
- charges (numeric) ← NOUVEAU
- periode (text: 'mois', 'jour', 'semaine') ← NOUVEAU
- source (text: URL source) ← NOUVEAU
- date_observation (date)
```

#### 🔴 PRIORITÉ 2: LOGEMENTS_TYPES (68% manquants)
**Objectif**: Passer de 31% à 70-80% de couverture

**Sources d'extraction**:
1. **ScrapingBee + LLM**: Pages "Nos logements" des sites
2. **Serper**: Recherche ciblée "[nom résidence] + types logements surfaces"
3. **Groq Vision**: Analyse plans/photos de logements

**Données cibles**:
```sql
- libelle (text) ← Déjà bon
- surface_min / surface_max (numeric) ← 0.1% actuellement ❌
- pmr (boolean) ← 8% actuellement
- domotique (boolean) ← 0% actuellement ❌
- meuble (boolean) ← 0% actuellement ❌
- plain_pied (boolean) ← 3.7% actuellement
- nb_unites (integer) ← Nouveau enrichissement
```

#### 🔴 PRIORITÉ 3: SERVICES (65% manquants + palette limitée)
**Objectif**: 
- Couverture: 34% → 80%
- Services disponibles: 6 → 25-30 types

**Nouveaux services à détecter** (via scraping + LLM):
```
Services existants (6):
- conciergerie, activités organisées, espace_partage
- commerces à pied, personnel de nuit, médecin intervenant

Services à ajouter (20+):
- coiffeur, pédicure, esthéticienne
- bibliothèque, salle de sport, piscine
- navette, parking, garage
- blanchisserie, ménage, aide à domicile
- infirmier, kinésithérapeute, ergothérapeute
- restaurant gastronomique, bar/salon de thé
- jardin, terrasse, potager
- wifi, télévision, téléphone
- sécurité 24/7, système alarme, télésurveillance
- animaux acceptés, garde animaux
- salon coiffure, spa, balnéothérapie
```

**Sources**:
1. **ScrapingBee**: Pages "Services" / "Prestations"
2. **Groq LLM**: Extraction liste services depuis HTML
3. **Serper**: Recherche "[nom] + services équipements"

#### ⚠️ PRIORITÉ 4: RESTAURATIONS (Enrichissement détails)
**Objectif**: Ajouter détails qualitatifs (au-delà des booléens)

**Nouvelles données à capturer** (extension schéma nécessaire):
```sql
ALTER TABLE restaurations ADD COLUMN IF NOT EXISTS:
- tarif_repas_min numeric
- tarif_repas_max numeric  
- type_cuisine text (traditionnelle, gastronomique, diététique)
- menu_semaine boolean
- formule_pension boolean (complète/demi-pension)
- horaires_flexibles boolean
```

---

## 🏗️ ARCHITECTURE DU PIPELINE

```
┌─────────────────────────────────────────────────────────────────┐
│         PIPELINE QUALITY CHECK + ENRICHISSEMENT AVANCÉ          │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  📋 ÉTAPE 1: CHARGEMENT & ANALYSE                                │
│  └─ Lecture base Supabase (2,795 établissements)                │
│     • Identification champs manquants par établissement          │
│     • Scoring priorité enrichissement (0-100)                    │
│     • Tri par priorité: tarifs > logements > services           │
│                                                                   │
│  ✅ ÉTAPE 2: QUALITY CHECK (Normalisation)                       │
│  └─ Corrections & validations automatiques                       │
│     • Normalisation départements (format "XX (Nom)")             │
│     • Validation sites web (hors agrégateurs)                    │
│     • Completion géolocalisation (Nominatim/Google)              │
│     • Validation emails (regex + MX records)                     │
│     • Détection doublons (fuzzy matching)                        │
│                                                                   │
│  💰 ÉTAPE 3: ENRICHISSEMENT TARIFICATIONS                        │
│  └─ Scraping + Vision + LLM (priorité max)                       │
│     ├─ ScrapingBee: Extraction pages tarifs                      │
│     │  • Navigation JS (click onglets tarifs)                    │
│     │  • Extraction tableaux HTML                                │
│     │  • Screenshots pour Vision                                 │
│     ├─ Groq Vision (llama-3.2-90b-vision): Analyse visuels      │
│     │  • OCR grilles tarifaires                                  │
│     │  • Extraction PDF/images brochures                         │
│     └─ Groq LLM (llama-3.3-70b): Structuration JSON             │
│        • Parsing HTML → données structurées                      │
│        • Extraction loyer, charges, période                      │
│        • Validation cohérence prix                               │
│                                                                   │
│  🏠 ÉTAPE 4: ENRICHISSEMENT LOGEMENTS                            │
│  └─ Scraping + Vision + Serper (priorité haute)                 │
│     ├─ ScrapingBee: Pages "Nos logements"                       │
│     ├─ Serper: Recherche "[nom] types logements surfaces"        │
│     ├─ Groq Vision: Analyse plans/photos                         │
│     └─ Groq LLM: Extraction surfaces, PMR, équipements          │
│                                                                   │
│  🛎️ ÉTAPE 5: ENRICHISSEMENT SERVICES                             │
│  └─ Scraping + LLM + Extension palette (priorité moyenne)        │
│     ├─ ScrapingBee: Pages services/prestations                  │
│     ├─ Groq LLM: Extraction liste exhaustive                    │
│     ├─ Matching avec base services étendue (30 types)            │
│     └─ Création nouveaux services si pertinents                  │
│                                                                   │
│  🍽️ ÉTAPE 6: ENRICHISSEMENT RESTAURATIONS                        │
│  └─ Scraping détails restauration (priorité basse)              │
│     ├─ ScrapingBee: Pages menus/tarifs repas                    │
│     └─ Groq LLM: Extraction prix, types cuisine, formules       │
│                                                                   │
│  ✨ ÉTAPE 7: VALIDATION QUALITÉ FINALE                           │
│  └─ Vérification cohérence & scoring qualité                     │
│     • Validation croisée données (prix cohérents)                │
│     • Calcul score qualité global (0-100)                        │
│     • Flag données suspectées (outliers)                         │
│                                                                   │
│  💾 ÉTAPE 8: MISE À JOUR BASE SUPABASE                           │
│  └─ Insertion batch avec gestion conflits                        │
│     • UPDATE établissements (champs normalisés)                  │
│     • UPSERT tarifications (avec source + date)                  │
│     • UPSERT logements_types (surfaces + équipements)            │
│     • UPSERT restaurations (détails enrichis)                    │
│     • INSERT etablissement_service (nouveaux services)           │
│     • Logging changes (audit trail)                              │
│                                                                   │
│  📊 ÉTAPE 9: REPORTING                                           │
│  └─ Génération rapport d'enrichissement                          │
│     • Statistiques avant/après par table                         │
│     • Taux couverture par champ                                  │
│     • Coûts API (Serper, ScrapingBee, Groq)                      │
│     • Liste établissements échoués                               │
│     • Recommandations actions manuelles                          │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔧 CONFIGURATION & APIs

### APIs Requises

1. **Groq** (LLM + Vision) - Aucune restriction crédit ✅
   ```
   Modèles:
   - llama-3.3-70b-versatile (texte, extraction structurée)
   - llama-3.2-90b-vision-preview (vision, OCR tarifs)
   - llama-3.1-8b-instant (fallback léger)
   ```

2. **ScrapingBee** - Aucune restriction crédit ✅
   ```
   Features:
   - JavaScript rendering
   - Screenshots (pour Vision)
   - Premium proxy rotation
   - Block resources (images si texte seul)
   ```

3. **Serper** - Aucune restriction crédit ✅
   ```
   Usage:
   - Recherches ciblées établissements
   - Complétion données manquantes
   - Validation sites officiels
   ```

4. **Nominatim** (Géocodage) - Gratuit, rate-limited
   ```
   Usage:
   - Géolocalisation adresses manquantes
   - Fallback si Google Maps non dispo
   ```

### Variables d'environnement
```env
# Base de données
DB_HOST=db.minwoumfgutampcgrcbr.supabase.co
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=***
DB_PORT=5432

# APIs
GROQ_API_KEY=***
SCRAPINGBEE_API_KEY=***
SERPER_API_KEY=***

# Configuration pipeline
BATCH_SIZE=50  # Traitement par lots
MAX_CONCURRENT_REQUESTS=10  # Parallélisation
QUALITY_CHECK_ONLY=false  # Mode quality check seul
ENRICHMENT_PRIORITY=tarifs,logements,services,restaurations
```

---

## 🎮 MODES D'EXÉCUTION

### Mode 1: Quality Check SEUL (rapide, ~5min pour 2,795 étabs)
```bash
python pipeline_quality_enrichment.py \
  --mode quality-check \
  --batch-size 100
```
**Résultat**: Normalisation + validation, pas d'enrichissement

### Mode 2: Enrichissement COMPLET (lent, ~8-12h pour 2,795 étabs)
```bash
python pipeline_quality_enrichment.py \
  --mode full \
  --priorities tarifs,logements,services,restaurations
```
**Résultat**: Quality check + tous enrichissements

### Mode 3: Enrichissement CIBLÉ (moyen, ~2-4h)
```bash
python pipeline_quality_enrichment.py \
  --mode targeted \
  --priorities tarifs,logements \
  --habitat-types residence,habitat_partage \
  --max-records 1000
```
**Résultat**: Quality check + enrichissement tarifs+logements sur 1000 résidences prioritaires

### Mode 4: DÉPARTEMENT SPÉCIFIQUE
```bash
python pipeline_quality_enrichment.py \
  --mode full \
  --department 77 \
  --priorities tarifs,logements,services
```
**Résultat**: Enrichissement complet d'un département

### Mode 5: RÉPARATION (établissements avec données incomplètes)
```bash
python pipeline_quality_enrichment.py \
  --mode repair \
  --min-quality-score 50 \
  --priorities tarifs
```
**Résultat**: Ré-enrichissement des établissements avec score qualité < 50

---

## 📈 RÉSULTATS ATTENDUS

### Objectifs de couverture (après exécution complète)

| Table | Avant | Objectif | Gain |
|-------|-------|----------|------|
| **TARIFICATIONS** | 9.3% | 60-70% | **+600-700%** ⭐ |
| **LOGEMENTS (surfaces)** | 0.1% | 50-60% | **+50,000%** ⭐ |
| **SERVICES** | 34.1% | 75-80% | **+120%** ⭐ |
| **RESTAURATIONS (détails)** | Booléens | Détails qualitatifs | Nouveau |
| **Géolocalisation** | 80.6% | 95%+ | +15% |
| **Emails** | 60.1% | 75%+ | +15% |

### Estimation coûts & temps

**Pour 2,795 établissements (enrichissement complet)**:

- **Groq LLM**: ~8,000 requêtes × $0 = **$0** (gratuit) ✅
- **Groq Vision**: ~2,000 requêtes × $0 = **$0** (gratuit) ✅
- **ScrapingBee**: ~5,500 requêtes × $0 = **$0** (crédits illimités) ✅
- **Serper**: ~3,000 requêtes × $0 = **$0** (crédits illimités) ✅

**COÛT TOTAL: $0** 🎉

**Temps d'exécution** (parallélisation max=10):
- Quality check: ~30 minutes
- Enrichissement tarifs: ~4-5 heures
- Enrichissement logements: ~3-4 heures  
- Enrichissement services: ~2-3 heures
- Enrichissement restaurations: ~1-2 heures

**TOTAL: 10-15 heures** en une seule exécution

---

## 🚦 PRIORITÉS & ROADMAP

### Phase 1: QUICK WINS (1-2 jours développement)
- ✅ Quality check (normalisation départements, validation sites)
- ✅ Enrichissement tarifications (priorité max, ROI énorme)
- ✅ CLI avec modes d'exécution flexibles

### Phase 2: ENRICHISSEMENT MAJEUR (3-4 jours développement)
- ✅ Enrichissement logements (surfaces, PMR, équipements)
- ✅ Extension palette services (6 → 30 types)
- ✅ Groq Vision pour extraction visuels tarifs/logements

### Phase 3: RAFFINEMENT (2-3 jours développement)
- ✅ Enrichissement restaurations (détails qualitatifs)
- ✅ Scoring qualité automatique
- ✅ Reporting avancé + dashboard metrics

### Phase 4: PRODUCTION (1-2 jours)
- ✅ Logging & monitoring
- ✅ Gestion erreurs robuste
- ✅ Reprise sur échec (checkpoint/resume)
- ✅ Dry-run mode (simulation sans écriture DB)

---

## 💡 INNOVATIONS TECHNIQUES

1. **Scoring Priorité Dynamique**: Chaque établissement reçoit un score 0-100 selon:
   - Importance (type résidence > habitat_partage)
   - Données manquantes (plus de gaps = plus prioritaire)
   - Qualité site web (bon site = plus extractible)
   - Département (urbain = plus de concurrence)

2. **Extraction Multi-Modale**:
   - HTML/Text → Groq LLM
   - Images/PDFs → Groq Vision
   - Fusion intelligente des sources

3. **Validation Croisée**:
   - Prix cohérents avec fourchette + département
   - Surfaces cohérentes avec type logement
   - Services cohérents avec type habitat

4. **Cache Intelligent**:
   - Cache Serper (éviter requêtes dupliquées)
   - Cache scraping (réutiliser HTML déjà extrait)
   - Cache LLM (même prompt = même réponse)

5. **Batch Processing Optimisé**:
   - Groupement requêtes similaires
   - Parallélisation intelligente (I/O-bound)
   - Rate limiting automatique

---

## 🎯 PROCHAINES ÉTAPES

1. **Validation architecture** avec vous ✅
2. **Développement modules Core** (quality check, orchestration)
3. **Développement enrichisseurs** (tarifs, logements, services)
4. **Tests sur échantillon** (département 77 = 26 établissements)
5. **Exécution complète** sur les 2,795 établissements
6. **Analyse résultats** + rapport détaillé

Voulez-vous que je commence le développement du pipeline ? 🚀
