# DIAGNOSTIC PIPELINE MODULES 3+4+4.5 - HABITAT SENIORS

## Vue d'ensemble

Ce document analyse en détail les modules du pipeline d'enrichissement automatique d'établissements d'habitat seniors/handicap, suite au test complet sur le département de l'Aube (10) effectué le 2 décembre 2025.

---

## OBJECTIF GLOBAL DU PIPELINE

Le pipeline vise à **automatiser la découverte et l'enrichissement** d'établissements d'habitat intermédiaire pour seniors et personnes handicapées en France, afin d'alimenter une base de données nationale complète et fiable.

### **Cible Métier - Classification Habitat**
- **residence** ← Résidence autonomie, MARPA, Résidence services seniors
- **habitat_partage** ← Habitat inclusif, Accueil familial, Maison d'accueil familial, Habitat intergénérationnel  
- **logement_independant** ← Béguinage, Village seniors, habitat regroupé (logement adapté PMR)

### **Processus Global**
```
MODULE 1 → MODULE 2 → MODULE 3 → MODULE 4 → MODULE 4.5 → CSV FINAL
   ↓         ↓         ↓         ↓          ↓
Scraping  Scraping  Recherche  Validation  Enrichi.  → Base Production
Officiel  Spécialisé  Web      LLM        Web
```

---

## ARCHITECTURE COMPLÈTE - 5 MODULES

### **MODULE 1 - SCRAPING ANNUAIRES OFFICIELS PA** 
**Fichier** : `mvp/scrapers/official_scraper.py`  
**Fonction** : Extraction automatisée des annuaires officiels personnes âgées

#### Capacités
- Scraping annuaires gouvernementaux pour-les-personnes-agees.gouv.fr
- Extraction Résidences autonomie + Résidences services seniors
- Parsing fiches établissements détaillées (nom, adresse, gestionnaire)
- Normalisation données extraites des sources officielles

#### Sources ciblées
```
- https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-autonomie/{dept}
- https://www.pour-les-personnes-agees.gouv.fr/annuaire-residences-service/{dept}
- Couverture: 70% des établissements (Résidences autonomie + MARPA)
```

### **MODULE 2 - SCRAPING CHAÎNES PRIVÉES**
**Fichier** : `architecture_mvp.py` (classe PrivateChainScraper)  
**Fonction** : Extraction annuaires chaînes privées spécialisées

#### Traitements appliqués  
- **Scraping chaînes privées** : DOMITYS, Espace & Vie, Senioriales, Happy Senior
- **Extraction sites corporate** : Pages localisateur d'établissements
- **Parsing annuaires métier** : API Résidence, autres réseaux
- **Couverture ciblée** : 15% des établissements (résidences services haut de gamme)

#### Chaînes ciblées
```python
# Réseaux privés spécialisés seniors
- DOMITYS (résidences services)
- Espace & Vie (résidences services)  
- Senioriales (résidences services)
- Happy Senior (résidences services)
- API Résidence (résidences services)
```

---

## FORMAT CSV FINAL ATTENDU

### **Structure Standard - 16 Colonnes Obligatoires**
```csv
nom,commune,code_postal,gestionnaire,adresse_l1,telephone,email,site_web,sous_categories,habitat_type,eligibilite_avp,presentation,departement,source,date_extraction,public_cible
```

### **Spécifications Détaillées**

| Colonne | Type | Obligatoire | Format/Valeurs | Exemple |
|---------|------|------------|----------------|---------|
| **nom** | string | ✅ | Nom officiel établissement | "Maison partagée Ages & Vie" |
| **commune** | string | ✅ | Nom commune française | "Troyes" |
| **code_postal** | string | ✅ | Format XXXXX | "10000" |
| **gestionnaire** | string | ✅ | Organisme/Particulier | "Ages & Vie SAS" |
| **adresse_l1** | string | ⚠️ | Adresse postale principale | "15 rue de la Paix" |
| **telephone** | string | ⚠️ | Format FR standard | "03 25 45 67 89" |
| **email** | string | ⚠️ | Email valide | "contact@agesetvie.com" |
| **site_web** | string | ⚠️ | URL complète | "https://agesetvie.com" |
| **sous_categories** | string | ✅ | Valeurs contrôlées | "Habitat partagé" |
| **habitat_type** | string | ✅ | Type standardisé | "habitat_partage" |
| **eligibilite_avp** | string | ✅ | Statut AVP | "a_verifier" |
| **presentation** | text | ⚠️ | Description 200-600 chars | "Habitat partagé de 8 logements..." |
| **departement** | string | ✅ | Nom département | "Aube" |
| **source** | string | ✅ | URL source extraction | "https://source.com/page" |
| **date_extraction** | date | ✅ | Format YYYY-MM-DD | "2025-12-02" |
| **public_cible** | string | ✅ | Population ciblée | "personnes_agees" |

### **Valeurs Contrôlées**

#### habitat_type (Champ Pivot)
```
- residence            # Résidence autonomie, MARPA, Résidence services seniors
- habitat_partage      # Habitat inclusif, Accueil familial, Maison d'accueil familial, Habitat intergénérationnel
- logement_independant # Béguinage, Village seniors, habitat regroupé (logement adapté PMR)
```

#### sous_categories (Classification Détaillée)
```
- "Habitat partagé"
- "Habitat inclusif" 
- "Résidence autonomie"
- "Accueil familial"
- "Béguinage"
- "MARPA"
- "Résidence services seniors"
- "Village seniors"
- "Logement adapté PMR"
```

#### eligibilite_avp (Éligibilité Aide à la Vie Partagée)
```
- avp_eligible    # Éligible aide à la vie partagée
- non_eligible    # Hors critères AVP
- a_verifier      # Statut indéterminé (défaut)
```

#### public_cible
```
- personnes_agees           # Seniors 60+
- personnes_handicapees     # Personnes en situation de handicap
- mixtes                    # Intergénérationnel
- alzheimer_accessible      # Spécialisé troubles cognitifs
```

### **Contraintes de Validation**
```python
# Champs obligatoires pour publication
REQUIRED_FIELDS = [
    'nom', 'commune', 'code_postal', 
    'gestionnaire', 'habitat_type'
]

# Validation géographique
- code_postal doit correspondre au departement
- commune doit exister dans le département
- geolocalisation cohérente (latitude/longitude)

# Format téléphone
- Regex: r"^(?:\+33|0)[1-9](?:[0-9]{8})$"
- Espaces autorisés: "01 23 45 67 89"

# Format email  
- RFC compliant avec domaine valide
- Pas d'emails génériques (noreply@, admin@)
```

---

## MODULE 3 - ALTERNATIVE SEARCH SCRAPER

### **Fonctionnalité**
Recherche automatique de candidats établissements via Google Search avec 3 stratégies complémentaires.

### **Fichier Principal**
**Script** : `mvp/scrapers/alternative_scraper.py`

### **Architecture**
```
alternative_scraper.py
├── Stratégie "réseaux_spécifiques" (30% poids)
├── Stratégie "ciblée_efficace" (50% poids) 
└── Stratégie "institutionnelle_légère" (20% poids)
```

### **Performance Mesurée (Aube)**
- **54 candidats totaux** trouvés via Google Search
- **26 candidats validés** par le pre-filtre interne (48%)
- **Durée d'exécution** : ~4 minutes

### **Stratégies de Recherche**

#### 1. Réseaux Spécifiques (30%)
```
"Ages & Vie habitat partagé Aube"
"CetteFamille maison accueil familial Aube"  
"UDAF 10 habitat inclusif"
```
**Résultat** : 9 candidats (focus sur opérateurs connus)

#### 2. Ciblée Efficace (50%)
```
"habitat partagé seniors Aube"
"habitat inclusif personnes âgées Aube"
"logement intergénérationnel Aube"
"habitat partagé personnes handicapées Aube"
"béguinage liste Aube"
```
**Résultat** : 33 candidats (recherche large par typologie)

#### 3. Institutionnelle Légère (20%)
```
"liste habitat inclusif Aube filetype:pdf"
"service accueil familial départemental Aube"
```
**Résultat** : 12 candidats (sources officielles)

### **✅ Points Forts**
- Couverture large et systématique
- Stratégies complémentaires efficaces
- Détection correcte des opérateurs majeurs (Ages & Vie)
- Pre-filtre fonctionnel

### **❌ Points Faibles**
- **Bruit important** : beaucoup de faux positifs (articles, annuaires vides)
- **Redondance** : mêmes sites trouvés par plusieurs stratégies
- **Manque de spécificité géographique** : résultats hors département

---

## MODULE 4 - LLM VALIDATOR V2

### **Fonctionnalité**
Validation et extraction en 3 étapes via API Groq (llama-3.1-8b-instant et llama-3.1-70B).

### **Fichier Principal**
**Script** : `mvp/scrapers/llm_validator_v2.py`

### **Architecture Pipeline**
```
llm_validator_v2.py
├── ÉTAPE 1: Pre-filtre gratuit (élimination évidente)
├── ÉTAPE 2: Qualification LLM légère (article vs établissement)
└── ÉTAPE 3: Extraction LLM lourde (données complètes)
```

### **Performance Mesurée (Aube)**
- **26 candidats** en entrée
- **26 candidats** après pre-filtre (100%)
- **12 candidats qualifiés** pour extraction (46.2%)
- **7 établissements extraits** (26.9% du total)
- **Coût total** : €0.0059
- **Durée** : 23.2 secondes

### **Problèmes Critiques Identifiés**

#### 1. **🧠 Hallucination LLM Majeure**
Le modèle Groq invente des établissements complets :
```csv
"Le Béguinage","Bar-sur-Aube","03 25 50 00 00","contact@lebeguinage.fr"
"L'Accueil Familial","Villemaur-sur-Vanne","03 25 20 00 00","contact@lacueilfamilial.fr"
```
**Impact** : 3/7 établissements totalement inventés

#### 2. **📝 Confusion Nominative**
Mauvaise interprétation des sources :
- Source : "colocation seniors CetteFamille" 
- Extraction : "La Maison des Aînés"
**Impact** : Noms d'établissements erronés

#### 3. **📍 Géolocalisation Incohérente**
- Page source concernant Troyes
- Établissements générés à Bar-sur-Aube, Villemaur-sur-Vanne
**Impact** : Localisation géographique incorrecte

#### 4. **🔧 Problèmes JSON Techniques**
Erreurs de parsing fréquentes :
```
"Extra data: line 3 column 1 (char 121)"
"Unterminated string starting at: line 81 column 7"
```
**Impact** : Taux d'échec extraction 5/12 (42%)

### **✅ Points Forts**
- Détection correcte des vrais établissements Ages & Vie
- Coût maîtrisé (€0.0059)
- Architecture 3-étapes cohérente
- Nettoyage JSON préambules Groq opérationnel

### **❌ Points Faibles Critiques**
- **Fiabilité inacceptable** : 71% d'établissements problématiques
- **Absence de validation** des coordonnées générées
- **Prompts insuffisamment stricts** contre l'invention
- **Pas de vérification cohérence source/extraction**

---

## MODULE 4.5 - ADAPTIVE ENRICHER

### **Fonctionnalité**
Enrichissement post-extraction des établissements incomplets via recherche web additionnelle.

### **Fichier Principal**
**Script** : `mvp/scrapers/adaptive_enricher.py`

### **Performance Mesurée (Aube)**
- **7 établissements** à enrichir en entrée
- **0 enrichissements** réussis (0%)
- **Échec total** : aucune amélioration des données

### **Causes d'Échec**
1. **Recherches infructueuses** : Pas de résultats Google pour les établissements inventés
2. **Établissements fictifs** : Impossible d'enrichir ce qui n'existe pas
3. **Requêtes inadaptées** : Termes de recherche trop génériques

### **❌ Points Faibles**
- **Inutile en l'état** : 0% d'efficacité
- **Dépendant de la qualité Module 4** : Ne peut corriger les hallucinations
- **Pas de validation préalable** de l'existence des établissements

---

## ANALYSE GLOBALE PIPELINE 3+4+4.5

### **Métriques de Performance**
```
📊 RÉSULTATS FINAUX
├── Candidats Module 3: 26
├── Établissements Module 4: 7  
├── Enrichissements Module 4.5: 0
├── Durée totale: 271.3s (4.5 min)
├── Coût: €0.0059
└── Taux de fiabilité: 28.6% (2/7 établissements valides)
```

### **Établissements Valides Extraits**
1. **Ages & Vie Essoyes** ✅
   - Vraie maison partagée, 8 chambres
   - Auxiliaires de vie 24h/24
   - Groupe Clariane/Ages & Vie

2. **Ages & Vie Charmont-sous-Barbuise** ✅
   - Même concept qu'Essoyes
   - Établissement opérationnel vérifié

### **Établissements Problématiques**
3. **"La Maison des Aînés"** ❌ - Nom inventé pour colocation CetteFamille
4. **"Le Béguinage Bar-sur-Aube"** ❌ - Totalement fabriqué
5. **"L'Accueil Familial"** ❌ - Invention complète
6. **"Mon Logis Projet 1"** ❌ - Données insuffisantes
7. **"Mon Logis Chantereigne"** ⚠️ - À vérifier (commune existante)

---

## PROBLÈMES SYSTÉMIQUES PRIORITAIRES

### **🔴 Critique - Fiabilité**
- **71% d'établissements non fiables**
- **Hallucinations LLM non contrôlées**
- **Risque de pollution base de données**

### **🟡 Majeur - Technique**
- **42% d'échecs parsing JSON**
- **Géolocalisation incohérente**
- **Absence de validation post-extraction**

### **🟡 Majeur - Qualité Données**
- **Coordonnées inventées non vérifiées**
- **Noms d'établissements erronés**
- **Confusion entre sources et extractions**

---

## RECOMMANDATIONS URGENTES

### **1. Renforcement Prompts LLM (Priorité 1)**
```
❌ Actuel: Prompts permissifs
✅ Objectif: Interdiction stricte d'invention
```
- Ajouter : "JAMAIS inventer d'établissement"
- Ajouter : "Si incertain, renvoyer établissement vide"
- Ajouter : "Vérifier cohérence nom/localisation/source"

### **2. Validation Post-Extraction (Priorité 1)**
```
❌ Actuel: Aucune validation
✅ Objectif: Contrôles automatiques
```
- Validation URLs/emails générés
- Vérification existence communes
- Cross-check coordonnées/géolocalisation

### **3. Amélioration Robustesse JSON (Priorité 2)**
```
❌ Actuel: 42% d'échecs parsing
✅ Objectif: <10% d'échecs
```
- Améliorer nettoyage réponses Groq
- Parser JSON plus tolérant
- Fallback sur extraction partielle

### **4. Module 4.5 - Refonte (Priorité 3)**
```
❌ Actuel: 0% d'efficacité
✅ Objectif: Enrichissement fonctionnel
```
- Validation préalable existence établissements
- Amélioration stratégies de recherche
- Intégration APIs métier (FINESS, etc.)

---

## OBJECTIFS DE FIABILITÉ

### **Cibles à Atteindre**
- **Taux de fiabilité** : >90% (vs 28.6% actuel)
- **Taux d'invention** : <5% (vs 43% actuel) 
- **Parsing JSON** : >90% succès (vs 58% actuel)
- **Enrichissement** : >30% efficace (vs 0% actuel)

### **Validation Qualité**
- Test sur 3 départements différents
- Validation manuelle 100% des extractions
- Monitoring continu taux d'erreur
- Alerte automatique si dérive qualité

---

**Document généré le 2 décembre 2025**  
**Pipeline Version** : Modules 1+2+3+4+4.5 complet  
**Test** : Département Aube (10) - 26 candidats  
**Status** : 🔴 Critique - Refonte nécessaire