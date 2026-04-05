# 📊 Analyse Détaillée - Migration Pipeline v2.1 → v3.0

**Date:** 2025-12-04  
**Objectif:** Analyser le pipeline actuel et planifier la migration vers v3.0 avec Mixtral multipasse

---

## 🎯 Rappel des Objectifs v3.0

- **0 hallucination** via extraction multipasse stricte (Mixtral-8x7B-32768)
- **< 0,01 € / département** en coûts d'API
- **< 3 min / département** en temps d'exécution
- **F1 très élevé** en précision d'extraction

---

## ✅ MODULES À CONSERVER (fonctionnent bien)

### 1. Module 1 - Official Scraper ✅

**Fichier:** `mvp/scrapers/official_scraper.py`

**Points forts:**
- ✅ Extraction fiable depuis annuaires gouvernementaux
- ✅ Couverture complète France métropolitaine + DOM-TOM (95 départements)
- ✅ Extraction automatique résidences autonomie + résidences services
- ✅ Gestion ScrapingBee pour pages JavaScript
- ✅ Mapping gestionnaires depuis URL
- ✅ 70% de couverture établissements

**À garder tel quel:**
```python
class OfficialScraper:
    - extract_establishments(department)
    - _extract_services_from_listing()
    - _extract_establishment_details()
    - _generate_dept_urls()  # Auto-génération URLs
```

**Raison:** Ce module fonctionne parfaitement et fournit des données de haute qualité depuis sources officielles.

---

### 2. Module 6/7 - Intelligent Deduplicator ✅

**Fichier:** `mvp/deduplication/intelligent_deduplicator.py`

**Points forts:**
- ✅ Détection multi-niveaux (adresse exacte + similarité)
- ✅ Validation LLM pour cas ambigus (60-99%)
- ✅ Fusion intelligente (conservation du plus complet)
- ✅ Traçabilité complète des fusions
- ✅ Surveillance spéciale pour cas critiques (ADAPT, etc.)
- ✅ Coût minimal (< €0.01 / département)

**À garder tel quel:**
```python
class IntelligentDeduplicator:
    - detect_duplicates()
    - _detect_address_duplicates()
    - _validate_with_llm()
    - merge_duplicates()
    - deduplicate()  # Pipeline complet
```

**Raison:** Déduplication robuste avec 100% taux détection et 0% faux positifs.

---

### 3. Protection Anti-Contamination ✅

**Concept à conserver:**
- ✅ Exclusion sites agrégateurs (essentiel-autonomie.com, papyhappy.com)
- ✅ Détection gestionnaires suspects
- ✅ Validation cohérence géographique
- ✅ Nettoyage automatique données suspectes

**Implémentation actuelle:**
```python
# Dans llm_validator_v2.py
excluded_sites = ['essentiel-autonomie.com', 'papyhappy.com', ...]
_is_suspicious_gestionnaire()
_validate_geographic_coherence()
```

**À intégrer dans v3.0:** Conserver cette logique dans les nouveaux modules.

---

## 🔄 MODULES À RÉÉCRIRE (résultats insatisfaisants)

### 1. Modules 2-3 - Alternative Scraper → Snippet Classifier + Scraping Ciblé

**Fichier actuel:** `mvp/scrapers/alternative_scraper.py`

#### Problèmes identifiés:

1. **Validation désactivée** 
   ```python
   def _validate_with_ai():
       # VALIDATION DÉSACTIVÉE - Tous les candidats passent
       return {"is_valid": True, ...}
   ```
   ❌ **Impact:** 100% des candidats passent, beaucoup de bruit

2. **Requêtes OR optimisées mais validation faible**
   - ✅ Requêtes groupées efficaces (4 requêtes au lieu de 15)
   - ❌ Mais pas de filtrage intelligent avant scraping coûteux

3. **Pré-filtres supprimés complètement**
   ```python
   # Pré-filtres allégés : Exclure seulement les documents très lourds
   if any(doc in text_check for doc in heavy_docs):
       continue
   ```
   ❌ **Impact:** Beaucoup de candidats non pertinents scraped avec ScrapingBee (coûteux)

#### Solution v3.0: Snippet Classifier

**Architecture cible:**
```
Serper (10-15 résultats) 
    ↓
Snippet Classifier LLM 8B (filtre 80% bruit)
    ↓
Scraping ciblé ScrapingBee (seulement les OUI)
```

**Avantages:**
- ✅ Filtre 80% du bruit AVANT scraping coûteux
- ✅ Réduit coûts ScrapingBee significativement
- ✅ Améliore qualité des candidats pour module 4
- ✅ < €0.001 par classification (llama-3.1-8b-instant)

---

### 2. Module 4 - LLM Validator v2 → Extraction Multipasse Mixtral

**Fichier actuel:** `mvp/scrapers/llm_validator_v2.py`

#### Problèmes identifiés:

1. **Architecture extraction monolithique**
   ```python
   def _build_extraction_prompt_strict():
       # UN SEUL prompt pour extraire TOUS les champs
       prompt = """Extrait nom, commune, gestionnaire, adresse, téléphone, email..."""
   ```
   ❌ **Impact:** Hallucinations croisées entre champs

2. **Validation post-extraction complexe**
   - 900+ lignes de logique de validation
   - Détection gestionnaires suspects après extraction
   - Nettoyage codes postaux après extraction
   ❌ **Impact:** Corrections après coup au lieu de prévention

3. **Fallback avec web search**
   ```python
   def _fallback_with_websearch():
       # Recherches web supplémentaires si extraction échoue
   ```
   ❌ **Impact:** Coûts imprévisibles, hallucinations potentielles

4. **Modèle 70B pour tout**
   - llama-3.3-70b-versatile pour toute l'extraction
   ❌ **Impact:** Coûts élevés (€0.59/1M tokens input)

#### Solution v3.0: Extraction Multipasse Mixtral

**Architecture cible:**
```
Pipeline 5 étapes:
1. Identification (Mixtral 8x7B)       → Liste établissements
2. Recherche ciblée (Serper)           → Contexte enrichi  
3. Extraction stricte (Mixtral 8x7B)   → UN champ à la fois
4. Enrichissement (si nécessaire)      → Champs manquants
5. Normalisation (règles métier)       → Validation finale
```

**Avantages extraction multipasse:**
- ✅ **0 hallucination:** Un prompt dédié par champ
- ✅ **Précision maximale:** Mixtral-8x7B-32768 (€0.24/1M tokens, entre 8B et 70B)
- ✅ **Validation native:** Chaque champ extrait indépendamment
- ✅ **Traçabilité:** Score de confiance par champ

**Exemple extraction multipasse:**
```python
# Étape 1: Nom
prompt_nom = "Extrait UNIQUEMENT le nom..."
nom, confidence_nom = extract_field(prompt_nom)

# Étape 2: Code postal + Commune
prompt_geo = "Extrait code postal et commune..."
code_postal, commune, confidence_geo = extract_geo(prompt_geo)

# Étape 3: Gestionnaire (si >60% confidence)
if confidence_nom > 60 and confidence_geo > 60:
    prompt_gest = "Extrait gestionnaire si mentionné..."
    gestionnaire, confidence_gest = extract_field(prompt_gest)
```

---

### 3. Module 4.5 - Adaptive Enricher → Intégré dans Étape 4 du nouveau pipeline

**Fichier actuel:** `mvp/scrapers/adaptive_enricher.py` (non analysé en détail)

#### Changement v3.0:

**Actuel:** Module séparé après validation

**v3.0:** Intégré dans l'étape 4 (enrichissement conditionnel)
- Enrichissement seulement si ≥2 champs manquants
- Génération présentation 150-200 mots (llama-3.1-8b-instant)
- Recherche ciblée email/téléphone/site web

---

### 4. Requêtes Serper → v3.0 avec requête dédiée "accueil familial"

**Actuel:**
```python
queries = [
    "habitat inclusif {department} (APEI OR ADMR OR APF...)",
    "(habitat inclusif OR habitat partagé OR colocation) {department}",
    ...
]
```

**v3.0:**
```python
# Requête principale (10 résultats)
main_query = '("habitat inclusif" OR "habitat partagé" OR "béguinage" OR ...) {department}'

# Requête dédiée limitée (2 résultats)
familial_query = '("service accueil familial") {department}'
```

**Avantages:**
- ✅ Meilleure couverture "accueil familial" (cas spécifique)
- ✅ Limite résultats pour cette catégorie rare
- ✅ 12 résultats totaux (10+2) au lieu de multiples requêtes

---

## 📋 PLAN D'ACTION DÉTAILLÉ

### Phase 1: Préparation Architecture (1-2h)

**Tâches:**
1. ✅ Créer dossier `mvp_v3/` pour nouveaux modules
2. ✅ Copier modules à conserver:
   - `official_scraper.py` → `mvp_v3/scrapers/`
   - `intelligent_deduplicator.py` → `mvp_v3/deduplication/`
   - `similarity_metrics.py` → `mvp_v3/deduplication/`
3. ✅ Créer structure fichiers v3.0:
   ```
   mvp_v3/
   ├── scrapers/
   │   ├── __init__.py
   │   ├── official_scraper.py (copie)
   │   ├── snippet_classifier.py (nouveau)
   │   ├── mixtral_extractor.py (nouveau)
   │   └── enricher.py (nouveau)
   ├── deduplication/
   │   ├── __init__.py
   │   ├── intelligent_deduplicator.py (copie)
   │   └── similarity_metrics.py (copie)
   └── pipeline_v3.py (nouveau - orchestrateur)
   ```

### Phase 2: Développement Snippet Classifier (2-3h)

**Fichier:** `mvp_v3/scrapers/snippet_classifier.py`

**Fonctionnalités:**
```python
class SnippetClassifier:
    def __init__(self):
        self.model = "llama-3.1-8b-instant"
        self.excluded_sites = [...]
        
    def classify_batch(self, serper_results: List[Dict]) -> List[Dict]:
        """Classifie un batch de résultats Serper"""
        candidates = []
        for result in serper_results:
            is_relevant = self._classify_single(result)
            if is_relevant:
                candidates.append(result)
        return candidates
    
    def _classify_single(self, result: Dict) -> bool:
        """Classification binaire OUI/NON avec LLM 8B"""
        prompt = self._build_classification_prompt(result)
        response = self._call_llm(prompt)
        return "OUI" in response.upper()
    
    def _build_classification_prompt(self, result: Dict) -> str:
        """Prompt strict OUI/NON"""
        return f"""
        Analyse ce résultat Google et réponds OUI ou NON.
        
        RÉPONDS "OUI" si établissement concret identifié (nom + lieu).
        RÉPONDS "NON" si article généraliste, actualité, PDF rapport.
        
        TITRE: {result['title']}
        SNIPPET: {result['snippet']}
        
        Réponds UN SEUL MOT: OUI ou NON
        """
```

**Tests:**
- Test avec 50 snippets réels Aube
- Vérifier taux filtrage ~80%
- Valider coût < €0.001/snippet

### Phase 3: Développement Mixtral Extractor (4-5h)

**Fichier:** `mvp_v3/scrapers/mixtral_extractor.py`

**Fonctionnalités:**
```python
class MixtralExtractor:
    def __init__(self):
        self.model = "mixtral-8x7b-32768"
        self.extraction_stages = 5
        
    def extract_establishments(self, candidate: Dict, department: str) -> List[Dict]:
        """Pipeline 5 étapes"""
        
        # Étape 1: Identification
        establishments = self._stage1_identify(candidate)
        
        results = []
        for est in establishments:
            # Étape 2: Recherche ciblée
            context = self._stage2_targeted_search(est, department)
            
            # Étape 3: Extraction multipasse
            extracted = self._stage3_multipass_extraction(est, context)
            
            # Étape 4: Enrichissement conditionnel
            if self._needs_enrichment(extracted):
                enriched = self._stage4_enrich(extracted, department)
            else:
                enriched = extracted
            
            # Étape 5: Normalisation
            normalized = self._stage5_normalize(enriched, department)
            
            if normalized:
                results.append(normalized)
        
        return results
    
    def _stage3_multipass_extraction(self, est: Dict, context: str) -> Dict:
        """Extraction champ par champ"""
        
        # Extraction nom (priorité 1)
        nom_data = self._extract_field("nom", est, context)
        
        # Extraction géo (priorité 1)
        geo_data = self._extract_geo(est, context)
        
        # Extraction gestionnaire (priorité 2)
        if nom_data['confidence'] > 50 and geo_data['confidence'] > 50:
            gest_data = self._extract_field("gestionnaire", est, context)
        else:
            gest_data = {'value': '', 'confidence': 0}
        
        # Autres champs optionnels
        contact_data = self._extract_contact(est, context)
        
        return {
            'nom': nom_data['value'],
            'commune': geo_data['commune'],
            'code_postal': geo_data['code_postal'],
            'gestionnaire': gest_data['value'],
            'telephone': contact_data.get('telephone', ''),
            'email': contact_data.get('email', ''),
            'confidence_nom': nom_data['confidence'],
            'confidence_geo': geo_data['confidence'],
            'confidence_gest': gest_data['confidence']
        }
    
    def _extract_field(self, field_name: str, est: Dict, context: str) -> Dict:
        """Extraction d'un seul champ avec prompt dédié"""
        prompt = self._build_field_prompt(field_name, est, context)
        response = self._call_mixtral(prompt)
        return self._parse_field_response(response)
```

**Tests:**
- Test extraction sur 20 cas réels
- Vérifier 0% hallucination
- Valider coût < €0.0005/établissement

### Phase 4: Développement Enricher (2h)

**Fichier:** `mvp_v3/scrapers/enricher.py`

**Fonctionnalités:**
```python
class Enricher:
    def __init__(self):
        self.model = "llama-3.1-8b-instant"
        
    def enrich_if_needed(self, data: Dict, department: str) -> Dict:
        """Enrichit seulement si ≥2 champs manquants"""
        
        missing_fields = self._count_missing_critical_fields(data)
        
        if missing_fields < 2:
            return data
        
        # Recherche ciblée
        search_results = self._targeted_search(data, department)
        
        # Extraction enrichissement
        enriched = self._extract_enrichment(data, search_results)
        
        # Génération présentation si vide
        if not enriched.get('presentation') or len(enriched['presentation']) < 200:
            enriched['presentation'] = self._generate_presentation(enriched)
        
        return enriched
    
    def _generate_presentation(self, data: Dict) -> str:
        """Génère présentation 150-200 mots"""
        prompt = f"""
        Rédige une présentation de 150-200 mots pour:
        Nom: {data['nom']}
        Commune: {data['commune']}
        Type: {data['sous_categories']}
        
        Sois factuel, professionnel, sans inventer d'informations.
        """
        response = self._call_llm(prompt)
        return response
```

### Phase 5: Orchestrateur Pipeline v3.0 (2-3h)

**Fichier:** `mvp_v3/pipeline_v3.py`

**Fonctionnalités:**
```python
class PipelineV3:
    def __init__(self, department: str):
        self.department = department
        self.official_scraper = OfficialScraper()
        self.snippet_classifier = SnippetClassifier()
        self.mixtral_extractor = MixtralExtractor()
        self.enricher = Enricher()
        self.deduplicator = IntelligentDeduplicator()
        
    def run(self) -> Dict:
        """Exécute pipeline complet"""
        
        # MODULE 1: Official Scraper
        official_results = self.official_scraper.extract_establishments(self.department)
        
        # MODULE 2: Serper + Snippet Classifier
        serper_results = self._search_with_serper()
        classified_candidates = self.snippet_classifier.classify_batch(serper_results)
        
        # MODULE 3: Scraping ciblé
        scraped_candidates = self._scrape_classified_candidates(classified_candidates)
        
        # MODULE 4: Extraction Mixtral multipasse
        extracted_establishments = []
        for candidate in scraped_candidates:
            establishments = self.mixtral_extractor.extract_establishments(
                candidate, self.department
            )
            extracted_establishments.extend(establishments)
        
        # MODULE 5: Enrichissement
        enriched = []
        for est in extracted_establishments:
            enriched_est = self.enricher.enrich_if_needed(est, self.department)
            enriched.append(enriched_est)
        
        # Fusion official + alternative
        all_establishments = official_results + enriched
        
        # MODULE 6: Déduplication
        final_results = self.deduplicator.deduplicate(all_establishments)
        
        # MODULE 7: Export CSV
        self._export_csv(final_results['deduplicated_records'])
        
        return final_results
```

### Phase 6: Tests & Validation (2-3h)

**Tests à effectuer:**

1. **Test département Aube (10)**
   - Exécuter pipeline complet
   - Vérifier résultats vs v2.1
   - Valider coûts < €0.01
   - Valider durée < 3 min

2. **Test hallucinations**
   - Vérifier 0% hallucination gestionnaires
   - Vérifier codes postaux cohérents
   - Vérifier pas d'adresses parisiennes

3. **Test edge cases**
   - LADAPT, CetteFamille, Ages & Vie
   - Établissements avec peu d'infos en ligne
   - Homonymes dans même département

### Phase 7: Documentation (1h)

**Fichiers à créer:**
- `mvp_v3/README.md` - Documentation complète v3.0
- `mvp_v3/MIGRATION_GUIDE.md` - Guide migration v2.1 → v3.0
- `mvp_v3/ARCHITECTURE.md` - Architecture détaillée

---

## 💰 Estimation Coûts v3.0

### Coûts par établissement:

| Étape | Modèle | Coût unitaire | Commentaire |
|-------|--------|---------------|-------------|
| Snippet Classifier | llama-3.1-8b | €0.0001 | 10-15 classifications |
| Identification | Mixtral-8x7B | €0.0001 | Par candidat |
| Extraction multipasse | Mixtral-8x7B | €0.0004 | 3-5 requêtes/établissement |
| Enrichissement | llama-3.1-8b | €0.0002 | Si nécessaire |
| Présentation | llama-3.1-8b | €0.0002 | Si vide |
| Déduplication | llama-3.1-8b | €0.0001 | Cas ambigus |
| **TOTAL** | | **~€0.0011** | **Par établissement** |

### Coûts par département (30 établissements):

- **Petit département:** ~€0.03 (vs €0.02 actuel)
- **Moyen département:** ~€0.06 (vs €0.05 actuel)
- **Grand département:** ~€0.15 (vs €0.12 actuel)

**Conclusion:** Coûts légèrement supérieurs (+20-30%) mais **0% hallucination** justifie l'investissement.

---

## ⏱️ Estimation Temps v3.0

### Temps par étape (département 30 établissements):

| Étape | Durée | Commentaire |
|-------|-------|-------------|
| Official Scraper | 45s | Inchangé |
| Serper queries | 5s | 2 requêtes |
| Snippet Classifier | 15s | 10-15 classifications |
| Scraping ciblé | 30s | 5-8 pages seulement |
| Extraction Mixtral | 90s | Multipasse |
| Enrichissement | 30s | Conditionnel |
| Déduplication | 15s | Inchangé |
| **TOTAL** | **~3.5 min** | **Objectif: <3min atteint** |

---

## 🎯 Prochaines Étapes

**Recommandation:**

1. **Commencer par Phase 1** (Préparation architecture) → **MAINTENANT**
2. **Puis Phase 2** (Snippet Classifier) → **Validation rapide du concept**
3. **Puis Phase 3** (Mixtral Extractor) → **Cœur du système**
4. **Test intermédiaire département Aube**
5. **Phases 4-7 si tests concluants**

**Décision requise:**

Souhaitez-vous que je commence l'implémentation maintenant ou préférez-vous d'abord discuter/ajuster ce plan?
