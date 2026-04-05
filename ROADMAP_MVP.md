# 🚀 ROADMAP MVP - Automatisation Habitat Seniors

## 📅 PLANNING DÉVELOPPEMENT (7 JOURS)

### **JOUR 1-2 : FONDATIONS & MODULE OFFICIEL**
- [x] Configuration API keys et environnement
- [x] Architecture modulaire définie
- [ ] **Module 1 : OfficialScraper** 
  - [ ] Scraping annuaire gouv.fr (BeautifulSoup)
  - [ ] Parser fiches établissements
  - [ ] Tests sur Lot-et-Garonne (référence)
  - [ ] Validation vs fichier data_47.csv

### **JOUR 3 : MODULE CHAÎNES PRIVÉES**  
- [ ] **Module 2 : PrivateChainScraper**
  - [ ] Scrapers Domitys, Espace & Vie, Senioriales
  - [ ] Gestion anti-bot (ScrapingBee)
  - [ ] Tests extraction données structurées
  - [ ] Validation sur établissements connus

### **JOUR 4 : EXTRACTION IA**
- [ ] **Module 4 : AIExtractor** 
  - [ ] Intégration Groq API (priorité coût)
  - [ ] Prompts optimisés extraction
  - [ ] Parser réponses JSON
  - [ ] Tests précision vs données manuelles

### **JOUR 5 : RECHERCHE ALTERNATIVE**
- [ ] **Module 3 : AlternativeSearchExtractor**
  - [ ] Intégration Serper.dev
  - [ ] Requêtes ciblées habitat inclusif
  - [ ] Extraction IA pages trouvées
  - [ ] Validation structures alternatives

### **JOUR 6 : PIPELINE & INTÉGRATION**
- [ ] **Module 5 : HabitatPipeline**
  - [ ] Orchestration des modules
  - [ ] Déduplication intelligente
  - [ ] Normalisation données finales
  - [ ] Export CSV format prompt
  - [ ] Tests end-to-end

### **JOUR 7 : VALIDATION & OPTIMISATION**
- [ ] **Tests complets**
  - [ ] Lot-et-Garonne (vs référence)
  - [ ] Aube (département vierge) 
  - [ ] Métriques précision/couverture
  - [ ] Optimisation coûts API
  - [ ] Documentation utilisation

## 🎯 CRITÈRES DE VALIDATION

### **Métriques quantitatives :**
- [ ] **≥ 23 établissements** Lot-et-Garonne (80% des 29 référence)
- [ ] **≥ 15 établissements** Aube (estimation baseline)
- [ ] **95% noms/téléphones** corrects
- [ ] **80% emails** trouvés
- [ ] **Coût ≤ €10** par département

### **Métriques qualitatives :**
- [ ] **Zéro donnée inventée**
- [ ] **Sources traçables** (URLs dans colonne source)
- [ ] **Types bien classifiés** (habitat_type + sous_categories)
- [ ] **Déduplication efficace** (pas de doublons nom+commune)

## 🛠️ STACK TECHNIQUE FINAL

### **Core Dependencies :**
```bash
pip install requests beautifulsoup4 pandas python-dotenv
pip install groq openai  # IA
pip install selenium playwright  # JS heavy sites
```

### **APIs Configuration :**
- **ScrapingBee** : 50k requêtes/mois (€29)
- **Serper.dev** : 5000 crédits achetés 
- **Groq** : Pay-as-you-go (€0.59/1M tokens)
- **OpenAI** : Fallback si Groq fail

### **Structure fichiers :**
```
ingest-habitat/
├── mvp/
│   ├── scrapers/
│   │   ├── official_scraper.py
│   │   ├── private_chains.py  
│   │   └── alternative_search.py
│   ├── ai/
│   │   └── extractor.py
│   ├── pipeline/
│   │   ├── orchestrator.py
│   │   └── normalizer.py
│   └── utils/
│       ├── validation.py
│       └── export.py
├── config_mvp.py
├── architecture_mvp.py  
└── run_mvp.py  # Point d'entrée
```

## 📊 TESTS & BENCHMARKS

### **Test 1 : Lot-et-Garonne (Référence)**
**Objectif :** Reproduire 80% du fichier data_47.csv
- **Input :** Département 47
- **Expected :** 23+ établissements trouvés  
- **Success criteria :** 
  - 8+ résidences autonomie (vs 9 référence)
  - 6+ MARPA (vs 7 référence)
  - 5+ résidences services seniors (vs 6 référence) 
  - 4+ structures alternatives (vs 7 référence)

### **Test 2 : Aube (Vierge)**
**Objectif :** Validation sur département non traité
- **Input :** Département 10
- **Expected :** 15-25 établissements (estimation)
- **Success criteria :**
  - Sources officielles exhaustives
  - Au moins 2-3 structures alternatives trouvées
  - Données cohérentes et complètes

## 🔧 OPTIMISATIONS PRÉVUES

### **Phase 1 (MVP) :**
- [ ] Cache requêtes pour éviter re-scraping
- [ ] Rate limiting respectueux
- [ ] Fallbacks en cas d'erreur API
- [ ] Logs détaillés pour debug

### **Phase 2 (Post-MVP) :**
- [ ] Parallélisation scrapers
- [ ] ML pour améliorer extraction IA
- [ ] APIs officielles quand disponibles
- [ ] Pipeline incrémental (delta updates)

## 💰 BUDGET & ROI

### **Coûts MVP (2 départements) :**
- ScrapingBee : €1-2 (usage réel)
- Serper : €5 (crédits achetés)
- Groq : €0.50-1 (tokens utilisés)
- **Total : €6.50-8** vs €800-1600 manuel

### **Industrialisation (96 départements) :**
- **Coût total :** €300-400
- **vs Manuel :** €40,000-80,000  
- **ROI :** 100x-200x 🚀

## 🚦 GO/NO-GO DECISION POINTS

### **Jour 2 :** Module officiel OK ?
- [ ] ≥15 établissements Lot-et-Garonne trouvés
- [ ] Données cohérentes vs référence  
- [ ] Performance acceptable (<10min)

### **Jour 4 :** IA extraction OK ?
- [ ] ≥70% données correctement extraites
- [ ] Coût ≤ €0.10 par établissement
- [ ] Pas d'hallucinations détectées

### **Jour 6 :** Pipeline complet OK ?
- [ ] End-to-end fonctionnel
- [ ] CSV export conforme schéma
- [ ] Métriques cibles atteintes

---

## 🎯 PROCHAINE ÉTAPE

**Validation finale configuration** puis démarrage développement Module 1 !

Voulez-vous que nous commencions par implémenter le `OfficialScraper` pour tester l'extraction depuis l'annuaire gouv.fr ?