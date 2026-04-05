# 🚀 PLAN D'AMÉLIORATION COMPLET - PIPELINE D'ENRICHISSEMENT HABITAT SENIOR

## 📊 ANALYSE DIAGNOSTIQUE

### ❌ ANOMALIES CRITIQUES DÉTECTÉES

#### 1. **CLARIFICATION : Règle eligibilite_statut pour habitat inclusif**
- **Gravité** : IMPORTANTE 🟠
- **Règle correcte** : Pour habitat inclusif
  - Si `avp_eligible` est déjà en entrée CSV → **NE PAS CHANGER** (déjà vérifié)
  - Si `non_eligible` OU vide → **Mettre `a_verifier`**
- **Fonction à corriger** : `deduce_eligibilite()` ligne ~465
- **Comportement actuel** : Ne distingue pas si la valeur vient du CSV (déjà vérifiée) ou de l'enrichissement

#### 3. **INCOHÉRENCE : Colonne eligibilite_avp vs eligibilite_statut**
- Dans le CSV d'entrée : colonne `eligibilite_avp` (valeurs : avp_eligible, non_eligible, a_verifier)
- Dans le schéma DB : colonne `eligibilite_statut` (enum identique)
- Dans le code : mélange des deux noms
- **Impact** : Confusion et bugs potentiels

#### 4. **MAPPING INCOMPLET : Sous-catégories**
- **Manquant** : "habitat regroupé" dans les mappings
- **Variations non gérées** : "logement adapté", "maison d'accueil familial" vs "maison d'accueil familial"
- **Impact** : Catégorisation incorrecte pour certains établissements

#### 5. **LOGIQUE AVP DÉFECTUEUSE**
- Les établissements avec sous_categorie "habitat inclusif" + `avp_eligible` dans le CSV d'entrée ne sont PAS marqués `a_verifier` comme requis
- La fonction `extract_avp_data_from_enrichment()` crée des données AVP même pour les non-éligibles
- **Impact** : Données AVP incorrectes en base

### ⚠️ PROBLÈMES DE PERFORMANCE

#### 1. **Scraping séquentiel bloquant**
- Timeout de 25 secondes par URL (réduit à 10s mais toujours bloquant)
- Pas de parallélisation
- Exploration récursive des pages tarifs (jusqu'à 3 pages)
- **Impact** : ~30-60 secondes par établissement avec site web

#### 2. **Appels IA inefficaces**
- Aucun cache des résultats IA
- Prompt de 1500+ tokens par établissement
- Fallback automatique qui réessaie plusieurs modèles
- **Impact** : ~2-5 secondes par établissement

#### 3. **Géocodage synchrone**
- Un appel API par établissement sans parallélisation
- Fallback commune si adresse échoue (2 appels successifs)
- **Impact** : ~1-3 secondes par établissement

#### 4. **Interface Streamlit non optimisée**
- Re-exécution complète du script à chaque interaction
- Session state utilisé mais pas de cache efficace
- Logs verbeux ralentissent l'affichage
- **Impact** : UX dégradée, temps d'attente long

#### 5. **Insertion DB séquentielle**
- INSERT un par un au lieu de batch
- Multiples requêtes par établissement (8-12 requêtes)
- Pas de COPY pour bulk insert
- **Impact** : ~500ms par établissement en insertion

### 🏗️ PROBLÈMES D'ARCHITECTURE

#### 1. **Code monolithique**
- Fichier unique de 2500+ lignes
- Mélange interface UI et logique métier
- Fonctions de 100-200 lignes
- **Impact** : Maintenabilité difficile, bugs cachés

#### 2. **Pas de séparation des responsabilités**
- Scraping, IA, DB, validation, normalisation dans le même fichier
- Pas de couche de service
- Logique métier couplée à Streamlit

#### 3. **Gestion d'erreurs basique**
- Try/except génériques qui masquent les erreurs
- Pas de logging structuré
- Erreurs silencieuses dans le scraping

#### 4. **Pas de tests**
- Aucun test unitaire
- Pas de test d'intégration
- Validation manuelle uniquement

#### 5. **Configuration en dur**
- Clés API hardcodées
- Pas de fichier de configuration centralisé
- Paramètres métier dispersés dans le code

## 🎯 PLAN D'AMÉLIORATION PRIORISÉ

### 🔴 PRIORITÉ 1 - CORRECTIONS CRITIQUES (Urgent - 2-3 jours)

#### A. Corriger la logique eligibilite_statut (avec préservation CSV)

**Fichier** : `enrichment/eligibilite_rules.py` (nouveau)
```python
def deduce_eligibilite_statut(sous_categorie: str, mention_avp_explicite: bool) -> str:
    """
    Déduire le statut d'éligibilité AVP selon les règles métier EXACTES
    
    Règles:
    1. JAMAIS éligibles: résidence services seniors, résidence autonomie, 
       accueil familial, MARPA, béguinage, village seniors
    2. Habitat inclusif: TOUJOURS 'a_verifier' (même si mention AVP détectée)
    3. Autres catégories: 'avp_eligible' si mention AVP, sinon 'non_eligible'
    """
    if not sous_categorie:
        return 'a_verifier'
    
    sous_cat_clean = sous_categorie.lower().strip()
    
    # Catégories JAMAIS éligibles AVP (liste exhaustive)
    jamais_eligibles = [
        'résidence services seniors', 'résidence services',
        'résidence autonomie',
        'accueil familial',
        'marpa',
        'béguinage', 'beguinage',
        'village seniors', 'village séniors'
    ]
    
    for pattern in jamais_eligibles:
        if pattern in sous_cat_clean:
            return 'non_eligible'
    
    # CAS SPÉCIAL : Habitat inclusif → TOUJOURS a_verifier
    if 'habitat inclusif' in sous_cat_clean:
        return 'a_verifier'
    
    # Catégories éligibles SI mention AVP explicite
    eligibles_si_mention = [
        'habitat intergénérationnel', 'habitat intergenerationnel',
        'colocation avec services',
        'habitat alternatif',
        'maison d\'accueil familial', 'maison d\'accueil familial'
    ]
    
    for pattern in eligibles_si_mention:
        if pattern in sous_cat_clean:
            return 'avp_eligible' if mention_avp_explicite else 'non_eligible'
    
    # Par défaut
    return 'a_verifier'
```

#### C. Corriger le CSV d'entrée

**Script** : `scripts/fix_eligibilite_csv.py`
```python
import pandas as pd

def fix_eligibilite_in_csv(input_file: str, output_file: str):
    """Corriger les valeurs eligibilite_avp dans le CSV"""
    df = pd.read_csv(input_file)
    
    # Appliquer les vraies règles
    def fix_eligibilite(row):
        sous_cat = str(row.get('sous_categories', '')).lower()
        
        # Habitat inclusif → a_verifier (règle prioritaire)
        if 'habitat inclusif' in sous_cat:
            return 'a_verifier'
        
        # Autres règles...
        return row.get('eligibilite_avp', 'a_verifier')
    
    df['eligibilite_avp'] = df.apply(fix_eligibilite, axis=1)
    df.to_csv(output_file, index=False)
    
    # Rapport
    print(f"✅ Fichier corrigé: {output_file}")
    print(f"📊 Répartition:")
    print(df['eligibilite_avp'].value_counts())
```

### 🟠 PRIORITÉ 2 - REFACTORISATION ARCHITECTURE (Important - 1 semaine)

#### A. Structure modulaire proposée

```
ingest-habitat/
├── config/
│   ├── __init__.py
│   ├── settings.py          # Configuration centralisée
│   └── business_rules.py    # Règles métier (sous-catégories, etc.)
├── core/
│   ├── __init__.py
│   ├── models.py            # Modèles de données (dataclasses)
│   └── exceptions.py        # Exceptions personnalisées
├── enrichment/
│   ├── __init__.py
│   ├── normalizer.py        # Normalisation (téléphone, email, etc.)
│   ├── eligibilite_rules.py # Logique éligibilité AVP
│   ├── scraper.py           # Web scraping
│   ├── ai_enricher.py       # Enrichissement IA
│   ├── geocoder.py          # Géocodage
│   └── validator.py         # Validation données
├── database/
│   ├── __init__.py
│   ├── connection.py        # Gestion connexions
│   ├── repositories.py      # Repositories (CRUD)
│   └── bulk_import.py       # Import en masse optimisé
├── services/
│   ├── __init__.py
│   └── enrichment_service.py # Orchestration enrichissement
├── ui/
│   ├── __init__.py
│   └── streamlit_app.py     # Interface Streamlit
├── scripts/
│   ├── fix_eligibilite_csv.py
│   └── migrate_avp_data.py
└── tests/
    ├── __init__.py
    ├── test_eligibilite.py
    ├── test_normalizer.py
    └── test_enrichment.py
```

#### B. Refactorisation des fonctions critiques

**1. Normalisation centralisée**

`enrichment/normalizer.py`:
```python
from typing import Optional
import re
from dataclasses import dataclass

@dataclass
class NormalizedData:
    """Données normalisées"""
    telephone: Optional[str] = None
    email: Optional[str] = None
    code_postal: Optional[str] = None
    sous_categorie: Optional[str] = None
    public_cible: list[str] = None
    
class DataNormalizer:
    """Classe de normalisation centralisée"""
    
    SOUS_CATEGORIES_MAPPING = {
        "residence autonomie": "résidence autonomie",
        "résidence autonomie": "résidence autonomie",
        "residence services seniors": "résidence services seniors",
        # ... tous les mappings
    }
    
    PUBLIC_CIBLE_MAPPING = {
        "personnes âgées": "personnes_agees",
        "seniors": "personnes_agees",
        # ... tous les mappings
    }
    
    def normalize_phone(self, phone: str) -> Optional[str]:
        """Normaliser téléphone français"""
        if not phone:
            return None
        digits = re.sub(r'\D', '', str(phone))
        if digits.startswith('33') and len(digits) == 11:
            digits = '0' + digits[2:]
        if len(digits) == 10 and digits.startswith('0'):
            return f"{digits[:2]} {digits[2:4]} {digits[4:6]} {digits[6:8]} {digits[8:10]}"
        return None
    
    def normalize_email(self, email: str) -> Optional[str]:
        """Valider et normaliser email"""
        if not email:
            return None
        email = str(email).strip().lower()
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return email if re.match(pattern, email) else None
    
    def normalize_sous_categorie(self, value: str) -> Optional[str]:
        """Normaliser sous-catégorie"""
        if not value:
            return None
        normalized = str(value).strip().lower()
        return self.SOUS_CATEGORIES_MAPPING.get(normalized, "habitat alternatif")
    
    def normalize_public_cible(self, value: str) -> list[str]:
        """Normaliser public_cible en liste"""
        if not value:
            return []
        values = [v.strip().lower() for v in str(value).split(',')]
        normalized = []
        for v in values:
            mapped = self.PUBLIC_CIBLE_MAPPING.get(v)
            if mapped and mapped not in normalized:
                normalized.append(mapped)
        return normalized
```

**2. Service d'enrichissement orchestré**

`services/enrichment_service.py`:
```python
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

from enrichment.scraper import WebScraper
from enrichment.ai_enricher import AIEnricher
from enrichment.geocoder import Geocoder
from enrichment.validator import DataValidator
from enrichment.normalizer import DataNormalizer

logger = logging.getLogger(__name__)

class EnrichmentService:
    """Service orchestrant l'enrichissement"""
    
    def __init__(self, config: dict):
        self.config = config
        self.normalizer = DataNormalizer()
        self.scraper = WebScraper(timeout=10)
        self.ai_enricher = AIEnricher(config)
        self.geocoder = Geocoder(config)
        self.validator = DataValidator()
        
    def enrich_batch(self, etablissements: List[Dict], 
                     mode: str = "websearch+ia",
                     max_workers: int = 5) -> List[Dict]:
        """
        Enrichir un lot d'établissements en parallèle
        
        Args:
            etablissements: Liste d'établissements à enrichir
            mode: Mode d'enrichissement (webscraping, ia, websearch+ia)
            max_workers: Nombre de workers parallèles
            
        Returns:
            Liste d'établissements enrichis
        """
        enriched = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._enrich_one, etab, mode): etab 
                for etab in etablissements
            }
            
            for future in as_completed(futures):
                etab = futures[future]
                try:
                    result = future.result(timeout=60)
                    enriched.append(result)
                except Exception as e:
                    logger.error(f"Erreur enrichissement {etab.get('nom')}: {e}")
                    enriched.append(etab)  # Retourner l'original en cas d'erreur
        
        return enriched
    
    def _enrich_one(self, etablissement: Dict, mode: str) -> Dict:
        """Enrichir un établissement (logique interne)"""
        # 1. Normalisation basique
        normalized = self._normalize_basic(etablissement)
        
        # 2. Enrichissement selon le mode
        if mode == "webscraping" and normalized.get('site_web'):
            scraped = self.scraper.scrape(normalized['site_web'])
            normalized = self._merge_data(normalized, scraped)
        
        elif mode == "ia":
            ai_data = self.ai_enricher.enrich(normalized)
            normalized = self._merge_data(normalized, ai_data)
        
        elif mode == "websearch+ia":
            # Recherche web + scraping
            search_results = self._web_search(normalized)
            for url in search_results[:2]:  # Limiter à 2 URLs
                scraped = self.scraper.scrape(url)
                normalized = self._merge_data(normalized, scraped)
            
            # Enrichissement IA final
            ai_data = self.ai_enricher.enrich(normalized)
            normalized = self._merge_data(normalized, ai_data)
        
        # 3. Géocodage si nécessaire
        if not normalized.get('geom'):
            geo_data = self.geocoder.geocode(normalized)
            if geo_data:
                normalized.update(geo_data)
        
        # 4. Validation finale
        validation_errors = self.validator.validate(normalized)
        if validation_errors:
            logger.warning(f"Validation {normalized['nom']}: {validation_errors}")
        
        return normalized
```

### 🟡 PRIORITÉ 3 - OPTIMISATION PERFORMANCE (Moyen terme - 1 semaine)

#### A. Parallélisation du scraping

`enrichment/scraper.py`:
```python
import asyncio
import aiohttp
from typing import List, Dict
from bs4 import BeautifulSoup

class AsyncWebScraper:
    """Scraper asynchrone pour performance"""
    
    def __init__(self, timeout: int = 10, max_concurrent: int = 10):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
    async def scrape_batch(self, urls: List[str]) -> List[Dict]:
        """Scraper plusieurs URLs en parallèle"""
        async with aiohttp.ClientSession(timeout=self.timeout) as session:
            tasks = [self._scrape_one(session, url) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            return [r for r in results if isinstance(r, dict)]
    
    async def _scrape_one(self, session: aiohttp.ClientSession, url: str) -> Dict:
        """Scraper une URL (async)"""
        async with self.semaphore:
            try:
                async with session.get(url) as response:
                    html = await response.text()
                    return self._extract_data(html, url)
            except Exception as e:
                logger.error(f"Erreur scraping {url}: {e}")
                return {}
    
    def _extract_data(self, html: str, url: str) -> Dict:
        """Extraire données du HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        # ... logique d'extraction
        return {"site_web": url, ...}
```

#### B. Cache Redis pour IA

`enrichment/ai_enricher.py`:
```python
import hashlib
import json
import redis
from typing import Optional

class CachedAIEnricher:
    """Enrichisseur IA avec cache Redis"""
    
    def __init__(self, config: dict):
        self.client = self._init_ai_client(config)
        self.cache = redis.Redis(
            host=config.get('REDIS_HOST', 'localhost'),
            port=config.get('REDIS_PORT', 6379),
            decode_responses=True
        )
        self.cache_ttl = 86400 * 7  # 7 jours
    
    def enrich(self, etablissement: Dict) -> Dict:
        """Enrichir avec cache"""
        cache_key = self._get_cache_key(etablissement)
        
        # Vérifier cache
        cached = self.cache.get(cache_key)
        if cached:
            logger.info(f"Cache hit pour {etablissement['nom']}")
            return json.loads(cached)
        
        # Appel IA
        result = self._call_ai(etablissement)
        
        # Mettre en cache
        self.cache.setex(cache_key, self.cache_ttl, json.dumps(result))
        
        return result
    
    def _get_cache_key(self, etablissement: Dict) -> str:
        """Générer clé de cache unique"""
        key_data = f"{etablissement['nom']}|{etablissement['commune']}|{etablissement.get('presentation', '')}"
        return f"ai:enrich:{hashlib.md5(key_data.encode()).hexdigest()}"
```

#### C. Bulk insert optimisé

`database/bulk_import.py`:
```python
from typing import List, Dict
import psycopg2
from psycopg2.extras import execute_batch

class BulkImporter:
    """Import en masse optimisé"""
    
    def __init__(self, connection):
        self.conn = connection
        self.batch_size = 500
    
    def import_etablissements(self, etablissements: List[Dict]):
        """Import en masse avec COPY"""
        cursor = self.conn.cursor()
        
        try:
            # 1. Préparer données
            values = self._prepare_values(etablissements)
            
            # 2. Bulk insert établissements
            etablissement_ids = self._bulk_insert_etablissements(cursor, values)
            
            # 3. Bulk insert relations (batch)
            self._bulk_insert_relations(cursor, etablissements, etablissement_ids)
            
            self.conn.commit()
            return etablissement_ids
            
        except Exception as e:
            self.conn.rollback()
            raise e
    
    def _bulk_insert_etablissements(self, cursor, values: List[tuple]) -> List[str]:
        """Insert en masse avec RETURNING ids"""
        query = """
        INSERT INTO public.etablissements 
        (nom, presentation, adresse_l1, code_postal, commune, ...)
        VALUES (%s, %s, %s, %s, %s, ...)
        RETURNING id
        """
        
        # Execute_batch pour performance
        execute_batch(cursor, query, values, page_size=self.batch_size)
        
        # Récupérer IDs
        ids = [row[0] for row in cursor.fetchall()]
        return ids
    
    def _bulk_insert_relations(self, cursor, etablissements: List[Dict], 
                               etablissement_ids: List[str]):
        """Insert relations en batch"""
        # Services
        services_values = []
        for idx, etab in enumerate(etablissements):
            etab_id = etablissement_ids[idx]
            for service in etab.get('services', []):
                services_values.append((etab_id, service))
        
        if services_values:
            execute_batch(cursor,
                "INSERT INTO public.etablissement_service (etablissement_id, service_id) "
                "SELECT %s, id FROM public.services WHERE libelle = %s "
                "ON CONFLICT DO NOTHING",
                services_values,
                page_size=self.batch_size
            )
        
        # Même logique pour restauration, tarifications, logements, etc.
```

### 🟢 PRIORITÉ 4 - TESTS ET QUALITÉ (Continu - 2 semaines)

#### A. Tests unitaires

`tests/test_eligibilite.py`:
```python
import pytest
from enrichment.eligibilite_rules import deduce_eligibilite_statut

class TestEligibiliteRules:
    """Tests des règles d'éligibilité AVP"""
    
    def test_habitat_inclusif_toujours_a_verifier(self):
        """Habitat inclusif doit TOUJOURS être a_verifier"""
        # Sans mention AVP
        assert deduce_eligibilite_statut("habitat inclusif", False) == "a_verifier"
        
        # Avec mention AVP explicite
        assert deduce_eligibilite_statut("habitat inclusif", True) == "a_verifier"
    
    def test_residence_services_seniors_non_eligible(self):
        """Résidence services seniors JAMAIS éligible"""
        assert deduce_eligibilite_statut("résidence services seniors", False) == "non_eligible"
        assert deduce_eligibilite_statut("résidence services seniors", True) == "non_eligible"
    
    def test_colocation_avec_mention_avp(self):
        """Colocation éligible SI mention AVP"""
        assert deduce_eligibilite_statut("colocation avec services", False) == "non_eligible"
        assert deduce_eligibilite_statut("colocation avec services", True) == "avp_eligible"
    
    def test_cas_par_defaut(self):
        """Catégorie inconnue → a_verifier"""
        assert deduce_eligibilite_statut("catégorie inconnue", False) == "a_verifier"
```

#### B. Tests d'intégration

`tests/test_enrichment.py`:
```python
import pytest
from services.enrichment_service import EnrichmentService

class TestEnrichmentService:
    """Tests d'enrichissement bout en bout"""
    
    @pytest.fixture
    def service(self):
        config = {"AI_PROVIDER": "mock", ...}
        return EnrichmentService(config)
    
    def test_enrich_one_etablissement(self, service):
        """Test enrichissement complet"""
        etab = {
            "nom": "Résidence Test",
            "commune": "Paris",
            "code_postal": "75001",
            "sous_categories": "Habitat inclusif",
            "site_web": "https://example.com"
        }
        
        enriched = service._enrich_one(etab, mode="ia")
        
        # Vérifications
        assert enriched['nom'] == "Résidence Test"
        assert enriched['eligibilite_statut'] == "a_verifier"  # Habitat inclusif
        assert 'presentation' in enriched
        assert 'services' in enriched
```

### 📊 GAINS DE PERFORMANCE ATTENDUS

| Optimisation | Gain | Impact |
|--------------|------|--------|
| Scraping parallèle (async) | **70-80%** | 60s → 12s pour 10 établissements |
| Cache Redis IA | **90%** | 3s → 0.3s (cache hit) |
| Bulk insert DB | **85%** | 500ms → 75ms par établissement |
| Géocodage parallèle | **60%** | 2s → 0.8s |
| **TOTAL PIPELINE** | **~75%** | **90s → 22s pour 10 établissements** |

### 🎯 PLANNING DE MISE EN ŒUVRE

#### Semaine 1 : Corrections critiques
- [x] Jour 1-2 : Créer table avp_infos + migration
- [x] Jour 3 : Corriger logique eligibilite_statut
- [x] Jour 4 : Corriger CSV d'entrée
- [x] Jour 5 : Tests validation

#### Semaine 2 : Refactorisation
- [ ] Jour 1-2 : Créer structure modulaire
- [ ] Jour 3 : Migrer normalisation
- [ ] Jour 4 : Migrer enrichissement
- [ ] Jour 5 : Tests unitaires

#### Semaine 3 : Optimisation
- [ ] Jour 1-2 : Implémenter scraping async
- [ ] Jour 3 : Implémenter cache Redis
- [ ] Jour 4 : Optimiser bulk insert
- [ ] Jour 5 : Tests performance

#### Semaine 4 : Finalisation
- [ ] Jour 1-2 : Tests d'intégration
- [ ] Jour 3 : Documentation
- [ ] Jour 4-5 : Déploiement et validation

### 📝 CHECKLIST DE VALIDATION

**Conformité métier**
- [ ] Habitat inclusif → TOUJOURS `a_verifier`
- [ ] Résidence services seniors → TOUJOURS `non_eligible`
- [ ] Table avp_infos créée et fonctionnelle
- [ ] Mapping sous-catégories complet
- [ ] Validation public_cible conforme

**Performance**
- [ ] Enrichissement < 3s par établissement (moyenne)
- [ ] Import DB < 100ms par établissement
- [ ] Cache IA fonctionnel (>80% hit rate)
- [ ] Scraping parallèle opérationnel

**Qualité code**
- [ ] Tests unitaires >80% couverture
- [ ] Tests d'intégration complets
- [ ] Logging structuré
- [ ] Documentation à jour
- [ ] Code review validé

### 🚨 RISQUES IDENTIFIÉS

| Risque | Probabilité | Impact | Mitigation |
|--------|-------------|--------|------------|
| Régression données existantes | Moyenne | Élevé | Tests sur copie DB, validation post-migration |
| Performance Redis en production | Faible | Moyen | Fallback sans cache, monitoring |
| Breaking changes API IA | Moyenne | Élevé | Abstraction provider, tests mock |
| Bugs logique AVP | Faible | Élevé | Tests exhaustifs, validation manuelle 10% |

### 📚 DOCUMENTATION NÉCESSAIRE

1. **Guide de migration** : Procédure pour passer de l'ancienne à la nouvelle version
2. **Documentation API** : Endpoints et paramètres du service d'enrichissement
3. **Guide des règles métier** : Documentation complète des règles AVP
4. **Runbook ops** : Monitoring, debugging, rollback
5. **Guide développeur** : Architecture, contribution, tests

---

**Date de création** : 29 octobre 2025
**Auteur** : Audit technique pipeline enrichissement
**Version** : 1.0
