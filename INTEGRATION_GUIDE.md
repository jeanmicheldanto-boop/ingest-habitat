# 📘 Guide d'Intégration des Améliorations

## 🎯 Objectif

Ce guide explique comment intégrer les nouveaux modules dans votre code existant (`app_enrichi_final.py`).

## ✅ Modules Créés et Testés

1. ✅ **enrichment/eligibilite_rules.py** - Règles AVP corrigées (22 tests)
2. ✅ **enrichment/normalizer.py** - Normalisation centralisée
3. ✅ **enrichment/scraper.py** - Scraping asynchrone
4. ✅ **scripts/fix_eligibilite_csv.py** - Correction des CSV

## 🔄 Plan d'Intégration Progressif

### Option 1 : Intégration Immédiate (Recommandée)

Remplacer les fonctions existantes par les nouveaux modules testés.

#### Étape 1 : Importer les nouveaux modules

Au début de `app_enrichi_final.py`, ajouter :

```python
# Nouveaux modules testés
from enrichment.eligibilite_rules import (
    deduce_eligibilite_statut, 
    is_avp_eligible,
    should_enrich_avp_data
)
from enrichment.normalizer import DataNormalizer
from enrichment.scraper import AsyncWebScraper
```

#### Étape 2 : Remplacer la fonction `deduce_eligibilite()`

**Ancienne fonction** (ligne ~465) :
```python
def deduce_eligibilite(sous_cat, mention_avp=False):
    # ... code existant ...
```

**Nouvelle implémentation** :
```python
def deduce_eligibilite(sous_cat, mention_avp=False, eligibilite_csv=None):
    """Wrapper pour compatibilité avec le nouveau module"""
    return deduce_eligibilite_statut(
        sous_categorie=sous_cat,
        mention_avp_explicite=mention_avp,
        eligibilite_csv=eligibilite_csv
    )
```

#### Étape 3 : Utiliser DataNormalizer

**Remplacer les fonctions de normalisation** :

```python
# Initialiser le normalizer (une seule fois)
normalizer = DataNormalizer()

# Ancienne façon
def normalize_phone_fr(phone):
    # ... code existant ...

# Nouvelle façon
def normalize_phone_fr(phone):
    """Wrapper pour compatibilité"""
    return normalizer.normalize_phone(phone)

# Idem pour email, sous_categorie, etc.
def normalize_email(email):
    return normalizer.normalize_email(email)
```

#### Étape 4 : Intégrer le scraper asynchrone

**Dans la fonction d'enrichissement web** :

```python
import asyncio

def enrich_batch_with_scraping(etablissements):
    """Enrichir un lot avec scraping parallèle"""
    scraper = AsyncWebScraper(timeout=10, max_concurrent=5)
    
    # Extraire les URLs
    urls = [etab.get('site_web') for etab in etablissements if etab.get('site_web')]
    
    # Scraping parallèle
    if urls:
        results = asyncio.run(scraper.scrape_batch(urls))
        
        # Mapper les résultats
        url_to_data = {r['site_web']: r for r in results}
        
        # Enrichir chaque établissement
        for etab in etablissements:
            url = etab.get('site_web')
            if url and url in url_to_data:
                scraped_data = url_to_data[url]
                # Fusionner les données
                etab.update(scraped_data)
    
    return etablissements
```

### Option 2 : Migration Progressive

Si vous préférez migrer progressivement :

#### Phase 1 : Utiliser uniquement les règles d'éligibilité
```python
from enrichment.eligibilite_rules import deduce_eligibilite_statut

# Dans votre code d'enrichissement
eligibilite = deduce_eligibilite_statut(
    sous_categorie=row['sous_categorie'],
    mention_avp_explicite=detect_mention_avp(row),
    eligibilite_csv=row.get('eligibilite_avp')  # IMPORTANT: passer la valeur CSV
)
```

#### Phase 2 : Ajouter la normalisation
```python
from enrichment.normalizer import DataNormalizer

normalizer = DataNormalizer()
# Utiliser normalizer.normalize_phone(), etc.
```

#### Phase 3 : Intégrer le scraping asynchrone
```python
from enrichment.scraper import AsyncWebScraper
# Utiliser comme dans l'Étape 4 ci-dessus
```

## 🔍 Points d'Attention Critiques

### 1. Préservation des valeurs CSV

**⚠️ IMPORTANT** : Toujours passer `eligibilite_csv` à `deduce_eligibilite_statut()`

```python
# ✅ CORRECT
eligibilite = deduce_eligibilite_statut(
    sous_categorie=sous_cat,
    mention_avp_explicite=mention_avp,
    eligibilite_csv=row.get('eligibilite_avp')  # Préserver la valeur du CSV
)

# ❌ INCORRECT (perd l'info du CSV)
eligibilite = deduce_eligibilite_statut(
    sous_categorie=sous_cat,
    mention_avp_explicite=mention_avp
    # Manque eligibilite_csv !
)
```

### 2. Détection mention AVP

La nouvelle fonction `deduce_eligibilite_statut()` ne détecte PAS automatiquement les mentions AVP. Vous devez continuer à utiliser votre logique de détection :

```python
def detect_mention_avp(row):
    """Détecter mention AVP dans les champs texte"""
    for field in ['presentation', 'description', 'source']:
        text = str(row.get(field, '')).lower()
        if any(kw in text for kw in ['avp', 'aide à la vie partagée', 'conventionné avp']):
            return True
    return False

# Utilisation
mention_avp = detect_mention_avp(row)
eligibilite = deduce_eligibilite_statut(
    sous_categorie=row['sous_categorie'],
    mention_avp_explicite=mention_avp,  # Passer le résultat
    eligibilite_csv=row.get('eligibilite_avp')
)
```

### 3. Scraping asynchrone et Streamlit

Streamlit peut avoir des problèmes avec asyncio. Utilisez ce pattern :

```python
import asyncio
import streamlit as st

def run_async_scraping(urls):
    """Helper pour exécuter du code async dans Streamlit"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    scraper = AsyncWebScraper(timeout=10)
    return loop.run_until_complete(scraper.scrape_batch(urls))

# Dans votre code Streamlit
if st.button("Enrichir"):
    results = run_async_scraping(urls)
```

## 🧪 Validation Après Intégration

### 1. Exécuter les tests

```bash
python -m pytest tests/test_eligibilite.py -v
```

Tous les 22 tests doivent passer ✅

### 2. Tester sur un petit échantillon

```python
# Créer un mini CSV de test
test_data = [
    {"nom": "Test 1", "sous_categorie": "habitat inclusif", "eligibilite_avp": "avp_eligible"},
    {"nom": "Test 2", "sous_categorie": "béguinage", "eligibilite_avp": "a_verifier"},
    {"nom": "Test 3", "sous_categorie": "colocation avec services", "eligibilite_avp": "a_verifier"}
]

# Tester la nouvelle logique
for row in test_data:
    new_elig = deduce_eligibilite_statut(
        row['sous_categorie'],
        mention_avp_explicite=False,
        eligibilite_csv=row['eligibilite_avp']
    )
    print(f"{row['nom']}: {row['eligibilite_avp']} → {new_elig}")

# Résultats attendus:
# Test 1: avp_eligible → avp_eligible (préservé)
# Test 2: a_verifier → non_eligible (béguinage jamais éligible)
# Test 3: a_verifier → non_eligible (pas de mention AVP)
```

### 3. Comparer avant/après

```bash
# Enrichir avec l'ancien code
python app_enrichi_final.py  # Sauvegarder le résultat

# Enrichir avec le nouveau code
python app_enrichi_final.py  # Comparer les résultats

# Ou utiliser le script de correction
python scripts/fix_eligibilite_csv.py data/ancien_resultat.csv data/corrige.csv
```

## 📊 Exemple Complet d'Intégration

Voici un exemple complet montrant l'intégration dans une fonction d'enrichissement :

```python
from enrichment.eligibilite_rules import deduce_eligibilite_statut
from enrichment.normalizer import DataNormalizer
from enrichment.scraper import AsyncWebScraper
import asyncio

# Initialisation (une seule fois)
normalizer = DataNormalizer()
scraper = AsyncWebScraper(timeout=10, max_concurrent=5)

def enrich_etablissement(row, enrichment_mode="websearch+ia"):
    """
    Enrichir un établissement avec les nouveaux modules
    
    Args:
        row: Dict avec les données de l'établissement
        enrichment_mode: Mode d'enrichissement
        
    Returns:
        Dict avec données enrichies
    """
    enriched = row.copy()
    
    # 1. NORMALISATION (utiliser DataNormalizer)
    if row.get('telephone'):
        enriched['telephone'] = normalizer.normalize_phone(row['telephone'])
    
    if row.get('email'):
        enriched['email'] = normalizer.normalize_email(row['email'])
    
    if row.get('sous_categorie'):
        enriched['sous_categorie'] = normalizer.normalize_sous_categorie(row['sous_categorie'])
    
    if row.get('public_cible'):
        enriched['public_cible'] = normalizer.normalize_public_cible(row['public_cible'])
    
    # 2. SCRAPING (si mode webscraping)
    if enrichment_mode in ["webscraping", "websearch+ia"] and row.get('site_web'):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        scraped = loop.run_until_complete(
            scraper._scrape_one(aiohttp.ClientSession(), row['site_web'])
        )
        enriched.update(scraped)
    
    # 3. ENRICHISSEMENT IA (votre logique existante)
    if enrichment_mode in ["ia", "websearch+ia"]:
        ia_data = call_ia_enrichment(enriched)  # Votre fonction
        enriched.update(ia_data)
    
    # 4. ELIGIBILITE AVP (avec les règles corrigées)
    mention_avp = detect_mention_avp(enriched)
    enriched['eligibilite_statut'] = deduce_eligibilite_statut(
        sous_categorie=enriched.get('sous_categorie', ''),
        mention_avp_explicite=mention_avp,
        eligibilite_csv=row.get('eligibilite_avp')  # IMPORTANT: préserver CSV
    )
    
    return enriched


def detect_mention_avp(row):
    """Détecter mention AVP dans les champs"""
    keywords = ['avp', 'aide à la vie partagée', 'conventionné avp', 'conventionne avp']
    for field in ['presentation', 'description', 'source']:
        text = str(row.get(field, '')).lower()
        if any(kw in text for kw in keywords):
            return True
    return False
```

## ✅ Checklist d'Intégration

- [ ] Importer les nouveaux modules
- [ ] Remplacer `deduce_eligibilite()` ou créer wrapper
- [ ] Passer `eligibilite_csv` dans tous les appels
- [ ] Utiliser `DataNormalizer` pour normalisation
- [ ] Intégrer `AsyncWebScraper` pour scraping
- [ ] Tester sur un échantillon
- [ ] Exécuter les tests unitaires
- [ ] Comparer résultats avant/après
- [ ] Déployer en production

## 🆘 En Cas de Problème

### Tests échouent
```bash
# Réinstaller les dépendances
pip install -r requirements.txt

# Vérifier l'import
python -c "from enrichment.eligibilite_rules import deduce_eligibilite_statut; print('OK')"

# Relancer avec détails
python -m pytest tests/test_eligibilite.py -vv
```

### Résultats différents
C'est normal ! Les corrections ont pour but de CORRIGER les anomalies. Utilisez le script de correction pour voir les changements :

```bash
python scripts/fix_eligibilite_csv.py data/votre_fichier.csv
# Examine le rapport généré
```

### Performance non améliorée
Le scraper asynchrone nécessite plusieurs URLs pour montrer ses gains. Testez avec au moins 10 établissements ayant des sites web.

---

**Dernière mise à jour** : 29 octobre 2025  
**Version** : 1.0
