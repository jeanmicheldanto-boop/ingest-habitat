# Stratégie d'Amélioration - Extraction Contacts & Classification Sources

**Date:** 2025-12-04  
**Version:** v3.1  
**Status:** ✅ Implémenté

## 🎯 Problèmes Identifiés

### 1. Contacts Manquants
- ❌ Très peu d'adresses extraites
- ❌ Très peu d'emails extraits  
- ❌ Peu de téléphones extraits
- ✅ Communes correctement extraites

### 2. Sites Non-Officiels
- ❌ La plupart des sites trouvés sont des annuaires (essentiel-autonomie.com, papyhappy.com, etc.)
- ❌ Rares sites officiels d'établissements
- ⚠️ Contamination par URLs d'agrégateurs

## 📋 Stratégie Implémentée

### Architecture 3 Couches

```
┌────────────────────────────────────────────┐
│ 1. SourceClassifier (nouveau)             │
│    Classification: officiel / gestionnaire│
│    / annuaire / article / autre           │
├────────────────────────────────────────────┤
│ 2. ContactExtractor (nouveau)             │
│    Recherche ciblée + Extraction robuste  │
│    Politique stricte par type de source   │
├────────────────────────────────────────────┤
│ 3. MixtralExtractor (amélioré)            │
│    Intégration ContactExtractor           │
│    Extraction nom/commune/gestionnaire    │
└────────────────────────────────────────────┘
```

## 🔧 Modules Créés

### 1. `source_classifier.py`

**Rôle:** Classifier chaque source web avant extraction

**Types de sources:**
- `officiel_etablissement` - Site officiel d'une résidence/structure
- `site_gestionnaire` - Site d'association/réseau (CCAS, Ages & Vie, etc.)
- `annuaire` - Site qui liste/compare des établissements
- `article` - Page d'actualité/information
- `autre` - Reste

**Méthode:**
- Détection rapide par URL (patterns d'annuaires connus)
- Classification LLM avec `llama-3.1-8b-instant`
- Fallback heuristique si LLM échoue

**Prompt LLM:**
```
Categories: officiel_etablissement, site_gestionnaire, annuaire, article, autre

Regles:
- Site qui propose "comparer", "devis", "être rappelé" → annuaire
- Plusieurs résidences du même réseau → site_gestionnaire
- Page centrée sur un seul lieu avec "Nos services", "Tarifs" → officiel_etablissement

Données: URL, TITRE, SNIPPET, EXTRAIT_PAGE (optionnel)

Réponse JSON:
{"type_source": "...", "confidence": 0-100, "reasoning": "..."}
```

**Patterns d'annuaires détectés:**
- essentiel-autonomie
- papyhappy
- pour-les-personnes-agees.gouv.fr
- retraiteplus
- logement-seniors.com
- capresidencesseniors.com

### 2. `contact_extractor.py`

**Rôle:** Extraire contacts avec politique stricte selon type de source

**Pipeline d'extraction:**

```
1. Recherche Serper ciblée
   ├─ Query: "{nom}" {commune} {code_postal} {gestionnaire}
   ├─ Exclusions: -site:essentiel-autonomie.com -site:papyhappy.com ...
   └─ 5 résultats max

2. Classification de chaque résultat
   └─ SourceClassifier sur URL + titre + snippet

3. Filtrage selon politique
   └─ Acceptation: officiel_etablissement, site_gestionnaire
   └─ Rejet: annuaire (mode strict), article, autre

4. Scraping sources validées (max 2)
   └─ ScrapingBee

5. Extraction multi-méthode
   ├─ Téléphone: regex prioritaire
   ├─ Email: regex avec validation
   ├─ Adresse: regex + LLM si nécessaire
   └─ Site web: URL source si officiel/gestionnaire

6. Validation anti-contamination
   ├─ Emails suspects rejetés (@essentiel-autonomie.com, etc.)
   ├─ Téléphones génériques rejetés (0800, etc.)
   └─ Adresses parisiennes agrégateurs rejetées (21 rue Laffitte, etc.)
```

**Politique par Type de Source:**

| Type Source | site_web | téléphone | email | adresse | Confiance |
|-------------|----------|-----------|-------|---------|-----------|
| **officiel_etablissement** | ✅ Oui | ✅ Oui | ✅ Oui | ✅ Oui | 90% |
| **site_gestionnaire** | ✅ Oui | ✅ Oui | ✅ Oui | ✅ Oui | 70% |
| **annuaire (mode strict)** | ❌ Non | ❌ Non | ❌ Non | ❌ Non | - |
| **annuaire (mode pragmatique)** | ❌ Non | ⚠️ Oui | ❌ Non | ❌ Non | 40% |
| **article** | ❌ Non | ❌ Non | ❌ Non | ❌ Non | - |
| **autre** | ❌ Non | ❌ Non | ❌ Non | ❌ Non | - |

**Modes:**
- **Mode strict** (par défaut) : Jamais utiliser annuaires
- **Mode pragmatique** : Téléphones depuis annuaires autorisés mais flaggés

**Patterns Regex:**
```python
# Téléphone
r'(\+33|0)[1-9](?:[\s\.\-]?\d{2}){4}'

# Email
r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

# Adresse (basique)
r'\d+[\s,]+(?:rue|avenue|boulevard|place|chemin|allée|impasse)[^,\.]{5,50}[,\s]+\d{5}[\s,]+[A-ZÀ-Ÿ][a-zà-ÿ\-\s]{2,30}'
```

### 3. `mixtral_extractor.py` (améliorations)

**Modifications:**

1. **Import ContactExtractor:**
```python
from contact_extractor import ContactExtractor
```

2. **Initialisation:**
```python
self.contact_extractor = ContactExtractor(
    groq_api_key=self.groq_api_key,
    serper_api_key=self.serper_api_key,
    scrapingbee_api_key=self.scrapingbee_api_key,
    mode="strict"  # ou "pragmatique"
)
```

3. **Extraction contacts:**
```python
def _extract_contact_robust(self, nom, commune, code_postal, gestionnaire):
    contact_data = self.contact_extractor.extract_contacts_for_establishment(
        nom=nom,
        commune=commune,
        code_postal=code_postal,
        gestionnaire=gestionnaire
    )
    
    return {
        'adresse': contact_data.adresse,
        'telephone': contact_data.telephone,
        'email': contact_data.email,
        'site_web': contact_data.site_web
    }
```

4. **Statistiques enrichies:**
Affichage des stats ContactExtractor (sources trouvées/rejetées)

## 📊 Flux Complet

```
ÉTABLISSEMENT DÉTECTÉ
   ↓
[Mixtral] Extraction nom/commune/code_postal/gestionnaire
   ↓
[ContactExtractor] Pour chaque établissement:
   ↓
   1. Recherche Serper ciblée
      • Query avec nom + commune + code_postal + gestionnaire
      • Exclusion annuaires dans query
      • 5 résultats max
   ↓
   2. Classification sources
      • [SourceClassifier] pour chaque résultat
      • Type: officiel/gestionnaire/annuaire/article/autre
      • Confiance: 0-100%
   ↓
   3. Filtrage politique
      • Mode strict: garder SEULEMENT officiel + gestionnaire
      • Rejeter: annuaire, article, autre
   ↓
   4. Scraping sources validées (max 2)
      • ScrapingBee
      • Extraction texte propre
   ↓
   5. Extraction multi-méthode
      • Téléphone: regex
      • Email: regex + validation
      • Adresse: regex (+ LLM si nécessaire)
      • Site web: URL source
   ↓
   6. Validation anti-contamination
      • Emails suspects → rejetés
      • Téléphones génériques → rejetés
      • Adresses parisiennes → rejetées
   ↓
CONTACT DATA
   • site_web (si officiel/gestionnaire)
   • telephone
   • email
   • adresse
   • Métadonnées: source_type, confidence, method
```

## 💰 Coûts Estimés

**Par établissement:**
- Recherche Serper: 1 requête = €0.003
- Classification sources: 5 × €0.0001 = €0.0005
- Scraping ScrapingBee: 2 pages max = €0.002
- Extraction LLM (si nécessaire): €0.0002

**Total: ~€0.006/établissement** (extraction contacts seulement)

**Département complet (avec modules existants):**
- Module 1 (Official): €0.003
- Module 2 (Classifier): €0.001
- Module 3 (Extractor): €0.003 (extraction nom/commune/gestionnaire)
- **Module 3bis (Contacts): €0.006 × 5-10 établissements = €0.030-0.060**
- Module 4 (Enricher): €0.002
- **Total: ~€0.09-0.15** (au lieu de €0.03-0.06 précédemment)

⚠️ **Surcoût:** +100-150% mais extraction contacts robuste

## 🎯 Résultats Attendus

### Avant (v3.0)
```csv
nom,commune,code_postal,gestionnaire,adresse_l1,telephone,email,site_web
Habitat LADAPT,Troyes,10000,LADAPT,,,,(vide ou annuaire)
```

### Après (v3.1)
```csv
nom,commune,code_postal,gestionnaire,adresse_l1,telephone,email,site_web
Habitat LADAPT,Troyes,10000,LADAPT,12 rue Test,07 57 40 86 79,troyes@ladapt.net,https://www.ladapt.net
```

**Améliorations:**
- ✅ **site_web** : jamais d'annuaire, uniquement sites officiels/gestionnaires
- ✅ **telephone** : extraction robuste depuis sites officiels
- ✅ **email** : extraction robuste depuis sites officiels
- ✅ **adresse** : extraction depuis sites officiels
- ✅ **Traçabilité** : type de source + confiance

## 🚀 Utilisation

### Configuration

**Mode strict (recommandé):**
```python
extractor = MixtralExtractor(
    groq_api_key="...",
    serper_api_key="...",
    scrapingbee_api_key="...",
    contact_extraction_mode="strict"  # Jamais annuaire
)
```

**Mode pragmatique:**
```python
extractor = MixtralExtractor(
    contact_extraction_mode="pragmatique"  # Téléphones annuaires OK
)
```

### Test Modules Individuels

**Test SourceClassifier:**
```bash
python mvp/scrapers/source_classifier.py
```

**Test ContactExtractor:**
```bash
python mvp/scrapers/contact_extractor.py
```

**Test intégration:**
```bash
python pipeline_v3_cli.py -d 10
```

## 📈 Métriques de Succès

**À mesurer sur test département (ex: Aube 10):**

| Métrique | Cible v3.1 |
|----------|------------|
| % établissements avec téléphone | >80% |
| % établissements avec email | >60% |
| % établissements avec adresse | >50% |
| % site_web = annuaire | 0% |
| % site_web = officiel/gestionnaire | >70% |
| Sources officielles trouvées / total | >50% |
| Sources rejetées (annuaires) | >30% |

## 🔧 Maintenance & Évolutions

### Ajustements Possibles

**1. Élargir gestionnaires connus:**
Ajouter dans `SourceClassifier.gestionnaire_keywords`

**2. Affiner regex adresses:**
Améliorer pattern dans `ContactExtractor._extract_adresse()`

**3. Ajouter extraction LLM adresse:**
Si regex insuffisante, prompt dédié avec Mixtral

**4. Mode hybride:**
Créer mode "hybride" avec logique plus fine :
- Contacts depuis officiel/gestionnaire prioritaires
- Fallback annuaire si rien trouvé
- Flag clair sur origine

### Évolutions Futures

**Phase 2 :**
- Extraction logo/photos depuis sites officiels
- Extraction horaires/services depuis sites officiels
- Validation croisée contacts (vérifier cohérence tél/email/adresse)

**Phase 3 :**
- Cache intelligent (ne pas re-scraper même établissement)
- Fusion intelligente si plusieurs sources officielles
- Score de fiabilité global par établissement

## 📝 Checklist Déploiement

- [x] Créer `source_classifier.py`
- [x] Créer `contact_extractor.py`
- [x] Modifier `mixtral_extractor.py`
- [ ] Tester sur département test (Aube 10)
- [ ] Analyser résultats et ajuster seuils
- [ ] Valider coûts réels vs estimations
- [ ] Déployer sur production si satisfaisant
- [ ] Mettre à jour README_PIPELINE_V3.md

## 🐛 Points d'Attention

**1. Coûts:**
⚠️ Surcoût significatif (×2-3) mais justifié par qualité

**2. Taux d'échec:**
⚠️ Certains établissements n'auront pas de contacts si aucun site officiel trouvé
→ Préférer données vides à données contaminées

**3. Rate limiting:**
⚠️ Scraping intensif → surveiller limites API ScrapingBee

**4. Performances:**
⚠️ Pipeline plus lent (recherches ciblées par établissement)
→ Estimer 5-8 minutes par département au lieu de 3-5 minutes

## ✅ Validation

**Critères de validation sur Aube (10):**

1. ✅ Aucun site_web = annuaire
2. ✅ >70% des établissements avec téléphone
3. ✅ >50% des établissements avec email OU téléphone
4. ✅ Habitat LADAPT Troyes a ses vrais contacts
5. ✅ Coût réel < €0.20 / département

---

**Version:** v3.1  
**Date:** 2025-12-04  
**Auteur:** Pipeline Enhancement Team  
**Status:** ✅ Prêt pour tests
