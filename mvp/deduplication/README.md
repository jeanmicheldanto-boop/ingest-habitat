# 🔍 Module de Déduplication Intelligente

## Vue d'ensemble

Le module de déduplication intelligente détecte et fusionne automatiquement les doublons dans les données d'établissements seniors, en utilisant une approche multi-niveaux combinant :

1. **Calcul de similarité automatique** (algorithmes de distance de Levenshtein, SequenceMatcher)
2. **Validation LLM** (pour les cas ambigus)
3. **Fusion intelligente** (conservation de l'établissement le plus complet)

## Architecture

```
mvp/deduplication/
├── __init__.py                  # Module exports
├── similarity_metrics.py        # Calculs de similarité
├── intelligent_deduplicator.py  # Logique principale
└── README.md                    # Documentation
```

## Utilisation

### Installation

Aucune dépendance supplémentaire requise. Le module utilise les bibliothèques standards Python et Groq (déjà installé).

### Exemple basique

```python
from mvp.deduplication import IntelligentDeduplicator
import csv

# Charger les données
records = []
with open('etablissements.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    records = list(reader)

# Dédupliquer
deduplicator = IntelligentDeduplicator()
results = deduplicator.deduplicate(records)

# Récupérer les résultats
deduplicated_records = results['deduplicated_records']
statistics = results['statistics']
merge_metadata = results['merge_metadata']

# Sauvegarder
with open('etablissements_deduplicated.csv', 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=deduplicated_records[0].keys())
    writer.writeheader()
    writer.writerows(deduplicated_records)
```

### Script de test complet

Un script de test complet est disponible : `test_deduplication.py`

```bash
python test_deduplication.py
```

Ce script :
- Charge le CSV source
- Exécute la déduplication
- Génère un CSV dédupliqué
- Crée un rapport détaillé

## Algorithme

### 1. Détection des doublons

Pour chaque paire d'établissements, le système calcule un **score de similarité global** (0-100%) basé sur :

| Critère | Poids | Description |
|---------|-------|-------------|
| **Nom** | 35% | Similarité du nom (Levenshtein + SequenceMatcher) |
| **Localisation** | 30% | Similarité de la commune |
| **Gestionnaire** | 20% | Similarité du gestionnaire |
| **Contact** | 15% | Téléphone ou email identiques |

### 2. Décision de fusion

| Score | Action | Description |
|-------|--------|-------------|
| **100%** | ✅ Fusion automatique | Doublon certain (noms identiques après normalisation) |
| **60-99%** | 🤖 Validation LLM | Cas ambigus nécessitant analyse sémantique |
| **< 60%** | ❌ Établissements distincts | Pas de fusion |

### 3. Fusion intelligente

Lorsque plusieurs établissements sont identifiés comme doublons :

1. **Calcul du score de complétude** pour chaque enregistrement :
   - Nombre de champs renseignés
   - Qualité des données (longueur, format)
   - Poids par importance de champ

2. **Conservation de l'établissement le plus complet**

3. **Complétion avec les données manquantes** des doublons

4. **Traçabilité** : métadonnées de fusion conservées

## Métriques de similarité

### Normalisation du texte

Avant comparaison, tous les textes sont normalisés :
- Conversion en minuscules
- Suppression des accents
- Suppression de la ponctuation
- Normalisation des espaces

Exemples :
```
"Ages & Vie Essoyes" → "ages vie essoyes"
"AGES & VIE CHARMONT-SOUS-BARBUISE" → "ages vie charmont sous barbuise"
```

### Distance de Levenshtein

Mesure le nombre d'opérations (insertion, suppression, substitution) nécessaires pour transformer une chaîne en une autre.

```python
levenshtein_distance("age vie", "ages vie") = 1
```

### SequenceMatcher

Calcule le ratio de similarité entre deux chaînes (basé sur les sous-séquences communes).

```python
SequenceMatcher("charmont sous barbuise", "charmont-sous-barbuise").ratio() = 0.96
```

## Validation LLM

Pour les cas ambigus (score 60-99%), le système utilise Groq (llama-3.1-8b-instant) pour analyser :

**Prompt type** :
```
Établissement 1: {nom, commune, gestionnaire, ...}
Établissement 2: {nom, commune, gestionnaire, ...}

Question: S'agit-il du MÊME établissement?
```

**Réponse attendue** :
```json
{
  "same": true/false,
  "confidence": 0-100,
  "reason": "explication"
}
```

### Coût LLM

- **Modèle** : llama-3.1-8b-instant
- **Tarif** : ~$0.05/1M tokens input, ~$0.08/1M tokens output
- **Coût moyen par validation** : ~$0.00001
- **Optimisation** : Validation uniquement si nécessaire (score 60-99%)

## Score de complétude

Le score de complétude évalue la qualité d'un enregistrement :

```python
{
    'nom': 10,           # Poids 10
    'commune': 10,       # Poids 10
    'adresse_l1': 15,    # Poids 15 (très important)
    'telephone': 12,     # Poids 12
    'email': 12,         # Poids 12
    'gestionnaire': 9,   # Poids 9
    'code_postal': 8,    # Poids 8
    'site_web': 8,       # Poids 8
    'presentation': 10   # Poids 10
}
```

**Bonus** : Les champs plus longs (plus d'information) reçoivent un bonus.

## Résultats attendus

### Cas d'usage : Aube (12 établissements)

**Avant déduplication** :
- 12 établissements
- ~30% de doublons (noms légèrement différents)

**Après déduplication** :
- 7-9 établissements uniques
- Doublons fusionnés intelligemment
- Coût < $0.01

### Exemples de doublons détectés

```
✓ "AGES & VIE ESSOYES" ≈ "Maison Ages & Vie de Essoyes"
✓ "AGES & VIE CHARMONT-SOUS-BARBUISE" ≈ "Age & Vie Charmont-sous-Barbuise"
✓ "Maison Ages & Vie de Charmont sous Barbuise" (x3 variations)
```

## Statistiques

Les statistiques suivantes sont collectées :

```python
{
    'total_records': 12,
    'duplicate_groups': 3,
    'automatic_merges': 0,
    'llm_validations': 8,
    'llm_confirmed': 8,
    'llm_rejected': 0,
    'final_records': 7,
    'llm_cost': 0.000086
}
```

## Métadonnées de fusion

Pour chaque fusion, des métadonnées sont générées :

```python
{
    'kept_record_index': 10,
    'kept_record_name': 'Maison Ages & Vie de Essoyes',
    'completeness_score': 24.4,
    'merged_from': [
        {
            'index': 0,
            'nom': 'AGES & VIE ESSOYES',
            'completeness': 23.8
        }
    ],
    'fusion_method': 'automatic',
    'total_merged': 2
}
```

## Bonnes pratiques

### 1. Validation manuelle

Après déduplication, vérifiez manuellement :
- Les établissements conservés sont bien les plus complets
- Aucun faux positif (établissements distincts fusionnés)
- Tous les doublons ont été détectés

### 2. Ajustement des seuils

Les seuils peuvent être ajustés dans `intelligent_deduplicator.py` :

```python
# Score 100%: Fusion automatique
if score >= 100:
    union(i, j)

# Score 60-99%: Validation LLM (ajustable)
elif score >= 60:  # Modifier ici pour changer le seuil
    is_duplicate = self._validate_with_llm(...)
```

### 3. Cache des comparaisons

Le système cache les résultats LLM pour éviter les appels redondants.

### 4. Mode sans LLM

Si aucune clé API Groq n'est fournie, le système utilise un fallback automatique basé sur le score (seuil à 75%).

## Limitations

1. **Établissements dans des communes différentes** : Non détectés comme doublons (par design)
2. **Noms très différents** : Peuvent ne pas être détectés même s'il s'agit du même établissement
3. **Fusion irréversible** : Une fois fusionnés, les doublons ne peuvent pas être séparés automatiquement

## Améliorations futures

- [ ] Support de la détection par coordonnées GPS (établissements très proches)
- [ ] Batch validation LLM (plusieurs comparaisons dans un seul prompt)
- [ ] Interface de validation manuelle pour les cas ambigus
- [ ] Export des métadonnées de fusion en JSON
- [ ] Détection de doublons inter-départements

## Support

Pour toute question ou problème :
1. Consultez le rapport généré (`DEDUPLICATION_REPORT.md`)
2. Vérifiez les logs de console (verbose)
3. Examinez les métadonnées de fusion

---

**Version** : 1.0.0  
**Dernière mise à jour** : 2025-12-02  
**Auteur** : Pipeline Module 6
