# Recherches Supplémentaires Spécifiques aux Départements

## Vue d'ensemble

Le pipeline v3.0 supporte maintenant des **recherches supplémentaires ciblées** pour les gros départements afin de maximiser la couverture des établissements.

## Configuration automatique

### Département 62 (Pas-de-Calais)

Pour le département 62, **3 requêtes supplémentaires** sont automatiquement ajoutées (3 résultats chacune = 9 résultats max) :

1. **Établissements spécifiques recherchés** :
   ```
   "habitat inclusif" ("Down up" OR "ferme sénéchal") Pas-de-Calais
   ```
   → Ciblage de Down Up et La Ferme Sénéchal

2. **Grandes villes** :
   ```
   "habitat inclusif" (Arras OR Lens OR Boulogne-sur-Mer) Pas-de-Calais
   ```
   → Couverture des 3 principales agglomérations

3. **Villes moyennes + public cible** :
   ```
   "habitat partagé" (seniors OR handicap) (Saint-Omer OR Béthune OR Calaisis) Pas-de-Calais
   ```
   → Extension aux villes moyennes avec ciblage seniors/handicap

## Utilisation CLI

### Mode automatique (département 62)
```bash
python pipeline_v3_cli.py -d 62
```
→ Les 3 requêtes supplémentaires sont ajoutées automatiquement

### Mode manuel (autres départements)
```bash
python pipeline_v3_cli.py -d 47 --extra-queries \
  "\"habitat inclusif\" Agen Lot-et-Garonne" \
  "\"colocation seniors\" (Villeneuve OR Marmande) Lot-et-Garonne"
```

### Désactiver les recherches supplémentaires (dept 62)
```bash
python pipeline_v3_cli.py -d 62 --extra-queries
```
→ Passer `--extra-queries` sans arguments désactive la configuration automatique

## Bonnes pratiques

### Stratégie de requêtes

1. **Ciblage établissements manquants** : Utiliser des noms spécifiques
2. **Couverture géographique** : Grandes villes en priorité
3. **Public cible** : Seniors ET handicap pour maximiser la couverture
4. **Limiter à 3 requêtes** : Éviter le surcoût API (9 résultats max)

### Syntaxe Google Search

- `"habitat inclusif"` : Recherche exacte (recommandé pour termes techniques)
- `(terme1 OR terme2)` : Opérateur booléen OR
- `nom_etablissement` : Sans guillemets pour variantes orthographiques
- Toujours inclure le nom du département en fin de requête

### Quand utiliser cette fonctionnalité ?

✅ **OUI** :
- Gros départements (>500k habitants)
- Établissements spécifiques non trouvés lors du run initial
- Couverture insuffisante des grandes villes

❌ **NON** :
- Petits départements (redondance avec requêtes standard)
- Première exécution exploratoire
- Budget API limité

## Impact

### Coûts
- **3 requêtes Serper** : 3 × $0.001 = $0.003
- **Classification Groq** : ~$0.001 (9 snippets à classifier)
- **Total par run** : ~$0.004 supplémentaires

### Bénéfices
- Couverture accrue des établissements ciblés
- Meilleure représentation des grandes villes
- Détection d'établissements avec faible visibilité web

## Configuration future

Pour ajouter d'autres départements avec recherches spécifiques, modifier `pipeline_v3_cli.py` :

```python
# Lot-et-Garonne (47): Exemple de configuration
if args.department == '47' and not extra_queries:
    extra_queries = [
        '"habitat inclusif" Agen Lot-et-Garonne',
        '"colocation seniors" (Villeneuve OR Marmande) Lot-et-Garonne',
        '"béguinage" Lot-et-Garonne'
    ]
```

## Logs

Les requêtes supplémentaires apparaissent dans les logs :

```
🎯 Requêtes supplémentaires spécifiques (3 requêtes)...

   📋 Requête spécifique 1/3: "habitat inclusif" ("Down up" OR "ferme sénéchal")...
      → 3 résultats
   
   📋 Requête spécifique 2/3: "habitat inclusif" (Arras OR Lens OR Boulogne-sur-Mer)...
      → 3 résultats
   
   📋 Requête spécifique 3/3: "habitat partagé" (seniors OR handicap) (Saint-Omer OR...
      → 3 résultats

📊 Total après requêtes spécifiques: 84 résultats
```
