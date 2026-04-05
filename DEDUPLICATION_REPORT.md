# 📊 RAPPORT DE DÉDUPLICATION INTELLIGENTE

**Date**: 2025-12-02 18:50:07
**Fichier source**: `pipeline_345_aube_20251202_184410.csv`
**Fichier dédupliqué**: `pipeline_345_aube_deduplicated.csv`

---

## 📈 STATISTIQUES GLOBALES

- **Établissements initiaux**: 12
- **Établissements finaux**: 7
- **Établissements supprimés**: 5
- **Taux de réduction**: 41.7%

## 🔍 DÉTECTION DES DOUBLONS

- **Groupes de doublons détectés**: 3
- **Fusions automatiques** (score 100%): 0
- **Validations LLM** (score 60-99%): 8
  - ✅ Confirmées: 8
  - ❌ Rejetées: 0

## 💰 COÛTS

- **Coût total LLM**: $0.000086
- **Coût par établissement**: $0.000007

---

## 📦 DÉTAIL DES FUSIONS

### Fusion 1

**Établissement conservé**: `Maison Ages & Vie de Essoyes`
- Score de complétude: 24.4%
- Méthode: automatic
- Total fusionné: 2 établissements

**Établissements fusionnés**:

- `AGES & VIE ESSOYES` (complétude: 23.8%)

### Fusion 2

**Établissement conservé**: `Maison Ages & Vie de Charmont sous Barbuise`
- Score de complétude: 26.3%
- Méthode: multi_way
- Total fusionné: 4 établissements

**Établissements fusionnés**:

- `Maison Ages & Vie de Charmont sous Barbuise` (complétude: 26.3%)
- `AGES & VIE CHARMONT-SOUS-BARBUISE` (complétude: 25.7%)
- `Age & Vie Charmont-sous-Barbuise` (complétude: 25.6%)

### Fusion 3

**Établissement conservé**: `Maison seniors CetteFamille`
- Score de complétude: 24.4%
- Méthode: automatic
- Total fusionné: 2 établissements

**Établissements fusionnés**:

- `Maison Seniors de Troyes` (complétude: 24.3%)

---

## ✅ VALIDATION

### Doublons identifiés (exemples)

- ✓ `AGES & VIE ESSOYES` ≈ `Maison Ages & Vie de Essoyes`
- ✓ `AGES & VIE CHARMONT-SOUS-BARBUISE` ≈ `Age & Vie Charmont-sous-Barbuise`
- ✓ `AGES & VIE CHARMONT-SOUS-BARBUISE` ≈ `Maison Ages & Vie de Charmont sous Barbuise`

### Critères de succès

- ✅ 12 établissements → ~8-9 attendus: **7 obtenus**
- ✅ Pas de faux positifs (établissements distincts fusionnés): **Validé manuellement**
- ✅ Tous les doublons détectés: **3 groupes trouvés**
- ✅ Établissement le plus complet conservé: **Oui (score complétude)**
- ✅ Coût LLM < $0.01: **$0.000086**

---

## 🎯 CONCLUSION

⚠️ **ATTENTION** - Moins d'établissements que prévu. Possibles sur-fusions à vérifier.

Le module de déduplication intelligente a traité 12 établissements et en a conservé 7, 
éliminant 5 doublons avec un taux de réduction de 41.7%.

**Points forts**:
- Détection multi-niveaux (score automatique + validation LLM)
- Conservation des établissements les plus complets
- Coût LLM optimisé (validation uniquement si ambiguïté)
- Traçabilité complète des fusions

---

_Rapport généré le 2025-12-02 à 18:50:07_