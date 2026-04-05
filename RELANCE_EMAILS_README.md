# Script d'Envoi de Relance - 250 Prospects

Processus de création et d'envoi d'emails de relance aux 250 prospects (excluant le Pas-de-Calais).

## 📋 Vue d'ensemble

### 1. **Préparation** (`send_follow_up_emails.py`)
- Charge le fichier `prospection_250_FINAL_FORMATE_V2.xlsx` (enrichi avec emails dirigeants)
- **Exclut automatiquement** les 4 prospects du Pas-de-Calais (62)
- Récupère les emails via HIÉRARCHIE OPTIMISÉE (6 niveaux):
  1. Email Dirigeant 1 (pattern reconstitué - meilleur!)
  2. Email Dirigeant 2 (variante)
  3. Email Dirigeant 3 (variante)
  4. email_contact (contact principal)
  5. Email Organisation (accueil/direction/siège)
  6. emails_generiques (autres contacts)
- Génère des emails personnalisés avec :
  - Salutation personnalisée (nom du dirigeant)
  - Sujet standardisé
  - Corps d'email custom (ConfidensIA)
- Sauvegarde les préparations en **CSV** et **JSON**

**Résultat**: 
- 246 prospects après filtrage Pas-de-Calais [OK]
- 237 prospects avec au moins 1 email [OK]
- 9 prospects sans source email [WARNING]
- **1085 emails préparés** (+375% vs before!) [OPTIMIZED]
- Moyenne: 4.6 emails par prospect

**Fichiers générés**:
```
outputs/relance_emails/preparation_emails_YYYYMMDD_HHMMSS.csv
outputs/relance_emails/preparation_emails_YYYYMMDD_HHMMSS.json
```

### 2. **Envoi** (`send_emails_elasticmail.py`)
- Charge les emails préparés
- Envoie via l'API Elasticmail v3
- Support du mode **DRY RUN** (simulation) ou **PRODUCTION** (envoi réel)
- Gère les délais entre les envois (1 sec par défaut)
- Génère un rapport d'envoi

**Modes**:
- **DRY RUN** (défaut): Simule l'envoi, affiche les emails
- **PRODUCTION**: Envoie réellement via Elasticmail

## 🚀 Utilisation

### Étape 1: Préparer les emails
```bash
python send_follow_up_emails.py
```

Sortie:
- Charge 250 prospects
- Exclut 4 du Pas-de-Calais [OK]
- Traite 246 prospects
- Identifie 9 sans email
- Prépare 1085 emails [OPTIMIZED]
- Sauvegarde CSV + JSON
- Affiche 3 exemples

### Étape 2: Envoyer les emails
```bash
python send_emails_elasticmail.py
```

Par défaut en **MODE TEST** (dry_run=True). Pour envoyer réellement:

1. Ouvrir `send_emails_elasticmail.py`
2. Ligne ~200: changer `dry_run=True` → `dry_run=False`
3. Relancer le script

## 📊 Contenu des emails

**Sujet**: `Démo gratuite ConfidensIA - Pseudonymisation pour ESSMS`

**Corps**: Message de relance personnalisé incluant:
- Rappel du courrier initial
- Lien vers la démo: https://confidensia.fr/demo
- Description des fonctionnalités
- Signature Patrick Danto

**Personnalisation**:
- Salutation avec nom du dirigeant
- Email ciblé (plusieurs par prospect si disponibles)

## 📂 Fichiers impliqués

| Fichier | Type | Rôle |
|---------|------|------|
| `prospection_250_FINAL_FORMATE_V2.xlsx` | Input | 250 prospects enrichis avec emails dirigeants |
| `send_follow_up_emails.py` | Script | Préparation des emails (hiérarchie 6 niveaux) |
| `send_emails_elasticmail.py` | Script | Envoi via Elasticmail |
| `outputs/relance_emails/*.csv` | Output | Résumé des emails |
| `outputs/relance_emails/*.json` | Output | Détail complet et corps |

## 🔧 Configuration

**.env requis:**
```
ELASTICMAIL_API_KEY=1245324F552DB3565B70DE8168428ADB4253483F1560797A015A5C9410004FE9DB519D24FC4FF1BC01C26F047EBBE64B
```

## 📊 Statistiques

### Filtrage Pas-de-Calais (62)
- **Prospects initiaux**: 250
- **Exclusions (62)**: 4
- **Prospects traités**: 246

### Génération d'emails
- **Emails générés**: 289
- **Prospects avec email**: 246
- **Prospects sans email**: 0 (après préparation)

## ⚠️ Notes importantes

1. **API Elasticmail actuellement en maintenance** 
   - Les tests d'envoi échouent avec "Coming Soon"
   - Les scripts sont prêts pour quand l'API revient

2. **Délai d'envoi**
   - 1 seconde par défaut entre chaque email
   - À adapter selon les limites d'Elasticmail

3. **Vérification avant production**
   - Toujours tester en DRY RUN d'abord
   - Vérifier les exemples générés
   - Confirmer les exclusions (Pas-de-Calais)

## 📝 Logs et rapports

- **CSV de préparation**: Résumé 1 ligne par email
- **JSON de préparation**: Détail complet (corps, sujet, etc.)

---

## [NEW] Version 2 - Hiérarchie Complète (22/02/2026)

**Mise à jour majeure**: Utilisation des emails dirigeants reconstruits

### Changements:
- **Fichier source**: outputs/prospection_250_FINAL_FORMATE_V2.xlsx (enrichi)
- **Hiérarchie d'extraction**: 6 niveaux vs 2 avant
- **Emails générés**: 1085 vs 289 (+375%!)
- **Average per prospect**: 4.6 vs 1.2

### Hiérarchie:
```
1. Email Dirigeant 1 (230 prospects) - MEILLEUR
2. Email Dirigeant 2 (~210 prospects)
3. Email Dirigeant 3 (~200 prospects)
4. email_contact (172 prospects)
5. Email Organisation (174 prospects)
6. emails_generiques (168 prospects)
```

### Dry Run Test Success (22/02/2026):
- 250 prospects loaded
- 4 exclusions (Pas-de-Calais)
- 246 processed
- 9 without email
- 237 with email
- **1085 emails generated** [SUCCESS]

**Voir**: MISE_A_JOUR_V2.md pour détails complets
- **Rapport d'envoi**: Statistiques finales et mode utilisé

## 🎯 Prochaines étapes

1. ✅ Préparation des emails (DONE)
2. ⏳ Attendre que Elasticmail revienne en ligne
3. 🚀 Lancer l'envoi en PRODUCTION
4. 📈 Suivre les taux de délivraison/ouverture
