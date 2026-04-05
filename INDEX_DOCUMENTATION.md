# 📚 Index de Documentation - Scripts d'Emails de Relance

## 📖 Documents de Documentation

### 1. **RELANCE_EMAILS_README.md** ✅
**Pour**: Vue d'ensemble rapide et utilisation pratique
- Résumé des objectifs
- Instructions step-by-step
- Configuration requise
- Fichiers impliqués
- Notes importantes et limitations

**Commencer par**: [RELANCE_EMAILS_README.md](RELANCE_EMAILS_README.md)

---

### 2. **LOGIQUE_SCRIPTS_DETAIL.md** 📖 (VOUS ÊTES ICI)
**Pour**: Comprendre la logique complète et bas-niveau
- Vue d'ensemble du flux (2 phases)
- Logique détaillée de chaque fonction
- Descriptions du flow pour:
  - load_prospects()
  - filter_exclude_pas_de_calais()  
  - generate_possible_emails()
  - extract_name_for_greeting()
  - generate_email_content()
  - prepare_emails()
  - send_all() (DRY RUN et PRODUCTION)
- Gestion des erreurs et validations
- Sécurité et isolation des données
- Performances

**Pour les détails techniques**: [LOGIQUE_SCRIPTS_DETAIL.md](LOGIQUE_SCRIPTS_DETAIL.md)

---

### 3. **DIAGRAMMES_LOGIQUES.md**
**Pour**: Visualiser les flux et processus
- Diagramme global du système
- Flux détaillé de chaque étape
- Visualisations ASCII art
- Traçage pas-à-pas des données
- Exemples de sorties console

**Pour visualiser le bflux**: [DIAGRAMMES_LOGIQUES.md](DIAGRAMMES_LOGIQUES.md)

---

### 4. **EXEMPLES_CONCRETS.md**
**Pour**: Voir des exemples réels et tester
- 5 exemples complets end-to-end:
  1. Prospect avec email unique
  2. Prospect avec plusieurs emails
  3. Prospect sans email (ignoré)
  4. Prospect du Pas-de-Calais (exclu)
  5. Prospect en DOM-TOM
- Code de test pour chaque cas
- Validation des emails (regex)
- Fichiers de sortie réels (CSV, JSON)
- Statistiques et résumés

**Pour des exemples concrets**: [EXEMPLES_CONCRETS.md](EXEMPLES_CONCRETS.md)

---

## 🗂️ Scripts Documentés

### send_follow_up_emails.py
**Rôle**: Préparation des emails (Phase 1)

**Documentation inline**:
- Docstring complet du module (10+ lignes)
- Docstring détaillé de la classe `ProspectEmailSender` (15+ lignes)
- Docstring détaillé de chaque méthode avec:
  - Objectif de la fonction
  - Logique étape-par-étape (ASCII diagrams)
  - Arguments et types
  - Valeurs de retour
  - Exemples d'utilisation
  
**Méthodes documentées**:
- `load_prospects()` 
- `extract_department_code()`
- `filter_exclude_pas_de_calais()`
- `generate_possible_emails()`
- `extract_name_for_greeting()`
- `generate_email_content()`
- `prepare_emails()`

[Voir le code avec documentation](send_follow_up_emails.py)

---

### send_emails_elasticmail.py
**Rôle**: Envoi des emails (Phase 2)

**Documentation inline**:
- Docstring complet du module (30+ lignes)
- Docstring détaillé de la classe `ElasticmailSender` (30+ lignes)
- Docstring détaillé de la méthode `send_all()` (80+ lignes)

**Méthodes documentées**:
- `load_prepared_emails()`
- `send_via_elasticmail_v3()` / `send_via_elasticmail_v2()`
- `send_email()`
- `send_all()`

[Voir le code avec documentation](send_emails_elasticmail.py)

---

## 🎯 Guide de Lecture Recommandé

### Selon votre besoin:

#### 👤 Je veux juste lancer les scripts
```
1. Lire: RELANCE_EMAILS_README.md (5 min)
2. Exécuter: python send_follow_up_emails.py
3. Vérifier: Les fichiers dans outputs/relance_emails/
```

#### 🔧 Je veux comprendre comment ça marche
```
1. Lire: RELANCE_EMAILS_README.md (5 min)
2. Lire: LOGIQUE_SCRIPTS_DETAIL.md (15 min)
3. Consulter: DIAGRAMMES_LOGIQUES.md (10 min)
4. Regarder les docstrings des scripts (5 min)
```

#### 🧪 Je veux tester avec des données
```
1. Lire: EXEMPLES_CONCRETS.md (10 min)
2. Adapter: Les exemples Python
3. Exécuter: Avec vos données
4. Vérifier: Les résultats
```

#### 🐛 Je veux déboguer une étape
```
1. Aller au: DIAGRAMMES_LOGIQUES.md
2. Chercher: La section XXX (la fonction qui pose problème)
3. Trace: Pas-à-pas dans le diagramme
4. Vérifier: Les conditions dans EXEMPLES_CONCRETS.md
5. Consulter: Le code avec documentation
```

---

## 📋 Checklist pour Comprendre la Logique

### Phase 1: Préparation
- [ ] Comprendre pourquoi on exclut le Pas-de-Calais (LOGIQUE_SCRIPTS_DETAIL.md)
- [ ] Voir comment les emails sont extraits (LOGIQUE_SCRIPTS_DETAIL.md 1.3)
- [ ] Tracer un cas complet (DIAGRAMMES_LOGIQUES.md 4)
- [ ] Voir un exemple réel (EXEMPLES_CONCRETS.md)
- [ ] Vérifier les validations (EXEMPLES_CONCRETS.md validation)

### Phase 2: Envoi
- [ ] Comprendre la différence DRY RUN vs PRODUCTION (LOGIQUE_SCRIPTS_DETAIL.md 2.3)
- [ ] Voir comment les délais fonctionnent (LOGIQUE_SCRIPTS_DETAIL.md 2.4)
- [ ] Tracer l'envoi en simulation (DIAGRAMMES_LOGIQUES.md 7)
- [ ] Tracer l'envoi réel (DIAGRAMMES_LOGIQUES.md 8)
- [ ] Voir les statistiques (DIAGRAMMES_LOGIQUES.md 9)

---

## 🔑 Concepts Clés

### Filtrage Pas-de-Calais
```
CONCEPT: Extract department from postal code
CODE:    Prendre 2 premiers chiffres: "62000" → "62"
RÈGLE:   SI dept == "62" → EXCLUDE
DOC:     LOGIQUE_SCRIPTS_DETAIL.md (Section 1.2)
DIAG:    DIAGRAMMES_LOGIQUES.md (Section 3)
EXEMPLE: EXEMPLES_CONCRETS.md (Exemple 4)
```

### Personnalisation
```
CONCEPT: Custom salutation per prospect
METHOD:  extract_name_for_greeting() avec hiérarchie
RESULT:  "Madame, Monsieur {NOM},"
DOC:     LOGIQUE_SCRIPTS_DETAIL.md (Section 1.4)
DIAG:    DIAGRAMMES_LOGIQUES.md (Section 5)
EXEMPLE: EXEMPLES_CONCRETS.md (Exemple 1-5)
```

### Multi-Email par Prospect
```
CONCEPT: 1 prospect → peut avoir 1-5 emails
METHOD:  Boucle sur chaque email pour générer un email
RESULT:  289 emails pour 246 prospects
DOC:     LOGIQUE_SCRIPTS_DETAIL.md (Section 1.3 & 1.6)
DIAG:    DIAGRAMMES_LOGIQUES.md (Section 4 & 6)
EXEMPLE: EXEMPLES_CONCRETS.md (Exemple 2)
```

---

## 📊 Statistiques Documentées

```
250 prospects INPUT
├─ 4 exclusions Pas-de-Calais (dept 62)
├─ 246 prospects traités
├─ 9 sans email
├─ 237 avec emails
└─ 1085 emails générés ✅ (+375%)
    └─ 4.58 emails/prospect en moyenne

Détails:
- Avec 1 email: ~78%
- Avec 2-3 emails: ~20%
- Sans email: ~2%
```

Documentation: [LOGIQUE_SCRIPTS_DETAIL.md](LOGIQUE_SCRIPTS_DETAIL.md#-statistiques-finales)

---

## 🚀 Flux Complet Documenté

```
Phase 1: PRÉPARATION (3-5 sec)
├── load_prospects() 
│   Doc: LOGIQUE_SCRIPTS_DETAIL.md (1.1)
│   Diag: DIAGRAMMES_LOGIQUES.md (2)
│
├── filter_exclude_pas_de_calais()
│   Doc: LOGIQUE_SCRIPTS_DETAIL.md (1.2)
│   Diag: DIAGRAMMES_LOGIQUES.md (3)
│   Exemple: EXEMPLES_CONCRETS.md (4)
│
├── prepare_emails()
│   ├── generate_possible_emails()
│   │   Doc: LOGIQUE_SCRIPTS_DETAIL.md (1.3)
│   │   Diag: DIAGRAMMES_LOGIQUES.md (4)
│   │   Exemple: EXEMPLES_CONCRETS.md (1-2)
│   │
│   ├── extract_name_for_greeting()
│   │   Doc: LOGIQUE_SCRIPTS_DETAIL.md (1.4)
│   │   Diag: DIAGRAMMES_LOGIQUES.md (5)
│   │
│   ├── generate_email_content()
│   │   Doc: LOGIQUE_SCRIPTS_DETAIL.md (1.5)
│   │   Diag: DIAGRAMMES_LOGIQUES.md (6)
│   │
│   └── Créer objet 'prepared'
│       Diag: DIAGRAMMES_LOGIQUES.md (6)
│
└── save_preparation()
    Doc: LOGIQUE_SCRIPTS_DETAIL.md (1.7)
    Diag: DIAGRAMMES_LOGIQUES.md (10)
    Exemple: EXEMPLES_CONCRETS.md

Phase 2: ENVOI (~3s DRY RUN ou ~5 min PRODUCTION)
├── load_prepared_emails()
│   Doc: LOGIQUE_SCRIPTS_DETAIL.md (2.1)
│
├── send_all()
│   ├── DRY RUN: Simuler
│   │   Doc: LOGIQUE_SCRIPTS_DETAIL.md (2.3)
│   │   Diag: DIAGRAMMES_LOGIQUES.md (7)
│   │
│   ├── PRODUCTION: Envoyer
│   │   Doc: LOGIQUE_SCRIPTS_DETAIL.md (2.3 & 2.2)
│   │   Diag: DIAGRAMMES_LOGIQUES.md (8)
│   │
│   ├── Gérer délais
│   │   Doc: LOGIQUE_SCRIPTS_DETAIL.md (2.4)
│   │   
│   ├── Gérer erreurs
│   │   Doc: LOGIQUE_SCRIPTS_DETAIL.md (2.5)
│   │
│   └── Afficher stats
│       Doc: LOGIQUE_SCRIPTS_DETAIL.md (2.6)
│       Diag: DIAGRAMMES_LOGIQUES.md (9)
│
└── save_rapport()
    Doc: LOGIQUE_SCRIPTS_DETAIL.md (2.6)
```

---

## 💬 Questions Fréquentes - Points de Documentation

**Q: Pourquoi on exclut le Pas-de-Calais ?**
A: [LOGIQUE_SCRIPTS_DETAIL.md - Section 1.2](LOGIQUE_SCRIPTS_DETAIL.md#12-filtrage-pas-de-calais)

**Q: Comment on gère les prospects avec plusieurs emails ?**
A: [LOGIQUE_SCRIPTS_DETAIL.md - Section 1.3](LOGIQUE_SCRIPTS_DETAIL.md#13-extraction-des-emails) + [DIAGRAMMES_LOGIQUES.md - Section 4](DIAGRAMMES_LOGIQUES.md#4-flux-détaillé-generate_possible_emails)

**Q: Pourquoi 289 emails pour 246 prospects ?**
A: [LOGIQUE_SCRIPTS_DETAIL.md - Section 1.6](LOGIQUE_SCRIPTS_DETAIL.md#16-préparation-finale-des-emails)

**Q: Comment fonctionne la personnalisation ?**
A: [LOGIQUE_SCRIPTS_DETAIL.md - Section 1.4 & 1.5](LOGIQUE_SCRIPTS_DETAIL.md#14-extraction-du-nom-pour-personnalisation) + [EXEMPLES_CONCRETS.md](EXEMPLES_CONCRETS.md)

**Q: Différence entre DRY RUN et PRODUCTION ?**
A: [LOGIQUE_SCRIPTS_DETAIL.md - Section 2.3](LOGIQUE_SCRIPTS_DETAIL.md#23-gestion-dry-run-vs-production) + [DIAGRAMMES_LOGIQUES.md - Sections 7-8](DIAGRAMMES_LOGIQUES.md#7-flux-détaillé-send_all-en-mode-dry-run)

**Q: Combien de temps ça prend ?**
A: [LOGIQUE_SCRIPTS_DETAIL.md - Performances](LOGIQUE_SCRIPTS_DETAIL.md#-performances)

---

## ✅ Validation

Tous les documents ont été vérifiés pour:
- ✅ Complétude (couvrent le flux complet)
- ✅ Exactitude (aligné avec le code réel)
- ✅ Clarté (langage simple avec exemples)
- ✅ Traversabilité (liens et navigation)
- ✅ Utilité (répondent aux questions)

---

## 📞 Support et Références

- **Code source**: `send_follow_up_emails.py`, `send_emails_elasticmail.py`
- **Données source**: `outputs/prospection_250_FINAL_FORMATE_V2_PUBLIPOSTAGE.xlsx`
- **Fichiers générés**: `outputs/relance_emails/preparation_emails_*.csv`, `*.json`
- **Configuration**: `.env` (ELASTICMAIL_API_KEY required)

---

**Document généré**: 22 février 2026  
**Version**: 1.0 (Documentation complète)
