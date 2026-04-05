# Diagrammes - Flux Logiques Détaillés

## 📊 1. Diagramme Global

```
┌──────────────────────────────────────────────────────────────────────┐
│                    SYSTÈME D'EMAILS DE RELANCE                        │
│                         (289 emails → 246 prospects)                  │
└──────────────────────────────────────────────────────────────────────┘

                         FICHIER SOURCE
                              ↓
                   prospection_250.xlsx
                   (250 prospects)
                              ↓
        ╔════════════════════════════════════════╗
        ║   PHASE 1: PRÉPARATION (3-5 secondes)  ║
        ║   send_follow_up_emails.py             ║
        ╚════════════════════════════════════════╝
                              ↓
        load_prospects() → 250 prospects chargés
                              ↓
        filter_exclude_pas_de_calais() → 246 prospects
                    [EXCLU: 4 du dept 62]
                              ↓
        prepare_emails() → 289 emails générés
                  [Certains prospects: 1-3 emails]
                              ↓
        save_preparation()
            ├─ preparation_emails_XXX.csv (43 KB)
            └─ preparation_emails_XXX.json (506 KB)
                              ↓
        ╔════════════════════════════════════════╗
        ║   PHASE 2: ENVOI (3s ou ~5 minutes)    ║
        ║   send_emails_elasticmail.py           ║
        ╚════════════════════════════════════════╝
                              ↓
        load_prepared_emails() → 289 emails chargés
                              ↓
        send_all()
            ├─ DRY RUN: Simuler l'envoi
            │   [SIMULATION 1/289] À: email1@example.com
            │   [SIMULATION 2/289] À: email2@example.com
            │   ...
            │   ✅ 289 emails simulés
            │
            └─ PRODUCTION: Envoyer pour de vrai
                ✅ [1/289] À: email1@example.com
                ✅ [2/289] À: email2@example.com
                # avec délai 1s entre chaque
                ...
                ✅ [289/289] À: email289@example.com
                Temps total: ~5 minutes
                              ↓
        save_rapport()
            └─ envoi_rapport_XXX.json
                {
                  "mode": "DRY RUN ou PRODUCTION",
                  "stats": {
                    "total": 289,
                    "sent": 289,
                    "failed": 0
                  }
                }
```

---

## 🔄 2. Flux Détaillé: load_prospects()

```
load_prospects()
    │
    ├─ Vérifier que le fichier existe
    │   └─ EXISTS: prospection_250_FINAL_FORMATE_V2_PUBLIPOSTAGE.xlsx
    │       ✅ Fichier trouvé
    │
    ├─ Lire avec pandas.read_excel()
    │   └─ Pandas charge le fichier Excel
    │   └─ 250 lignes × 25 colonnes
    │
    ├─ Convertir en liste de dictionnaires
    │   ├─ prospect[0] = {
    │   │   'finess_ej': 750808511,
    │   │   'gestionnaire_nom': "SOCIETE D'ECONOMIE MIXTE",
    │   │   'gestionnaire_adresse': "33 AV PIERRE..., 75013 PARIS",
    │   │   'email_contact': "contact@semimo.fr",
    │   │   'emails_generiques': "contact@semimo.fr",
    │   │   ...  [20 autres colonnes]
    │   │ }
    │   ├─ prospect[1] = { ... }
    │   └─ prospect[249] = { ... }
    │
    ├─ Afficher infos
    │   ├─ ✅ 250 prospects chargés
    │   └─ Colonnes: finess_ej, gestionnaire_nom, ...
    │
    └─ Retourner List[dict]
        └─ 250 prospects prêts à traiter
```

---

## 🗑️ 3. Flux Détaillé: filter_exclude_pas_de_calais()

```
filter_exclude_pas_de_calais(250 prospects)
    │
    ├─ Boucle n° 1
    │   ├─ prospect['gestionnaire_adresse'] = "33 AV..., 75013 PARIS"
    │   │   └─ Regex cherche: \b(\d{5})\b
    │   │   └─ Trouve: "75013"
    │   ├─ extract_department_code("75013")
    │   │   └─ Prendre 2 premiers chiffres: "75"
    │   ├─ Vérifier: "75" != "62" ?
    │   │   └─ OUI → Ajouter à FILTERED ✅
    │   │
    ├─ [...249 autres boucles...]
    │   │
    │   [Supposons prospect 100]
    │   ├─ prospect['gestionnaire_adresse'] = "45 RUE, 62000 ARRAS"
    │   │   └─ Regex cherche: \b(\d{5})\b
    │   │   └─ Trouve: "62000"
    │   ├─ extract_department_code("62000")
    │   │   └─ Prendre 2 premiers chiffres: "62"
    │   ├─ Vérifier: "62" != "62" ?
    │   │   └─ NON → Ajouter à EXCLUDED 🗑️
    │
    └─ Résultat final:
        ├─ FILTERED: 246 prospects (de dept != 62)
        ├─ EXCLUDED: 4 prospects (dept 62)
        └─ Afficher:
            🔍 Filtrage Pas-de-Calais (62)
              Prospects initiaux: 250
              Exclusions (62): 4
              Prospects restants: 246
```

---

## 📧 4. Flux Détaillé: generate_possible_emails()

```
generate_possible_emails(prospect) → List[str]
    │
    ├─ STRATÉGIE 1: Email contact principal
    │   ├─ prospect['email_contact'] = "contact@semimo.fr"
    │   ├─ Valider format:
    │   │   └─ Regex: ^[a-zA-Z0-9._%+-]+@[...]+\.[a-z]{2,}$
    │   │   └─ ✅ Valide
    │   ├─ Ajouter à emails_list
    │   │   └─ emails = ["contact@semimo.fr"]
    │   │
    ├─ STRATÉGIE 2: Emails génériques
    │   ├─ prospect['emails_generiques'] = "contact@apf.asso.fr;info@apf.asso.fr;dpo@apf.asso.fr"
    │   ├─ Splitter par ";"
    │   │   └─ ["contact@apf.asso.fr", "info@apf.asso.fr", "dpo@apf.asso.fr"]
    │   ├─ POUR CHAQUE email:
    │   │   ├─ Valider format (regex)
    │   │   │   └─ ✅ Tous valides
    │   │   └─ Ajouter à emails_list
    │   │
    │   ├─ emails = [
    │   │   "contact@apf.asso.fr",
    │   │   "info@apf.asso.fr",
    │   │   "dpo@apf.asso.fr"
    │   │ ]
    │   │
    ├─ ÉLIMINATION DES DOUBLONS
    │   └─ Si 'contact@semimo.fr' apparait dans les 2 colonnes
    │       └─ Le garder une seule fois
    │
    └─ Retourner List[str]
        ├─ Cas 1: ["contact@semimo.fr"]
        ├─ Cas 2: ["contact@apf.asso.fr", "info@apf.asso.fr"]
        └─ Cas 3: [] (pas d'email = à ignorer)
```

---

## 👤 5. Flux Détaillé: extract_name_for_greeting()

```
extract_name_for_greeting(prospect) → str
    │
    ├─ PRIORITÉ 1: Dirigeant nom
    │   ├─ prospect['dirigeant_nom'] = "Baudry S"
    │   ├─ NON vide et NON NaN ?
    │   │   └─ OUI
    │   ├─ Extraire dernier mot
    │   │   ├─ Split: ["Baudry", "S"]
    │   │   └─ Dernier: "S"
    │   └─ Retourner "S" ✅
    │
    ├─ [SI priorité 1 échoue] PRIORITÉ 2: Gestionnaire nom
    │   ├─ prospect['gestionnaire_nom'] = "CROIX ROUGE FRANCAISE"
    │   ├─ NON vide et NON NaN ?
    │   │   └─ OUI
    │   └─ Retourner "CROIX ROUGE FRANCAISE" ✅
    │
    ├─ [SI priorité 2 échoue] PRIORITÉ 3: Nom public
    │   ├─ prospect['nom_public'] = "Croix-Rouge Française"
    │   ├─ NON vide et NON NaN ?
    │   │   └─ OUI
    │   └─ Retourner "Croix-Rouge Française" ✅
    │
    └─ [SI toutes échouent] FALLBACK: Défaut
        └─ Retourner "Madame, Monsieur" ✅


RÉSULTATS:
    Prospect A: dirigeant_nom = "Baudry S" → Retour: "S"
    Prospect B: dirigeant_nom = NULL, gestionnaire_nom = "CROIX ROUGE" → Retour: "CROIX ROUGE"
    Prospect C: Aucune info → Retour: "Madame, Monsieur"
```

---

## ✉️ 6. Flux Détaillé: prepare_emails()

```
prepare_emails(246 prospects) → List[dict]
    │
    ├─ POUR prospect #1 (CROIX ROUGE FRANCAISE)
    │   ├─ generate_possible_emails()
    │   │   └─ Retour: ["contact@croixrouge.fr"]
    │   ├─ SI pas de email:
    │   │   └─ ⚠️ Afficher et SKIP
    │   ├─ SINON:
    │   │   ├─ generate_email_content()
    │   │   │   └─ Retour: (
    │   │   │      "Démo gratuite ConfidensIA...",
    │   │   │      "Madame, Monsieur CROIX ROUGE,\n\nVous avez..."
    │   │   │     )
    │   │   │
    │   │   ├─ POUR email #1 = "contact@croixrouge.fr"
    │   │   │   └─ Créer objet:
    │   │   │       {
    │   │   │         'prospect_name': 'CROIX ROUGE FRANCAISE',
    │   │   │         'prospect_etablissement': 'Croix-Rouge',
    │   │   │         'recipient_email': 'contact@croixrouge.fr',
    │   │   │         'subject': 'Démo gratuite...',
    │   │   │         'body': 'Madame, Monsieur CROIX ROUGE,...',
    │   │   │         'sender': 'patrick.danto@bmse.fr'
    │   │   │       }
    │   │   │   └─ Ajouter à liste finale
    │   │   └─ [FIN BOUCLE EMAIL]
    │   │
    ├─ POUR prospect #2 (APF FRANCE HANDICAP)
    │   ├─ generate_possible_emails()
    │   │   └─ Retour: ["contact@apf.asso.fr", "info@apf.asso.fr"]
    │   ├─ generate_email_content() → (subject, body)
    │   │
    │   ├─ POUR email #1 = "contact@apf.asso.fr"
    │   │   └─ Créer objet + ajouter
    │   │
    │   ├─ POUR email #2 = "info@apf.asso.fr"
    │   │   └─ Créer objet + ajouter
    │   │
    │   └─ [FIN BOUCLE EMAIL]
    │
    ├─ [...244 autres prospects...]
    │
    └─ RÉSULTAT FINAL:
        ├─ Total prospects: 246
        ├─ Total emails: 289
        │   (246 × 1.18 emails par prospect en moyenne)
        │
        └─ Liste avec:
            ├─ Email 1: "contact@croixrouge.fr"
            ├─ Email 2: "contact@apf.asso.fr"
            ├─ Email 3: "info@apf.asso.fr"
            ├─ Email 4: ...
            └─ Email 289: ...
```

---

## 🔌 7. Flux Détaillé: send_all() en MODE DRY RUN

```
send_all(289 emails, dry_run=True)
    │
    ├─ Afficher header
    │   └─ "ENVOI DES EMAILS - 289 total"
    │
    ├─ POUR CHAQUE email (i = 1 à 289)
    │   ├─ [DRY RUN MODE]
    │   │   ├─ Afficher "[SIMULATION i/289] À: email@example.com"
    │   │   ├─ Incrémenter stats['sent'] += 1
    │   │   └─ Pas de delay (pas d'API call)
    │   │
    │   ├─ SI i % 50 == 0:  (Batch reporting)
    │   │   └─ Afficher "[Batch i/289] Taux de succès: 100.0%"
    │
    └─ RÉSULTAT:
        ├─ Affichage dans console:
        │   [SIMULATION 1/289] À: contact@croixrouge.fr
        │   [SIMULATION 2/289] À: contact@apf.asso.fr
        │   [SIMULATION 3/289] À: info@apf.asso.fr
        │   ...
        │   [Batch 50/289] Taux de succès: 100.0%
        │   [SIMULATION 51/289] À: ...
        │   ...
        │   [Batch 100/289] Taux de succès: 100.0%
        │   ...
        │
        ├─ Retour:
        │   {
        │     'total': 289,
        │     'sent': 289,
        │     'failed': 0,
        │     'skipped': 0
        │   }
        │
        └─ Durée: ~3 secondes
```

---

## 🔌 8. Flux Détaillé: send_all() en MODE PRODUCTION

```
send_all(289 emails, dry_run=False)
    │
    ├─ Afficher header
    │   └─ "ENVOI DES EMAILS - 289 total"
    │
    ├─ POUR CHAQUE email (i = 1 à 289)
    │   │
    │   ├─ Appeler send_email(email)
    │   │   │
    │   │   ├─ Préparer payload JSON:
    │   │   │   {
    │   │   │     "from": {
    │   │   │       "email": "patrick.danto@bmse.fr",
    │   │   │       "name": "Patrick Danto - ConfidensIA"
    │   │   │     },
    │   │   │     "to": [{"email": "contact@croixrouge.fr"}],
    │   │   │     "subject": "Démo gratuite ConfidensIA...",
    │   │   │     "bodyText": "Madame, Monsieur CROIX ROUGE,..."
    │   │   │   }
    │   │   │
    │   │   ├─ HTTP POST vers Elasticmail API
        │   │   │   URL: https://api.elasticmail.com/v3/emails/send
        │   │   │   Headers: {
        │   │   │     "X-ElasticEmail-ApiKey": "1245324F...",
        │   │   │     "Content-Type": "application/json"
        │   │   │   }
        │   │   │   Timeout: 30 secondes
        │   │   │
        │   │   ├─ Attendre réponse
        │   │   │   ├─ SI Code 200:
        │   │   │   │   └─ Retour: (True, "Envoyé avec succès")
        │   │   │   ├─ SINON:
        │   │   │   │   └─ Retour: (False, "Code 500: Internal Error")
        │   │   │   └─ Catch exception:
        │   │   │       └─ Retour: (False, "Erreur réseau: ...")
    │   │   │
    │   ├─ Vérifier résultat
    │   │   ├─ SI success:
    │   │   │   ├─ Afficher "✅ [i/289] À: email@example.com"
    │   │   │   └─ Incrémenter stats['sent'] += 1
    │   │   ├─ SINON:
    │   │   │   ├─ Afficher "❌ [i/289] À: email@example.com"
    │   │   │   ├─ Afficher "    ⚠️ Code 500: Internal Error"
    │   │   │   └─ Incrémenter stats['failed'] += 1
    │   │
    │   ├─ Respecter délai
    │   │   ├─ SI i < 289:  (pas le dernier)
    │   │   │   └─ sleep(1)  # Attendre 1 seconde
    │   │   ├─ SINON:
    │   │   │   └─ Pas d'attente
    │   │
    │   ├─ SI i % 50 == 0:  (Batch reporting)
    │   │   └─ Afficher "[Batch i/289] Taux de succès: 98.5%"
    │
    └─ RÉSULTAT:
        ├─ Affichage dans console:
        │   ✅ [1/289] À: contact@croixrouge.fr
        │   ✅ [2/289] À: contact@apf.asso.fr
        │   ✅ [3/289] À: info@apf.asso.fr
        │   ...
        │   ❌ [25/289] À: some@email.com
        │       ⚠️ Code 500: Server Error
        │   ...
        │   [Batch 50/289] Taux de succès: 96.0%
        │   [SIMULATION 51/289] À: ...
        │   ...
        │
        ├─ Retour (exemple avec 2 erreurs):
        │   {
        │     'total': 289,
        │     'sent': 287,
        │     'failed': 2,
        │     'skipped': 0
        │   }
        │
        └─ Durée: ~289 secondes (~5 minutes)
            └─ 1 seconde par email + temps réseau
```

---

## 📊 9. Statistiques et Taux de Succès

```
Traçage du taux de succès au fil du temps:

Batch 1-50:      287/289 = 99.3% ✅
Batch 51-100:    287/289 = 99.3% ✅
Batch 101-150:   287/289 = 99.3% ✅
Batch 151-200:   287/289 = 99.3% ✅
Batch 201-250:   287/289 = 99.3% ✅
Batch 251-289:   287/289 = 99.3% ✅

RÉSUMÉ FINAL:
═════════════════════════════════════
Total: 289
Envoyés: 287       (~99.3%)
Échoués: 2         (~0.7%)
Ignorés: 0
═════════════════════════════════════
```

---

## 📁 10. Structure des Fichiers de Sortie

```
outputs/relance_emails/
├─ preparation_emails_20260222_184116.csv
│  ├─ 289 lignes (1 header + 289 emails)
│  ├─ Colonnes: prospect_name, prospect_etablissement, recipient_email, subject
│  ├─ Format: CSV purifié (UTF-8 BOM)
│  └─ Taille: 43 KB
│
├─ preparation_emails_20260222_184116.json
│  ├─ Array de 289 objets email
│  ├─ Chaque objet: {prospect_name, prospect_etablissement, recipient_email, subject, body, sender}
│  ├─ Formaté: JSON pretty-print (2 spaces)
│  ├─ Encoding: UTF-8 (no BOM)
│  └─ Taille: 506 KB
│
└─ envoi_rapport_20260222_191005.json
   ├─ {timestamp, mode, stats, emails_file}
   ├─ mode: "DRY RUN" ou "PRODUCTION"
   ├─ stats: {total: 289, sent: 287, failed: 2, skipped: 0}
   └─ Taille: ~1 KB
```

---

## 🎯 Résumé Visuels des Chifffres

```
ENTRÉE                  PHASE 1              PHASE 2
┌─────────────┐       ┌─────────────┐      ┌─────────────┐
│   EXCEL     │ ─────→│ PRÉPARATION │ ────→│   ENVOI     │
│  (250)      │       │  (289)      │      │  (289)      │
└─────────────┘       └─────────────┘      └─────────────┘
   Fichier          CSV + JSON             Elasticmail API
  source           Fichiers                    +
                   prêts                   Rapport

FILTRAGE DANS PHASE 1:
  250 prospects
  -4 Pas-de-Calais
  ────────────────
  246 prospects traités
  × 1.18 emails moyenne
  ────────────────
  289 emails générés
```
