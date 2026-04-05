# Documentation - Scripts d'Envoi de Relance Email

## 📋 Vue d'ensemble de la logique

Le système fonctionne en **2 phases**:

```
┌─────────────────────────────────────────────────────────────┐
│  PHASE 1: PRÉPARATION (send_follow_up_emails.py)           │
│  ├─ Charger 250 prospects de l'Excel                       │
│  ├─ Filtrer les exclusions (Pas-de-Calais 62)            │
│  ├─ Extraire/générer les emails de contact               │
│  ├─ Personnaliser le contenu du mail                      │
│  └─ Sauvegarder en CSV + JSON                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
                   289 emails générés
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  PHASE 2: ENVOI (send_emails_elasticmail.py)               │
│  ├─ Charger les emails préparés                            │
│  ├─ [DRY RUN] Simuler ou [PRODUCTION] Envoyer             │
│  ├─ Respecter les délais (1 sec/email)                     │
│  └─ Générer rapport d'envoi                                │
└─────────────────────────────────────────────────────────────┘
```

---

## 📖 PHASE 1: Préparation des Emails

### 1.1 Chargement des Prospects

**Fonction**: `load_prospects()`

```python
INPUT_FILE = 'outputs/prospection_250_FINAL_FORMATE_V2_PUBLIPOSTAGE.xlsx'
```

**Logique**:
```
1. Vérifier que le fichier existe
2. Lire le fichier Excel avec pandas
3. Convertir en liste de dictionnaires (chaque ligne = 1 prospect)
4. Retourner les 250 prospects
```

**Colonnes utilisées** (du fichier Excel):
- `gestionnaire_nom` → Nom de la structure
- `gestionnaire_adresse` → Adresse (contient le code postal)
- `nom_public` → Nom public de l'établissement
- `dirigeant_nom` → Nom du dirigeant/responsable
- `email_contact` → Email principal de contact
- `emails_generiques` → Autres emails possibles

**Exemple**:
```json
{
  "gestionnaire_nom": "CROIX ROUGE FRANCAISE",
  "gestionnaire_adresse": "98 RUE DIDOT, 75014 PARIS",
  "nom_public": "Croix-Rouge Française",
  "dirigeant_nom": "Martin J",
  "email_contact": "contact@croixrouge.fr",
  "emails_generiques": "contact@croixrouge.fr"
}
```

---

### 1.2 Filtrage Pas-de-Calais

**Fonction**: `filter_exclude_pas_de_calais()`

**Objectif**: Exclure les 4 prospects du Pas-de-Calais (département 62)

**Logique du filtrage**:
```
POUR CHAQUE prospect:
  1. Extraire la code postal de 'gestionnaire_adresse'
     └─ Utiliser REGEX pour chercher 5 chiffres: \b(\d{5})\b
  
  2. Appeler extract_department_code(code_postal)
     └─ Prendre les 2 premiers chiffres
     └─ Exemple: "62025" → "62"
  
  3. SI dept == "62":
     └─ Ajouter à liste EXCLUDED
  SINON:
     └─ Ajouter à liste FILTERED
```

**Fonction helper**: `extract_department_code()`

```python
def extract_department_code(code_postal: str) -> str:
    """
    Règles de conversion code_postal → département:
    
    Métropole:     code_postal[:2]  # "75013" → "75"
    DOM-TOM 97xxx: code_postal[:3]  # "97411" → "974"
    DOM-TOM 98xxx: code_postal[:3]  # "98849" → "988"
    """
```

**Exemple**:
```
Input:  250 prospects
        4 avec code postal "62xxx" (Pas-de-Calais)
Output: 246 prospects (4 exclus)
```

---

### 1.3 Extraction des Emails

**Fonction**: `generate_possible_emails()`

**Logique**:
```
Pour chaque prospect, chercher les emails dans cet ordre:

1. EMAIL_CONTACT
   ├─ Vérifier colonne 'email_contact'
   └─ Si valide → Ajouter à la liste

2. EMAILS_GÉNÉRIQUES  
   ├─ Vérifier colonne 'emails_generiques'
   ├─ Peut contenir plusieurs emails séparés par ";"
   └─ Pour chaque email valide → Ajouter à la liste

3. [FALLBACK] Valider le format
   ├─ Regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$
   └─ Retourner uniquement les emails valides
```

**Résultat possibles**:
```
Prospect 1: ["contact@semimo.fr"]
Prospect 2: ["contact@apf.asso.fr", "info@apf.asso.fr", "dpo@apf.asso.fr"]
Prospect 3: [] → IGNORÉ (pas d'email)
```

**Validation**: `_is_valid_email()`
```python
pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
return bool(re.match(pattern, email.lower()))
```

---

### 1.4 Extraction du Nom pour Personnalisation

**Fonction**: `extract_name_for_greeting()`

**Logique hiérarchique**:
```
1. Si 'dirigeant_nom' existe:
   └─ Extraire le NOM (dernier mot)
   └─ Exemple: "Martin Jean-Paul" → "Paul"

2. SINON si 'gestionnaire_nom' existe:
   └─ Utiliser directement
   └─ Exemple: "CROIX ROUGE FRANCAISE" → "CROIX ROUGE FRANCAISE"

3. SINON si 'nom_public' existe:
   └─ Utiliser directement

4. FALLBACK:
   └─ "Madame, Monsieur"
```

**Résultat**:
- `"Baudry S"` → Nom: `"S"` → Salutation: `"Madame, Monsieur S,"`
- Pas de nom → Salutation: `"Madame, Monsieur,"`

---

### 1.5 Génération du Contenu Personnalisé

**Fonction**: `generate_email_content()`

```
INPUT: prospect dict
OUTPUT: (subject, body_personnalise)

SUJET (constant):
  "Démo gratuite ConfidensIA - Pseudonymisation pour ESSMS"

CORPS (personnalisé):
  1. Salutation personnalisée
     └─ "Madame, Monsieur {NOM},"
  
  2. Template fixe (variable EMAIL_BODY)
     └─ Texte de relance
     └─ Description de la démo
     └─ Appel à l'action
     └─ Signature Patrick Danto
```

**Structure du corps**:
```
{salutation}

Vous avez reçu il y a quelques semaines un courrier...
[...]
Bien cordialement,
Patrick Danto
```

---

### 1.6 Préparation Finale des Emails

**Fonction**: `prepare_emails()`

**Logique**:
```
POUR CHAQUE prospect (246):
  1. Générer les emails possibles
     └─ Peut retourner 1, 2, 3+ emails
  
  2. SI pas d'email:
     └─ Afficher ⚠️ et sauter le prospect
  
  3. SI emails trouvés:
     └─ Générer le contenu personnalisé
     └─ POUR CHAQUE email possible:
        ├─ Créer objet 'prepared':
        │  ├─ prospect_name
        │  ├─ prospect_etablissement
        │  ├─ recipient_email
        │  ├─ subject
        │  ├─ body
        │  └─ sender
        └─ Ajouter à liste
```

**Résultat**:
```
246 prospects
  ↓
Certains avec 1 email, certains avec 2-3 emails
  ↓
289 emails préparés au total
```

---

### 1.7 Sauvegarde des Résultats

**Fonction**: `save_preparation()`

**Génère 2 fichiers**:

#### A) CSV - Résumé (43 KB)
```
outputs/relance_emails/preparation_emails_YYYYMMDD_HHMMSS.csv

Colonnes:
- prospect_name: Nom de la structure
- prospect_etablissement: Établissement
- recipient_email: Email destination
- subject: Sujet (tous identiques)

Exemple:
CROIX ROUGE FRANCAISE,Croix-Rouge Française,contact@croixrouge.fr,Démo gratuite ConfidensIA...
```

#### B) JSON - Complet (506 KB)
```
outputs/relance_emails/preparation_emails_YYYYMMDD_HHMMSS.json

Structure:
[
  {
    "prospect_name": "CROIX ROUGE FRANCAISE",
    "prospect_etablissement": "Croix-Rouge Française",
    "recipient_email": "contact@croixrouge.fr",
    "subject": "...",
    "body": "Texte complet de l'email",
    "sender": "patrick.danto@bmse.fr"
  },
  ...
]
```

---

## 📤 PHASE 2: Envoi des Emails

### 2.1 Chargement des Emails Préparés

**Fonction**: `load_prepared_emails()`

```python
# Cherche le dernier fichier JSON généré
json_files = sorted(Path('outputs/relance_emails').glob('*.json'))
latest_json = json_files[-1]  # Le plus recent

# Charge les 289 emails
emails = json.load(file)
```

---

### 2.2 Logique d'Envoi avec Elasticmail

**Fonction**: `send_email()`

**Mode v3 (JSON)** - Endpoint moderne:
```
POST https://api.elasticmail.com/v3/emails/send

Headers:
  X-ElasticEmail-ApiKey: {API_KEY}
  Content-Type: application/json

Body:
{
  "from": {
    "email": "patrick.danto@bmse.fr",
    "name": "Patrick Danto - ConfidensIA"
  },
  "to": [
    {
      "email": "contact@croixrouge.fr"
    }
  ],
  "subject": "Démo gratuite ConfidensIA...",
  "bodyText": "Madame, Monsieur,\n\nVous avez reçu..."
}
```

---

### 2.3 Gestion DRY RUN vs PRODUCTION

**Fonction**: `send_all()`

```
MODE DRY RUN (default):
├─ Boucler sur 289 emails
├─ Afficher "[SIMULATION N/289] À: email@example.com"
├─ Incrémenter compteur
├─ NE PAS faire de HTTP request
└─ Résultat: Simulation d'un envoi de 289 emails

MODE PRODUCTION (dry_run=False):
├─ Boucler sur 289 emails
├─ Faire HTTP POST vers Elasticmail
├─ Attendre réponse (timeout 30s)
├─ Incrémenter compteur de succès/erreur
├─ Respecter délai 1 sec entre chaque
└─ Résultat: Envoi réel de 289 emails
```

---

### 2.4 Gestion des Délais

**Logique**:
```
POUR CHAQUE email (289):
  1. Envoyer l'email
  2. SI pas le dernier email:
     └─ Attendre 1 seconde (sleep(1))
  3. SINON:
     └─ Continuer (pas d'attente après le dernier)

Résultat: ~289 secondes = ~5 min pour envoyer 289 emails
```

**Raison**: Éviter de surcharger l'API (rate limiting)

---

### 2.5 Gestion des Erreurs

```
try:
  response = requests.post(
    url,
    json=payload,
    headers=headers,
    timeout=30
  )
  
  if response.status_code == 200:
    return True, "Envoyé avec succès"
  else:
    return False, f"Code {status_code}: {error_message}"

except RequestException as e:
  return False, f"Erreur réseau: {e}"
except Exception as e:
  return False, f"Erreur: {e}"
```

---

### 2.6 Rapports et Statistiques

**Fonction**: `send_all()` - Affichage périodique

```
Tous les 50 emails:
  [Batch 50/289] Taux de succès: 98.2%

À la fin:
  ✅ Total: 289
  ✅ Envoyés: 289 (ou autre nombre)
  ❌ Échoués: 0
  ⏭️  Ignorés: 0
```

**Fichier de rapport**:
```json
outputs/relance_emails/envoi_rapport_YYYYMMDD_HHMMSS.json

{
  "timestamp": "20260222_184116",
  "mode": "DRY RUN ou PRODUCTION",
  "stats": {
    "total": 289,
    "sent": 289,
    "failed": 0,
    "skipped": 0
  },
  "emails_file": "preparation_emails_20260222_184116.json"
}
```

---

## 🔄 Flux Complet avec Exemple

```
ENTRÉE: prospection_250_FINAL_FORMATE_V2_PUBLIPOSTAGE.xlsx

┌─────────────────────────────────────────────────────────┐
│ ÉTAPE 1: load_prospects()                              │
│ 250 prospects chargés ✅                               │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ÉTAPE 2: filter_exclude_pas_de_calais()               │
│ 4 du Pas-de-Calais exclus → 246 restants ✅           │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ÉTAPE 3: prepare_emails()                             │
│ Pour chaque prospect:                                  │
│  ├─ generate_possible_emails()   → 1-3 emails        │
│  ├─ extract_name_for_greeting()  → Nom personnalisé  │
│  ├─ generate_email_content()     → Sujet + corps     │
│  └─ Créer objet 'prepared'                           │
│ Total: 289 emails préparés ✅                         │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ÉTAPE 4: save_preparation()                           │
│ ├─ preparation_emails.csv (43 KB)  ✅                │
│ └─ preparation_emails.json (506 KB) ✅               │
└─────────────────────────────────────────────────────────┘
                    ↓
             FICHIERS PRÊTS

ENVOI (Phase 2):
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ÉTAPE 1: load_prepared_emails()                       │
│ Charger 289 emails depuis JSON ✅                     │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ÉTAPE 2: send_all()                                   │
│ MODE: DRY RUN (par défaut)                            │
│ Boucle sur 289 emails:                                │
│  ├─ Afficher "(SIMULATION N/289) À: email"           │
│  └─ Incrémenter compteur                             │
│ Taux de succès: 100%                                  │
└─────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ÉTAPE 3: save_rapport()                               │
│ envoi_rapport_YYYYMMDD_HHMMSS.json ✅               │
└─────────────────────────────────────────────────────────┘
```

---

## 🎯 Cas d'Usage et Variations

### Cas 1: Prospect avec 1 email
```
CROIX ROUGE FRANCAISE
├─ email_contact: "contact@croixrouge.fr"
└─ Généré: 1 email
```

### Cas 2: Prospect avec plusieurs emails
```
APF FRANCE HANDICAP
├─ email_contact: "contact@apf.asso.fr"
├─ emails_generiques: "info@apf.asso.fr;dpo@apf.asso.fr"
└─ Généré: 3 emails
```

### Cas 3: Prospect sans email
```
SOME ASSOCIATION
├─ email_contact: null
├─ emails_generiques: null
└─ Résultat: ⚠️ Pas d'email pour SOME ASSOCIATION
```

### Cas 4: Prospect du Pas-de-Calais
```
STRUCTURE IN ARRAS
├─ gestionnaire_adresse: "123 RUE, 62000 ARRAS"
├─ Code postal extrait: "62000"
├─ Département: "62"
└─ Résultat: 🗑️ EXCLU du filtrage
```

---

## 📊 Statistiques Finales

```
INPUT:  250 prospects (fichier Excel)
  ↓
FILTRAGE RÉGION:
  -4 du Pas-de-Calais (62)
  = 246 prospects restants
  ↓
EXTRACTION EMAILS:
  × 246 prospects
  ~ 1.18 emails par prospect
  = 289 emails générés
  ↓
PERSONNALISATION:
  × 289 emails
  × 1 sujet unique
  × 289 corps personnalisés (avec nom)
  ↓
OUTPUT:
  ✅ 289 emails prêts pour envoi
  ✅ CSV de suivi (43 KB)
  ✅ JSON détaillé (506 KB)
```

---

## 🔐 Sécurité et Validation

### Validation des Emails
```python
def _is_valid_email(email: str) -> bool:
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email.lower()))

# Valides: contact@croixrouge.fr, info@apf.asso.fr
# Invalides: test@domain (pas d'extension), @domain.com (pas de local)
```

### Isolation des Données
```
- Chaque prospect = objet dict indépendant
- Pas de mutation d'état global
- Chaque email généré = nouvelle instance
```

### Gestion des Clés API
```
ELASTICMAIL_API_KEY = os.getenv('ELASTICMAIL_API_KEY')
SENDER_EMAIL = 'patrick.danto@bmse.fr'

Stockées dans .env (jamais en dur dans le code)
```

---

## 📈 Performances

| Opération | Temps | Notes |
|-----------|-------|-------|
| Charger 250 prospects | < 1s | Fichier Excel |
| Filtrer + préparer | ~2s | Logique Python |
| Générer CSV + JSON | < 1s | Écriture disque |
| Envoyer 289 emails | ~289s | 1 sec/email (respect API) |
| **Total SIMULATION** | ~3s | Juste la préparation |
| **Total PRODUCTION** | ~5 min | Surtout l'envoi réseau |

---

## 🚀 Commandes d'Exécution

```bash
# Phase 1: Préparer (obligatoire)
python send_follow_up_emails.py

# Phase 2: Envoyer en simulation (test sans risque)
python send_emails_elasticmail.py  # dry_run=True par défaut

# Phase 2: Envoyer en production (modifier une ligne puis)
# Changer ligne 200: dry_run=False
python send_emails_elasticmail.py
```

---

## ⚠️ Notes Importantes

1. **API Elasticmail actuellement en maintenance**
   - Les scripts restent valides
   - Reprendront dès que l'API revient

2. **Pas de dédoublonnage**
   - Si 1 prospect a 3 emails → 3 emails distincts envoyés
   - C'est voulu (toucher tous les contacts possibles)

3. **Pas de limite de volume**
   - 289 emails = OK pour Elasticmail
   - Pour 1000+ envisager batching plus avancé

4. **Idempotence**
   - Relancer 2x le script crée 2x les préparations
   - Les anciens fichiers ne sont pas supprimés
