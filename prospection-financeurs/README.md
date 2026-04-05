# Pipeline de prospection financeurs ESSMS

Identification et enrichissement des contacts clés chez les **101 conseils départementaux**, **9 DIRPJJ** et **18 ARS** — décideurs et responsables tarification ESSMS.

## État de la base (mars 2026)

| Niveau | Actifs | Invalides | Total |
|--------|-------:|----------:|------:|
| `direction` | 279 | 29 | 308 |
| `dga` | 79 | 2 | 81 |
| `direction_adjointe` | 32 | 0 | 32 |
| `responsable_tarification` | 20 | 4 | 24 |
| `operationnel` | 10 | 1 | 11 |
| **Total** | **420** | **36** | **456** |

- **128 entités** : 101 CD + 9 DIRPJJ + 18 ARS
- **Coverage DGA** : ~79/101 départements couverts
- **Coverage tarification** : ~20/101 départements couverts (profils peu publics)

## Architecture

```
prospection-financeurs/
├── config/
│   ├── settings.py              # Clés API, DB, paramètres
│   ├── departements.json        # Référentiel 101 départements
│   ├── dirpjj.json              # Référentiel 9 DIRPJJ
│   ├── ars.json                 # Référentiel 18 ARS
│   └── postes_cibles.json       # Postes et variantes par type
├── src/
│   ├── pipeline.py              # Orchestrateur principal (point d'entrée)
│   ├── pipeline_quality.py      # Passes de qualité (9 modes, voir ci-dessous)
│   ├── pipeline_signals.py      # Détection signaux tension tarification
│   ├── serper_client.py         # Client Serper (retry + cache)
│   ├── mistral_client.py        # Client Mistral (extraction structurée)
│   ├── contact_finder.py        # Identification des contacts
│   ├── email_reconstructor.py   # Reconstruction et variantes email
│   ├── linkedin_finder.py       # Recherche profils LinkedIn
│   ├── normalizer.py            # Normalisation noms, accents, tirets
│   ├── exporter.py              # Export JSON + CSV
│   └── supabase_db.py           # Persistance Supabase (3 tables)
├── data/output/                 # contacts_enrichis.json / .csv
├── logs/                        # pipeline.log
├── schema.sql                   # DDL Supabase (référence)
├── requirements.txt
└── .env.example
```

## Tables Supabase

Quatre tables isolées (aucune interaction avec les autres tables du projet) :

| Table | Rôle |
|-------|------|
| `prospection_entites` | Référentiel des 128 entités (CD + DIRPJJ + ARS) |
| `prospection_email_patterns` | Pattern email par domaine (cache partagé) |
| `prospection_contacts` | Contacts enrichis (email + LinkedIn) |
| `prospection_signaux` | Signaux de tension tarification/financement par département |

Le schéma complet est dans [schema.sql](schema.sql). Les tables sont aussi créées automatiquement au premier lancement du pipeline.

### Champs clés de `prospection_contacts`

| Champ | Type | Valeurs |
|-------|------|---------|
| `niveau` | TEXT | `dga` \| `direction` \| `direction_adjointe` \| `responsable_tarification` \| `operationnel` |
| `confiance_nom` | TEXT | `haute` \| `moyenne` \| `basse` \| **`invalide`** (exclu de tous les exports) |
| `confiance_email` | TEXT | `haute` \| `moyenne` \| `basse` \| `inconnue` |
| `email_valide_web` | BOOLEAN | Validation croisée par Serper |
| `linkedin_url` | TEXT | URL profil LinkedIn (si trouvé) |

> **Convention** : `confiance_nom='invalide'` marque les contacts à exclure (faux nom, doublon, hors-scope) sans les supprimer. C'est le seul mécanisme de soft-delete.

## Installation

Le projet réutilise le venv du projet principal :

```powershell
# Depuis la racine du repo ingest-habitat
.venv\Scripts\pip install -r prospection-financeurs/requirements.txt
```

## Configuration

```powershell
Copy-Item prospection-financeurs\.env.example prospection-financeurs\.env
# Éditer .env : renseigner DB_PASSWORD, SERPER_API_KEY, MISTRAL_API_KEY
```

Le fichier `.env` rechargera automatiquement `DB_PASSWORD` depuis le `.env` du projet racine si celui-ci est déjà configuré — ou vous pouvez créer un fichier `.env` dédié dans le dossier `prospection-financeurs/`.

## Lancement

```powershell
# Depuis la racine du repo
cd prospection-financeurs

# Traiter toutes les entités
..\\.venv\Scripts\python src/pipeline.py

# Traiter uniquement un type
..\\.venv\Scripts\python src/pipeline.py --types departement

# Tester sur quelques codes spécifiques
..\\.venv\Scripts\python src/pipeline.py --codes 77 91 ARS-IDF DIRPJJ-IDF

# Forcer le retraitement (ignorer la reprise)
..\\.venv\Scripts\python src/pipeline.py --force

# Sans validation croisée des emails (économise ~256 requêtes Serper)
..\\.venv\Scripts\python src/pipeline.py --no-validate-emails
```

## Reprise sur interruption

Le pipeline est idempotent : il skippe automatiquement les entités déjà présentes dans Supabase (au moins un contact) et celles déjà dans `data/output/contacts_enrichis.json`. Utilisez `--force` pour tout retraiter.

## Sorties

| Fichier | Description |
|---------|-------------|
| `data/output/contacts_enrichis.json` | Export complet avec métadonnées de confiance |
| `data/output/contacts_enrichis.csv` | Export aplati pour CRM / emailing |
| Supabase `prospection_contacts` | Source de vérité, requêtable en SQL |
| `logs/pipeline.log` | Log structuré de chaque opération |

## Volume d'appels API estimé (run complet)

| API | Appels estimés |
|-----|---------------|
| Serper | ~1 150 |
| Mistral | ~256 |

---

## Pipeline qualité (`pipeline_quality.py`)

Script de passes de qualité indépendantes, à exécuter après le pipeline initial. Chaque mode est idempotent.

```powershell
# Depuis la racine du repo
$env:PYTHONUTF8="1" ; .venv\Scripts\python.exe prospection-financeurs/src/pipeline_quality.py [MODE] [--dry-run]
```

### Modes disponibles

| Flag | Mode | Description |
|------|------|-------------|
| `--check-names` | 1 | Détection noms suspects (trop courts, intitulés de poste, etc.) |
| `--audit-postes` | 2 | Audit des intitulés de poste incohérents avec le niveau déclaré |
| `--validate-dirs` | 3 | Validation des directeurs généraux (niveau `direction`) |
| `--check-territorial` | 4 | Vérification de la cohérence entité ↔ type géographique |
| `--apply-replacements` | 5 | Application des corrections manuelles (fichier `config/replacements.json`) |
| `--reclassify-niveaux` | 6 | Reclassification LLM des niveaux suspects via Mistral |
| `--requalify-tarification` | 7 | Requalification des contacts `responsable_tarification` existants |
| `--find-missing-contacts` | 8 | Recherche Serper+LLM de contacts manquants (requiert `--niveau`) |
| `--enrich-linkedin` | 9 | Recherche de profils LinkedIn manquants via Serper |

### Options communes

| Option | Description |
|--------|-------------|
| `--dry-run` | Simulation sans écriture en base |
| `--niveau dga\|responsable_tarification` | Requis pour `--find-missing-contacts` |

### Mode `--find-missing-contacts` en détail

Recherche active de contacts pour les entités sans coverage sur le niveau demandé.
Restreint aux **départements uniquement**.

**Requêtes Serper pour `dga` :**
```
"[Entité]" "directeur général adjoint" solidarités
"[Entité]" DGA solidarités site:fr
organigramme "[Entité]" DGA solidarités
```

**Requêtes Serper pour `responsable_tarification` :**
```
"[Entité]" ("chef de service" OR "responsable") "tarification" ESSMS
"[Entité]" "chef de service" tarification autonomie OR enfance
"[Entité]" "responsable" "financement de l'offre" médico-social OR ESSMS
```

**Actions LLM retournées :**

| Action | Niveau inséré | Condition |
|--------|--------------|----------|
| `INSERER` | `dga` | Titre DGA/Directeur Général Adjoint explicite |
| `INSERER_ADJOINT` | `direction_adjointe` | Adjoint DGA ou directeur solidarités sans titre DGA |
| `VERIFIER` | — | Source ambiguë ou >2 ans |
| `INCONNU` | — | Aucune personne identifiable |

---

## Interface de gestion (à venir)

La base Supabase est la **source de vérité**. L'interface à construire permettra :

- **Visualisation** : liste des contacts par entité, filtrée par niveau / confiance / LinkedIn
- **Correction manuelle** : modifier `niveau`, `poste_exact`, `confiance_nom`, `email_principal`
- **Validation** : passer `confiance_nom='invalide'` pour exclure un contact sans le supprimer
- **Déduplication** : détecter les doublons inter-entités (même nom, LinkedIn identique)
- **Prospection** : sélection + export d'une liste de contacts pour une campagne email
- **Historique** : traçabilité des modifications via `updated_at`

### Requêtes utiles (SQL Supabase)

```sql
-- Contacts actifs avec LinkedIn, triés par entité
SELECT e.nom, c.niveau, c.nom_complet, c.poste_exact, c.linkedin_url
FROM prospection_contacts c
JOIN prospection_entites e ON e.id = c.entite_id
WHERE c.confiance_nom != 'invalide'
  AND c.linkedin_url IS NOT NULL
ORDER BY e.nom, c.niveau;

-- Départements sans DGA
SELECT e.nom
FROM prospection_entites e
WHERE e.type_entite = 'departement'
  AND NOT EXISTS (
    SELECT 1 FROM prospection_contacts c
    WHERE c.entite_id = e.id
      AND c.niveau = 'dga'
      AND c.confiance_nom != 'invalide'
  );

-- Distribution des niveaux (actifs)
SELECT niveau, COUNT(*) AS nb
FROM prospection_contacts
WHERE confiance_nom != 'invalide'
GROUP BY niveau
ORDER BY nb DESC;
```
