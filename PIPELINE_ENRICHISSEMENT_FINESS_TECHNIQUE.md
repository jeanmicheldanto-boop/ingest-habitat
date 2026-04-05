# Pipeline d'enrichissement FINESS — Documentation technique complète
## ConfidensIA / BMSE — Social & Médico-Social

**Dernière mise à jour :** 3 mars 2026  
**Périmètre :** Champ social et médico-social (adultes, enfants, tout secteur ESSMS)  
**Test initial :** Département 65 — Hautes-Pyrénées  
**Déploiement cible :** Google Cloud Run Jobs (batch)

---

## Table des matières

1. [Vue d'ensemble](#1-vue-densemble)
2. [Prérequis et environnement](#2-prérequis-et-environnement)
3. [Architecture des tables Supabase](#3-architecture-des-tables-supabase)
4. [Étape 0 — Ingestion FINESS brute (LOCAL)](#4-étape-0--ingestion-finess-brute-local-uniquement)
5. [Étape 1 — Déduction par règles métier](#5-étape-1--déduction-par-règles-métier)
6. [Étape 2 — Géolocalisation Nominatim](#6-étape-2--géolocalisation-nominatim-adresses-siège-et-établissement)
7. [Étape 3 — Enrichissement Serper](#7-étape-3--enrichissement-serper-recherche-web)
8. [Étape 4 — Scraping ciblé](#8-étape-4--scraping-ciblé-des-pages-web)
9. [Étape 5 — Qualification LLM (Gemini)](#9-étape-5--qualification-llm-gemini)
10. [Étape 6 — Enrichissement dirigeants (+ DAF)](#10-étape-6--enrichissement-dirigeants--identification-daf)
11. [Étape 7 — Reconstruction emails](#11-étape-7--reconstruction-emails)
12. [Étape 8 — Signaux de tension](#12-étape-8--signaux-de-tension-et-actualités)
13. [Étape 9 — CPOM et territorial](#13-étape-9--enrichissement-cpom-et-territorial)
14. [Script principal : `enrich_finess_dept.py`](#14-script-principal--enrich_finess_deptpy)
15. [Test local — Hautes-Pyrénées (65)](#15-test-local--hautes-pyrénées-65)
16. [Déploiement Cloud Run Jobs](#16-déploiement-cloud-run-jobs)
17. [Stratégie de requêtes Serper](#17-stratégie-de-requêtes-serper-sans-limitation)
18. [Prompts LLM détaillés](#18-prompts-llm-détaillés)
19. [Gestion des erreurs et reprise](#19-gestion-des-erreurs-et-reprise)
20. [Monitoring et rapports](#20-monitoring-et-rapports)
21. [Cohabitation avec les tables habitat intermédiaire](#21-cohabitation-avec-les-tables-habitat-intermédiaire)

---

## 1. Vue d'ensemble

### Objectif

Constituer un actif de données structuré et enrichi à partir de la base FINESS, couvrant **tous les ESSMS** (Établissements et Services Sociaux et Médico-Sociaux) :
- Handicap enfant (IME, IMPRO, SESSAD, CAMSP, ITEP...)
- Handicap adulte (MAS, FAM, FH, FV, ESAT, SAVS, SAMSAH...)
- Personnes âgées (EHPAD, SSIAD, USLD, SPASAD, Résidence Autonomie...)
- Protection de l'enfance (MECS, Foyer de l'Enfance, AEMO...)
- Addictologie (CSAPA, CAARUD...)
- Insertion / Précarité (CHRS, CHU, LHSS, LAM...)
- Santé mentale (CMP, CATTP, Hôpital de Jour psy...)

### Flux global

```
╔══════════════════════════════════════════════════════════════════╗
║  MODE LOCAL (une seule fois, depuis le poste développeur)       ║
╚══════════════════════════════════════════════════════════════════╝

FINESS (CSV etalab data.gouv.fr, dossier database/)
        │
        ▼
[0. Ingestion locale → Supabase] ── Lecture CSV + INSERT dans finess_etablissement,
        │                            finess_gestionnaire (données brutes)
        │                            + Tag deja_prospecte_250 (croisement Excel 250 contacts)
        │
        ▼
[PostgreSQL Supabase] ─────────── Données brutes prêtes à enrichir

╔══════════════════════════════════════════════════════════════════╗
║  MODE CLOUD RUN BATCH (lit et écrit directement dans Supabase) ║
╚══════════════════════════════════════════════════════════════════╝

[PostgreSQL Supabase] ─ SELECT établissements WHERE enrichissement_statut = 'brut'
        │
        ▼
[1. Déduction règles métier] ──── financeur, catégorie normalisée, type_tarification (sans API)
        │
        ▼
[2. Géolocalisation Nominatim] ── Geocoding adresse siège (gestionnaire) + adresse établissement
        │                          → latitude, longitude, geocode_precision
        │
        ▼
[3. Enrichissement Serper]         ×5-8 requêtes/établissement (sans limitation crédits)
        │                          ├── Site web officiel
        │                          ├── Pages "missions", "public accueilli"
        │                          ├── Dirigeants et organigramme
        │                          ├── Actualités et signaux
        │                          └── LinkedIn gestionnaire/dirigeants
        │
        ▼
[4. Scraping ciblé] ────────────── requests simple (sans ScrapingBee)
        │                          ├── Page "Qui sommes-nous" / "Nos missions"
        │                          ├── Page équipe / organigramme
        │                          └── Page "contact" (domaine mail)
        │
        ▼
[5. Qualification LLM Gemini] ─── type de public, tranches d'âge, pathologies
        │                          type d'accueil, période ouverture
        │                          dirigeants normalisés, structure mail
        │
        ▼
[6. Enrichissement dirigeants] ── Serper LinkedIn + site officiel → LLM
        │                          + Identification spécifique du DAF
        │
        ▼
[7. Reconstruction emails] ────── Pattern mail + reconstitution par dirigeant
        │
        ▼
[8. Signaux de tension] ────────── Serper actualités → LLM extraction signaux
        │
        ▼
[9. CPOM + territorial] ────────── data.gouv (CPOM) + STATISS (zones dotation)
        │
        ▼
[PostgreSQL Supabase enrichi] ──→ CRM / Hub ESSMS / Prospection  (écriture DIRECTE depuis Cloud Run)
```

> **Architecture clé** : L'ingestion CSV est une opération locale unique (étape 0). Toutes les étapes d'enrichissement (1-9) sont exécutées dans Cloud Run et lisent/écrivent **directement dans Supabase** — aucune donnée ne transite par un fichier local intermédiaire.

---

## 2. Prérequis et environnement

### Variables d'environnement (`.env`)

Le projet réutilise la configuration existante dans `config.py` et le fichier `.env` :

```dotenv
# === APIs existantes (réutilisées) ===
SERPER_API_KEY=<clé serper>
GEMINI_API_KEY=<clé gemini>
GEMINI_MODEL=gemini-2.0-flash

# === Base de données Supabase (existante) ===
DB_HOST=db.minwoumfgutampcgrcbr.supabase.co
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=<mot de passe>
DB_PORT=5432
```

### Fichiers FINESS source (déjà présents)

Les fichiers CSV etalab sont dans le dossier `database/` :

| Fichier | Contenu | Rôle |
|---------|---------|------|
| `etalab-cs1100501-stock-*.csv` | `structureej` — Entités Juridiques (gestionnaires) | Identité, adresse, SIREN des EJ |
| `etalab-cs1100502-stock-*.csv` | `structureet` — Établissements (détails) | Catégorie FINESS, adresse, capacité |
| `etalab-cs1100505-stock-*.csv` | `equipementsocial` + mapping ET→EJ | Équipements sociaux, lien ET→gestionnaire |

### Dépendances Python

```
# requirements.txt (enrichissement FINESS)
psycopg2-binary>=2.9.9
requests>=2.31.0
python-dotenv>=1.0.0
geopy>=2.4.0          # Géocodage Nominatim (adresses siège/établissement)
pandas>=2.0.0         # Croisement fichier 250 contacts (ingestion locale uniquement)
```

Identiques aux dépendances Cloud Run existantes (`cloudrun_ref/requirements.txt`).

---

## 3. Architecture des tables Supabase

> **IMPORTANT** : Les tables habitat intermédiaire existantes (`etablissements`, `categories`, `sous_categories`, `services`, `propositions`, `proposition_items`, etc.) ne sont **pas modifiées**. Les tables FINESS sont un jeu séparé, préfixé `finess_`.

### 3.1. Table `finess_gestionnaire`

Entité juridique gestionnaire (EJ dans la nomenclature FINESS).

```sql
CREATE TABLE IF NOT EXISTS finess_gestionnaire (
    id_gestionnaire         TEXT PRIMARY KEY,  -- Numéro FINESS EJ (9 chiffres)
    siren                   TEXT,              -- SIREN (9 chiffres) si disponible
    raison_sociale          TEXT NOT NULL,
    sigle                   TEXT,
    forme_juridique_code    TEXT,              -- Code statut juridique FINESS
    forme_juridique_libelle TEXT,              -- Libellé (Association loi 1901, EPAS, Fondation...)
    reseau_federal          TEXT,              -- NEXEM, FEHAP, Croix-Rouge, UNIOPSS, APF...
    
    -- Adresse siège
    adresse_numero          TEXT,
    adresse_type_voie       TEXT,
    adresse_lib_voie        TEXT,
    adresse_complement      TEXT,
    adresse_complete        TEXT,              -- Concaténation formatée
    code_postal             TEXT,
    commune                 TEXT,
    departement_code        TEXT,
    departement_nom         TEXT,
    region                  TEXT,
    
    -- Géolocalisation siège (Nominatim)
    latitude                NUMERIC,
    longitude               NUMERIC,
    geocode_precision       TEXT,              -- rooftop, street, locality
    
    -- Contact & web
    telephone               TEXT,
    site_web                TEXT,
    domaine_mail            TEXT,              -- ex: mongestionnaire.fr
    structure_mail          TEXT,              -- ex: prenom.nom / p.nom / prenom
    linkedin_url            TEXT,
    
    -- Métriques
    nb_etablissements       INTEGER DEFAULT 0,
    nb_essms                INTEGER DEFAULT 0,  -- Sous-ensemble ESSMS uniquement
    budget_consolide_estime NUMERIC,
    categorie_taille        TEXT,              -- ">100", ">50", ">20", ">10", "<=10"
    dominante_type          TEXT,              -- Type dominant (EHPAD, IME, MECS...)
    
    -- Secteur d'activité principal (déduit par règles — Section 5)
    secteur_activite_principal TEXT,           -- Déduit de la dominante catégorie des établissements
                                               -- Valeurs : Handicap Enfant, Handicap Adulte, Personnes Âgées,
                                               -- Protection de l'Enfance, Addictologie, Hébergement Social,
                                               -- Aide à Domicile, Santé Mentale, Multi-secteurs
    
    -- DAF (Directeur Administratif et Financier)
    daf_nom                 TEXT,              -- Nom complet du DAF identifié
    daf_prenom              TEXT,
    daf_email               TEXT,              -- Email reconstitué du DAF
    daf_telephone           TEXT,
    daf_linkedin_url        TEXT,
    daf_source              TEXT,              -- URL source de l'identification
    daf_confiance           TEXT DEFAULT 'moyenne', -- haute / moyenne / basse
    
    -- Signaux
    signal_tension          BOOLEAN DEFAULT FALSE,
    signal_tension_detail   TEXT,
    
    -- Tag prospection antérieure
    deja_prospecte_250      BOOLEAN DEFAULT FALSE, -- TRUE si ce gestionnaire fait partie des 250 contacts
                                                    -- déjà traités par enrich_prospection_gt50.py
    deja_prospecte_250_date TIMESTAMP,             -- Date du traitement antérieur
    
    -- Métadonnées enrichissement
    date_ingestion          TIMESTAMP DEFAULT NOW(),
    date_enrichissement     TIMESTAMP,
    source_enrichissement   TEXT,              -- 'serper+gemini', 'regles_metier', etc.
    enrichissement_statut   TEXT DEFAULT 'brut', -- brut / en_cours / enrichi / erreur
    enrichissement_log      JSONB,             -- Log détaillé des étapes
    
    CONSTRAINT finess_gestionnaire_statut_check 
        CHECK (enrichissement_statut IN ('brut', 'en_cours', 'enrichi', 'erreur'))
);

CREATE INDEX IF NOT EXISTS idx_finess_gest_dept ON finess_gestionnaire(departement_code);
CREATE INDEX IF NOT EXISTS idx_finess_gest_statut ON finess_gestionnaire(enrichissement_statut);
CREATE INDEX IF NOT EXISTS idx_finess_gest_taille ON finess_gestionnaire(categorie_taille);
CREATE INDEX IF NOT EXISTS idx_finess_gest_prospecte ON finess_gestionnaire(deja_prospecte_250) WHERE deja_prospecte_250 = TRUE;
CREATE INDEX IF NOT EXISTS idx_finess_gest_geo ON finess_gestionnaire(latitude, longitude) WHERE latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_finess_gest_secteur ON finess_gestionnaire(secteur_activite_principal);
```

### 3.2. Table `finess_etablissement`

Établissement terrain (ET dans la nomenclature FINESS).

```sql
CREATE TABLE IF NOT EXISTS finess_etablissement (
    id_finess               TEXT PRIMARY KEY,  -- Numéro FINESS ET (9 chiffres)
    id_gestionnaire         TEXT REFERENCES finess_gestionnaire(id_gestionnaire),
    
    -- Identité
    raison_sociale          TEXT,
    sigle                   TEXT,
    
    -- Catégorisation FINESS
    categorie_code          TEXT,              -- Code catégorie (3 chiffres)
    categorie_libelle       TEXT,              -- Libellé officiel FINESS
    categorie_normalisee    TEXT,              -- Label simplifié (IME, EHPAD, MAS...)
    groupe_code             TEXT,              -- Code groupe
    groupe_libelle          TEXT,              -- Libellé groupe
    
    -- Secteur d'activité (déduit par règles — Section 5)
    secteur_activite        TEXT,              -- Handicap Enfant, Handicap Adulte, Personnes Âgées,
                                               -- Protection de l'Enfance, Addictologie, Hébergement Social,
                                               -- Aide à Domicile, Santé Mentale
    
    -- Public accueilli (enrichi par LLM — Section 8)
    type_public             TEXT,              -- Libellé principal normalisé
    type_public_synonymes   TEXT[],            -- Variantes et synonymes
    pathologies_specifiques TEXT[],            -- TSA, Polyhandicap, Alzheimer, etc.
    tranches_age            TEXT,              -- "6-20 ans", "Adultes 18-60 ans"
    age_min                 INTEGER,
    age_max                 INTEGER,
    
    -- Accueil et fonctionnement (enrichi par LLM)
    type_accueil            TEXT[],            -- Internat, Semi-internat, Accueil de jour, Ambulatoire
    periode_ouverture       TEXT,              -- "365 jours", "210 jours", "Semaine"
    ouverture_365           BOOLEAN,           -- Flag internat 365j (tension maximale)
    
    -- Capacité
    places_autorisees       INTEGER,
    places_installees       INTEGER,
    taux_occupation         NUMERIC,
    
    -- Financement (déduit par règles — Section 5)
    financeur_principal     TEXT,
    financeur_secondaire    TEXT,
    type_tarification       TEXT,              -- SERAFIN-PH, prix de journée, dotation globale...
    
    -- CPOM
    cpom                    BOOLEAN,
    cpom_date_echeance      DATE,
    
    -- Adresse
    adresse_numero          TEXT,
    adresse_type_voie       TEXT,
    adresse_lib_voie        TEXT,
    adresse_complement      TEXT,
    adresse_complete        TEXT,
    code_postal             TEXT,
    commune                 TEXT,
    departement_code        TEXT,
    departement_nom         TEXT,
    region                  TEXT,
    
    -- Contact & web
    telephone               TEXT,
    email                   TEXT,
    site_web                TEXT,
    
    -- Géolocalisation
    latitude                NUMERIC,
    longitude               NUMERIC,
    geocode_precision       TEXT,              -- rooftop, street, locality
    
    -- Territorial
    zone_dotation           TEXT,              -- Sur-dotée, Sous-dotée, Equilibrée (STATISS)
    
    -- Signaux (enrichi par LLM)
    signaux_recents         JSONB,             -- Actualités, recrutements, appels d'offres
    
    -- Métadonnées enrichissement
    date_ingestion          TIMESTAMP DEFAULT NOW(),
    date_enrichissement     TIMESTAMP,
    source_enrichissement   TEXT,
    enrichissement_statut   TEXT DEFAULT 'brut',
    enrichissement_log      JSONB,
    
    CONSTRAINT finess_etab_statut_check 
        CHECK (enrichissement_statut IN ('brut', 'en_cours', 'enrichi', 'erreur'))
);

CREATE INDEX IF NOT EXISTS idx_finess_etab_gest ON finess_etablissement(id_gestionnaire);
CREATE INDEX IF NOT EXISTS idx_finess_etab_dept ON finess_etablissement(departement_code);
CREATE INDEX IF NOT EXISTS idx_finess_etab_cat ON finess_etablissement(categorie_normalisee);
CREATE INDEX IF NOT EXISTS idx_finess_etab_statut ON finess_etablissement(enrichissement_statut);
CREATE INDEX IF NOT EXISTS idx_finess_etab_365 ON finess_etablissement(ouverture_365) WHERE ouverture_365 = TRUE;
CREATE INDEX IF NOT EXISTS idx_finess_etab_secteur ON finess_etablissement(secteur_activite);
```

### 3.3. Table `finess_dirigeant`

Dirigeants identifiés par l'enrichissement web + LLM.

```sql
CREATE TABLE IF NOT EXISTS finess_dirigeant (
    id                      SERIAL PRIMARY KEY,
    id_gestionnaire         TEXT REFERENCES finess_gestionnaire(id_gestionnaire),
    id_finess_etablissement TEXT REFERENCES finess_etablissement(id_finess),  -- NULL si rattaché au siège
    
    -- Identité
    civilite                TEXT,              -- M., Mme
    nom                     TEXT,
    prenom                  TEXT,
    
    -- Fonction
    fonction_brute          TEXT,              -- Texte exact trouvé sur la source
    fonction_normalisee     TEXT,              -- Président / DG / DSI / Directeur Innovation / DRH / Directeur
    
    -- Contact
    email_reconstitue       TEXT,              -- Déduit du pattern mail gestionnaire
    email_verifie           BOOLEAN DEFAULT FALSE,
    email_organisation      TEXT,              -- Email public (direction@, siege@, etc.)
    telephone_direct        TEXT,
    linkedin_url            TEXT,
    
    -- Source
    source_url              TEXT,              -- URL d'où l'info a été trouvée
    source_type             TEXT,              -- 'site_officiel', 'linkedin_serper', 'annuaire', 'presse'
    confiance               TEXT DEFAULT 'moyenne', -- haute / moyenne / basse
    
    -- Métadonnées
    date_enrichissement     TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT finess_dirigeant_confiance_check 
        CHECK (confiance IN ('haute', 'moyenne', 'basse'))
);

CREATE INDEX IF NOT EXISTS idx_finess_dir_gest ON finess_dirigeant(id_gestionnaire);
CREATE INDEX IF NOT EXISTS idx_finess_dir_etab ON finess_dirigeant(id_finess_etablissement);
CREATE INDEX IF NOT EXISTS idx_finess_dir_fonction ON finess_dirigeant(fonction_normalisee);
```

### 3.4. Table `finess_enrichissement_log`

Journal d'enrichissement pour traçabilité et reprise.

```sql
CREATE TABLE IF NOT EXISTS finess_enrichissement_log (
    id                  SERIAL PRIMARY KEY,
    id_finess           TEXT,                -- id_finess de l'établissement (ou id_gestionnaire)
    entite_type         TEXT,                -- 'etablissement' ou 'gestionnaire'
    etape               TEXT,                -- 'ingestion', 'regles_metier', 'serper', 'scraping', 'llm', 'dirigeants', 'emails'
    statut              TEXT,                -- 'succes', 'erreur', 'skip'
    details             JSONB,               -- Détails (requêtes, réponses, erreurs)
    serper_requetes     INTEGER DEFAULT 0,   -- Nombre de requêtes Serper consommées
    gemini_tokens       INTEGER DEFAULT 0,   -- Tokens Gemini consommés (estimé)
    duree_ms            INTEGER,             -- Durée de l'étape en ms
    date_execution      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_finess_log_finess ON finess_enrichissement_log(id_finess);
CREATE INDEX IF NOT EXISTS idx_finess_log_etape ON finess_enrichissement_log(etape);
CREATE INDEX IF NOT EXISTS idx_finess_log_statut ON finess_enrichissement_log(statut);
```

### 3.5. Table `finess_cache_serper`

Cache des résultats Serper pour éviter les doublons entre runs.

```sql
CREATE TABLE IF NOT EXISTS finess_cache_serper (
    id              SERIAL PRIMARY KEY,
    query_hash      TEXT UNIQUE NOT NULL,    -- SHA256 de la requête
    query_text      TEXT,
    results         JSONB,
    nb_results      INTEGER,
    date_requete    TIMESTAMP DEFAULT NOW(),
    expire_at       TIMESTAMP DEFAULT (NOW() + INTERVAL '30 days')
);

CREATE INDEX IF NOT EXISTS idx_finess_cache_hash ON finess_cache_serper(query_hash);
CREATE INDEX IF NOT EXISTS idx_finess_cache_expire ON finess_cache_serper(expire_at);
```

---

## 4. Étape 0 — Ingestion FINESS brute (LOCAL uniquement)

> **IMPORTANT** : L'ingestion CSV est une opération **locale unique**, exécutée depuis le poste développeur. Elle n'est **pas** incluse dans le pipeline Cloud Run. Les fichiers CSV ne sont jamais copiés dans l'image Docker. Cloud Run ne fait que lire et écrire dans Supabase.

### Source : fichiers etalab CSV

Les fichiers FINESS etalab sont téléchargeables sur [data.gouv.fr](https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/) et sont déjà présents dans `database/` :

- `etalab-cs1100501-stock-*.csv` — Entités Juridiques (EJ / gestionnaires)
- `etalab-cs1100502-stock-*.csv` — Établissements (ET / détails structureet)
- `etalab-cs1100505-stock-*.csv` — Équipements sociaux + mapping ET→EJ

### Parsing des fichiers (existant)

Le code de parsing est déjà implémenté dans `scripts/export_gestionnaires_essms_gt10.py` et sera réutilisé :

```python
# Lecture EJ (gestionnaires) — cs1100501
# Format : structureej;FINESS_EJ;RS_court;RS_long;sigle;numvoie;typevoie;libvoie;...

# Lecture ET→EJ mapping + ESSMS — cs1100505
# Record type "structureet" : finess_et → finess_ej
# Record type "equipementsocial" : identifie les ET qui ont un équipement social

# Lecture catégories ET — cs1100502
# Format structureet : col[18]=categorie_code, col[19]=categorie_lib, col[20]=groupe_code, col[21]=groupe_lib
```

### Filtrage département 65 (Hautes-Pyrénées)

Pour le test initial, on filtre sur le code département `65` :
- EJ : `departement_code = '65'` (colonne 13 de cs1100501) 
- ET : `departement_code = '65'` (à extraire de cs1100502, ou par le code postal commençant par `65`)
- On inclut également les EJ gérant des ET dans le 65 même si l'EJ est situé ailleurs (gestion multi-sites)

### Script d'ingestion

```python
# scripts/ingest_finess.py (à créer — exécution LOCALE uniquement)

def ingest_finess_to_supabase(
    path_cs1100501: str,
    path_cs1100502: str,
    path_cs1100505: str,
    departement_filter: str = None,  # ex: "65"
    path_prospection_250: str = None,  # ex: "outputs/prospection_250_FINAL_FORMATE_V2.xlsx"
):
    """
    1. Lire les 3 fichiers CSV FINESS
    2. Filtrer par département si spécifié
    3. INSERT/UPSERT dans finess_gestionnaire et finess_etablissement
    4. Calculer nb_etablissements et nb_essms par gestionnaire
    5. Croiser avec le fichier des 250 gestionnaires déjà prospectés :
       - Charger la colonne finess_ej du fichier Excel
       - Mettre deja_prospecte_250 = TRUE pour les gestionnaires déjà traités
       - Renseigner deja_prospecte_250_date avec la date de modification du fichier
    """
```

### Croisement avec les 250 gestionnaires déjà prospectés

Le pipeline précédent (`scripts/enrich_prospection_gt50.py` + `scripts/complete_dirigeants_linkedin.py`) a déjà traité ~250 gestionnaires avec >50 ESSMS. Le fichier résultat est `outputs/prospection_250_FINAL_FORMATE_V2.xlsx` (ou `prospection_250_FINAL_FORMATE_V2_PUBLIPOSTAGE.xlsx`).

Lors de l'ingestion, on croise les `finess_ej` de ce fichier avec `id_gestionnaire` pour tagger les gestionnaires déjà traités :

```python
import pandas as pd

def tag_gestionnaires_deja_prospectes(cur, path_xlsx: str):
    """
    Marque les gestionnaires déjà traités dans le pipeline des 250 contacts.
    
    Le fichier contient une colonne 'finess_ej' identifiant chaque gestionnaire.
    """
    df = pd.read_excel(path_xlsx)
    finess_ej_list = df['finess_ej'].dropna().astype(str).str.strip().tolist()
    
    if not finess_ej_list:
        print("[WARN] Aucun finess_ej trouvé dans le fichier des 250 contacts")
        return
    
    cur.execute("""
        UPDATE finess_gestionnaire SET
            deja_prospecte_250 = TRUE,
            deja_prospecte_250_date = NOW()
        WHERE id_gestionnaire = ANY(%s)
    """, (finess_ej_list,))
    
    print(f"[TAG] {cur.rowcount} gestionnaires marqués comme déjà prospectés (250 contacts)")
```

> Ce tag permet ensuite de différencier en prospection les gestionnaires **déjà contactés** de ceux **jamais prospectés** et d'adapter la stratégie d'approche.

### Colonnes extraites par fichier

**cs1100501 → `finess_gestionnaire`** :

| Index CSV | Champ | Colonne DB |
|-----------|-------|------------|
| 1 | FINESS EJ | `id_gestionnaire` |
| 2 | Raison sociale courte | (fallback) |
| 3 | Raison sociale longue | `raison_sociale` |
| 4 | Sigle | `sigle` |
| 5-10 | Adresse (num, type voie, lib voie, compl 1-3) | `adresse_*` |
| 12 | CP + Ville | `code_postal`, `commune` |
| 13 | Code département | `departement_code` |
| 14 | Nom département | `departement_nom` |
| 15 | Téléphone | `telephone` |
| 16 | Code statut juridique | `forme_juridique_code` |
| 17 | Libellé statut juridique | `forme_juridique_libelle` |
| 20 | SIREN | `siren` |

**cs1100502 → `finess_etablissement`** :

| Index CSV | Champ | Colonne DB |
|-----------|-------|------------|
| 1 | FINESS ET | `id_finess` |
| 2-3 | Raison sociale | `raison_sociale` |
| 5-10 | Adresse | `adresse_*` |
| 12 | CP + Ville | `code_postal`, `commune` |
| 13 | Dept code | `departement_code` |
| 14 | Dept nom | `departement_nom` |
| 15 | Téléphone | `telephone` |
| 18 | Catégorie code | `categorie_code` |
| 19 | Catégorie libellé | `categorie_libelle` |
| 20 | Groupe code | `groupe_code` |
| 21 | Groupe libellé | `groupe_libelle` |

**cs1100505** (mapping ET→EJ) :
- Record `structureet` : col[1] = FINESS ET, col[2] = FINESS EJ → renseigne `id_gestionnaire`
- Record `equipementsocial` : identifie les ET ayant un équipement social (confirmation ESSMS)

---

## 5. Étape 1 — Déduction par règles métier

Ces enrichissements sont **déterministes** — ils ne nécessitent aucun appel API.

### 5.1. Catégorie normalisée

Mapping du libellé FINESS vers un label simplifié :

```python
CATEGORIE_NORMALISEE = {
    # Handicap enfant
    "Institut Médico-Éducatif": "IME",
    "Inst.Médico-Educatif (I.M.E.)": "IME",
    "Institut Médico-Professionnel": "IMPRO",
    "Inst.Médico-Profes.(I.M.Pro)": "IMPRO",
    "Service Éducation Spéciale Soins Domicile": "SESSAD",
    "S.E.S.S.A.D.": "SESSAD",
    "Centre Action Médico-Sociale Précoce": "CAMSP",
    "C.A.M.S.P.": "CAMSP",
    "Inst. Thérapeutique Éducatif et Pédagogique": "ITEP",
    "I.T.E.P.": "ITEP",
    "Institut d'Education Motrice": "IEM",
    "I.E.M.": "IEM",
    "Inst.Education Sensorielle Sourds": "IES-Sourds",
    "Inst.Education Sensorielle Aveugles": "IES-Aveugles",
    "Centre de Pré-Orientation": "CPRO",
    # Handicap adulte
    "Maison Accueil Spécialisée": "MAS",
    "M.A.S.": "MAS",
    "Foyer Accueil Médicalisé": "FAM",
    "F.A.M.": "FAM",
    "Foyer de Vie": "FV",
    "Foyer d'Hébergement": "FH",
    "Foyer Héberg. Adultes Handicapés": "FH",
    "Etab.et Serv.Aide par le Travail": "ESAT",
    "E.S.A.T.": "ESAT",
    "Serv.Accomp.Vie Sociale": "SAVS",
    "S.A.V.S.": "SAVS",
    "Serv. Accomp. Médico-Social Adultes Handicapés": "SAMSAH",
    "S.A.M.S.A.H.": "SAMSAH",
    "Centre de Rééducation Professionnelle": "CRP",
    # Personnes âgées
    "E.H.P.A.D.": "EHPAD",
    "Établissement d'Hébergement pour Personnes Âgées Dépendantes": "EHPAD",
    "Serv.Soins Infirmiers Domicile": "SSIAD",
    "S.S.I.A.D.": "SSIAD",
    "Unité de Soins de Longue Durée": "USLD",
    "U.S.L.D.": "USLD",
    "Résidence Autonomie": "RA",
    "Accueil de Jour": "AJ",
    "Hébergement Temporaire": "HT",
    "S.P.A.S.A.D.": "SPASAD",
    # Protection de l'enfance
    "Maison d'Enfants à Caractère Social": "MECS",
    "M.E.C.S.": "MECS",
    "Foyer de l'Enfance": "FDE",
    "Club de Prévention": "CP",
    "Action Éducative en Milieu Ouvert": "AEMO",
    "A.E.M.O.": "AEMO",
    "Pouponnière à Caractère Social": "PCS",
    "Maison Maternelle": "MM",
    # Addictologie
    "Centre Soins Accomp. Prév. Addictologie": "CSAPA",
    "C.S.A.P.A.": "CSAPA",
    "Centre d'Accueil et d'Accompagnement à la Réduction des Risques pour Usagers de Drogues": "CAARUD",
    "C.A.A.R.U.D.": "CAARUD",
    # Insertion
    "Centre d'Héberg. et de Réadaptation Sociale": "CHRS",
    "C.H.R.S.": "CHRS",
    "Centre d'Hébergement d'Urgence": "CHU",
    "Lits Halte Soins Santé": "LHSS",
    "Lits d'Accueil Médicalisés": "LAM",
}
```

### 5.2. Secteur d'activité par catégorie

Chaque établissement se voit attribuer un **secteur d'activité** déduit de sa catégorie normalisée. Le gestionnaire reçoit le secteur dominant parmi ses établissements.

```python
SECTEUR_PAR_CATEGORIE = {
    # Handicap enfant
    "IME":      "Handicap Enfant",
    "IMPRO":    "Handicap Enfant",
    "SESSAD":   "Handicap Enfant",
    "CAMSP":    "Handicap Enfant",
    "ITEP":     "Handicap Enfant",
    "IEM":      "Handicap Enfant",
    "IES-Sourds":   "Handicap Enfant",
    "IES-Aveugles": "Handicap Enfant",
    "CPRO":     "Handicap Enfant",
    # Handicap adulte
    "MAS":      "Handicap Adulte",
    "FAM":      "Handicap Adulte",
    "FH":       "Handicap Adulte",
    "FV":       "Handicap Adulte",
    "ESAT":     "Handicap Adulte",
    "SAVS":     "Handicap Adulte",
    "SAMSAH":   "Handicap Adulte",
    "CRP":      "Handicap Adulte",
    # Personnes âgées
    "EHPAD":    "Personnes Âgées",
    "SSIAD":    "Personnes Âgées",  # Peut aussi être SAD, selon le public
    "USLD":     "Personnes Âgées",
    "RA":       "Personnes Âgées",
    "AJ":       "Personnes Âgées",
    "HT":       "Personnes Âgées",
    "SPASAD":   "Personnes Âgées",
    # Protection de l'enfance
    "MECS":     "Protection de l'Enfance",
    "FDE":      "Protection de l'Enfance",
    "AEMO":     "Protection de l'Enfance",
    "PCS":      "Protection de l'Enfance",
    "MM":       "Protection de l'Enfance",
    "CP":       "Protection de l'Enfance",
    # Addictologie
    "CSAPA":    "Addictologie",
    "CAARUD":   "Addictologie",
    # Hébergement social / Insertion / Migrants
    "CHRS":     "Hébergement Social",
    "CHU":      "Hébergement Social",
    "LHSS":     "Hébergement Social",
    "LAM":      "Hébergement Social",
    # Aide à domicile (si catégorie SAD détectée)
    # "SAAD":   "Aide à Domicile",
    # "SPASAD": "Aide à Domicile",  -- déjà mappé en Personnes Âgées
}
```

### Détermination du secteur principal du gestionnaire

Le secteur du gestionnaire est calculé à partir de la **dominante** parmi ses établissements :

```python
from collections import Counter

def determiner_secteur_gestionnaire(etabs_du_gestionnaire: list[dict]) -> str:
    """
    Détermine le secteur d'activité principal du gestionnaire
    en comptant les secteurs de ses établissements.
    
    Returns:
        Le secteur majoritaire, ou "Multi-secteurs" si ≥2 secteurs
        représentent chacun ≥30% des établissements.
    """
    secteurs = [e.get("secteur_activite") for e in etabs_du_gestionnaire 
                if e.get("secteur_activite")]
    
    if not secteurs:
        return None
    
    compteur = Counter(secteurs)
    total = len(secteurs)
    top_secteur, top_count = compteur.most_common(1)[0]
    
    # Si le top secteur représente ≥70% → mono-secteur
    if top_count / total >= 0.7:
        return top_secteur
    
    # Si 2+ secteurs significatifs (≥30% chacun) → Multi-secteurs
    secteurs_significatifs = [s for s, c in compteur.items() if c / total >= 0.3]
    if len(secteurs_significatifs) >= 2:
        return "Multi-secteurs"
    
    # Sinon, retourner le dominant
    return top_secteur
```

### Application en base

```python
def apply_secteur_activite(cur, departement: str):
    """Attribue le secteur d'activité à chaque établissement et gestionnaire."""
    
    # 1. Établissements : mapping direct catégorie → secteur
    for cat_norm, secteur in SECTEUR_PAR_CATEGORIE.items():
        cur.execute("""
            UPDATE finess_etablissement SET secteur_activite = %s
            WHERE categorie_normalisee = %s AND departement_code = %s
              AND secteur_activite IS NULL
        """, (secteur, cat_norm, departement))
    
    # 2. Gestionnaires : secteur dominant parmi leurs établissements
    cur.execute("""
        WITH secteurs_par_gest AS (
            SELECT id_gestionnaire, secteur_activite,
                   COUNT(*) AS nb,
                   COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY id_gestionnaire) AS pct
            FROM finess_etablissement
            WHERE departement_code = %s AND secteur_activite IS NOT NULL
            GROUP BY id_gestionnaire, secteur_activite
        ),
        dominant AS (
            SELECT id_gestionnaire,
                   CASE 
                       WHEN COUNT(*) FILTER (WHERE pct >= 30) >= 2 THEN 'Multi-secteurs'
                       ELSE (SELECT secteur_activite FROM secteurs_par_gest s2 
                             WHERE s2.id_gestionnaire = secteurs_par_gest.id_gestionnaire 
                             ORDER BY nb DESC LIMIT 1)
                   END AS secteur_principal
            FROM secteurs_par_gest
            GROUP BY id_gestionnaire
        )
        UPDATE finess_gestionnaire g SET 
            secteur_activite_principal = d.secteur_principal
        FROM dominant d
        WHERE g.id_gestionnaire = d.id_gestionnaire
          AND g.secteur_activite_principal IS NULL
    """, (departement,))
```

### 5.3. Financeur par catégorie

```python
FINANCEUR_PAR_CATEGORIE = {
    # Handicap enfant
    "IME":      {"principal": "ARS", "secondaire": "Éducation Nationale"},
    "IMPRO":    {"principal": "ARS"},
    "SESSAD":   {"principal": "ARS"},
    "CAMSP":    {"principal": "ARS + Assurance Maladie"},
    "ITEP":     {"principal": "ARS"},
    "IEM":      {"principal": "ARS"},
    # Handicap adulte
    "MAS":      {"principal": "Assurance Maladie (100%)"},
    "FAM":      {"principal": "ARS + Conseil Départemental (50/50)"},
    "FH":       {"principal": "Conseil Départemental"},
    "FV":       {"principal": "Conseil Départemental"},
    "SAVS":     {"principal": "Conseil Départemental"},
    "SAMSAH":   {"principal": "ARS + Conseil Départemental"},
    "ESAT":     {"principal": "DREETS (ex-DIRECCTE)"},
    "CRP":      {"principal": "Assurance Maladie"},
    # Personnes âgées
    "EHPAD":    {"principal": "ARS + Conseil Départemental + Usager"},
    "SSIAD":    {"principal": "Assurance Maladie"},
    "USLD":     {"principal": "Assurance Maladie"},
    "RA":       {"principal": "Conseil Départemental + Usager"},
    "SPASAD":   {"principal": "ARS + Conseil Départemental"},
    # Protection de l'enfance
    "MECS":     {"principal": "Conseil Départemental"},
    "FDE":      {"principal": "Conseil Départemental"},
    "AEMO":     {"principal": "Conseil Départemental"},
    "PCS":      {"principal": "Conseil Départemental"},
    # Addictologie
    "CSAPA":    {"principal": "ARS"},
    "CAARUD":   {"principal": "ARS"},
    # Insertion
    "CHRS":     {"principal": "DREETS"},
    "CHU":      {"principal": "DREETS"},
    "LHSS":     {"principal": "ARS"},
    "LAM":      {"principal": "ARS"},
}
```

### 5.4. Type de tarification

```python
TARIFICATION_PAR_CATEGORIE = {
    "IME": "Prix de journée / Dotation globale",
    "IMPRO": "Prix de journée / Dotation globale",
    "SESSAD": "Dotation globale",
    "CAMSP": "Dotation globale",
    "ITEP": "Prix de journée / Dotation globale",
    "MAS": "Dotation globale (100% AM)",
    "FAM": "Tarification ternaire (ARS+CD+Usager)",
    "FH": "Prix de journée (APL)",
    "FV": "Prix de journée",
    "ESAT": "Dotation globale DREETS",
    "EHPAD": "Tarification ternaire (Soins+Dépendance+Hébergement)",
    "SSIAD": "Dotation globale ARS",
    "MECS": "Prix de journée CD",
    "CHRS": "Dotation globale BOP 177",
    "CSAPA": "Dotation globale ARS",
}
```

### 5.5. Champ `ouverture_365` — enrichi par LLM uniquement

La déduction de `ouverture_365` (hébergement permanent 365 jours/an) **ne peut pas se faire par règle métier seule** : la catégorie FINESS ne suffit pas (un IME peut être en semi-internat ou en internat, un FH peut être en semaine ou 365j, etc.).

Ce champ est donc renseigné **uniquement par le LLM** (étape 5) à partir du contenu web scrapé. Si aucun site web n'est trouvé ou si le LLM ne peut pas déterminer l'info, le champ reste `NULL`.

```python
# PAS de présomption par catégorie — ouverture_365 est renseigné
# exclusivement par le LLM via le prompt PROMPT_QUALIFICATION_PUBLIC
# à partir du contenu des pages web de l'établissement.
#
# Le prompt demande explicitement :
# "ouverture_365: true si internat permanent, hébergement 365j, 24h/24 est explicite"
```

---

## 6. Étape 2 — Géolocalisation Nominatim (adresses siège et établissement)

> Cette étape est exécutée dans Cloud Run après la déduction par règles métier. Elle utilise l'API gratuite Nominatim (OpenStreetMap) pour géocoder les adresses, avec un fallback sur l'API Adresse data.gouv.fr.

### Principe

La géolocalisation s'applique à **deux niveaux** :
- **Adresse du siège** (gestionnaire) → `finess_gestionnaire.latitude`, `finess_gestionnaire.longitude`
- **Adresse de l'établissement** → `finess_etablissement.latitude`, `finess_etablissement.longitude`

Le projet dispose déjà d'une classe `GeocodingService` dans `geocoding.py` qui encapsule Nominatim via `geopy`. On la réutilise.

### Implémentation

```python
from geopy.geocoders import Nominatim
import time

# Réutilise le service existant (geocoding.py)
geocoder = Nominatim(user_agent="ConfidensIA-FINESS-Enrichment/1.0")

def geocode_finess_address(
    adresse_complete: str,
    code_postal: str,
    commune: str
) -> dict:
    """
    Géocode une adresse FINESS via Nominatim.
    
    Returns:
        {"latitude": float|None, "longitude": float|None, "geocode_precision": str}
    """
    # Construire l'adresse complète
    parts = [p for p in [adresse_complete, code_postal, commune, "France"] if p and p.strip()]
    full_address = ", ".join(parts)
    
    try:
        time.sleep(1.1)  # Rate limit Nominatim : 1 req/s
        location = geocoder.geocode(full_address, timeout=10)
        
        if location:
            # Précision estimée
            import re
            if re.search(r'\d+', (adresse_complete or "").split(",")[0]):
                precision = "rooftop"  # Adresse avec numéro de rue
            else:
                precision = "locality"  # Commune seulement
            
            return {
                "latitude": round(location.latitude, 6),
                "longitude": round(location.longitude, 6),
                "geocode_precision": precision,
            }
        
        # Fallback : API Adresse data.gouv.fr (plus fiable pour la France)
        return geocode_via_api_adresse(full_address)
        
    except Exception as e:
        print(f"   [GEO] Erreur Nominatim: {e}")
        return {"latitude": None, "longitude": None, "geocode_precision": f"erreur: {e}"}


def geocode_via_api_adresse(query: str) -> dict:
    """Fallback via l'API Adresse du gouvernement français (BAN)."""
    import requests
    try:
        resp = requests.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": query, "limit": 1},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("features"):
                coords = data["features"][0]["geometry"]["coordinates"]
                score = data["features"][0]["properties"].get("score", 0)
                precision = "rooftop" if score > 0.7 else "street" if score > 0.5 else "locality"
                return {
                    "latitude": round(coords[1], 6),
                    "longitude": round(coords[0], 6),
                    "geocode_precision": precision,
                }
    except Exception:
        pass
    return {"latitude": None, "longitude": None, "geocode_precision": "non_trouvé"}
```

### Application en batch

```python
def geocode_all_entities(cur, departement: str):
    """
    Géocode tous les gestionnaires et établissements d'un département
    qui n'ont pas encore de coordonnées.
    """
    # 1. Gestionnaires (adresse siège)
    cur.execute("""
        SELECT id_gestionnaire, adresse_complete, code_postal, commune
        FROM finess_gestionnaire
        WHERE departement_code = %s AND latitude IS NULL
    """, (departement,))
    
    for row in cur.fetchall():
        geo = geocode_finess_address(row["adresse_complete"], row["code_postal"], row["commune"])
        if geo["latitude"]:
            cur.execute("""
                UPDATE finess_gestionnaire SET
                    latitude = %s, longitude = %s, geocode_precision = %s
                WHERE id_gestionnaire = %s
            """, (geo["latitude"], geo["longitude"], geo["geocode_precision"], row["id_gestionnaire"]))
    
    # 2. Établissements
    cur.execute("""
        SELECT id_finess, adresse_complete, code_postal, commune
        FROM finess_etablissement
        WHERE departement_code = %s AND latitude IS NULL
    """, (departement,))
    
    for row in cur.fetchall():
        geo = geocode_finess_address(row["adresse_complete"], row["code_postal"], row["commune"])
        if geo["latitude"]:
            cur.execute("""
                UPDATE finess_etablissement SET
                    latitude = %s, longitude = %s, geocode_precision = %s
                WHERE id_finess = %s
            """, (geo["latitude"], geo["longitude"], geo["geocode_precision"], row["id_finess"]))
```

### Dépendance

```
# requirements.txt (ajout)
geopy>=2.4.0
```

> **Note Cloud Run** : Les appels Nominatim sont limités à 1 req/s. Pour le département 65 (~150 ET + ~40 EJ), le géocodage prend ~3-4 minutes. L'API Adresse data.gouv.fr n'a pas de rate limit strict et sert de fallback.

---

## 7. Étape 3 — Enrichissement Serper (recherche web)

### Stratégie sans limitation de crédits

Puisque les crédits Google Cloud / Serper ne sont **pas limités**, on peut être exhaustif :

**5 à 8 requêtes Serper par établissement** (au lieu de 2 en mode limité) :

```python
def build_serper_queries(etab: dict) -> list[str]:
    """Construit les requêtes Serper pour un établissement ESSMS."""
    nom = etab["raison_sociale"]
    commune = etab["commune"]
    cat = etab.get("categorie_normalisee", "")
    dept = etab.get("departement_nom", "")
    
    queries = [
        # Q1 — Recherche principale : site officiel
        f'"{nom}" {commune}',
        
        # Q2 — Type de public et missions
        f'"{nom}" public accueilli missions {cat}',
        
        # Q3 — Dirigeants et équipe
        f'directeur "{nom}" {commune}',
        
        # Q4 — Actualités et signaux
        f'"{nom}" actualité recrutement projet',
        
        # Q5 — LinkedIn dirigeants/gestionnaire
        f'site:linkedin.com/in "{nom}" directeur',
        
        # Q6 — Contact et mail
        f'"{nom}" contact email @',
        
        # Q7 — Président du gestionnaire (si EJ identifié)
        # Ajouté dynamiquement si gestionnaire connu
        
        # Q8 — CPOM / autorisation ARS
        f'"{nom}" CPOM autorisation ARS {dept}',
    ]
    return queries
```

### Requêtes additionnelles par gestionnaire (EJ)

Pour chaque gestionnaire (EJ) distinct, on ajoute :

```python
def build_serper_queries_gestionnaire(gest: dict) -> list[str]:
    """Requêtes Serper niveau gestionnaire (mutualisées pour tous ses ET)."""
    nom = gest["raison_sociale"]
    commune = gest.get("commune", "")
    
    return [
        # G1 — Site officiel du gestionnaire
        f'"{nom}" association fondation site officiel',
        
        # G2 — Organigramme / équipe de direction
        f'"{nom}" organigramme direction équipe',
        
        # G3 — LinkedIn
        f'site:linkedin.com/company "{nom}"',
        
        # G4 — Réseau fédéral
        f'"{nom}" NEXEM FEHAP UNIOPSS URIOPSS membre adhérent',
        
        # G5 — Rapport activité / comptes
        f'"{nom}" rapport activité comptes annuels',
    ]
```

### Cache des requêtes

Chaque requête est hashée (SHA256) et stockée dans `finess_cache_serper`. Si un résultat existe et n'est pas expiré (30 jours), on le réutilise.

```python
import hashlib

def get_or_search_serper(query: str, api_key: str, cur) -> list[dict]:
    """Recherche Serper avec cache PostgreSQL."""
    query_hash = hashlib.sha256(query.encode()).hexdigest()
    
    # Vérifier le cache
    cur.execute("""
        SELECT results FROM finess_cache_serper 
        WHERE query_hash = %s AND expire_at > NOW()
    """, (query_hash,))
    row = cur.fetchone()
    if row:
        return json.loads(row[0]) if row[0] else []
    
    # Appel Serper
    results = serper_search(query, num=10, api_key=api_key)
    
    # Stocker en cache
    cur.execute("""
        INSERT INTO finess_cache_serper (query_hash, query_text, results, nb_results)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (query_hash) DO UPDATE SET results = EXCLUDED.results, 
            nb_results = EXCLUDED.nb_results, date_requete = NOW(),
            expire_at = NOW() + INTERVAL '30 days'
    """, (query_hash, query, json.dumps(results, ensure_ascii=False), len(results)))
    
    return results
```

### Identification du site officiel

```python
SITE_EXCLUSIONS = [
    "facebook.com", "linkedin.com", "twitter.com", "youtube.com",
    "instagram.com", "finess.sante.gouv.fr", "annuaire.action-sociale.org",
    "pagesjaunes.fr", "societe.com", "infogreffe.fr", "pappers.fr",
    "google.com", "wikipedia.org", "indeed.com", "emploi-collectivites.fr",
]

def extraire_site_officiel(results: list[dict], nom_etab: str) -> str | None:
    """Identifie le site officiel parmi les résultats Serper."""
    for r in results:
        url = r.get("link", "")
        domain = urlparse(url).netloc.lower()
        
        # Exclure les sites génériques
        if any(excl in domain for excl in SITE_EXCLUSIONS):
            continue
        
        # Favoriser les sites qui contiennent le nom ou un mot-clé ESSMS
        title = (r.get("title", "") + " " + r.get("snippet", "")).lower()
        nom_lower = nom_etab.lower()
        
        # Match si le titre contient le nom de l'établissement
        if any(word in title for word in nom_lower.split() if len(word) > 3):
            return url
    
    # Fallback : premier résultat non exclu
    for r in results:
        url = r.get("link", "")
        domain = urlparse(url).netloc.lower()
        if not any(excl in domain for excl in SITE_EXCLUSIONS):
            return url
    
    return None
```

---

## 8. Étape 4 — Scraping ciblé des pages web

### Pages à scraper par établissement

```python
PAGES_CIBLES = [
    "",                     # Page d'accueil
    "/qui-sommes-nous",
    "/nos-missions",
    "/public-accueilli",
    "/equipe",
    "/direction",
    "/organigramme",
    "/contact",
    "/a-propos",
    "/l-etablissement",
    "/presentation",
]
```

### Logique de scraping

Réutilise le pattern existant de `enrich_dept_prototype.py` :

```python
def scrape_etablissement_pages(site_url: str) -> dict:
    """Scrape les pages clés d'un site d'établissement ESSMS.
    
    Utilise requests simple uniquement (pas de ScrapingBee).
    Les sites JavaScript/SPA qui ne rendent pas côté serveur seront simplement
    ignorés (texte vide) — les snippets Serper compensent partiellement.
    
    Returns:
        {
            "combined_text": str,           # Tout le texte concaténé (pour LLM)
            "page_equipe_text": str,        # Texte de la page équipe (pour dirigeants)
            "page_contact_text": str,       # Texte de la page contact (pour emails)
            "pages_scrapped": list[str],    # URLs scrapées avec succès
            "emails_trouves": list[str],    # Emails trouvés dans les pages
        }
    """
    combined_text = ""
    page_equipe_text = ""
    page_contact_text = ""
    pages_ok = []
    emails_found = set()
    
    base_url = site_url.rstrip("/")
    
    for suffix in PAGES_CIBLES:
        url = base_url + suffix
        status, final_url, text = fetch_page_text(url)
        
        if status == 200 and text and len(text) > 100:
            combined_text += f"\n--- PAGE: {final_url} ---\n{text[:15000]}\n"
            pages_ok.append(final_url)
            
            # Identifier les pages spécifiques
            if any(kw in suffix for kw in ["/equipe", "/direction", "/organigramme"]):
                page_equipe_text += text[:10000]
            if "/contact" in suffix:
                page_contact_text += text[:5000]
            
            # Extraire les emails
            found = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
            emails_found.update(found)
        
        time.sleep(0.3)  # Politesse
    
    return {
        "combined_text": combined_text[:50000],  # Limiter pour le LLM
        "page_equipe_text": page_equipe_text,
        "page_contact_text": page_contact_text,
        "pages_scrapped": pages_ok,
        "emails_trouves": list(emails_found),
    }
```

> **Note** : Pas de fallback ScrapingBee (abonnement résilié). Pour les sites en JavaScript pur (SPA/React/Angular), on s'appuie sur les snippets Serper qui contiennent déjà un résumé du contenu. La grande majorité des sites ESSMS (associations, fondations) utilisent des CMS classiques (WordPress, Joomla) qui rendent côté serveur.

---

## 9. Étape 5 — Qualification LLM (Gemini)

### LLM utilisé

**Gemini 2.0 Flash** via l'API REST Google (même API que `enrich_dept_prototype.py`).

Pourquoi Gemini :
- Déjà intégré et éprouvé dans le projet (`gemini_generate_text`, `gemini_generate_json`)
- Crédits Google Cloud illimités 
- Excellent en extraction structurée JSON
- Fenêtre de contexte large (1M tokens pour Gemini 2.0)

### Prompt 1 — Qualification du public et de l'accueil

```python
PROMPT_QUALIFICATION_PUBLIC = """
Tu es un expert du secteur social et médico-social français (ESSMS).

À partir des informations suivantes sur un établissement :
- Nom : {raison_sociale}
- Catégorie FINESS : {categorie_libelle} ({categorie_normalisee})
- Département : {departement_nom}
- Commune : {commune}
- Contenu des pages web : 
{texte_pages_web}

Ta tâche : extraire et structurer les informations sur le public accueilli et le fonctionnement.

Réponds STRICTEMENT en JSON valide :
{{
  "type_public": "<libellé principal normalisé, ex: Enfants et adolescents déficients intellectuels>",
  "type_public_synonymes": ["<synonyme1>", "<synonyme2>"],
  "pathologies_specifiques": ["<TSA>", "<Polyhandicap>", ...] ou [],
  "age_min": <entier ou null>,
  "age_max": <entier ou null>,
  "tranche_age_label": "<libellé humain ex: Enfants 6-16 ans>",
  "type_accueil": ["<Internat>", "<Semi-internat>", "<Accueil de jour>", "<Ambulatoire>"],
  "periode_ouverture": "<365 jours / 210 jours / Semaine / etc.>",
  "ouverture_365": <true/false>,
  "places_info": "<info trouvée sur la capacité, ou null>",
  "site_web_officiel": "<URL du site officiel si confirmé, ou null>",
  "email_contact": "<email de contact trouvé, ou null>",
  "telephone": "<téléphone trouvé, ou null>",
  "confidence": <0.0 à 1.0>
}}

Règles :
- Si une info n'est pas trouvée dans le texte, mettre null (pas d'invention).
- Pour type_public, être précis : "Enfants et adolescents autistes (TSA)" plutôt que juste "handicap".
- Les pathologies_specifiques doivent être normalisées : TSA, Polyhandicap, Déficience intellectuelle, Trouble du comportement, Handicap moteur, Déficience visuelle, Déficience auditive, Handicap psychique, Alzheimer, etc.
- Pour ouverture_365, mettre true si internat permanent, hébergement 365j, 24h/24 est explicite.
- confidence = estimation de ta fiabilité globale.

Réponds uniquement en JSON valide, sans commentaire ni markdown.
"""
```

### Prompt 2 — Extraction des dirigeants

```python
PROMPT_EXTRACTION_DIRIGEANTS = """
Tu es un assistant d'extraction de données sur les dirigeants d'organisations sociales et médico-sociales.

Organisation : {raison_sociale}
Commune : {commune}
Département : {departement_nom}

Texte des pages web (site officiel + résultats de recherche) :
{texte_combine}

Extrais TOUS les dirigeants et responsables mentionnés. Pour chaque personne, retourne :

{{
  "dirigeants": [
    {{
      "civilite": "<M.|Mme|null>",
      "nom": "<NOM>",
      "prenom": "<Prénom>",
      "fonction_brute": "<texte exact trouvé>",
      "fonction_normalisee": "<une parmi : Président / DG / Directeur / DSI / DRH / Directeur Innovation / Directeur Adjoint / Directeur Administratif et Financier / Médecin Directeur / Chef de Service>",
      "source_url": "<URL de la page où l'info a été trouvée, ou null>",
      "confiance": "<haute/moyenne/basse>"
    }}
  ]
}}

Règles :
- Ne pas inventer. Si tu n'es pas sûr du nom complet, indiquer confiance "basse".
- Normaliser la fonction vers les catégories listées.
- Distinguer le président (CA associatif) du directeur général (opérationnel).
- Si le texte mentionne "le directeur M. Dupont", extraire nom="DUPONT" prenom=null.

Réponds uniquement en JSON valide.
"""
```

### Prompt 3 — Signaux de tension

```python
PROMPT_SIGNAUX_TENSION = """
Tu es un analyste du secteur social et médico-social.

Établissement : {raison_sociale} ({categorie_normalisee})
Commune : {commune}, {departement_nom}

Extraits d'actualités et résultats de recherche :
{texte_actualites}

Identifie les signaux de tension ou d'opportunité stratégique parmi :
- Recrutement massif / difficultés de recrutement
- Fermeture / menace de fermeture
- Fusion / regroupement avec un autre établissement
- Projet de construction / extension / rénovation
- Grève / conflit social
- Inspection / rapport critique ARS
- Attribution nouveau CPOM
- Transformation de l'offre (virage inclusif, habitat inclusif...)
- Appel à projets ARS remporté
- Changement de direction

Réponds en JSON :
{{
  "signaux": [
    {{
      "type": "<recrutement|fermeture|fusion|extension|conflit|inspection|cpom|transformation|appel_projet|changement_direction>",
      "resume": "<résumé en 1-2 phrases>",
      "date_approx": "<YYYY-MM ou null>",
      "source_url": "<URL source>",
      "impact": "<positif|négatif|neutre>"
    }}
  ],
  "signal_tension": <true/false>,
  "signal_tension_detail": "<résumé global en 1 phrase, ou null>"
}}

Si aucun signal trouvé, retourner {{"signaux": [], "signal_tension": false, "signal_tension_detail": null}}.
Réponds uniquement en JSON valide.
"""
```

### Prompt 4 — Réseau fédéral du gestionnaire

```python
PROMPT_RESEAU_FEDERAL = """
À partir de ces informations sur l'organisme gestionnaire :
- Nom : {raison_sociale}
- Forme juridique : {forme_juridique_libelle}
- Extraits web : {texte_web}

Identifie le réseau fédéral / tête de réseau d'appartenance parmi :
NEXEM, FEHAP, Croix-Rouge française, UNIOPSS, URIOPSS, APF France handicap, 
UNAPEI, Adapei, ADMR, Mutualité Française, Les PEP, Apprentis d'Auteuil,
Fondation de France, Armée du Salut, Emmaüs, Coallia, AGEFIPH, LADAPT,
ou autre (préciser).

Réponds en JSON :
{{
  "reseau_federal": "<nom du réseau ou null>",
  "confiance": "<haute/moyenne/basse>",
  "source": "<élément qui t'a permis de déduire>"
}}

Si aucun réseau identifié, retourner {{"reseau_federal": null, "confiance": null, "source": null}}.
"""
```

---

## 10. Étape 6 — Enrichissement dirigeants (+ identification DAF)

### Pipeline multi-sources

Pour chaque gestionnaire (EJ), on recherche ses dirigeants via **4 sources** cumulées :

```python
async def enrich_dirigeants(gestionnaire: dict, context: dict, api_keys: dict) -> list[dict]:
    """
    Pipeline complet de recherche de dirigeants.
    
    Sources (par ordre de fiabilité) :
    1. Site officiel (page équipe/organigramme) — confiance haute
    2. LinkedIn via Serper — confiance haute
    3. Résultats Serper généraux — confiance moyenne
    4. Presse locale / actualités — confiance moyenne-basse
    """
    
    dirigeants_bruts = []
    
    # Source 1 : Page équipe du site officiel
    if context.get("page_equipe_text"):
        dir_site = gemini_generate_json(
            api_key=api_keys["gemini"],
            model=api_keys["gemini_model"],
            prompt=PROMPT_EXTRACTION_DIRIGEANTS.format(
                raison_sociale=gestionnaire["raison_sociale"],
                commune=gestionnaire["commune"],
                departement_nom=gestionnaire["departement_nom"],
                texte_combine=context["page_equipe_text"][:20000],
            )
        )
        for d in (dir_site.get("dirigeants") or []):
            d["source_type"] = "site_officiel"
            dirigeants_bruts.append(d)
    
    # Source 2 : LinkedIn via Serper
    queries_linkedin = [
        f'site:linkedin.com/in directeur "{gestionnaire["raison_sociale"]}"',
        f'site:linkedin.com/in président "{gestionnaire["raison_sociale"]}"',
    ]
    for q in queries_linkedin:
        results = get_or_search_serper(q, api_keys["serper"], cur)
        linkedin_text = "\n".join([
            f"- {r.get('title', '')} — {r.get('snippet', '')} ({r.get('link', '')})"
            for r in results[:5]
        ])
        if linkedin_text.strip():
            dir_li = gemini_generate_json(
                api_key=api_keys["gemini"],
                model=api_keys["gemini_model"],
                prompt=PROMPT_EXTRACTION_DIRIGEANTS.format(
                    raison_sociale=gestionnaire["raison_sociale"],
                    commune=gestionnaire["commune"],
                    departement_nom=gestionnaire["departement_nom"],
                    texte_combine=linkedin_text,
                )
            )
            for d in (dir_li.get("dirigeants") or []):
                d["source_type"] = "linkedin_serper"
                dirigeants_bruts.append(d)
    
    # Source 3 : Résultats Serper généraux (déjà collectés à l'étape 3)
    # Traité dans le prompt principal si contexte suffisant
    
    # Dédoublonnage par nom
    return deduplicate_dirigeants(dirigeants_bruts)
```

### Identification spécifique du DAF

Le **Directeur Administratif et Financier** (DAF) fait l'objet d'une recherche dédiée car c'est un interlocuteur clé pour la prospection. Ses coordonnées sont stockées directement dans `finess_gestionnaire` (colonnes `daf_*`) en plus de la table `finess_dirigeant`.

```python
def identify_daf(gestionnaire: dict, dirigeants: list[dict], context: dict, 
                 api_keys: dict, cur) -> dict | None:
    """
    Identifie spécifiquement le DAF du gestionnaire.
    
    1. D'abord chercher dans les dirigeants déjà identifiés
    2. Si pas trouvé, lancer une requête Serper ciblée DAF/RAF
    3. Stocker dans finess_gestionnaire.daf_*
    
    Returns:
        dict avec nom, prenom, email, confiance ou None
    """
    # 1. Chercher dans les dirigeants déjà trouvés
    daf_synonymes = {"DAF", "Directeur Administratif et Financier", "RAF",
                     "Responsable Administratif et Financier", "Directeur Financier",
                     "Directeur des Finances", "Secrétaire Général"}
    
    for d in dirigeants:
        fn = (d.get("fonction_normalisee") or "").strip()
        fb = (d.get("fonction_brute") or "").strip()
        if fn == "DAF" or any(s.lower() in fb.lower() for s in daf_synonymes):
            return d  # DAF déjà identifié dans les résultats généraux
    
    # 2. Requête Serper ciblée si pas trouvé
    queries_daf = [
        f'"directeur administratif et financier" "{gestionnaire["raison_sociale"]}"',
        f'"DAF" "{gestionnaire["raison_sociale"]}" site:linkedin.com/in',
        f'"responsable administratif et financier" "{gestionnaire["raison_sociale"]}"',
    ]
    
    for q in queries_daf:
        results = get_or_search_serper(q, api_keys["serper"], cur)
        if results:
            daf_text = "\n".join([
                f"- {r.get('title', '')} — {r.get('snippet', '')} ({r.get('link', '')})"
                for r in results[:5]
            ])
            daf_result = gemini_generate_json(
                api_key=api_keys["gemini"],
                model=api_keys["gemini_model"],
                prompt=PROMPT_EXTRACTION_DIRIGEANTS.format(
                    raison_sociale=gestionnaire["raison_sociale"],
                    commune=gestionnaire["commune"],
                    departement_nom=gestionnaire["departement_nom"],
                    texte_combine=daf_text,
                )
            )
            for d in (daf_result.get("dirigeants") or []):
                fn = (d.get("fonction_normalisee") or "").strip()
                fb = (d.get("fonction_brute") or "").strip()
                if fn == "DAF" or any(s.lower() in fb.lower() for s in daf_synonymes):
                    d["source_type"] = "serper_daf_cible"
                    return d
    
    return None


def store_daf_on_gestionnaire(gestionnaire_id: str, daf: dict, cur):
    """Stocke le DAF identifié directement dans la table finess_gestionnaire."""
    cur.execute("""
        UPDATE finess_gestionnaire SET
            daf_nom = %s,
            daf_prenom = %s,
            daf_email = %s,
            daf_linkedin_url = %s,
            daf_source = %s,
            daf_confiance = %s
        WHERE id_gestionnaire = %s
    """, (
        daf.get("nom"),
        daf.get("prenom"),
        daf.get("email_reconstitue"),  # Sera renseigné à l'étape 7 (reconstruction emails)
        daf.get("linkedin_url"),
        daf.get("source_url"),
        daf.get("confiance", "moyenne"),
        gestionnaire_id,
    ))
```

> **Pourquoi le DAF en colonnes dédiées ?** Le DAF est un contact privilégié pour les propositions de services financiers/comptables (externalisation paie, logiciels de gestion, etc.). Le stocker directement sur `finess_gestionnaire` facilite les exports et le publipostage sans jointure.

### Fonctions ciblées

| Fonction normalisée | Priorité prospection | Synonymes à rechercher |
|---------------------|---------------------|------------------------|
| Président | Haute | Président du CA, Président de l'association, Président du conseil |
| DG | Haute | Directeur général, PDG, CEO |
| Directeur | Haute | Directeur d'établissement, Chef d'établissement |
| DSI | Très haute | Directeur des Systèmes d'Information, Responsable SI, CIO |
| Directeur Innovation | Très haute | Chief Digital Officer, Responsable transformation numérique |
| DRH | Moyenne | Directeur des Ressources Humaines, RRH |
| **DAF** | **Haute** | **Directeur Administratif et Financier, RAF, Secrétaire Général** |
| Directeur Adjoint | Moyenne | Directeur adjoint, DGA |
| Médecin Directeur | Moyenne | Médecin coordonnateur, Médecin chef |
| Chef de Service | Basse | Chef de service éducatif, Chef de service médical |

---

## 11. Étape 7 — Reconstruction emails

### Détection du pattern mail

Réutilise la logique de `scripts/reconstruct_emails_dirigeants_v2.py` :

```python
def detect_mail_pattern(emails_trouves: list[str], domaine: str) -> dict:
    """
    Analyse les emails trouvés pour déduire la structure mail.
    
    Retourne:
        {
            "domaine": "asso-exemple.fr",
            "structure": "prenom.nom",  # ou "p.nom", "prenom", etc.
            "confiance": "haute",
            "exemple": "jean.dupont@asso-exemple.fr"
        }
    """
    # Filtrer les emails du domaine
    domain_emails = [e for e in emails_trouves if e.endswith(f"@{domaine}")]
    
    # Exclure les emails génériques
    person_emails = [e for e in domain_emails if is_person_email(e)]
    
    if not person_emails:
        # Fallback LLM si aucun email de personne trouvé
        return {"domaine": domaine, "structure": None, "confiance": "basse"}
    
    # Analyser le pattern du local part
    patterns = []
    for email in person_emails:
        local = email.split("@")[0]
        if "." in local:
            parts = local.split(".")
            if len(parts) == 2:
                if len(parts[0]) == 1:
                    patterns.append("p.nom")
                else:
                    patterns.append("prenom.nom")
            elif len(parts) == 3:
                patterns.append("prenom.nom.ext")
        elif "-" in local:
            patterns.append("prenom-nom")
        elif "_" in local:
            patterns.append("prenom_nom")
        else:
            patterns.append("autre")
    
    # Pattern dominant
    from collections import Counter
    if patterns:
        most_common = Counter(patterns).most_common(1)[0]
        return {
            "domaine": domaine,
            "structure": most_common[0],
            "confiance": "haute" if most_common[1] >= 2 else "moyenne",
            "exemples": person_emails[:3],
        }
    
    return {"domaine": domaine, "structure": None, "confiance": "basse"}
```

### Reconstitution d'email par dirigeant

```python
import unicodedata

def normalise_for_email(text: str) -> str:
    """Normalise un texte pour usage email (minuscules, sans accents, sans espaces)."""
    text = text.lower().strip()
    # Retirer les accents
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    # Retirer les caractères spéciaux
    text = re.sub(r'[^a-z0-9]', '', text)
    return text

def reconstituer_email(prenom: str, nom: str, structure: str, domaine: str) -> str | None:
    """Reconstitue un email à partir du pattern détecté."""
    if not prenom or not nom or not structure or not domaine:
        return None
    
    p = normalise_for_email(prenom)
    n = normalise_for_email(nom)
    
    templates = {
        "prenom.nom": f"{p}.{n}@{domaine}",
        "p.nom": f"{p[0]}.{n}@{domaine}",
        "prenom": f"{p}@{domaine}",
        "nom.prenom": f"{n}.{p}@{domaine}",
        "prenom-nom": f"{p}-{n}@{domaine}",
        "prenom_nom": f"{p}_{n}@{domaine}",
    }
    
    return templates.get(structure)
```

---

## 12. Étape 8 — Signaux de tension et actualités

### Requêtes Serper dédiées

```python
def rechercher_signaux(etab: dict, api_key: str, cur) -> list[dict]:
    """Recherche des signaux d'actualité pour un établissement."""
    queries = [
        f'"{etab["raison_sociale"]}" actualité {etab["commune"]}',
        f'"{etab["raison_sociale"]}" recrutement emploi',
        f'"{etab["raison_sociale"]}" projet transformation',
    ]
    
    all_results = []
    for q in queries:
        results = get_or_search_serper(q, api_key, cur)
        all_results.extend(results)
    
    return all_results
```

### Qualification LLM des signaux

Le prompt `PROMPT_SIGNAUX_TENSION` (section 8) est appliqué au texte combiné des résultats d'actualités.

### Mise à jour en base

```python
def update_signaux(etab_id: str, signaux_result: dict, cur):
    """Met à jour les signaux dans finess_etablissement et finess_gestionnaire."""
    cur.execute("""
        UPDATE finess_etablissement SET
            signaux_recents = %s,
            date_enrichissement = NOW()
        WHERE id_finess = %s
    """, (json.dumps(signaux_result.get("signaux", []), ensure_ascii=False), etab_id))
    
    if signaux_result.get("signal_tension"):
        cur.execute("""
            UPDATE finess_gestionnaire SET
                signal_tension = TRUE,
                signal_tension_detail = COALESCE(signal_tension_detail || '; ', '') || %s
            WHERE id_gestionnaire = (
                SELECT id_gestionnaire FROM finess_etablissement WHERE id_finess = %s
            )
        """, (signaux_result.get("signal_tension_detail", ""), etab_id))
```

---

## 13. Étape 9 — Enrichissement CPOM et territorial

### CPOM (Contrats Pluriannuels d'Objectifs et de Moyens)

Source : data.gouv.fr — fichiers CPOM publiés par les ARS.

```python
def enrich_cpom(etab: dict, cur):
    """
    Enrichit les infos CPOM :
    - Booléen cpom
    - Date d'échéance
    - Déduit de Serper si pas trouvé dans data.gouv
    """
    # Recherche dans les résultats déjà collectés (requête Q8 de l'étape 3)
    # Le LLM a peut-être extrait l'info dans les signaux
    pass
```

### Zone de dotation territoriale (STATISS)

```python
# Valeurs possibles : Sur-dotée, Sous-dotée, Equilibrée
# Source : données STATISS ARS par département
ZONE_DOTATION_65 = "Equilibrée"  # À confirmer avec les données ARS Occitanie

# Pour le test Hautes-Pyrénées, on peut le renseigner manuellement
# En production, ces données seront importées depuis les publications ARS
```

---

## 14. Script principal : `enrich_finess_dept.py`

### Architecture du script

Le script principal suit le modèle de `scripts/enrich_dept_prototype.py` (habitat) en l'adaptant aux ESSMS.

> **IMPORTANT** : Ce script est utilisé **aussi bien en local qu'en Cloud Run**. Il n'effectue **aucune ingestion CSV** — il lit les données brutes depuis Supabase et écrit les résultats enrichis directement dans Supabase. L'ingestion est une opération locale préalable séparée (voir section 4).

```python
"""
scripts/enrich_finess_dept.py — Enrichissement FINESS par département

Ce script LIT depuis Supabase et ÉCRIT directement dans Supabase.
Aucun fichier CSV n'est nécessaire. L'ingestion est une étape locale préalable.

Usage:
    # Test local Hautes-Pyrénées (dry-run)
    python scripts/enrich_finess_dept.py --departements 65 --dry-run --out-dir outputs/finess

    # Test local avec écriture en base
    python scripts/enrich_finess_dept.py --departements 65 --out-dir outputs/finess

    # Plusieurs départements
    python scripts/enrich_finess_dept.py --departements 65,31,32 --out-dir outputs/finess

    # Cloud Run batch (identique — lit/écrit directement dans Supabase)
    python scripts/enrich_finess_dept.py --departements all --out-dir /tmp/outputs

Options:
    --departements  : Code(s) département séparés par virgule, ou "all"
    --limit         : Nombre max d'établissements par département (0 = pas de limite)
    --dry-run       : Ne pas écrire en base, uniquement générer des rapports
    --out-dir       : Dossier de sortie pour les rapports CSV/JSON
    --skip-serper   : Ne pas faire les requêtes Serper (utiliser le cache + base existante)
    --skip-llm      : Ne pas appeler le LLM (ne faire que les règles métier + géocodage)
    --skip-geocode  : Ne pas géocoder les adresses (Nominatim)
    --etape         : Étape spécifique à exécuter (1-9)

Env vars:
    - GEMINI_API_KEY (requis pour étapes 5-8)
    - SERPER_API_KEY (requis pour étapes 3, 6, 8)
    - DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT
"""
```

### Boucle principale

```python
def main():
    args = parse_args()
    db = DatabaseManager()
    
    # Résoudre les clés API
    gemini_key = os.getenv("GEMINI_API_KEY", "").strip()
    gemini_model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash").strip()
    serper_key = os.getenv("SERPER_API_KEY", "").strip()
    
    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            
            # Charger les établissements à enrichir (depuis Supabase)
            etabs = load_etabs_to_enrich(cur, args.departements, args.limit)
            print(f"[FINESS] {len(etabs)} établissements à enrichir")
            
            # Charger les gestionnaires associés
            gestionnaires = load_gestionnaires(cur, args.departements)
            print(f"[FINESS] {len(gestionnaires)} gestionnaires associés")
            
            # ÉTAPE 1 : Règles métier (batch, avant la boucle)
            apply_all_business_rules(cur, args.departements)
            conn.commit()
            
            # ÉTAPE 2 : Géolocalisation Nominatim (batch)
            if not args.skip_geocode:
                geocode_all_entities(cur, args.departements)
                conn.commit()
            
            for i, etab in enumerate(etabs):
                print(f"\n[{i+1}/{len(etabs)}] {etab['raison_sociale']} ({etab['categorie_normalisee']})")
                t0 = time.time()
                
                try:
                    # ÉTAPE 3 : Serper
                    if not args.skip_serper and serper_key:
                        context = enrich_via_serper(etab, serper_key, cur)
                    else:
                        context = {"combined_text": "", "emails_trouves": []}
                    
                    # ÉTAPE 4 : Scraping (requests simple, sans ScrapingBee)
                    if etab.get("site_web") and not args.skip_serper:
                        scrape_result = scrape_etablissement_pages(
                            etab["site_web"]
                        )
                        context.update(scrape_result)
                    
                    # ÉTAPE 5 : Qualification LLM
                    if not args.skip_llm and gemini_key and context.get("combined_text"):
                        qualification = qualify_with_llm(etab, context, gemini_key, gemini_model)
                        apply_qualification(etab, qualification, cur)
                    
                    # ÉTAPE 6-7 : Dirigeants + DAF + emails (niveau gestionnaire)
                    gest = get_gestionnaire(etab, gestionnaires)
                    if gest and not gest.get("_dirigeants_done"):
                        if not args.skip_llm and gemini_key:
                            dirigeants = enrich_dirigeants_and_emails(gest, context, cur,
                                gemini_key, gemini_model, serper_key)
                            
                            # Identification spécifique du DAF
                            daf = identify_daf(gest, dirigeants, context,
                                {"gemini": gemini_key, "gemini_model": gemini_model, "serper": serper_key}, cur)
                            if daf:
                                store_daf_on_gestionnaire(gest["id_gestionnaire"], daf, cur)
                                print(f"   [DAF] {daf.get('prenom', '')} {daf.get('nom', '')} identifié")
                            
                            gest["_dirigeants_done"] = True
                    
                    # ÉTAPE 8 : Signaux de tension
                    if not args.skip_serper and serper_key and not args.skip_llm and gemini_key:
                        signaux = enrich_signaux(etab, context, cur,
                            gemini_key, gemini_model, serper_key)
                    
                    # Marquer comme enrichi (écriture directe dans Supabase)
                    mark_enriched(etab["id_finess"], cur)
                    conn.commit()
                    
                    elapsed = time.time() - t0
                    print(f"   ✅ Enrichi en {elapsed:.1f}s")
                    
                except Exception as e:
                    conn.rollback()
                    mark_error(etab["id_finess"], str(e), cur)
                    conn.commit()
                    print(f"   ❌ Erreur: {e}")
                
                # Rate limiting
                time.sleep(0.5)
            
            # ÉTAPE 9 : CPOM + territorial (batch)
            enrich_cpom_territorial(cur, args.departements)
            conn.commit()
    
    # Générer les rapports (optionnel, pour suivi)
    generate_reports(args.out_dir, args.departements)
```

> **Toutes les écritures** (`UPDATE`, `INSERT`) se font directement dans Supabase via la connexion `psycopg2` — identique en local et en Cloud Run. Il n'y a aucun fichier intermédiaire.
```

### Rapports de sortie

Le script génère dans `--out-dir` :

| Fichier | Contenu |
|---------|---------|
| `finess_65_etablissements.csv` | Tous les établissements ESSMS du département avec enrichissement |
| `finess_65_gestionnaires.csv` | Gestionnaires avec nb_essms, dirigeants, réseau, signaux |
| `finess_65_dirigeants.csv` | Dirigeants identifiés avec emails reconstitués |
| `finess_65_stats.json` | Statistiques globales (couverture, nb requêtes, durée) |
| `finess_65_errors.json` | Établissements en erreur avec détails |

---

## 15. Test local — Hautes-Pyrénées (65)

### Données attendues département 65

Le département des Hautes-Pyrénées (65) contient environ :
- ~120-150 établissements ESSMS
- ~30-40 gestionnaires (EJ)
- Principaux types : EHPAD, SSIAD, IME, ESAT, FAM, FH, MECS, CHRS

### Procédure de test (2 phases)

**Phase 1 — Ingestion locale (une seule fois)**

```bash
# 1. S'assurer que les fichiers FINESS sont présents
ls database/etalab-cs1100501-stock-*.csv
ls database/etalab-cs1100502-stock-*.csv
ls database/etalab-cs1100505-stock-*.csv

# 2. Créer les tables (exécuter le SQL de la section 3)
# Via psql ou l'éditeur SQL Supabase

# 3. Ingérer les données FINESS dans Supabase (LOCAL uniquement)
python scripts/ingest_finess.py \
    --departement 65 \
    --prospection-250 outputs/prospection_250_FINAL_FORMATE_V2.xlsx

# 4. Vérifier l'ingestion
# SELECT count(*) FROM finess_etablissement WHERE departement_code = '65';
# SELECT count(*) FROM finess_gestionnaire WHERE departement_code = '65';
# SELECT count(*) FROM finess_gestionnaire WHERE deja_prospecte_250 = TRUE;
```

**Phase 2 — Enrichissement (local ou Cloud Run — lit/écrit directement dans Supabase)**

```bash
# 5. Test dry-run (pas d'écriture en base)
python scripts/enrich_finess_dept.py \
    --departements 65 \
    --dry-run \
    --out-dir outputs/finess_test

# 6. Vérifier les rapports
cat outputs/finess_test/finess_65_stats.json

# 7. Si OK, lancer l'enrichissement complet (écriture directe dans Supabase)
python scripts/enrich_finess_dept.py \
    --departements 65 \
    --out-dir outputs/finess_65

# 8. Vérifier en base
# SELECT count(*) FROM finess_etablissement WHERE departement_code = '65' AND enrichissement_statut = 'enrichi';
# SELECT count(*) FROM finess_etablissement WHERE departement_code = '65' AND latitude IS NOT NULL;
# SELECT count(*) FROM finess_gestionnaire WHERE departement_code = '65' AND latitude IS NOT NULL;
# SELECT count(*) FROM finess_gestionnaire WHERE departement_code = '65' AND daf_nom IS NOT NULL;
# SELECT count(*) FROM finess_dirigeant fd JOIN finess_gestionnaire fg ON fd.id_gestionnaire = fg.id_gestionnaire WHERE fg.departement_code = '65';
```

### Résultats attendus (test 65)

| Métrique | Objectif |
|----------|----------|
| Établissements ingérés | ~120-150 |
| Catégories normalisées | 100% |
| Financeurs déduits | 100% |
| Géolocalisés (établissements) | >90% |
| Géolocalisés (sièges gestionnaires) | >90% |
| Sites web trouvés | >50% |
| Type de public qualifié (LLM) | >40% (des établissements avec site web) |
| Dirigeants trouvés | >30% des gestionnaires |
| DAF identifiés | >15% des gestionnaires |
| Emails reconstitués | >20% des dirigeants trouvés |
| Gestionnaires tagués "déjà prospectés" | Variable (croisement avec les 250) |
| Signaux de tension | Variable |

### Estimation des appels API (dept 65)

| API | Estimation | Coût |
|-----|-----------|------|
| Serper | ~1000-1500 requêtes (8/etab × 150 + 5/gest × 40 + 3/gest DAF × 40) | Crédits illimités |
| Gemini | ~400-500 appels (2-3/etab qualification + dirigeants + DAF + signaux) | Crédits illimités |
| Nominatim | ~190 requêtes (150 ET + 40 EJ) | Gratuit (rate limit 1/s) |

---

## 16. Déploiement Cloud Run Jobs

> **Architecture Cloud Run** : L'instance Cloud Run **lit les données brutes depuis Supabase** et **écrit les résultats enrichis directement dans Supabase**. Aucun fichier CSV n'est embarqué dans l'image Docker. L'ingestion CSV est une étape locale préalable (section 4). Le script `ingest_finess.py` n'est pas inclus dans l'image.

### Dockerfile

Réutilise le Dockerfile existant (`cloudrun_ref/Dockerfile`) avec le script d'enrichissement FINESS uniquement :

```dockerfile
# Ajouter dans le Dockerfile existant :
COPY scripts/enrich_finess_dept.py /app/scripts/enrich_finess_dept.py
COPY scripts/enrich_finess_config.py /app/scripts/enrich_finess_config.py

# NB : PAS de COPY des fichiers CSV ni de ingest_finess.py
# L'ingestion est une opération locale, PAS exécutée en Cloud Run.

# Ajouter geopy pour le géocodage Nominatim
# (dans requirements.txt ou directement) :
RUN pip install geopy>=2.4.0

# Nouveau entrypoint pour FINESS :
# ENTRYPOINT ["python", "scripts/enrich_finess_dept.py"]
```

### Job YAML (Cloud Run)

```yaml
# cloudrun_jobs/finess_job_65.yaml
apiVersion: run.googleapis.com/v1
kind: Job
metadata:
  name: finess-enrich-65
spec:
  template:
    spec:
      taskCount: 1
      template:
        spec:
          containers:
          - image: europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest
            command:
            - python
            args:
            - scripts/enrich_finess_dept.py
            - --departements
            - "65"
            - --out-dir
            - /tmp/outputs
            # Pas de --skip-ingestion : l'ingestion n'existe plus dans ce script
            # Le script lit directement depuis Supabase
            env:
            - name: PYTHONUNBUFFERED
              value: "1"
            - name: GEMINI_MODEL
              value: gemini-2.0-flash
            - name: DB_HOST
              value: db.minwoumfgutampcgrcbr.supabase.co
            - name: DB_NAME
              value: postgres
            - name: DB_USER
              value: postgres
            - name: DB_PORT
              value: "5432"
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: DB_PASSWORD
                  key: latest
            - name: SERPER_API_KEY
              valueFrom:
                secretKeyRef:
                  name: SERPER_API_KEY
                  key: latest
            - name: GEMINI_API_KEY
              valueFrom:
                secretKeyRef:
                  name: GEMINI_API_KEY
                  key: latest
            resources:
              limits:
                cpu: "2"
                memory: 1Gi
            timeoutSeconds: 3600  # 1 heure max
          maxRetries: 1
```

### Déploiement national (tous départements)

Pour traiter tous les départements français, on génère des jobs parallèles (~10 jobs couvrant chacun ~10 départements) :

```python
# scripts/generate_finess_cloudrun_jobs.py

DEPT_BATCHES = [
    "01,02,03,04,05,06,07,08,09,10",
    "11,12,13,14,15,16,17,18,19,20",
    "21,22,23,24,25,26,27,28,29,30",
    "31,32,33,34,35,36,37,38,39,40",
    "41,42,43,44,45,46,47,48,49,50",
    "51,52,53,54,55,56,57,58,59,60",
    "61,62,63,64,65,66,67,68,69,70",
    "71,72,73,74,75,76,77,78,79,80",
    "81,82,83,84,85,86,87,88,89,90",
    "91,92,93,94,95,2A,2B,971,972,973,974,976",
]
```

### Commandes de déploiement

```powershell
# Build et push de l'image Docker
docker build -f cloudrun_ref/Dockerfile -t europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest .
docker push europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest

# Créer le job Cloud Run (test dept 65)
# Note : le script lit/écrit directement dans Supabase — pas de CSV embarqué
gcloud run jobs create finess-enrich-65 --image europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest --region europe-west1 --command python --args "scripts/enrich_finess_dept.py,--departements,65,--out-dir,/tmp/outputs" --set-secrets DB_PASSWORD=DB_PASSWORD:latest,SERPER_API_KEY=SERPER_API_KEY:latest,GEMINI_API_KEY=GEMINI_API_KEY:latest --cpu 2 --memory 1Gi --max-retries 1 --task-timeout 3600

# Exécuter le job (enrichissement direct Supabase→Supabase)
gcloud run jobs execute finess-enrich-65 --region europe-west1
```

---

## 17. Stratégie de requêtes Serper (sans limitation)

### Budget requêtes par entité enrichie

| Entité | Requêtes Serper | Objectif |
|--------|----------------|----------|
| **Établissement (ET)** | 5-8 | Site web, public, actualités, contact |
| **Gestionnaire (EJ)** | 3-5 | Site siège, organigramme, LinkedIn, réseau, rapports |
| **Dirigeant** | 2-3 | LinkedIn nominatif, email |
| **Total estimé par département** | ~1000-2000 | Couverture exhaustive |
| **Total national (~45 000 ESSMS)** | ~300 000-400 000 | Budget illimité |

### Requêtes spécifiques par Type d'établissement

```python
# Requêtes supplémentaires pour EHPAD (beaucoup d'info en ligne)
EXTRA_QUERIES_EHPAD = [
    '"{nom}" tarif hébergement prix journée',
    '"{nom}" GIR PMP Pathos taux occupation',
    '"{nom}" avis familles résidents',
]

# Requêtes supplémentaires pour IME/ITEP (public spécifique)
EXTRA_QUERIES_HANDICAP_ENFANT = [
    '"{nom}" projet établissement autisme TSA déficience',
    '"{nom}" internat semi-internat SESSAD',
    '"{nom}" unité enseignement UEMA UEEA',
]

# Requêtes supplémentaires pour MECS/Protection enfance
EXTRA_QUERIES_PROTECTION_ENFANCE = [
    '"{nom}" habilitation justice ASE conseil départemental',
    '"{nom}" accueil urgence placement',
]
```

---

## 18. Prompts LLM détaillés

### Stratégie de prompts

Tous les prompts suivent le pattern éprouvé de `enrich_dept_prototype.py` :

1. **Rôle** : "Tu es un expert du secteur social et médico-social français"
2. **Contexte** : nom, catégorie, commune, département, texte web
3. **Tâche** : extraction structurée précise
4. **Format** : JSON strict, sans commentaire
5. **Garde-fous** : "Si absent, mettre null (pas d'invention)"

### Paramètres Gemini

```python
GEMINI_CONFIG = {
    "model": "gemini-2.0-flash",
    "temperature": 0.15,           # Bas pour extraction factuelle
    "max_output_tokens": 1200,     # Suffisant pour un JSON enrichi
    "timeout_s": 60,
    "max_retries": 3,
    "retry_backoff": 1.2,
}
```

### Détail des 4 prompts principaux

Voir section 8 ci-dessus pour les prompts complets :
- `PROMPT_QUALIFICATION_PUBLIC` — Type de public, accueil, 365j
- `PROMPT_EXTRACTION_DIRIGEANTS` — Dirigeants et fonctions
- `PROMPT_SIGNAUX_TENSION` — Actualités et signaux stratégiques
- `PROMPT_RESEAU_FEDERAL` — Réseau d'appartenance du gestionnaire

---

## 19. Gestion des erreurs et reprise

### Statuts d'enrichissement

Chaque établissement et gestionnaire a un `enrichissement_statut` :

| Statut | Signification | Action |
|--------|--------------|--------|
| `brut` | Données FINESS importées, pas d'enrichissement | À enrichir |
| `en_cours` | Enrichissement en cours ou interrompu | Reprendre |
| `enrichi` | Enrichissement terminé avec succès | Terminé |
| `erreur` | Erreur pendant l'enrichissement | Investiguer + relancer |

### Reprise après interruption

```python
def load_etabs_to_enrich(cur, departements: list[str], limit: int) -> list[dict]:
    """Charge les établissements à enrichir (exclut ceux déjà enrichis)."""
    dept_clause = "AND departement_code = ANY(%s)" if departements != ["all"] else ""
    limit_clause = f"LIMIT {limit}" if limit > 0 else ""
    
    cur.execute(f"""
        SELECT * FROM finess_etablissement
        WHERE enrichissement_statut IN ('brut', 'en_cours', 'erreur')
        {dept_clause}
        ORDER BY 
            CASE enrichissement_statut 
                WHEN 'en_cours' THEN 1  -- Reprendre en priorité
                WHEN 'erreur' THEN 2    -- Puis les erreurs
                WHEN 'brut' THEN 3      -- Puis les nouveaux
            END,
            id_finess
        {limit_clause}
    """, (departements,) if departements != ["all"] else ())
    
    return cur.fetchall()
```

### Log d'enrichissement

Chaque étape écrit dans `finess_enrichissement_log` pour traçabilité :

```python
def log_enrichissement(cur, id_finess: str, entite_type: str, etape: str, 
                       statut: str, details: dict, serper_reqs: int = 0, 
                       gemini_tokens: int = 0, duree_ms: int = 0):
    cur.execute("""
        INSERT INTO finess_enrichissement_log 
        (id_finess, entite_type, etape, statut, details, serper_requetes, gemini_tokens, duree_ms)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (id_finess, entite_type, etape, statut, 
          json.dumps(details, ensure_ascii=False), serper_reqs, gemini_tokens, duree_ms))
```

---

## 20. Monitoring et rapports

### Requêtes de suivi

```sql
-- Avancement global par département
SELECT departement_code, departement_nom,
    COUNT(*) AS total,
    SUM(CASE WHEN enrichissement_statut = 'enrichi' THEN 1 ELSE 0 END) AS enrichis,
    SUM(CASE WHEN enrichissement_statut = 'erreur' THEN 1 ELSE 0 END) AS erreurs,
    ROUND(100.0 * SUM(CASE WHEN enrichissement_statut = 'enrichi' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_enrichi
FROM finess_etablissement
GROUP BY departement_code, departement_nom
ORDER BY departement_code;

-- Couverture par type d'enrichissement
SELECT 
    COUNT(*) AS total_etabs,
    SUM(CASE WHEN site_web IS NOT NULL THEN 1 ELSE 0 END) AS avec_site_web,
    SUM(CASE WHEN type_public IS NOT NULL THEN 1 ELSE 0 END) AS avec_type_public,
    SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) AS geolocalisés,
    SUM(CASE WHEN ouverture_365 IS NOT NULL THEN 1 ELSE 0 END) AS avec_ouverture_365,
    SUM(CASE WHEN signaux_recents IS NOT NULL AND signaux_recents != '[]' THEN 1 ELSE 0 END) AS avec_signaux
FROM finess_etablissement
WHERE departement_code = '65';

-- Couverture géolocalisation et DAF par gestionnaire
SELECT fg.departement_code,
    COUNT(*) AS total_gestionnaires,
    SUM(CASE WHEN fg.latitude IS NOT NULL THEN 1 ELSE 0 END) AS sieges_geolocalisés,
    SUM(CASE WHEN fg.daf_nom IS NOT NULL THEN 1 ELSE 0 END) AS daf_identifies,
    SUM(CASE WHEN fg.deja_prospecte_250 THEN 1 ELSE 0 END) AS deja_prospectes_250
FROM finess_gestionnaire fg
WHERE fg.departement_code = '65'
GROUP BY fg.departement_code;

-- Dirigeants trouvés par département
SELECT fg.departement_code, fg.departement_nom,
    COUNT(DISTINCT fg.id_gestionnaire) AS gestionnaires,
    COUNT(DISTINCT fd.id) AS dirigeants_trouves,
    SUM(CASE WHEN fd.email_reconstitue IS NOT NULL THEN 1 ELSE 0 END) AS emails_reconstitues
FROM finess_gestionnaire fg
LEFT JOIN finess_dirigeant fd ON fg.id_gestionnaire = fd.id_gestionnaire
WHERE fg.departement_code = '65'
GROUP BY fg.departement_code, fg.departement_nom;

-- Consommation API
SELECT etape,
    SUM(serper_requetes) AS total_serper,
    SUM(gemini_tokens) AS total_gemini_tokens,
    COUNT(*) AS nb_appels,
    AVG(duree_ms) AS duree_moyenne_ms
FROM finess_enrichissement_log
WHERE date_execution > NOW() - INTERVAL '24 hours'
GROUP BY etape
ORDER BY etape;

-- Prospects prioritaires (croisements stratégiques)
SELECT fe.raison_sociale, fe.categorie_normalisee, fe.commune,
    fe.latitude, fe.longitude,
    fe.ouverture_365, fe.places_autorisees,
    fg.raison_sociale AS gestionnaire,
    fg.nb_essms, fg.signal_tension,
    fg.deja_prospecte_250,
    fg.daf_nom, fg.daf_prenom, fg.daf_email,
    fg.latitude AS siege_lat, fg.longitude AS siege_lon,
    fd.prenom || ' ' || fd.nom AS dirigeant,
    fd.fonction_normalisee, fd.email_reconstitue
FROM finess_etablissement fe
JOIN finess_gestionnaire fg ON fe.id_gestionnaire = fg.id_gestionnaire
LEFT JOIN finess_dirigeant fd ON fg.id_gestionnaire = fd.id_gestionnaire 
    AND fd.fonction_normalisee IN ('DG', 'Directeur', 'DSI', 'Directeur Innovation', 'DAF')
WHERE fe.ouverture_365 = TRUE
    AND fe.departement_code = '65'
ORDER BY fg.nb_essms DESC, fe.places_autorisees DESC;
```

---

## 21. Cohabitation avec les tables habitat intermédiaire

### Principe d'isolation

Les tables FINESS sont isolées des tables habitat intermédiaire :

| Tables Habitat (existantes — NE PAS TOUCHER) | Tables FINESS (nouvelles) |
|----------------------------------------------|---------------------------|
| `etablissements` | `finess_etablissement` |
| `categories` | — (catégories dans finess_etablissement) |
| `sous_categories` | — |
| `services` | — |
| `propositions` | `finess_enrichissement_log` |
| `proposition_items` | — |
| `departements` | — (dans finess_etablissement) |

### Aucune FK croisée

Les tables `finess_*` n'ont **aucune clé étrangère** vers les tables habitat existantes. Cela garantit :
- Aucun risque de conflit de données
- Aucun impact sur les performances des tables habitat
- Migration/suppression indépendante possible

### Connexion partagée 

Les deux jeux de tables partagent la même base Supabase (`postgres` sur `db.minwoumfgutampcgrcbr.supabase.co`) et les mêmes credentials. Le `DatabaseManager` existant est réutilisé tel quel.

---

## Annexe A — Référentiel ESSMS par public

### Enfants / Adolescents — Handicap mental
| Structure | Public | Âge typique | Ouverture |
|-----------|--------|-------------|-----------|
| IME | Déficience intellectuelle | 6-14 ans | Semi-internat / Internat |
| IMPRO | Déficience intellectuelle (formation pro) | 14-20 ans | Semi-internat / Internat |
| SESSAD | Tous handicaps (ambulatoire) | 0-20 ans | Ambulatoire |
| CAMSP | Dépistage précoce | 0-6 ans | Ambulatoire |
| ITEP | Troubles du comportement | 6-20 ans | Internat / Semi-internat |

### Adultes — Handicap
| Structure | Public | Âge typique | Ouverture |
|-----------|--------|-------------|-----------|
| MAS | Polyhandicap lourd | 18-60+ | 365j |
| FAM | Handicap avec soins | 18-60+ | 365j |
| FH | Travailleurs handicapés (ESAT) | 18-60 | Semaine / 365j |
| FV | Handicap modéré (vie sociale) | 18-60+ | 365j |
| ESAT | Travail adapté | 18-60 | Semaine |
| SAVS | Accompagnement vie sociale | 18+ | Ambulatoire |
| SAMSAH | Accompagnement médico-social | 18+ | Ambulatoire |

### Personnes âgées
| Structure | Public | Âge typique | Ouverture |
|-----------|--------|-------------|-----------|
| EHPAD | PA dépendantes | 60+ | 365j |
| USLD | PA très dépendantes (soins lourds) | 60+ | 365j |
| SSIAD | PA à domicile (soins) | 60+ | Ambulatoire |
| Résidence Autonomie | PA autonomes | 60+ | 365j |
| SPASAD | PA à domicile (soins + aide) | 60+ | Ambulatoire |

### Protection de l'enfance
| Structure | Public | Âge typique | Ouverture |
|-----------|--------|-------------|-----------|
| MECS | Enfants placés ASE | 3-18 | 365j |
| Foyer de l'Enfance | Accueil d'urgence enfants | 0-18 | 365j |
| AEMO | Suivi en milieu ouvert | 0-18 | Ambulatoire |
| Pouponnière | Nourrissons placés | 0-3 | 365j |

### Addictologie
| Structure | Public | Âge typique | Ouverture |
|-----------|--------|-------------|-----------|
| CSAPA | Usagers dépendants (alcool, drogues, jeux) | 16+ | Ambulatoire |
| CAARUD | Réduction des risques (drogues) | 16+ | Ambulatoire |

### Insertion / Précarité
| Structure | Public | Âge typique | Ouverture |
|-----------|--------|-------------|-----------|
| CHRS | Personnes en difficulté sociale | 18+ | 365j / Semaine |
| CHU | Hébergement d'urgence | 18+ | 365j |
| LHSS | Sans-abri nécessitant des soins | 18+ | 365j |
| LAM | Sans-abri pathologies lourdes | 18+ | 365j |

---

## Annexe B — Commandes SQL création des tables

```sql
-- Exécuter dans l'éditeur SQL Supabase ou via psql

-- 1. Table gestionnaire
CREATE TABLE IF NOT EXISTS finess_gestionnaire (
    id_gestionnaire TEXT PRIMARY KEY,
    siren TEXT,
    raison_sociale TEXT NOT NULL,
    sigle TEXT,
    forme_juridique_code TEXT,
    forme_juridique_libelle TEXT,
    reseau_federal TEXT,
    adresse_numero TEXT,
    adresse_type_voie TEXT,
    adresse_lib_voie TEXT,
    adresse_complement TEXT,
    adresse_complete TEXT,
    code_postal TEXT,
    commune TEXT,
    departement_code TEXT,
    departement_nom TEXT,
    region TEXT,
    telephone TEXT,
    site_web TEXT,
    domaine_mail TEXT,
    structure_mail TEXT,
    linkedin_url TEXT,
    nb_etablissements INTEGER DEFAULT 0,
    nb_essms INTEGER DEFAULT 0,
    budget_consolide_estime NUMERIC,
    categorie_taille TEXT,
    dominante_type TEXT,
    secteur_activite_principal TEXT,
    signal_tension BOOLEAN DEFAULT FALSE,
    signal_tension_detail TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    geocode_precision TEXT,
    daf_nom TEXT,
    daf_prenom TEXT,
    daf_email TEXT,
    daf_telephone TEXT,
    daf_linkedin_url TEXT,
    daf_source TEXT,
    daf_confiance TEXT DEFAULT 'moyenne',
    deja_prospecte_250 BOOLEAN DEFAULT FALSE,
    deja_prospecte_250_date TIMESTAMP,
    date_ingestion TIMESTAMP DEFAULT NOW(),
    date_enrichissement TIMESTAMP,
    source_enrichissement TEXT,
    enrichissement_statut TEXT DEFAULT 'brut',
    enrichissement_log JSONB,
    CONSTRAINT finess_gestionnaire_statut_check CHECK (enrichissement_statut IN ('brut','en_cours','enrichi','erreur'))
);
CREATE INDEX IF NOT EXISTS idx_finess_gest_dept ON finess_gestionnaire(departement_code);
CREATE INDEX IF NOT EXISTS idx_finess_gest_statut ON finess_gestionnaire(enrichissement_statut);
CREATE INDEX IF NOT EXISTS idx_finess_gest_taille ON finess_gestionnaire(categorie_taille);
CREATE INDEX IF NOT EXISTS idx_finess_gest_prospecte ON finess_gestionnaire(deja_prospecte_250) WHERE deja_prospecte_250 = TRUE;
CREATE INDEX IF NOT EXISTS idx_finess_gest_geo ON finess_gestionnaire(latitude, longitude) WHERE latitude IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_finess_gest_secteur ON finess_gestionnaire(secteur_activite_principal);

-- 2. Table établissement
CREATE TABLE IF NOT EXISTS finess_etablissement (
    id_finess TEXT PRIMARY KEY,
    id_gestionnaire TEXT REFERENCES finess_gestionnaire(id_gestionnaire),
    raison_sociale TEXT,
    sigle TEXT,
    categorie_code TEXT,
    categorie_libelle TEXT,
    categorie_normalisee TEXT,
    groupe_code TEXT,
    groupe_libelle TEXT,
    secteur_activite TEXT,
    type_public TEXT,
    type_public_synonymes TEXT[],
    pathologies_specifiques TEXT[],
    tranches_age TEXT,
    age_min INTEGER,
    age_max INTEGER,
    type_accueil TEXT[],
    periode_ouverture TEXT,
    ouverture_365 BOOLEAN,
    places_autorisees INTEGER,
    places_installees INTEGER,
    taux_occupation NUMERIC,
    financeur_principal TEXT,
    financeur_secondaire TEXT,
    type_tarification TEXT,
    cpom BOOLEAN,
    cpom_date_echeance DATE,
    adresse_numero TEXT,
    adresse_type_voie TEXT,
    adresse_lib_voie TEXT,
    adresse_complement TEXT,
    adresse_complete TEXT,
    code_postal TEXT,
    commune TEXT,
    departement_code TEXT,
    departement_nom TEXT,
    region TEXT,
    telephone TEXT,
    email TEXT,
    site_web TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    geocode_precision TEXT,
    zone_dotation TEXT,
    signaux_recents JSONB,
    date_ingestion TIMESTAMP DEFAULT NOW(),
    date_enrichissement TIMESTAMP,
    source_enrichissement TEXT,
    enrichissement_statut TEXT DEFAULT 'brut',
    enrichissement_log JSONB,
    CONSTRAINT finess_etab_statut_check CHECK (enrichissement_statut IN ('brut','en_cours','enrichi','erreur'))
);
CREATE INDEX IF NOT EXISTS idx_finess_etab_gest ON finess_etablissement(id_gestionnaire);
CREATE INDEX IF NOT EXISTS idx_finess_etab_dept ON finess_etablissement(departement_code);
CREATE INDEX IF NOT EXISTS idx_finess_etab_cat ON finess_etablissement(categorie_normalisee);
CREATE INDEX IF NOT EXISTS idx_finess_etab_statut ON finess_etablissement(enrichissement_statut);
CREATE INDEX IF NOT EXISTS idx_finess_etab_365 ON finess_etablissement(ouverture_365) WHERE ouverture_365 = TRUE;
CREATE INDEX IF NOT EXISTS idx_finess_etab_secteur ON finess_etablissement(secteur_activite);

-- 3. Table dirigeant
CREATE TABLE IF NOT EXISTS finess_dirigeant (
    id SERIAL PRIMARY KEY,
    id_gestionnaire TEXT REFERENCES finess_gestionnaire(id_gestionnaire),
    id_finess_etablissement TEXT REFERENCES finess_etablissement(id_finess),
    civilite TEXT,
    nom TEXT,
    prenom TEXT,
    fonction_brute TEXT,
    fonction_normalisee TEXT,
    email_reconstitue TEXT,
    email_verifie BOOLEAN DEFAULT FALSE,
    email_organisation TEXT,
    telephone_direct TEXT,
    linkedin_url TEXT,
    source_url TEXT,
    source_type TEXT,
    confiance TEXT DEFAULT 'moyenne',
    date_enrichissement TIMESTAMP DEFAULT NOW(),
    CONSTRAINT finess_dirigeant_confiance_check CHECK (confiance IN ('haute','moyenne','basse'))
);
CREATE INDEX IF NOT EXISTS idx_finess_dir_gest ON finess_dirigeant(id_gestionnaire);
CREATE INDEX IF NOT EXISTS idx_finess_dir_etab ON finess_dirigeant(id_finess_etablissement);
CREATE INDEX IF NOT EXISTS idx_finess_dir_fonction ON finess_dirigeant(fonction_normalisee);

-- 4. Table log d'enrichissement
CREATE TABLE IF NOT EXISTS finess_enrichissement_log (
    id SERIAL PRIMARY KEY,
    id_finess TEXT,
    entite_type TEXT,
    etape TEXT,
    statut TEXT,
    details JSONB,
    serper_requetes INTEGER DEFAULT 0,
    gemini_tokens INTEGER DEFAULT 0,
    duree_ms INTEGER,
    date_execution TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_finess_log_finess ON finess_enrichissement_log(id_finess);
CREATE INDEX IF NOT EXISTS idx_finess_log_etape ON finess_enrichissement_log(etape);
CREATE INDEX IF NOT EXISTS idx_finess_log_statut ON finess_enrichissement_log(statut);

-- 5. Table cache Serper
CREATE TABLE IF NOT EXISTS finess_cache_serper (
    id SERIAL PRIMARY KEY,
    query_hash TEXT UNIQUE NOT NULL,
    query_text TEXT,
    results JSONB,
    nb_results INTEGER,
    date_requete TIMESTAMP DEFAULT NOW(),
    expire_at TIMESTAMP DEFAULT (NOW() + INTERVAL '30 days')
);
CREATE INDEX IF NOT EXISTS idx_finess_cache_hash ON finess_cache_serper(query_hash);
CREATE INDEX IF NOT EXISTS idx_finess_cache_expire ON finess_cache_serper(expire_at);
```

---

## Annexe C — Fichiers à créer / modifier

| Fichier | Action | Rôle | Exécution |
|---------|--------|------|-----------|
| `scripts/ingest_finess.py` | **Créer** | Ingestion CSV FINESS → tables Supabase + tag 250 contacts | **LOCAL uniquement** |
| `scripts/enrich_finess_dept.py` | **Créer** | Script principal d'enrichissement par département (lit/écrit Supabase) | Local + **Cloud Run** |
| `scripts/enrich_finess_config.py` | **Créer** | Constantes métier (financeurs, catégories, prompts) | Local + Cloud Run |
| `cloudrun_ref/Dockerfile` | **Modifier** | Ajouter COPY `enrich_finess_dept.py` + `enrich_finess_config.py` (PAS `ingest_finess.py`) | Docker build |
| `cloudrun_ref/requirements.txt` | **Modifier** | Ajouter `geopy>=2.4.0` pour le géocodage Nominatim | Docker build |
| `cloudrun_jobs/finess_job_65.yaml` | **Créer** | Job Cloud Run test Hautes-Pyrénées | Cloud Run |
| `cloudrun_jobs/finess_job_XX.yaml` | **Créer** | Jobs Cloud Run par batch de départements | Cloud Run |

> **Rappel architecture** : Les fichiers CSV FINESS et le script `ingest_finess.py` ne sont **jamais** copiés dans l'image Docker. L'ingestion est une opération locale unique. Cloud Run ne fait que lire et écrire dans Supabase.

---

*Document ConfidensIA — Usage interne — Mars 2026*
