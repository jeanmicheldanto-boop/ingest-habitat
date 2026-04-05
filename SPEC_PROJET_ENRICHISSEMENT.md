# 🎯 PROJET ENRICHISSEMENT - SPÉCIFICATIONS TECHNIQUES COMPLÈTES

## 📊 ÉTAT ACTUEL DE LA BASE (30/01/2026 - APRÈS CORRECTIONS)

### Vue d'ensemble
- **Total**: 3,325 établissements réels (0 test)
- **Types**: 2,095 résidences | 567 habitat_partage | 97 logement_independant | 36 non défini
- **Statut éditorial**: 2,743 publiés | 52 draft
- **Éligibilité**: 1,742 à_verifier | 1,031 non_eligible | 22 avp_eligible

### ✅ CORRECTIONS APPLIQUÉES (Janvier 2026)

#### Phase 1 : Quality Check & Corrections Automatiques

**Pipeline de correction exécuté avec succès** - 3 passes successives :

1. **Départements normalisés** : ✅ 2,923 corrections
   - Format imposé : "Nom Département (XX)"
   - "01" → "Ain (01)"
   - "Seine-et-Marne" → "Seine-et-Marne (77)"
   - "Département (57)" → "Moselle (57)"
   - **Résultat** : 100% des départements au bon format (0 problème restant)

2. **Codes postaux** : ✅ 93 corrections
   - Ajout zéros manquants départements 01-09
   - "4200" → "04200"
   - **Résultat** : 0 codes à 4 chiffres
   - **Restant** : 191 codes NULL ou invalides (nécessitent validation manuelle)

3. **Emails nettoyés** : ✅ 9 corrections
   - Suppression préfixes numériques suspects
   - "00contact@..." → "contact@..."
   - **Résultat** : 1 email suspect restant (validation manuelle)

4. **Sous-catégories fusionnées** : ✅ 1,812 établissements migrés
   - 10 paires de doublons fusionnées
   - Format cible : Majuscules + accents ("Résidence autonomie")
   - **Résultat** : 0 doublons restants

5. **Géolocalisations ajoutées** : ✅ 636 établissements géolocalisés
   - Via Google Geocoding API (100% succès)
   - Stratégie : "nom établissement, commune, France" si pas d'adresse
   - Précision : rooftop, range_interpolated, locality
   - **Résultat** : 100% des établissements géolocalisés (0 manquant)

**Logs des corrections** :
- LOG_CORRECTIONS_20260130_183940.md (passe 1)
- LOG_CORRECTIONS_20260130_185057.md (passe 2)
- LOG_CORRECTIONS_20260130_185154.md (passe 3)

**Rapport quality check actualisé** : RAPPORT_QUALITY_CHECK.md

---

## 📋 TÂCHES RESTANTES - FEUILLE DE ROUTE

### ✅ COMPLÉTÉ (Janvier 2026)
- [x] Normalisation départements : 100% au format "Nom (XX)"
- [x] Codes postaux : Zéros manquants corrigés pour depts 01-09
- [x] Emails : Préfixes numériques nettoyés
- [x] Sous-catégories : Doublons fusionnés (1,812 migrations)
- [x] Géolocalisation : 100% des établissements géolocalisés via Google Maps API

### 🔴 PRIORITÉ HAUTE (En attente)

#### 1. Validation sites web (565 sites suspects)
- **Cible** : Habitat inclusif, habitat partagé, logement indépendant uniquement
- **Méthode** : Groq LLM avec extraction snippet page
- **Exclusion** : Résidence autonomie & Résidence services seniors (source gouvernementale fiable)
- **Durée estimée** : ~2h pour 565 sites
- **Sources fiables identifiées** (19) : agesetvie.com, autisme06, udaf, habitat-humanisme, unapei, adapei, ensemble2generations, coeur-de-vie, residences-commetoit, senioriales, soliha, cettefamille, espoir35, vivre-devenir, adimc35, cosima, associationbatir, vitalliance, pour-les-personnes-agees.gouv.fr

#### 2. Enrichissement TARIFS (9% → 60%)
- **Objectif** : Passer de 259 à ~1,995 établissements avec tarifs
- **Méthode** : ScrapingBee (pages tarifs) + Groq LLM (extraction structurée)
- **Données** : prix_min, prix_max, loyer_base, charges, fourchette_prix
- **Formule fourchette** : <750€=euro, 750-1500€=deux_euros, >1500€=trois_euros
- **Test** : Département 76 (Seine-Maritime) avant déploiement complet

#### 3. Enrichissement SERVICES (34% → 80%, GARDER 6 types)
- **Important** : L'utilisateur a précisé "je ne cherche à en récupérer que 6"
- **NE PAS étendre** à 30+ services comme proposé initialement
- **6 services actuels** : activités organisées, commerces à pied, conciergerie, espace_partage, médecin intervenant, personnel de nuit
- **Objectif** : Améliorer couverture de ces 6 services (952 → ~2,660 établissements)
- **Méthode** : ScrapingBee + Groq LLM sur pages "Services" / "Prestations"

### 🟡 PRIORITÉ MOYENNE

#### 4. Codes postaux NULL (191 restants)
- **Situation** : 191 établissements avec code_postal NULL ou invalide
- **Action** : Validation manuelle ou extraction depuis adresse complète
- **Méthode** : Regex sur adresse_l1/adresse_l2 ou API BAN (Base Adresse Nationale)

#### 5. Email suspect (1 restant)
- **Action** : Vérification manuelle de l'email avec préfixe numérique restant

#### 6. Incohérences habitat_type (38 cas)
- **Problème** : habitat_type ne correspond pas aux sous-catégories associées
- **Exemple** : habitat_type='residence' mais sous_categorie='Habitat inclusif'
- **Action** : Analyse manuelle + correction ciblée

### 🟢 PRIORITÉ BASSE (Optionnel)

#### 7. Enrichissement LOGEMENTS surfaces (0.1% → 40%)
- **Défi** : Surfaces difficiles à extraire (souvent dans PDFs ou plans)
- **Méthode** : Groq Vision sur images + ScrapingBee
- **Coût** : Élevé en ressources, à évaluer selon budget

#### 8. Enrichissement RESTAURATIONS (détails supplémentaires)
- **Actuellement** : 100% avec données basiques (4 booléens)
- **Extension possible** : Tarifs repas, types cuisine, horaires
- **Nécessite** : Extension du schéma base de données

### 📅 STRATÉGIE DE DÉPLOIEMENT

1. **Phase 1** : Test sur département 76 (Seine-Maritime)
   - Exécuter tâches priorité haute (sites web + tarifs + services)
   - Validation manuelle échantillon 10-20 résultats
   - Ajustements des prompts LLM si nécessaire

2. **Phase 2** : Déploiement progressif par régions
   - Batch de 5-10 départements à la fois
   - Monitoring taux succès / qualité données
   - Logs détaillés pour chaque batch

3. **Phase 3** : Couverture nationale complète
   - Traitement des 3,325 établissements
   - Génération rapport final avec métriques

4. **Phase 4** : Tâches priorité basse (selon budget/temps disponible)

---

## 🗄️ SCHÉMA BASE DE DONNÉES - CONTRAINTES & ATTENDUS

### 📋 Table `etablissements` (principale)

#### ENUM Types définis dans PostgreSQL

```sql
-- habitat_type (OBLIGATOIRE)
CREATE TYPE habitat_type AS ENUM (
    'logement_independant',
    'residence',
    'habitat_partage'
);

-- eligibilite_statut
CREATE TYPE eligibilite_statut AS ENUM (
    'avp_eligible',
    'non_eligible',
    'a_verifier'
);

-- statut_editorial
CREATE TYPE statut_editorial AS ENUM (
    'draft',
    'soumis',
    'valide',
    'publie',
    'archive'
);

-- geocode_precision
CREATE TYPE geocode_precision AS ENUM (
    'rooftop',
    'range_interpolated',
    'street',
    'locality',
    'unknown'
);
```

#### Champs avec contraintes

| Champ | Type | Contraintes | Validation |
|-------|------|-------------|------------|
| **id** | UUID | PRIMARY KEY, auto-généré | `gen_random_uuid()` |
| **nom** | text | NOT NULL | Requis |
| **habitat_type** | habitat_type | DEFAULT 'residence' | ENUM: logement_independant/residence/habitat_partage |
| **eligibilite_statut** | eligibilite_statut | NULL | ENUM: avp_eligible/non_eligible/a_verifier |
| **statut_editorial** | statut_editorial | DEFAULT 'draft' | ENUM + can_publish() |
| **departement** | text | NULL | **Format: "Seine-et-Marne (77)"** ✅ 100% normalisés |
| **code_postal** | text | NULL | **5 chiffres, commence par 0 si dept 01-09** ⚠️ 191 NULL restants |
| **email** | text | NULL | Regex: `^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$` |
| **geom** | geometry(Point,4326) | NULL | PostGIS, proj EPSG:4326 | ✅ 100% géolocalisés (636 ajoutés)
| **geocode_precision** | geocode_precision | NULL | ENUM | ✅ Toutes précisions renseignées |
| **confiance_score** | double precision | CHECK 0-1 | Entre 0.0 et 1.0 |
| **is_test** | boolean | DEFAULT false NOT NULL | Booléen |
| **created_at** | timestamptz | DEFAULT now() NOT NULL | Auto |
| **updated_at** | timestamptz | DEFAULT now() NOT NULL | Trigger auto |

#### Fonction de validation publication
```sql
-- Un établissement peut être publié SEULEMENT SI:
CREATE FUNCTION can_publish(p_etab uuid) RETURNS boolean AS $$
  -- 1) nom NON NULL
  -- 2) adresse_l1 OU adresse_l2 NON NULL
  -- 3) commune NON NULL
  -- 4) code_postal NON NULL
  -- 5) geom (géolocalisation) NON NULL
  -- 6) gestionnaire NON NULL
  -- 7) habitat_type NON NULL OU au moins une sous-catégorie
  -- 8) email au bon format regex
$$;
```

---

### 💰 Table `tarifications`

#### ENUM fourchette_prix
```sql
CREATE TYPE fourchette_prix AS ENUM (
    'euro',      -- < 750€/mois
    'deux_euros', -- 750-1500€/mois
    'trois_euros' -- > 1500€/mois
);
```

#### Seuils de tarification (trouvés dans `app.py` ligne 405-407)
```python
if prix < 750:
    fourchette_prix = "euro"
elif 750 <= prix <= 1500:
    fourchette_prix = "deux_euros"
else:
    fourchette_prix = "trois_euros"
```

#### Structure table
| Champ | Type | Contraintes | Validation |
|-------|------|-------------|------------|
| **id** | UUID | PRIMARY KEY | gen_random_uuid() |
| **etablissement_id** | UUID | NOT NULL, FK → etablissements | ON DELETE CASCADE |
| **logements_type_id** | UUID | NULL, FK → logements_types | ON DELETE SET NULL |
| **periode** | text | NULL | 'mois', 'semaine', 'jour' |
| **fourchette_prix** | fourchette_prix | NULL | ENUM |
| **prix_min** | numeric | NULL | ≥ 0 |
| **prix_max** | numeric | NULL | ≥ prix_min |
| **loyer_base** | numeric | NULL | Loyer hors charges |
| **charges** | numeric | NULL | Charges seules |
| **devise** | text | DEFAULT 'EUR' | EUR |
| **source** | text | NULL | URL source tarif |
| **date_observation** | date | NULL | Date extraction |

**⚠️ État actuel**: 259/2,795 établissements (9.3%) ont des tarifs

---

### 🏠 Table `logements_types`

| Champ | Type | Contraintes | Validation |
|-------|------|-------------|------------|
| **id** | UUID | PRIMARY KEY | gen_random_uuid() |
| **etablissement_id** | UUID | NOT NULL, FK → etablissements | ON DELETE CASCADE |
| **libelle** | text | NULL | Type logement (T1, T2, Studio, etc.) |
| **surface_min** | numeric | NULL | m² |
| **surface_max** | numeric | NULL | m² |
| **meuble** | boolean | NULL | true/false |
| **pmr** | boolean | NULL | PMR accessible |
| **domotique** | boolean | NULL | Équipé domotique |
| **nb_unites** | integer | NULL | Nombre unités disponibles |
| **plain_pied** | boolean | DEFAULT false NOT NULL | Plain-pied |

**⚠️ État actuel**: 
- 882/2,795 établissements (31.6%) ont des types logements
- Surfaces: 0.1% seulement !
- PMR: 8.2%, Domotique: 0%, Meublé: 0.05%

---

### 🍽️ Table `restaurations`

| Champ | Type | Contraintes | Validation |
|-------|------|-------------|------------|
| **id** | UUID | PRIMARY KEY | gen_random_uuid() |
| **etablissement_id** | UUID | NOT NULL, FK, UNIQUE | ON DELETE CASCADE, 1 seul par établissement |
| **kitchenette** | boolean | DEFAULT false NOT NULL | Kitchenette dans logement |
| **resto_collectif_midi** | boolean | DEFAULT false NOT NULL | Resto collectif midi |
| **resto_collectif** | boolean | DEFAULT false NOT NULL | Resto collectif général |
| **portage_repas** | boolean | DEFAULT false NOT NULL | Service portage repas |
| **created_at** | timestamptz | DEFAULT now() NOT NULL | Auto |
| **updated_at** | timestamptz | DEFAULT now() NOT NULL | Trigger auto |

**⚠️ État actuel**: 2,794/2,795 établissements (100%) ont une entrée, mais données basiques

---

### 🛎️ Table `services` + `etablissement_service`

#### Table `services` (référentiel)
| Champ | Type | Contraintes |
|-------|------|-------------|
| **id** | UUID | PRIMARY KEY, gen_random_uuid() |
| **libelle** | text | NOT NULL, UNIQUE |

#### Services existants (6 actuellement - ⚠️ **ENRICHISSEMENT À FAIRE**)
```
• activités organisées
• commerces à pied
• conciergerie
• espace_partage
• médecin intervenant
• personnel de nuit
```

**⚠️ TÂCHE PRIORITAIRE** : L'utilisateur a précisé "je ne cherche à en récupérer que 6" - garder ces 6 types uniquement, ne PAS étendre à 30+ services.

#### Table de liaison `etablissement_service` (many-to-many)
| Champ | Type | Contraintes |
|-------|------|-------------|
| **etablissement_id** | UUID | NOT NULL, FK → etablissements, PRIMARY KEY(1/2) |
| **service_id** | UUID | NOT NULL, FK → services, PRIMARY KEY(2/2) |

**Relations**: ON DELETE CASCADE sur les 2 FKs

**⚠️ État actuel**: 952/2,795 établissements (34.1%) ont des services

---

### 🏷️ Table `sous_categories` + `etablissement_sous_categorie`

#### Table `sous_categories` (référentiel)
| Champ | Type | Contraintes |
|-------|------|-------------|
| **id** | UUID | PRIMARY KEY, gen_random_uuid() |
| **categorie_id** | UUID | NOT NULL, FK → categories |
| **libelle** | text | NOT NULL |
| **alias** | text | NULL |

#### Sous-catégories existantes (10 - ✅ **DOUBLONS FUSIONNÉS**)
```
✅ Format normalisé (Majuscules + accents):
  • Résidence autonomie
  • Résidence services seniors
  • Habitat inclusif
  • Habitat intergénérationnel
  • Colocation avec services
  • Maison d'accueil familial
  • Accueil familial
  • Béguinage
  • MARPA
  • Village seniors

❌ Anciens formats supprimés:
  • residence_autonomie → fusionné
  • residence_services_seniors → fusionné
  • habitat_inclusif → fusionné
  • habitat_intergenerationnel → fusionné
  • colocation_avec_services → fusionné
  • maison_accueil_familial → fusionné
  • beguinage → fusionné
  • marpa → fusionné
  • village_seniors → fusionné

**Note** : 1,812 établissements ont été migrés automatiquement vers le format normalisé.
```

#### Table de liaison `etablissement_sous_categorie` (many-to-many)
| Champ | Type | Contraintes |
|-------|------|-------------|
| **etablissement_id** | UUID | NOT NULL, FK → etablissements, PRIMARY KEY(1/2) |
| **sous_categorie_id** | UUID | NOT NULL, FK → sous_categories, PRIMARY KEY(2/2) |

**Relations**: ON DELETE CASCADE sur les 2 FKs

**⚠️ État actuel**: 2,786/2,795 établissements (99.7%) ont des sous-catégories

---

## 🔗 MAPPING HABITAT_TYPE ↔ SOUS_CATEGORIES

### Règles logiques attendues

| habitat_type | Sous-catégories compatibles |
|--------------|----------------------------|
| **residence** | residence_autonomie, residence_services_seniors, marpa, village_seniors |
| **habitat_partage** | habitat_inclusif, colocation_avec_services, habitat_intergenerationnel, maison_accueil_familial, accueil_familial |
| **logement_independant** | beguinage, logement_adapte |

### ⚠️ Vérifications à faire dans quality check
- Détecter incohérences (ex: habitat_type=residence mais sous_categorie=habitat_inclusif)
- Proposer corrections automatiques
- Logger les cas ambigus

---

## 🔍 PROBLÈMES IDENTIFIÉS - ÉTAT APRÈS CORRECTIONS

### 1. ✅ DÉPARTEMENTS - CORRIGÉ (2,923 corrections)

#### Format IMPOSÉ (respecté à 100%)
```
"Seine-et-Marne (77)"
"Nord (59)"
"Ain (01)"
"Corse (20)"
```

#### Formats INCORRECTS corrigés
```
✅ "77" → "Seine-et-Marne (77)" (code seul)
✅ "Seine-et-Marne" → "Seine-et-Marne (77)" (nom seul)
✅ "Département (57)" → "Moselle (57)" (format générique)
```

**Résultat** : 0 problème restant

---

### 2. ✅ CODES POSTAUX - PARTIELLEMENT CORRIGÉ (93 corrections)

#### Zéros manquants corrigés
```
✅ "1000" → "01000" (Ain)
✅ "2100" → "02100" (Aisne)
✅ "7300" → "07300" (Ardèche)
```

#### ⚠️ Restant à traiter : 191 codes NULL/invalides
- Nécessitent validation manuelle ou extraction via API BAN

---

### 3. ✅ EMAILS - CORRIGÉ (9 corrections)

#### Préfixes suspects supprimés
```
✅ "00contact@cettefamille.com" → "contact@cettefamille.com"
```

#### ⚠️ Restant : 1 email suspect
- Nécessite vérification manuelle

---

### 4. ✅ SOUS-CATÉGORIES - CORRIGÉ (1,812 migrations)

### 4. ✅ SOUS-CATÉGORIES - CORRIGÉ (1,812 migrations)

#### Normalisation appliquée
Tout migré vers format "Majuscules + accents"
```
✅ residence_autonomie → Résidence autonomie
✅ habitat_inclusif → Habitat inclusif
✅ marpa → MARPA
✅ beguinage → Béguinage
```

**Résultat** : 0 doublon restant, 10 sous-catégories uniques

---

### 5. ✅ GÉOLOCALISATION - CORRIGÉ (636 ajouts)

#### Google Geocoding API utilisée
```
✅ 550 géolocalisations (passe 1)
✅ 86 géolocalisations (passe 2)
```

#### Stratégie appliquée
- Si adresse : "adresse_l1, commune, France"
- Si pas d'adresse : "nom établissement, commune, France"
- Précisions : rooftop, range_interpolated, locality, street

**Résultat** : 100% des établissements géolocalisés (0 manquant)

---

### 6. ⚠️ SITES WEB - À TRAITER (565 suspects)

#### Sites à VALIDER (habitat inclusif, habitat partagé, logement indépendant)

**Sites agrégateurs à EXCLURE**:
```
❌ essentiel-autonomie.com
❌ papyhappy.fr
❌ retraite.com
❌ annuairessehpad.com
❌ pour-les-personnes-agees.gouv.fr (si pas RSS/RA)
```

**Sites suspects**:
```
❌ Agences immobilières
❌ Portails génériques
❌ Sites qui ne mentionnent pas l'établissement spécifique
```

#### Validation avec Groq LLM
```python
# Prompt validation site
"""
Site web: {site_web}
Nom établissement: {nom}
Commune: {commune}

Questions:
1. Ce site parle-t-il spécifiquement de cet établissement ?
2. Est-ce un site agrégateur/annuaire ?
3. Est-ce le site officiel de l'établissement ou de son gestionnaire ?

Répondre JSON: {"est_valide": bool, "raison": "..."}
"""
```

#### ✅ Sites FIABLES (à NE PAS valider)
- Source = "pour-les-personnes-agees.gouv.fr" ET sous_categorie IN (residence_autonomie, residence_services_seniors)
- Domaines gestionnaires connus: domitys.fr, girandieres.com, jardins-arcadie.fr, domusvi.com, colisee.fr, etc.

---

### 6. 🟠 GÉOLOCALISATION - 19% manquants

#### État actuel
- 2,253/2,795 établissements (80.6%) ont une géoloc
- 542 établissements (19.4%) SANS géoloc

#### 🛠️ Outils utilisés
1. **Google Maps Geocoding API** (636 ajouts avec 100% succès)
   - Stratégie : "nom, commune, France" si pas d'adresse
   - Précisions obtenues : rooftop, range_interpolated, locality

**✅ Résultat final** : 100% géolocalisés (0 manquant)

#### Format stockage
```sql
geom = ST_GeomFromText('POINT(longitude latitude)', 4326)
geocode_precision = 'rooftop' | 'street' | 'locality' | 'unknown'
```

---

## 📝 QUALITY CHECK - ACTIONS DÉTAILLÉES

### Phase 1: Normalisation (sans API, rapide ~2min)

1. **Départements**
   - Normaliser format → "Nom (XX)"
   - Détecter erreurs commune/département
   - Mapper codes → noms complets

2. **Codes postaux**
   - Ajouter zéros manquants (dept 01-09)
   - Valider format 5 chiffres
   - Vérifier cohérence avec département

3. **Emails**
   - Nettoyer préfixes suspects
   - Valider regex format
   - Détecter domaines suspects

4. **Téléphones**
   - Normaliser format unique
   - Options: `01 23 45 67 89` ou `0123456789` ou `01.23.45.67.89`

5. **Sous-catégories**
   - Fusionner doublons snake_case/Capitalisé
   - Standardiser vers snake_case
   - Mettre à jour liaisons etablissement_sous_categorie

6. **Cohérence habitat_type vs sous_categories**
   - Vérifier mappings logiques
   - Logger incohérences
   - Proposer corrections (sans forcer)

### Phase 2: Validation sites web (avec Groq LLM, ~30min pour 1000 sites)

**Cibler UNIQUEMENT** :
- habitat_type IN ('habitat_partage', 'logement_independant')
- OU sous_categorie NOT IN ('residence_autonomie', 'residence_services_seniors')

**Processus**:
1. Extraire snippet page (ScrapingBee ou Serper)
2. Groq LLM valide si site légitime
3. Marquer sites suspects → field `url_source_valide` (booléen à ajouter?)

### Phase 3: Complétion géolocalisation (~10min pour 542 adresses)

1. Nominatim (gratuit, 1 req/s)
2. Stocker precision
3. Logger échecs pour traitement manuel

---

## 🚀 ENRICHISSEMENT AVANCÉ - SPÉCIFICATIONS

### Priorité 1: TARIFICATIONS (objectif 9% → 60%)

#### Sources d'extraction

**1. ScrapingBee** (scraping pages tarifs)
- Navigate to "Tarifs" / "Prix" / "Nos tarifs" sections
- Extract HTML tables
- Follow pagination (max 3 pages)
- Take screenshots pour Vision si nécessaire

**2. Groq LLM** (extraction structurée)
```python
# Prompt extraction tarifs
"""
HTML page tarifs:
{html_content}

Extraire:
- prix_min (€/mois)
- prix_max (€/mois)
- loyer_base (€/mois, hors charges)
- charges (€/mois)
- periode ('mois', 'semaine', 'jour')

Répondre JSON: {
  "prix_min": number | null,
  "prix_max": number | null,
  "loyer_base": number | null,
  "charges": number | null,
  "periode": "mois" | "semaine" | "jour" | null
}
"""
```

**3. Calcul fourchette_prix** (automatique)
```python
if prix_min is not None:
    if prix_min < 750:
        fourchette_prix = "euro"
    elif 750 <= prix_min <= 1500:
        fourchette_prix = "deux_euros"
    else:
        fourchette_prix = "trois_euros"
```

#### Insertion base
```sql
INSERT INTO tarifications (
  etablissement_id,
  fourchette_prix,
  prix_min,
  prix_max,
  loyer_base,
  charges,
  periode,
  source,
  date_observation
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, CURRENT_DATE
)
ON CONFLICT (etablissement_id, periode) 
DO UPDATE SET
  fourchette_prix = EXCLUDED.fourchette_prix,
  prix_min = EXCLUDED.prix_min,
  ...
  date_observation = EXCLUDED.date_observation;
```

---

### Priorité 2: SERVICES (objectif 34% → 80%, **GARDER 6 types**)

**⚠️ IMPORTANT** : L'utilisateur a spécifié "je ne cherche à en récupérer que 6"

#### Services actuels à enrichir (6 types uniquement)
```
• activités organisées
• commerces à pied
• conciergerie
• espace_partage
• médecin intervenant
• personnel de nuit
```

**NE PAS étendre** à 30+ services (coiffeur, pédicure, etc.) contrairement à la proposition initiale.

#### Objectif
- **Avant** : 952/3,325 établissements (34.1%) ont des services
- **Cible** : ~2,660/3,325 établissements (80%)
- **Méthode** : Améliorer la couverture des 6 services existants uniquement

#### Processus d'enrichissement
1. ScrapingBee → pages "Services" / "Prestations" / "Équipements"
2. Groq LLM → extraction liste services
3. Matching avec référentiel étendu
4. Insertion `services` (si nouveau)
5. Insertion `etablissement_service`

---

### Priorité 3: LOGEMENTS (objectif 32% → 70%, surfaces 0.1% → 40%)

**Objectif réaliste**: Surfaces difficiles à extraire, viser 40% seulement

#### Sources
- ScrapingBee: pages "Nos logements" / "Appartements"
- Groq Vision: analyse plans si images
- Serper: recherche "[nom] surfaces m2 logements"

#### Données à extraire
```python
{
  "libelle": "Studio", "T1", "T2", etc.
  "surface_min": 25,  # m²
  "surface_max": 30,  # m²
  "pmr": true/false,
  "domotique": true/false,
  "meuble": true/false,
  "plain_pied": true/false,
  "nb_unites": 10
}
```

⚠️ **Basse priorité si trop coûteux en ressources**

---

### Priorité 4: RESTAURATIONS (enrichissement détails)

**Données actuelles**: Seulement 4 booléens

**Nouvelles données à extraire** (schéma à enrichir?):
```python
{
  "tarif_repas_min": 8.50,  # €/repas
  "tarif_repas_max": 15.00,
  "type_cuisine": "traditionnelle" | "gastronomique" | "diététique",
  "menu_semaine": true/false,
  "formule_pension": "complète" | "demi-pension" | null,
  "horaires_flexibles": true/false
}
```

**⚠️ Nécessite extension schéma** → Décision à prendre

---

## 🧪 TEST SUR ÉCHANTILLON

### Département 76 (Seine-Maritime)

#### Requête pour compter
```sql
SELECT COUNT(*) 
FROM etablissements 
WHERE departement LIKE '%76%' 
  AND is_test = false;
```

#### Stratégie test
1. Exécuter quality check complet
2. Enrichir tarifs (priorité 1)
3. Enrichir services (priorité 2)
4. Valider manuellement échantillon de 10-20 résultats
5. Ajuster stratégies avant déploiement complet

---

## 💾 STRUCTURE CODE PIPELINE

### Architecture recommandée

```
pipeline_quality_enrichment.py (CLI principal)
├── core/
│   ├── __init__.py
│   ├── database.py (connexion Supabase, requêtes)
│   ├── models.py (dataclasses pour établissements, tarifs, etc.)
│   └── config.py (configuration, ENUMs, seuils)
│
├── quality_check/
│   ├── __init__.py
│   ├── normalizer.py (départements, CP, emails, etc.)
│   ├── validator.py (sites web, cohérence habitat_type)
│   └── geocoder.py (Nominatim, Google Maps)
│
├── enrichment/
│   ├── __init__.py
│   ├── tarifs_enricher.py (extraction tarifs)
│   ├── services_enricher.py (extraction services)
│   ├── logements_enricher.py (extraction logements)
│   └── restaurations_enricher.py (extraction restos)
│
├── utils/
│   ├── __init__.py
│   ├── scraper.py (ScrapingBee wrapper)
│   ├── llm_client.py (Groq API client)
│   └── logger.py (logging structuré)
│
└── tests/
    ├── test_normalizer.py
    ├── test_enrichers.py
    └── fixtures/
```

### CLI Arguments
```bash
# Mode quality check seul
python pipeline_quality_enrichment.py \
  --mode quality-check \
  --department 76

# Mode enrichissement ciblé
python pipeline_quality_enrichment.py \
  --mode enrichment \
  --department 76 \
  --priorities tarifs,services \
  --limit 50

# Mode complet (quality + enrichment)
python pipeline_quality_enrichment.py \
  --mode full \
  --department 76

# Dry-run (simulation sans écriture DB)
python pipeline_quality_enrichment.py \
  --mode full \
  --department 76 \
  --dry-run
```

---

## 📊 MÉTRIQUES & REPORTING

### Rapport à générer (JSON + Markdown)

```json
{
  "execution_date": "2026-01-30T15:30:00Z",
  "department": "76",
  "mode": "full",
  "duration_seconds": 1234,
  
  "quality_check": {
    "departements_normalises": 25,
    "codes_postaux_corriges": 3,
    "emails_nettoyes": 2,
    "sous_categories_fusionnees": 8,
    "sites_web_invalides": 1,
    "geolocalisations_ajoutees": 5
  },
  
  "enrichment": {
    "tarifs_avant": 10,
    "tarifs_apres": 45,
    "tarifs_taux_succes": "78%",
    
    "services_avant": 20,
    "services_apres": 55,
    "services_nouveaux_types": 12,
    
    "logements_avant": 15,
    "logements_apres": 38,
    "surfaces_avant": 0,
    "surfaces_apres": 12
  },
  
  "couts_apis": {
    "groq_requetes": 120,
    "groq_cout_euros": 0.00,
    "scrapingbee_requetes": 85,
    "scrapingbee_cout_euros": 0.00,
    "serper_requetes": 30,
    "serper_cout_euros": 0.00
  },
  
  "erreurs": [
    {
      "etablissement_id": "uuid...",
      "nom": "Résidence X",
      "type_erreur": "scraping_failed",
      "message": "Timeout après 3 tentatives"
    }
  ]
}
```

---

## ✅ CHECKLIST AVANT LANCEMENT

- [ ] `.env` configuré avec toutes les clés API
- [ ] Connexion Supabase testée
- [ ] Département 76 identifié pour test
- [ ] Backup base avant exécution
- [ ] Dry-run exécuté et validé
- [ ] Logging configuré
- [ ] Gestion erreurs robuste
- [ ] Métriques & reporting implémentés

---

## 🎯 DÉCISIONS À PRENDRE

1. **Sous-catégories**: Confirmer fusion vers snake_case ?
2. **Restaurations**: Étendre schéma pour détails qualitatifs ?
3. **Sites web**: Ajouter champ `url_source_valide` booléen ?
4. **Logements surfaces**: Vraiment basse priorité ? (Oui confirmé)
5. **Ordre exécution**: Quality check → Test 76 → Full deploy ?

**Prêt à lancer le développement ?** 🚀
