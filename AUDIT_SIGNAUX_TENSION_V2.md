# Audit du système de détection des signaux de tension — ESSMS gestionnaires
## État actuel · Dysfonctionnements · Propositions pour une passe V2

*Document de travail — Mars 2026*

---

## 1. Contexte et objectif de cet audit

La passe d'enrichissement actuelle attribue un booléen `signal_tension` et un champ texte
`signal_tension_detail` à chaque gestionnaire ESSMS. L'objectif de cet audit est triple :

1. **Comprendre précisément ce que fait le pipeline actuel** (requêtes Serper, prompt LLM, stockage)
2. **Identifier les failles structurelles** qui expliquent les omissions (ex. APF France Handicap)
   et les faux-positifs par agrégation
3. **Proposer une architecture complémentaire en 3 couches** qui préserve les données V1 (valeur
   business maintenue), qualifie les signaux existants par mots-clés, puis lance des recherches
  ciblées uniquement pour les gestionnaires prioritaires — en ne retenant que les difficultés
  individuelles réellement actionnables

---

## 2. Description du pipeline actuel (Step 8 de `enrich_finess_dept.py`)

### 2.1 Déclenchement

Le signal est calculé **par gestionnaire** (pas par établissement), intégré dans la boucle
principale d'enrichissement, après le scraping du site web et l'extraction des dirigeants.
Il est conditionné à `--skip-serper` = false et `--skip-llm` = false.

```python
# enrich_finess_dept.py : l. 1981-1987
if not args.skip_serper and serper_key and not args.skip_llm and llm_key:
    sig = enrich_signaux_gestionnaire(gest, context, llm_key, llm_model, serper_key, cur)
    update_signaux_gestionnaire(gest_id, sig, cur, args.dry_run)
```

### 2.2 Requêtes Serper (3 par gestionnaire, fixe)

```python
# enrich_finess_dept.py : l. 1371-1381
for q in [
    f'"{raison_sociale}" actualité {commune}',
    f'"{raison_sociale}" recrutement emploi',
    f'"{raison_sociale}" projet transformation extension',
]:
```

Ces requêtes sont concaténées au `context["combined_text"]` déjà accumulé (scraping du site
+ requêtes Serper des étapes précédentes) pour constituer le `texte_actualites` soumis au LLM.

### 2.3 Prompt LLM (`PROMPT_SIGNAUX_TENSION`)

Le LLM est invité à détecter indifféremment 10 types de signaux :

| Signal demandé         | Polarité naturelle |
|------------------------|--------------------|
| `recrutement`          | ⚠ ambiguë (besoin normal ou crise RH) |
| `fermeture`            | ❌ négatif |
| `fusion`               | ⚠ ambiguë |
| `extension`            | ✅ souvent positif |
| `conflit`              | ❌ négatif |
| `inspection`           | ❌ négatif |
| `cpom`                 | ✅ souvent positif |
| `transformation`       | ✅ souvent positif |
| `appel_projet`         | ✅ positif |
| `changement_direction` | ⚠ ambiguë |

La réponse LLM produit :
```json
{
  "signaux": [...],
  "signal_tension": true,
  "signal_tension_detail": "texte libre"
}
```

### 2.4 Stockage en base

```sql
UPDATE finess_gestionnaire SET
    signaux_recents      = <json>,
    signal_tension       = <bool>,
    signal_tension_detail = <text>
WHERE id_gestionnaire = %s;
```

---

## 3. Dysfonctionnements identifiés

### 3.1 Agrégation de signaux de natures radicalement différentes

**Problème fondamental.** Un booléen `signal_tension = TRUE` peut résulter aussi bien d'un
plan de sauvegarde judiciaire (APF France Handicap, déficit d'exploitation de 40 M€ en 2023)
que d'un recrutement d'éducateurs ou de l'attribution d'un CPOM — ce qui est une bonne nouvelle.

L'analyse produite dans la session précédente montrait un taux de 16,5 % de gestionnaires avec
signal de tension. **Ce chiffre est probablement surestimé pour les difficultés réelles et sous-
estimé pour les gestionnaires en trouble vérifiable** : les signaux positifs (CPOM, extension)
gonflent le numérateur pendant que les difficultés financières sérieuses, médiatisées dans des
supports spécialisés peu référencés par Google généraliste, restent sous le radar.

### 3.2 Requêtes Serper inadaptées à la détection de difficultés financières

| Requête actuelle | Défaut |
|-----------------|--------|
| `"<RS>" actualité <commune>` | Trop générique, volume élevé de bruit local |
| `"<RS>" recrutement emploi` | Déclenche sur TOUS les employeurs en croissance normale |
| `"<RS>" projet transformation extension` | Détecte essentiellement des projets **positifs** |

Les termes clés pour les difficultés financières réelles (**déficit**, **plan de sauvegarde**,
**redressement judiciaire**, **PSE**, **procédure collective**, **liquidation**, **fermeture
administrative**) n'apparaissent dans aucune requête.

Les médias spécialisés qui couvrent ces sujets (APMnews, Hospimedia, Lien Social, Direction[s],
Gazette Santé-Social) sont peu favorisés par des requêtes Google généralistes sans guillemets
sur des termes sectoriels précis.

### 3.3 Cas APF France Handicap — analyse de l'échec

APF France Handicap est une association nationale avec ~250 établissements, en grande difficulté
financière depuis 2022 (déficit structurel déclaré, fermetures et regroupements multiples,
plan d'économies national). Raisons probables de la non-détection :

1. **Raison sociale en base** : le gestionnaire peut être enregistré sous un libellé FINESS
   local (ex. « APF France Handicap — Délégation Régionale … ») et non sous le nom national ;
   les requêtes avec guillemets autour de la raison sociale locale ne renvoient pas les articles
   nationaux.
2. **La requête "recrutement emploi"** : APF recrute, donc ce signal sera positif sans
   caractériser la tension.
3. **La requête "projet transformation extension"** : APF mène des projets de regroupement —
   détectables, mais interprétés comme positifs par le LLM (type `transformation`).
4. **Absence de requête financière ciblée** : aucune des 3 requêtes ne contient "déficit",
   "plan de redressement", "difficultés financières", "Tribunal judiciaire".
5. **Score `signal_tension`** : même si des signaux sont trouvés, ils peuvent être de type
   `transformation` (considéré neutre ou positif) → `signal_tension = FALSE`.

### 3.4 Économie de requêtes illusoire

Le pipeline exécute **exactement 3 requêtes Serper par gestionnaire**, soit ~34 000 requêtes
pour 11 291 gestionnaires éligibles. Or :

- ~83 % des gestionnaires n'ont **aucun signal réel** dans la presse (micro-structures locales
  totalement hors radar médiatique avec 1-3 établissements)
- Ces 3 requêtes sont gaspillées sur des gestionnaires qui ne généreront jamais de résultats
- Les rares gestionnaires à **fort impact** (>20 ET, secteurs PA et Handicap) ne reçoivent
  pas plus de requêtes que les structures de 1 ET

### 3.5 Pas de notion de fraîcheur / confiance au niveau du signal

Le champ `signal_tension_detail` est un texte libre sans date, sans source, sans niveau de
confiance. Impossible de distinguer une information de 2024 d'un article de 2019.

### 3.6 Absence de déduplication à la source

Si le même article APMnews apparaît dans plusieurs requêtes (actualité + recrutement + projet),
il est concaténé plusieurs fois dans le `combined_text` → biais du LLM vers ce signal.

### 3.7 Bruit sectoriel dans un signal qui devrait rester individuel

Les mobilisations collectives du secteur ne sont pas un objet d'analyse utile ici. Le vrai
problème est plus simple : la V1 laisse entrer dans `signal_tension` des contenus qui ne disent
rien sur la situation propre du gestionnaire.

Pour l'usage visé, un signal utile doit renvoyer à une difficulté **individuelle et actionnable** :
licenciements, problèmes de trésorerie, risque sur la continuité de service, procédure
judiciaire, injonction qualité grave.

Tout le reste doit être traité comme du bruit et **exclu du périmètre V2** : mobilisation de
branche, communiqué fédératif, débat tarifaire général, reportage sur les tensions du secteur.

Autrement dit, la V2 ne doit pas mieux documenter ces cas ; elle doit simplement éviter de les
compter comme signaux de difficulté.

---

## 4. Architecture proposée pour la passe V2

### 4.1 Principe général : architecture en 3 couches

L'objectif est de **compléter les données V1 sans les détruire** — les 10 types de signaux
existants dans `signaux_recents` ont une valeur business (prospection en cours, projets de
transformation, changements de direction) et doivent être conservés.

**Couche 0 — V1 préservée** (données déjà en base, rien à recalculer)
- `signaux_recents` (JSONB) : 10 types de signaux tels que détectés par le pipeline actuel
- `signal_tension` / `signal_tension_detail` : maintenu pour compatibilité

**Couche 1 — Classification automatique par mots-clés** (0 requête Serper supplémentaire)
- Lecture des `signaux_recents` déjà stockés
- Détection de mots-clés dans `signal_tension_detail` et dans les snippets V1
- Alimentation de 4 colonnes booléennes axialisées, avec exclusion du bruit non actionnable

> **Point de vigilance** : cette couche dépend fortement de la qualité de `signal_tension_detail`
> et du contenu textuel réellement conservé en V1. L'hypothèse est crédible, mais elle doit être
> validée sur échantillon avant généralisation.

**Couche 2 — Qualification Serper ciblée** (uniquement pour gestionnaires prioritaires)
- Requêtes axées exclusivement sur les difficultés **individuelles**
- Déclenchée sur : gestionnaires volumineux, établissements isolés à vérifier, signaux V1 de type
  fermeture/conflit/inspection, ou Couche 1 ambiguë

---

**4 axes de difficultés réelles** :

| Axe | Colonne DB | Ce qui compte |
|-----|-----------|---------------|
| Financier / Continuité | `signal_financier` | Déficit déclaré, PSE, trésorerie, fermeture pour raisons financières, plan de sauvegarde |
| RH grave | `signal_rh` | Grève avec arrêt de service, plan social, fermeture faute de personnel (≠ recrutement normal) |
| Qualité / Inspection | `signal_qualite` | Injonction ARS, mise en demeure, fermeture administrative, incident grave, maltraitance avérée |
| Juridique | `signal_juridique` | Procédure collective, liquidation, mise sous administration provisoire, condamnation pénale |

**Signaux à valeur business pure** (issus V1, gardés dans `signaux_recents`) :
- `transformation`, `extension`, `cpom`, `appel_projet` → prospects en développement actif
- `changement_direction` → opportunité de prospection (nouveau décideur)
- `fusion` → mouvement stratégique à surveiller

**2 niveaux de passe Serper** — pour économiser les requêtes sans manquer les cas unitaires critiques :

- **Passe A — Détection rapide** (1 requête, tous gestionnaires sur lesquels Couche 1 est insuffisante)
- **Passe B — Qualification approfondie** (2-3 requêtes, uniquement priorités)

### 4.2 Modifications du schéma PostgreSQL

```sql
-- Migration additive : aucune colonne V1 supprimée
ALTER TABLE public.finess_gestionnaire
    -- Axes de difficultés réelles
    ADD COLUMN IF NOT EXISTS signal_financier               BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_financier_detail        TEXT,
    ADD COLUMN IF NOT EXISTS signal_financier_sources       TEXT[],
    ADD COLUMN IF NOT EXISTS signal_rh                      BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_rh_detail               TEXT,
    ADD COLUMN IF NOT EXISTS signal_qualite                 BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_qualite_detail          TEXT,
    ADD COLUMN IF NOT EXISTS signal_qualite_sources         TEXT[],
    ADD COLUMN IF NOT EXISTS signal_juridique               BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_juridique_detail        TEXT,
    -- Méta
    ADD COLUMN IF NOT EXISTS signal_v2_confiance            TEXT,       -- 'haute'|'moyenne'|'basse'
    ADD COLUMN IF NOT EXISTS signal_v2_date                 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS signal_v2_methode              TEXT;       -- 'keywords_v1'|'serper_passe_a'|'serper_passe_b'
```

**Logique de rétrocompatibilité :** `signal_tension` existant est préservé tel quel. Une vue
  calcule `signal_difficulte_v2 = (signal_financier OR signal_qualite OR signal_juridique OR signal_rh)`
  qui représentera spécifiquement les difficultés individuelles retenues par la V2.

### 4.3 Couche 1 : classification par mots-clés sur les données V1 (0 requête Serper)

**Principe** : avant de lancer la moindre requête Serper, exploiter ce qu'on a déjà —
`signaux_recents` (JSONB) et `signal_tension_detail` contiennent du texte issu des snippets
originaux. Une passe de détection par mots-clés peut classer ~60-70 % des cas sans coût.

Avant industrialisation, il faut **mesurer réellement cette hypothèse** sur un échantillon.
Proposition minimale :

- 50 gestionnaires avec `signal_tension = TRUE` en V1, répartis par taille (`1 ET`, `2-10`, `11-50`, `>50`)
- 20 gestionnaires avec `signal_tension = FALSE` mais connus du terrain comme fragiles si la liste existe
- Double lecture manuelle : `signal_tension_detail`, `signaux_recents`, puis verdict attendu sur les 4 axes

Objectif : estimer la précision de la Couche 1 avant de décider si elle peut être utilisée comme
filtre massif ou seulement comme pré-classement assisté.

#### Règles de classification par mots-clés

```python
# À implémenter dans scripts/enrich_signaux_v2.py
KEYWORDS_FINANCIER = [
    "déficit", "plan de sauvegarde", "trésorerie", "plan social", "PSE",
    "difficultés financières", "licenciements", "plan de redressement",
    "fermeture financière", "cessation", "dettes",
]
KEYWORDS_JURIDIQUE = [
    "redressement judiciaire", "liquidation judiciaire", "procédure collective",
    "administrateur judiciaire", "mise sous administration provisoire",
    "tribunal de commerce", "CJIP", "condamnation",
]
KEYWORDS_RH_GRAVE = [
    "grève", "mouvement social", "grévistes", "piquet de grève",
    "plan social", "plan de départs", "fermeture faute de personnel",
]
KEYWORDS_QUALITE = [
    "injonction ARS", "mise en demeure", "fermeture administrative",
    "maltraitance", "incident grave", "signalement", "procédure contradictoire",
    "rapport ARS négatif", "non-conformité grave",
]
# Mots-clés de bruit à exclure du périmètre V2
KEYWORDS_BRUIT = [
    "mobilisation", "pétition", "manifestation", "inter-associatif",
    "branche professionnelle", "convention collective", "accord de branche",
    "non-revalorisation tarifaire", "sous-dotation chronique",
    "Nexem", "FEHAP", "UNIOPSS", "Synerpa",  # fédérations mobilisées
]
```

#### Algorithme

```
Pour chaque gestionnaire avec signal_tension_detail non-null :
  texte = signal_tension_detail + concat(signaux_recents[*].resume)
  
  si any(kw in texte pour kw in KEYWORDS_BRUIT) et
     not any(kw in texte pour kw in KEYWORDS_FINANCIER + KEYWORDS_JURIDIQUE + KEYWORDS_RH_GRAVE + KEYWORDS_QUALITE) :
       → exclure le cas du périmètre V2
       → signal_v2_methode = 'keywords_v1_excluded'
       → arrêt (pas de Serper nécessaire)
  
  sinon :
    si any(kw in texte pour kw in KEYWORDS_FINANCIER) : signal_financier = TRUE (provisoire)
    si any(kw in texte pour kw in KEYWORDS_JURIDIQUE) : signal_juridique = TRUE
    si any(kw in texte pour kw in KEYWORDS_RH_GRAVE)  : signal_rh = TRUE
    si any(kw in texte pour kw in KEYWORDS_QUALITE)   : signal_qualite = TRUE
    
    si au moins 1 signal détecté :
      → signal_v2_methode = 'keywords_v1', signal_v2_confiance = 'basse'
      → envoyer en Passe B pour confirmation Serper
    sinon :
      → classé sans signal (peut être envoyé en Passe A selon priorité)
```

**Économie immédiate** : une part significative des `signal_tension = TRUE` en V1 correspond
à des signaux positifs (CPOM, extension) ou à du bruit non exploitable. Cette classification
écarte ces cas sans requête Serper supplémentaire.

### 4.4 Passe A : détection rapide (1 requête Serper, gestionnaires prioritaires sans V1 exploitable)

**Déclenchée pour** :
- Les gestionnaires `nb_etablissements > 10` dont le V1 ne contenait aucun signal exploitable
- Les établissements isolés (`nb_etablissements = 1`) dont la Couche 1 est vide ou faible, car ce
  sont souvent précisément eux qui concentrent les difficultés réelles locales
- Les grands groupes (>50 ET) dont les articles nationaux ne seraient pas couverts par les
  requêtes locales de la V1

Le seuil `>10 ET` ne doit donc pas être interprété comme un seuil d'intérêt métier. C'est un
seuil de priorisation partielle, pas un filtre de périmètre. Pour ton usage, les isolés en
difficulté doivent rester pleinement dans le radar.

```python
# Utiliser sigle si disponible (plus connu que la raison sociale FINESS locale)
naming = sigle if sigle and len(sigle) >= 3 else raison_sociale

# Pour les opérateurs connus : utiliser le nom public connu (mapping curatif)
NOM_PUBLIC_CONNU = {
  # raison_sociale_contient / sigle / alias interne → nom à utiliser dans la requête
    "MEDICA"     : "Medica",
    "COLISEE"    : "Colisée France",
    "DOMUSVI"    : "DomusVi",
    "KORIAN"     : "Korian",
    "ORPEA"      : "Orpea",
    "EMEIS"      : "Emeis",   # ex-Orpea
    "APF"        : "APF France Handicap",
    "VYV"        : "VYV3",
    "FONDATION ANAIS" : "Fondation Anaïs",
    "LADAPT"     : "LADAPT",
    "FRANCE HORIZON" : "France Horizon",
    "COALLIA"    : "Coallia",
    "SOS VILLAGES" : "SOS Villages d'Enfants",
}

  # Variante compacte pour limiter le bruit de l'empilement des OR
  QUERY_PASSE_A = (
    f'"{naming}" '
    f'(déficit OR trésorerie OR licenciement OR liquidation OR '
    f'"redressement judiciaire" OR "procédure collective" OR PSE OR '
    f'"plan social" OR fermeture)'
  )
```

  La version ci-dessus reste large, mais elle garde le **nom du gestionnaire comme pivot obligatoire**
  et limite le risque d'avoir des résultats trop diffus. Si Serper interprète mal la parenthèse,
  prévoir 2 variantes courtes au lieu d'une seule requête trop permissive :

  ```python
  QUERY_PASSE_A_1 = f'"{naming}" (déficit OR trésorerie OR "plan social" OR PSE)'
  QUERY_PASSE_A_2 = f'"{naming}" (liquidation OR "redressement judiciaire" OR "procédure collective" OR fermeture)'
  ```

  En pratique, cette variante en 2 requêtes courtes sera probablement plus robuste qu'une seule
  requête longue avec trop de `OR`.

**Règle de décision (rapide, pas de LLM) :**
- 0 résultat → aucun signal V2, arrêt
- ≥ 1 résultat avec mot-clé négatif dans snippet → envoyer en Passe B
- Uniquement mots-clés de bruit ou signaux positifs → exclure du périmètre V2, arrêt

**Économie estimée** (par rapport à la V1) :
- Couche 1 traite ~11 291 gestionnaires avec signal V1 existant (0 Serper)
- Passe A cible ~1 500 à 2 500 gestionnaires (volumineux, isolés à vérifier, sans signal V1 exploitable)
- Passe B cible ~700 à 1 200 gestionnaires (signaux ambigus, prioritaires ou localement sensibles)
- Total : ~2 200 requêtes Serper vs 33 873 en V1 — **économie de >93 %**

### 4.5 Passe B : qualification approfondie (2-3 requêtes Serper ciblées)

Déclenchée pour :
- Gestionnaires avec signal Couche 1 positif mais `confiance = basse`
- Gestionnaires `>50 ET` (systématiquement, qu'il y ait signal V1 ou non)
- Gestionnaires avec signal V1 de type `fermeture`, `conflit` ou `inspection`
- Établissements isolés ou petites structures avec indice fort de difficulté locale

#### Requêtes par axe (utiliser le nom public connu en priorité)

La variable `{nom_req}` est `sigle` si disponible et ≥ 3 caractères, sinon `raison_sociale`.
Pour les opérateurs nationaux connus, utiliser le `NOM_PUBLIC_CONNU` mapping (cf. 4.4).

Le terme juste n'est pas "nom commercial" mais **nom public connu** : dans ce secteur, il peut
s'agir d'un sigle, d'un acronyme d'usage, d'un nom historique, d'une nouvelle marque, ou d'un
nom court sous lequel la presse et les acteurs métier identifient réellement l'opérateur.

Conséquence pratique : le mapping ne doit pas être pensé comme une petite table statique, mais
comme un référentiel vivant d'alias publics, idéalement alimenté par :

- `raison_sociale`
- `sigle`
- variantes textuelles déjà vues dans les snippets Serper
- noms historiques / rebrandings
- exceptions curées manuellement pour les opérateurs importants

**Axe financier + juridique (1 requête combinée)**
```
"{nom_req}" déficit OR "plan social" OR PSE OR licenciements OR
"redressement judiciaire" OR "procédure collective" OR liquidation
```

**Axe qualité / inspection (1 requête)**
```
"{nom_req}" ARS inspection injonction OR "mise en demeure" OR
"fermeture administrative" OR "incident grave" OR maltraitance
```

**Axe RH grave (1 requête, uniquement si signal V1 `conflit` présent)**
```
"{nom_req}" grève OR "grévistes" OR "piquet" OR "plan de départs" OR
"plan social" personnel soignant
```

> **Note sur la BQR** : la Base Qualité Risques est une source pertinente pour les signaux
> qualité/inspection (rapports d'évaluation, injonctions ARS). Elle nécessite un accès
> spécifique et n'est pas directement interrogeable via Serper. À explorer comme source
> complémentaire pour les gestionnaires dont le signal qualité est ambiguë en Passe B.

### 4.6 Prompts LLM pour la Passe B

Un seul prompt unifié par gestionnaire en Passe B (au lieu de 4 prompts séparés) pour
réduire les appels LLM. Le LLM reçoit les snippets Serper et les signaux V1 existants.

```
Tu es un analyste du secteur médico-social français.

Gestionnaire : {nom_req} ({secteur_activite_principal}, {nb_etablissements} établissements)
Département : {departement_nom}

Signaux déjà détectés en V1 :
{signaux_v1_resume}

Nouveaux extraits de recherche :
{snippets_serper}

Ta mission : identifier uniquement les difficultés INDIVIDUELLES réelles de ce gestionnaire.

DIFFICULTÉS INDIVIDUELLES RÉELLES (à classer) :
- Financières : déficit déclaré avec montant, PSE, plan social, trésorerie critique,
  fermeture d'un ou plusieurs établissements pour raisons financières,
  plan de sauvegarde, mise en redressement
- RH graves : grève avec arrêt de service, plan social, fermeture faute de personnel
- Qualité : injonction ARS, mise en demeure, fermeture administrative, maltraitance avérée
- Juridiques : procédure collective, liquidation, mise sous administration provisoire

CONTENUS À EXCLURE DU PÉRIMÈTRE (bruit non actionnable) :
- Participation à une mobilisation inter-associative (Nexem, FEHAP, UNIOPSS, syndicats branch.)
- Communiqué collectif contre la politique tarifaire du CD ou de l'ARS
- Appel à projets sectoriel, signature de pétition nationale
- Reportage sur les difficultés du secteur EHPAD/IME/ESAT en général

À EXCLURE (signaux positifs ou neutres, à garder dans signaux_recents V1 mais pas signal tension) :
- Recrutement en cours normal
- Attribution d'un CPOM, d'un appel à projets ARS
- Extension, construction, rénovation
- Changement de direction normal

Réponds en JSON :
{{
  "signal_financier": true|false,
  "signal_financier_detail": "<1-2 phrases factuelles avec chiffres si dispo, ou null>",
  "signal_rh": true|false,
  "signal_rh_detail": "<1-2 phrases ou null>",
  "signal_qualite": true|false,
  "signal_qualite_detail": "<1-2 phrases ou null>",
  "signal_juridique": true|false,
  "signal_juridique_detail": "<1-2 phrases ou null>",
  "sources": ["<url1>", "<url2>"],
  "confiance": "haute|moyenne|basse",
  "periode": "<YYYY ou YYYY-YYYY ou null>"
}}

Critères de confiance :
- haute : source spécialisée (APMnews, Hospimedia, Lien Social, Direction[s], La Gazette Santé-Social,
  les Echos, AFP) OU document officiel (ARS, Tribunal, rapport d'inspection) datant < 18 mois
- moyenne : presse régionale, syndicats, LinkedIn, annonce officielle non datée
- basse : source ancienne (>2 ans), blog, forum, source indirecte

Si aucun signal individuel : mettre tous les booléens à false.
```

### 4.7 Priorisation des gestionnaires

| Étape | Méthode | Gestionnaires ciblés | Requêtes Serper |
|-------|---------|---------------------|------------------|
| Couche 1 | Mots-clés sur V1 | Tous avec `signal_tension_detail` non null | 0 |
| Passe A | 1 à 2 requêtes Serper | `nb_et > 10`, isolés à vérifier, grands groupes sans signal V1 exploitable | ~1 500 à 2 500 |
| Passe B | 2-3 requêtes Serper | Signaux Couche 1 ambigus + `nb_et > 50` systématique + cas locaux forts | ~700 à 1 200 |
| **Total** | | | **~2 200 à 3 700** (vs 33 873 en V1) |

Ordre de traitement Passe B : `nb_etablissements > 50` → `>20` → secteur PA `>10` → isolés avec
indices forts → reste.

### 4.8 Utilisation du cache Serper existant

Le pipeline dispose déjà de `finess_cache_serper` (table PostgreSQL, TTL 30 jours).
**Avant toute requête Serper, interroger le cache avec `get_or_search_serper()`**
(déjà implémenté dans `fix_data_quality.py`). Les requêtes lancées pour la V1
(actualité, recrutement, transformation) peuvent partiellement nourrir la Couche 1
sans aller en Serper.

### 4.9 Sources spécialisées à prioriser

Évaluer le poids des sources dans les snippets (score de confiance) :

```python
SOURCES_HAUTE_CONFIANCE = [
    "apmnews.fr",
    "hospimedia.fr",
    "lien-social.com",
    "directions.fr",           # magazine Direction[s]
    "gazette-sante-social.fr",
    "sanitaire-social.com",
    "lesechos.fr", "lemonde.fr", "liberation.fr",  # presse nationale si affaire importante
]

SOURCES_JURIDIQUES = [
    "infogreffe.fr",           # redressements / liquidations judiciaires en cours
    "actulegales.fr",          # procédures collectives
    # BODACC : données officielles mais couverture partielle des associations
    # (les associations non-commerciales y apparaissent rarement)
    # → à utiliser avec parcimonie, pas en requête systématique
]
```

> **Note sur la BQR (Base Qualité Risques)** :
> Source potentiellement très pertinente pour l'axe qualité/inspection (rapports ARS,
> évaluations, injonctions). Nécessite un accès institutionnel spécifique — à explorer
> comme source complémentaire manuelle ou via partenariat, pas automatisable via Serper.

---

## 5. Plan de mise en œuvre

### Phase 0 — Préparation (sans requête Serper, sans LLM)
- [ ] Migration DB : `ALTER TABLE` additive (cf. Annexe A) — aucune colonne V1 supprimée
- [ ] Créer `scripts/enrich_signaux_v2.py` (script dédié, indépendant du pipeline principal)
- [ ] Implémenter le mapping `NOM_PUBLIC_CONNU` et les listes de mots-clés
- [ ] Implémenter `classify_by_keywords(signal_detail, signaux_recents)` → retourne les 4 flags V2 ou une exclusion de périmètre
- [ ] Préparer un protocole d'échantillonnage manuel pour tester la Couche 1 avant généralisation

### Phase 1 — Couche 1 : classification par mots-clés (0 Serper, tous les gestionnaires)
- [ ] Lire `signal_tension_detail` + `signaux_recents` pour chaque gestionnaire
- [ ] Appliquer `classify_by_keywords()` en batch SQL/Python
- [ ] Écrire les 4 flags et `signal_v2_methode = 'keywords_v1'` ou `keywords_v1_excluded`
- [ ] **Checkpoint** : compter les gest. par catégorie (financier/rh/qualite/juridique)
  et valider manuellement 20 cas avant de passer à la suite

### Phase 2 — Passe A Serper (gestionnaires prioritaires sans V1 exploitable)
- [ ] Sélectionner les gestionnaires `nb_et > 10`, les grands groupes, et les isolés avec signaux faibles ou absents mais à vérifier
- [ ] Appliquer le mapping `NOM_PUBLIC_CONNU` pour construire les requêtes
- [ ] Stocker dans `finess_cache_serper`, ne PAS appeler si cache < 30 jours
- [ ] Tester si 1 requête compacte suffit, sinon basculer vers 2 requêtes courtes plus robustes
- [ ] Décision rapide par mots-clés snippet (pas de LLM en Passe A)

### Phase 3 — Passe B Serper + LLM (gestionnaires prioritaires + ambigus)
- [ ] Traiter par priorité décroissante : `>50 ET` → `>20` → secteur PA `>10`
- [ ] Appliquer le prompt LLM unifié (section 4.6) avec snippets Serper + résumé V1
- [ ] Écrire les 4 booléens + détails + sources + confiance
- [ ] `signal_v2_methode = 'serper_passe_b'`

### Phase 4 — Validation manuelle (échantillon)
- [ ] Valider l'échantillon Couche 1 avant généralisation (au moins 70 cas relus)
- [ ] Vérifier 30 gestionnaires avec `signal_financier = TRUE`
- [ ] Vérifier APF France Handicap + 9 autres cas connus-en-difficulté (faux négatifs ?)
- [ ] Vérifier 20 exclusions de périmètre (`keywords_v1_excluded`) pour confirmer qu'aucun cas individuel utile n'est perdu
- [ ] Tirer au hasard 10 gestionnaires dans la population d'angle mort structurel : `signal_tension = FALSE` en V1, `nb_et <= 10`, aucun indice fort, non envoyés en Passe A
- [ ] Relire ces 10 cas pour estimer le taux résiduel de faux négatifs "par construction" et documenter cette limite connue du système V1+V2

### Phase 5 — Rétrocompatibilité SQL
- [ ] Créer la vue `v_gestionnaires_signaux_v2` (cf. Annexe A)
- [ ] Mettre à jour `analyse_tensions_gestionnaires_non_saa.sql` pour exposer les 4 axes de difficultés individuelles

---

## 6. Résumé des gains attendus

| Dimension | Avant (V1) | Après (V2) |
|-----------|-----------|-----------|
| Données V1 existantes | Utilisées telles quelles | **Préservées et réexploitées** (0 perte) |
| Qualité de la Couche 1 | Supposée | Mesurée sur échantillon avant généralisation |
| Faux positifs | Nombreux (CPOM, recrutement, bruit sectoriel) | Écartés du périmètre V2 dès la couche de qualification |
| Faux négatifs (difficultés réelles) | Élevés (APF, grands groupes, pas de requêtes ciblées) | Réduits (mots-clés + requêtes financières + nom public connu) |
| Faux négatifs structurels | Non mesurés | Estimés par sondage sur la population hors V1, hors Passe A, `nb_et <= 10` |
| Requêtes Serper | ~33 873 (V1) | ~2 200 à 3 700 (V2) selon couverture des isolés — **économie >89 %** |
| Périmètre d'analyse | Mélange signaux utiles et bruit | Focalisé sur les seules difficultés individuelles |
| Valeur business des signaux positifs | Noyée dans le booléen tension | Conservée dans `signaux_recents` V1 (transformation, CPOM, direction…) |
| Confiance et fraîcheur | Pas gérées | `signal_v2_confiance` + `signal_v2_date` |
| Traçabilité sources | Texte libre | `signal_financier_sources TEXT[]`, `signal_qualite_sources TEXT[]` |

---

## Annexe A — DDL complet (migration additive)

```sql
-- Aucune colonne V1 supprimée : signal_tension, signal_tension_detail, signaux_recents sont préservés
ALTER TABLE public.finess_gestionnaire
    -- Axes difficultés individuelles réelles
    ADD COLUMN IF NOT EXISTS signal_financier               BOOLEAN      DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_financier_detail        TEXT,
    ADD COLUMN IF NOT EXISTS signal_financier_sources       TEXT[],
    ADD COLUMN IF NOT EXISTS signal_rh                      BOOLEAN      DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_rh_detail               TEXT,
    ADD COLUMN IF NOT EXISTS signal_qualite                 BOOLEAN      DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_qualite_detail          TEXT,
    ADD COLUMN IF NOT EXISTS signal_qualite_sources         TEXT[],
    ADD COLUMN IF NOT EXISTS signal_juridique               BOOLEAN      DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS signal_juridique_detail        TEXT,
    -- Méta-données de la passe V2
    ADD COLUMN IF NOT EXISTS signal_v2_confiance            TEXT,        -- haute|moyenne|basse
    ADD COLUMN IF NOT EXISTS signal_v2_date                 TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS signal_v2_methode              TEXT;        -- keywords_v1|serper_passe_a|serper_passe_b

-- Vue synthétique V2
CREATE OR REPLACE VIEW v_gestionnaires_signaux_v2 AS
SELECT
    g.id_gestionnaire,
    g.raison_sociale,
    g.sigle,
    g.secteur_activite_principal,
    g.categorie_taille,
    g.nb_etablissements,
    -- Colonne V1 d'origine (compatibilité)
    g.signal_tension                                           AS signal_v1,
    g.signal_tension_detail                                    AS signal_v1_detail,
    -- Difficultés individuelles V2
    (g.signal_financier OR g.signal_qualite OR g.signal_juridique OR g.signal_rh)
                                                               AS signal_difficulte_v2,
    (
      g.signal_financier::int +
      g.signal_rh::int +
      g.signal_qualite::int +
      g.signal_juridique::int
    )                                                          AS signal_v2_nb_axes,
    g.signal_financier,  g.signal_financier_detail,
    g.signal_rh,         g.signal_rh_detail,
    g.signal_qualite,    g.signal_qualite_detail,
    g.signal_juridique,  g.signal_juridique_detail,
    -- Méta
    g.signal_v2_confiance,
    g.signal_v2_date,
    g.signal_v2_methode,
    CASE
        WHEN g.signal_juridique THEN 'juridique'
        WHEN g.signal_financier THEN 'financier'
        WHEN g.signal_qualite   THEN 'qualite'
        WHEN g.signal_rh        THEN 'rh'
        ELSE NULL
    END AS signal_type_dominant_v2
FROM public.finess_gestionnaire g;
```

## Annexe B — Requêtes SQL d'analyse adaptées

```sql
-- KPI : répartition par axe (hors SAA)
WITH elig AS (
    SELECT g.*
    FROM finess_gestionnaire g
    WHERE EXISTS (
        SELECT 1 FROM finess_etablissement e
        WHERE e.id_gestionnaire = g.id_gestionnaire
          AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
    )
)
SELECT
    COUNT(*) FILTER (WHERE signal_financier)           AS nb_financier,
    COUNT(*) FILTER (WHERE signal_rh)                  AS nb_rh_grave,
    COUNT(*) FILTER (WHERE signal_qualite)             AS nb_qualite_inspection,
    COUNT(*) FILTER (WHERE signal_juridique)           AS nb_juridique,
  COUNT(*) FILTER (WHERE (signal_financier::int + signal_rh::int + signal_qualite::int + signal_juridique::int) >= 2)
                             AS nb_multi_axes,
    COUNT(*) FILTER (WHERE signal_financier OR signal_qualite
                        OR signal_juridique OR signal_rh)
                                                       AS nb_difficulte_individuelle_v2,
    -- Comparaison avec V1
    COUNT(*) FILTER (WHERE signal_tension)             AS nb_signal_v1_original
FROM elig;

-- Top 50 gestionnaires en difficulté réelle (multi-axes priorisés)
SELECT
    id_gestionnaire, raison_sociale, sigle,
    secteur_activite_principal, categorie_taille, nb_etablissements,
  (signal_financier::int + signal_rh::int + signal_qualite::int + signal_juridique::int) AS signal_v2_nb_axes,
    signal_financier, signal_financier_detail,
    signal_juridique, signal_juridique_detail,
    signal_qualite,   signal_qualite_detail,
    signal_rh,        signal_rh_detail,
    signal_v2_confiance, signal_v2_date
FROM finess_gestionnaire
WHERE (signal_financier OR signal_juridique OR signal_qualite OR signal_rh)
  AND EXISTS (
    SELECT 1 FROM finess_etablissement e
    WHERE e.id_gestionnaire = finess_gestionnaire.id_gestionnaire
      AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  )
ORDER BY
  -- Prioriser les cas multi-axes, puis par volume ET
  (signal_juridique::int + signal_financier::int + signal_qualite::int + signal_rh::int) DESC,
    nb_etablissements DESC
LIMIT 50;

-- Échantillon de validation : exclusions de périmètre keywords_v1_excluded
SELECT
    id_gestionnaire,
    raison_sociale,
    sigle,
    secteur_activite_principal,
    categorie_taille,
    nb_etablissements,
    signal_tension,
    signal_tension_detail,
    signaux_recents,
    signal_v2_methode
FROM finess_gestionnaire
WHERE signal_v2_methode = 'keywords_v1_excluded'
ORDER BY random()
LIMIT 20;

-- Échantillon de validation : angle mort structurel V1+V2
-- Hypothèse : pas de signal V1, petite structure, aucun passage dans les étapes ciblées V2
SELECT
    id_gestionnaire,
    raison_sociale,
    sigle,
    secteur_activite_principal,
    categorie_taille,
    nb_etablissements,
    signal_tension,
    signal_tension_detail,
    signaux_recents,
    signal_v2_methode
FROM finess_gestionnaire
WHERE COALESCE(signal_tension, FALSE) = FALSE
  AND COALESCE(nb_etablissements, 0) <= 10
  AND signal_v2_methode IS NULL
  AND EXISTS (
      SELECT 1 FROM finess_etablissement e
      WHERE e.id_gestionnaire = finess_gestionnaire.id_gestionnaire
        AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  )
ORDER BY random()
LIMIT 10;

-- Taille de la population d'angle mort structurel pour contextualiser le sondage
SELECT COUNT(*) AS nb_population_angle_mort_structurel
FROM finess_gestionnaire
WHERE COALESCE(signal_tension, FALSE) = FALSE
  AND COALESCE(nb_etablissements, 0) <= 10
  AND signal_v2_methode IS NULL
  AND EXISTS (
      SELECT 1 FROM finess_etablissement e
      WHERE e.id_gestionnaire = finess_gestionnaire.id_gestionnaire
        AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
  );
```

## Annexe C — Protocole minimal de revue manuelle

Pour chaque cas tiré dans les échantillons de validation, consigner au minimum :

| Champ | Valeur attendue |
|-------|-----------------|
| `id_gestionnaire` | Identifiant FINESS gestionnaire |
| `raison_sociale` | Libellé en base |
| `taille` | `nb_etablissements` |
| `échantillon` | `keywords_v1_excluded` ou `angle_mort_structurel` |
| `lecture_humaine_signal` | `oui` / `non` |
| `axe_principal` | `financier` / `rh` / `qualite` / `juridique` / `aucun` |
| `niveau_confiance_humain` | `fort` / `moyen` / `faible` |
| `source_verifiee` | URL, article, connaissance terrain, ou `aucune` |
| `commentaire` | pourquoi le système a raison ou tort |

Règle d'interprétation :

- si les 20 `keywords_v1_excluded` ne contiennent presque aucun vrai cas individuel, la logique d'exclusion est saine ;
- si les 10 cas d'angle mort structurel révèlent plusieurs cas réels, le système a une limite de couverture forte sur les petites structures silencieuses ;
- ce taux n'est pas un taux global de faux négatifs, mais une estimation locale de la zone aveugle résiduelle du dispositif.

## Annexe D — État d'avancement opérationnel (22/03/2026)

### D.1 Ce qui a été effectivement exécuté

1. **Migration V2 appliquée**
  - script : `scripts_sql/16_ajouter_signaux_v2.sql`
  - colonnes ajoutées et vues créées (`v_gestionnaires_signaux_v2`)

2. **Backfill keywords initial exécuté**
  - script : `scripts_sql/17_backfill_signaux_v2_keywords.sql`
  - première qualification automatique des axes V2

3. **Recalibration stricte exécutée**
  - script : `scripts_sql/18_recalibrate_signaux_v2_keywords_strict.sql`
  - objectif : réduire les faux positifs liés aux phrases neutres du type
    "aucune information sur ..." contenant des mots-clés techniques

4. **Validation SQL exécutée**
  - scripts :
    - `scripts_sql/analyse_signaux_v2_validation.sql`
    - `scripts_sql/analyse_angles_morts_v1_pre_v2.sql`

5. **Batch QA 20/20/10 généré pour revue humaine**
  - fichier CSV : `outputs/qa_v2_review_20_20_10_20260322_192429.csv`
  - mémo batch : `outputs/qa_v2_review_20_20_10_20260322_192429.md`
  - composition : 20 `keywords_v1`, 20 `keywords_v1_excluded`, 10 angle mort structurel

6. **Corrections QA intégrées dans la règle stricte + nouveau lot prérempli**
  - script ajusté : `scripts_sql/18_recalibrate_signaux_v2_keywords_strict.sql`
  - corrections vérifiées sur cas réels :
    - `930110036` (tensions financières + conflits) -> financier + RH
    - `140024449` (menace de fermeture + conflit) -> financier + RH (+ juridique si présent)
    - `350025649` (fermeture de service) -> reclassé en `keywords_v1` (non exclu)
  - nouveau lot QA prérempli :
    - `outputs/qa_v2_review_20_20_10_corrected_prefill_20260322_193302.csv`
    - `outputs/qa_v2_review_20_20_10_corrected_prefill_20260322_193302.md`

### D.2 KPI observés après recalibration stricte

| Indicateur | Valeur |
|-----------|--------|
| `keywords_v1` | 957 |
| `keywords_v1_excluded` | 9 584 |
| `signal_v2_methode IS NULL` | 6 128 |
| `signal_financier = TRUE` | 571 |
| `signal_rh = TRUE` | 322 |
| `signal_qualite = TRUE` | 76 |
| `signal_juridique = TRUE` | 134 |
| `au moins 1 axe V2` | 957 |
| `multi-axes` | 79 |
| angle mort structurel (`signal_tension=FALSE`, `nb_et<=10`, `signal_v2_methode IS NULL`) | 612 |

### D.3 Lecture rapide des résultats

- Le pipeline V2 est **opérationnel techniquement** (migration + backfill + requêtes de validation).
- Les corrections QA ont renforcé la détection financière (notamment sur formulations "tensions financières" et "menace de fermeture").
- Le volume `keywords_v1` augmente fortement (957) : c'est cohérent avec la nouvelle règle, mais cela implique une **revue qualité prioritaire** sur les nouveaux financiers détectés.
- Une zone aveugle structurelle subsiste (612 cas dans la définition actuelle) : elle est mesurée, mais pas encore réduite par une seconde passe ciblée.

### D.4 Exécution Cloud Run réelle du 22/03 au soir

Un batch réel Passe B a ensuite été exécuté sur Cloud Run avec un lot de 100 gestionnaires.
L'exécution la plus récente validée est `finess-signaux-v2-passb-zl589`, terminée avec succès
en environ 10 minutes. Le point bloquant n'est donc plus l'infrastructure, mais la qualité de
qualification produite.

Analyse isolée sur la fenêtre réelle du run (`signal_v2_methode = 'serper_passe_b'`,
`signal_v2_date` entre `2026-03-22 21:33` et `21:44`) :

| Indicateur | Valeur observée |
|-----------|-----------------|
| gestionnaires mis à jour | 107 |
| `confiance = haute` | 50 |
| `confiance = moyenne` | 47 |
| `confiance = basse` | 10 |
| `signal_financier = TRUE` | 54 |
| `signal_rh = TRUE` | 70 |
| `signal_qualite = TRUE` | 57 |
| `signal_juridique = TRUE` | 36 |
| au moins 2 axes | 74 |
| au moins 3 axes | 34 |
| 4 axes | 8 |
| 0 axe malgré écriture Passe B | 6 |

Lecture métier : ces ratios restent **trop élevés pour une passe de qualification supposée
spécifique et discriminante**. Le volume de cas multi-axes, en particulier sur des entités de
taille moyenne ou petite, suggère encore une inflation de signaux par agrégation de snippets
hétérogènes.

### D.5 Limite principale observée : le LLM ne reste pas assez spécifique au gestionnaire

Le défaut central mis en évidence par le batch réel n'est pas seulement un problème de mots-clés.
La qualification LLM agrège encore trop facilement des éléments qui ne documentent pas la
**situation propre du gestionnaire FINESS ciblé**.

Trois dérives se cumulent :

1. **Confusion entre gestionnaire exact et groupe élargi**
  - un snippet peut parler du groupe, d'une filiale, d'une entité soeur, d'un rapprochement ou
    d'un sauvetage externe ; le LLM le rattache ensuite au gestionnaire traité comme s'il s'agissait
    d'un fait propre à sa situation.
  - cas emblématique : `440061901` `VYV3 PAYS DE LA LOIRE`, pour lequel le financier retenu est
    une **augmentation de trésorerie** du groupe VYV, tandis que l'axe juridique reprend un
    redressement Fedosad traité par une autre entité du groupe.

2. **Généralisation abusive d'un signal local ou limité**
  - fermeture d'un établissement, rapport d'inspection local, contentieux ponctuel ou conflit
    limité à un site : le LLM remonte le fait au niveau du gestionnaire entier, sans vérifier si
    l'événement révèle réellement une tension structurelle de l'organisation.
  - le problème est particulièrement visible quand un gestionnaire de 7 à 20 établissements
    ressort avec 3 ou 4 axes à partir d'un petit nombre d'indices faibles.

3. **Mélange entre signal défavorable réel et contexte descriptif ou positif**
  - des formulations de type `trésorerie positive`, `augmentation de la trésorerie`,
    `sauvetage`, `maintien de l'emploi`, `rapport d'inspection publié`, `prud'hommes`,
    `Cour d'appel` restent parfois traitées comme preuves de tension alors qu'elles sont neutres,
    positives, indirectes ou trop génériques.

### D.6 Exemples concrets de faux rattachement ou de surqualification

Exemples relevés dans le batch QA et dans le run Cloud Run réel :

1. **`VYV3 PAYS DE LA LOIRE` (`440061901`)**
  - financier : faux positif manifeste sur `augmentation de la trésorerie (+460 M€)` ; le signal
    devrait être **écarté**, pas retenu.
  - juridique : rattachement indirect à Fedosad via une autre entité du groupe ; ce n'est pas une
    preuve suffisante d'un risque juridique propre au gestionnaire ciblé.
  - qualité : `cessation d'activité de 5 domiciles collectifs du groupe VYV` est trop global et
    insuffisamment attribué au gestionnaire précis.

2. **Cas multi-axes à 4 axes sur entités modestes**
  - le run isolé a fait ressortir plusieurs cas à 4 axes sur des structures autour de 7 à 19
    établissements (`ASSOCIATION EMERGENCE[S]`, `ATHERBEA`, `MSA SERVICES LIMOUSIN`,
    `DIRECTION DE LA VIE FAMILIALE ET SOCIALE`, `CH SENS`).
  - ce profil est possible dans l'absolu, mais sa fréquence est trop élevée pour être crédible à ce
    stade ; elle signale plutôt une **accumulation opportuniste de signaux faibles**.

3. **Axe juridique encore trop permissif**
  - tout litige, recours, prud'hommes, mention de cour d'appel ou contentieux administratif ne
    doit pas devenir un `signal_juridique = TRUE`.
  - en pratique, l'axe juridique reste déclenché par des éléments procéduraux ordinaires sans lien
    démontré avec une difficulté structurelle du gestionnaire.

### D.7 Diagnostic consolidé sur la qualification LLM

Le constat à ce stade est net : **le LLM est utile pour reformuler et synthétiser, mais il n'est
pas encore assez fiable comme arbitre principal de la portée du signal**.

Plus précisément, il échoue encore sur quatre tâches distinctes :

1. **Résolution d'entité**
  - distinguer le gestionnaire FINESS exact de son groupe, d'une marque ombrelle, d'une filiale,
    d'un établissement ou d'un partenaire.

2. **Qualification de portée**
  - distinguer un problème local, limité à un site ou à une opération ponctuelle, d'un signal
    réellement généralisable au gestionnaire.

3. **Qualification de polarité**
  - distinguer une information défavorable d'un fait neutre, descriptif ou favorable contenant des
    mots-clés trompeurs.

4. **Discipline multi-axes**
  - ne pas convertir un même faisceau de snippets ambigus en 3 ou 4 axes simultanés.

En conséquence, la Passe B actuelle ne doit pas être considérée comme une couche de décision
autonome. Elle doit être recadrée comme une couche de **proposition** soumise à des garde-fous
déterministes plus stricts.

### D.8 Pistes correctives prioritaires avant relance large

#### Piste 1 — Ajouter une étape explicite de contrôle de portée avant toute qualification d'axe

Cette piste reste la bonne réponse architecturale, mais **pas sous la forme d'un unique appel
LLM qui doit résoudre l'entité et qualifier le signal en même temps**. Dans ce format, le modèle
tend à sacrifier la résolution d'entité au profit de la qualification métier.

L'option la plus propre est une séquence en **deux appels distincts** :

1. un premier appel court, auditable, qui classe chaque snippet dans une catégorie de portée ;
2. un second appel qui ne reçoit que les snippets conservés (`gestionnaire_exact` ou
  `etablissement_local` recevable) pour qualifier les axes.

Le surcoût en tokens est réel, mais le premier appel peut rester très léger : prompt court,
JSON minimal, aucune synthèse métier. En contrepartie, on gagne un point essentiel : le filtrage
de portée devient vérifiable mécaniquement avant d'autoriser la qualification.

Avant d'autoriser `financier`, `rh`, `qualite` ou `juridique`, chaque snippet doit d'abord être
classé dans une variable de portée :

- `gestionnaire_exact`
- `entite_du_groupe`
- `etablissement_local`
- `secteur_general`
- `hors_perimetre`

Règle de base : **on ne qualifie un axe négatif que si la portée est `gestionnaire_exact`, ou
éventuellement `etablissement_local` avec impact explicite sur la continuité de service du
gestionnaire**. Tout ce qui relève du groupe élargi ou du secteur doit être exclu du calcul.

#### Piste 2 — Exiger une preuve textuelle d'ancrage au gestionnaire

Cette piste est complémentaire de la précédente, mais ne suffit pas seule. Si le snippet contient
`VYV` et que le gestionnaire cible est `VYV3 PAYS DE LA LOIRE`, le LLM considérera facilement que
l'ancrage est acceptable. Il faut donc que la logique de matching soit **plus stricte que le réflexe
naturel du modèle**, d'où l'intérêt d'un contrôle de portée en amont.

Pour accepter un signal, demander au moteur de fournir :

- la mention exacte de la raison sociale ou d'un alias validé (`NOM_PUBLIC_CONNU`) ;
- la phrase de preuve ;
- le niveau de portée (`exact`, `local`, `groupe`, `hors_perimetre`).

Si le LLM ne peut pas citer explicitement l'ancrage, le signal ne doit pas être retenu. Et si la
preuve ne contient qu'un alias trop large ou ambigu, le snippet doit rester filtré tant qu'une
correspondance plus stricte n'est pas démontrée.

#### Piste 3 — Rendre l'axe juridique beaucoup plus strict

Cette piste est **immédiatement implémentable en déterministe**, avant même toute évolution du
prompt LLM.

Ne retenir l'axe juridique que pour les cas suivants :

- procédure collective propre au gestionnaire ;
- liquidation / redressement / sauvegarde ;
- administration provisoire ;
- condamnation grave ou décision de police administrative ayant un effet opérationnel direct.

À exclure par défaut :

- prud'hommes ordinaires ;
- recours administratifs ;
- contentieux civils courants ;
- simple mention d'une cour d'appel ou d'un tribunal sans gravité métier démontrée.

Règle pratique recommandée : si le seul signal juridique détecté dans les snippets est de type
`prud'hommes`, `cour d'appel`, `tribunal administratif`, `recours` ou `contentieux`, sans
co-occurrence avec `procédure collective`, `liquidation`, `redressement`, `sauvegarde` ou
`administration provisoire`, alors l'axe juridique doit être **court-circuité automatiquement**.

#### Piste 4 — Rendre l'axe financier plus spécifique et plus asymétrique

Là aussi, le bon levier immédiat est un **pré-filtrage déterministe** qui neutralise les faux
positifs flagrants avant toute lecture LLM.

Le financier doit devenir un axe **à charge**, pas un axe descriptif. Il faut exiger des preuves
de difficulté et non de simple actualité financière. À exclure par défaut :

- augmentation de trésorerie ;
- trésorerie positive ;
- retour à l'équilibre ;
- sauvetage réussi d'une autre structure ;
- données sectorielles générales sur les Ehpad ou le médico-social.

À retenir seulement si le snippet décrit explicitement le gestionnaire ciblé avec un vocabulaire
de risque ou de dégradation (`déficit`, `cessation de paiements`, `plan d'économies`,
`procédure collective`, `menace de fermeture`, `tension de trésorerie`, etc.).

Ce pré-filtrage permettrait d'éliminer immédiatement des cas comme `VYV3 PAYS DE LA LOIRE`, où
`augmentation de la trésorerie` ne doit jamais produire un axe financier positif.

#### Piste 5 — Imposer un quota de preuve par axe et plafonner les cas multi-axes

Le meilleur positionnement de cette piste est en **post-filtre déterministe**. Il est plus simple
de laisser le LLM proposer ses axes, puis de rétrograder les cas insuffisamment étayés, que de lui
demander de compter lui-même ses preuves de manière fiable.

Proposition simple avant une relance large :

- 1 axe = au moins 1 snippet fort et bien rattaché ;
- 2 axes = 2 preuves distinctes ;
- 3 axes ou 4 axes = au moins 3 preuves distinctes, provenant idéalement de sources ou URLs
  différentes.

Sans ce quota, les cas 3-4 axes doivent être rétrogradés en `confiance = basse` avec
`review_required = TRUE` côté QA automatique, ou exclus du lot de confiance haute.

#### Piste 6 — Sortir du tout-ou-rien booléen pour les cas ambigus

L'idée de conserver les cas ambigus est bonne, mais **sans introduire un troisième état logique**
dans toutes les vues et requêtes aval. Le schéma actuel peut rester lisible avec :

- les 4 booléens existants ;
- `signal_v2_confiance = 'basse'` pour les cas douteux ;
- un champ textuel additionnel de traçabilité, par exemple `signal_v2_scope_issue TEXT`, pour
  documenter les cas ambigus (`groupe_non_imputable`, `ancrage_faible`, `signal_local_limite`).

Cette option préserve l'information utile sans propager un nouveau modèle à trois états dans toute
la couche SQL et dans les exports QA.

### D.9 Décision opérationnelle issue du batch réel

Conclusion opérationnelle au 22/03 au soir :

- **l'infrastructure Cloud Run est valide** ;
- **la qualité de qualification métier ne l'est pas encore assez** pour lancer sereinement toute la
  population restante de Passe B ;
- la relance large doit donc être conditionnée à un durcissement supplémentaire des garde-fous sur
  la portée gestionnaire et sur la discipline multi-axes.

En l'état, le principal risque n'est plus le faux négatif pur, mais le **faux positif de
rattachement** : un signal réel existe quelque part dans l'écosystème du groupe, mais il n'est pas
attribuable avec assez de précision au gestionnaire FINESS analysé.

Conséquence pratique : le problème de qualification paraît **soluble par des garde-fous
déterministes supplémentaires**, plus que par un simple changement de modèle LLM. L'effort doit
porter d'abord sur le filtrage, le cadrage de portée et la discipline de post-traitement.

### D.10 Ce qui reste à faire (priorité)

1. **Implémenter d'abord les garde-fous déterministes à faible coût**
  - pré-filtrage juridique (exclusions des contentieux ordinaires sans gravité structurante)
  - pré-filtrage financier (neutralisateurs de polarité positive ou descriptive)
  - post-filtrage multi-axes (rétrogradation des cas 3+ axes insuffisamment prouvés)

2. **QA manuelle ciblée sur le run Cloud Run réel**
  - isoler et relire un sous-lot des cas `serper_passe_b` du run `zl589`
  - prioriser tous les cas `4 axes`, puis un échantillon des `3 axes`
  - vérifier séparément les cas de groupes multi-entités (`VYV`, `ADEF`, fondations nationales)

3. **Tester ensuite la passe de portée en deux appels (expérimentation restreinte)**
  - appel 1 court : classification de portée snippet par snippet
  - appel 2 : qualification métier uniquement sur snippets filtrés
  - mesurer le gain de précision sur un lot de 20 à 30 gestionnaires avant industrialisation

4. **Conserver un schéma simple pour les cas ambigus**
  - utiliser `signal_v2_confiance = 'basse'`
  - ajouter si besoin un champ de traçabilité du type `signal_v2_scope_issue`
  - éviter l'introduction d'états intermédiaires supplémentaires dans les vues aval

5. **Seulement après cela, relancer un batch Cloud Run de validation**
  - sur le même périmètre fonctionnel que `zl589`
  - comparer les métriques avant/après : répartition par axe, volume de `3+ axes`, cas `4 axes`,
    et part des faux positifs de rattachement

6. **N'envisager la campagne large qu'après comparaison concluante**
  - l'infrastructure est prête
  - le go/no-go doit désormais dépendre uniquement de la qualité observée après garde-fous

### D.11 Brief de reprise pour une nouvelle conversation

Si reprise dans une nouvelle session, point de départ recommandé :

> "La migration V2 et les recalibrations SQL sont faites, et Cloud Run fonctionne.
> Un batch réel Passe B (`zl589`) a toutefois montré un défaut persistant de qualification LLM :
> trop de signaux rattachés au groupe au lieu du gestionnaire exact, trop de généralisation de
> signaux locaux, et trop de cas multi-axes. On veut maintenant durcir la portée gestionnaire,
> resserrer financier/juridique, puis rerun un lot de validation avant toute campagne large."

### D.12 Résultat Passe B complémentaire (23/03/2026 matin)

Un run Cloud Run unique a été relancé avec `batch-offset=0`, `batch-size=2200`,
`scope_filter_llm=true` et timeout élargi à 6h.

**Statut d'exécution**

- exécution : `finess-signaux-v2-passb-zfw7t`
- statut : `Completed=True`
- durée : ~1h24
- résultat Cloud Run : succès technique (`succeededCount=1`)

**Effet réellement observé en base (fenêtre de run)**

- gestionnaires mis à jour : `481`
- confiance : `haute=127`, `moyenne=239`, `basse=115`

**Diagnostic racine (pourquoi 481 et non 2200)**

La requête `fetch_candidates_b` ne cible pas l'ensemble des restants : elle ne prend que les
gestionnaires satisfaisant au moins un critère prioritaire (`nb_etablissements > 50`,
`signal_v2_methode='keywords_v1'`, ou type V1 sensible `fermeture|conflit|inspection`) puis
exclut ceux déjà marqués `serper_passe_b`.

Comptages observés :

- pool prioritaire total : `1894`
- déjà en `serper_passe_b` : quasi totalité
- encore éligibles non traités au moment du diagnostic : `1`

Conclusion : ce run n'a pas « échoué à couvrir 2200 », il a **épuisé ce qui restait dans le
pool prioritaire actuel**. Les autres restants (`signal_v2_methode` nul) sont majoritairement
hors périmètre de sélection de la Passe B actuelle, surtout des petites structures.

### D.13 Stratégie exhaustive révisée (objectif: couvrir TOUS les restants)

Le système doit devenir **exhaustif en couverture**, tout en restant sobre en requêtes coûteuses.
La bonne approche est un entonnoir en trois niveaux :

1. couverture universelle légère (tous les restants),
2. approfondissement ciblé (seulement signaux potentiels),
3. qualification stricte avec garde-fous d'imputabilité.

#### D.13.1 Principes de décision

- on accepte qu'une part importante des gestionnaires n'ait aucun signal public exploitable ;
- absence d'information ou actualité banale doit être une sortie valide (`pas_de_signal_probant`) ;
- un gestionnaire est considéré **couvert** dès la fin de G0 si aucun signal potentiel n'est détecté ;
- un signal n'est retenu que s'il est imputable au gestionnaire cible et non au secteur/groupe voisin.

#### D.13.2 Imputabilité sans dépendre SIREN/SIRET/FINESS dans les sources

Les identifiants administratifs sont rarement présents dans les snippets publics ; ils ne doivent
pas être un prérequis. Le rattachement doit reposer sur un faisceau d'indices textuels et contextuels.

Score d'imputabilité recommandé (déterministe, avant ou après LLM) :

- **Alias fort** : raison sociale canonique, sigle, nom public connu, alias historiques.
- **Alias faible** : tokens partiels uniquement si co-présence de contexte local.
- **Contexte sectoriel** : cohérence avec le type d'activité (MECS, EHPAD, IME, etc.).
- **Contexte géographique** : le département est un indice utile mais non suffisant (beaucoup de
  gestionnaires couvrent plusieurs départements).
- **Conflit d'entité** : présence explicite d'une autre entité homonyme ou d'un groupe tiers.

Décision de portée par snippet :

- `gestionnaire_exact`
- `etablissement_local`
- `entite_du_groupe`
- `secteur_general`
- `hors_perimetre`

Règle : seuls `gestionnaire_exact` (+ certains `etablissement_local` à impact explicite) peuvent
alimenter les axes V2.

Niveau d'imputabilité recommandé (complémentaire au scope) :

- `gestionnaire_certain` : alias fort + cohérence sectorielle + au moins un indice contextuel
  (géographie, structure locale, source explicite) sans conflit d'entité.
- `gestionnaire_probable` : alias acceptable + cohérence partielle, mais un doute résiduel subsiste
  (ex. homonymie possible, géographie ambiguë, mention groupe).

Règle d'usage :

- `gestionnaire_certain` peut alimenter automatiquement les axes ;
- `gestionnaire_probable` peut alimenter les axes jusqu'à `confiance = moyenne` (jamais `haute`) ;
- `gestionnaire_probable` peut forcer `review_required` selon le niveau de risque ;
- passage `gestionnaire_probable` -> `gestionnaire_certain` : exiger au moins **1 indice fort supplémentaire** ;
- en cas de conflit fort d'entité, classer `hors_perimetre`.

#### D.13.3 Plan de requêtage global (exhaustif mais contrôlé)

**Passe G0 — Discovery universelle (TOUS les restants, 1 requête)**

- but : détecter existence de présence web exploitable ;
- requête courte orientée identité (`"{nom_public}"` + éventuellement variante locale) ;
- sortie : `no_web`, `web_banal`, `web_suspect`.

**Passe G1 — Criblage tension (seulement `web_suspect`, 1 à 2 requêtes)**

- requêtes risque (financier/juridique/qualité/rh grave) ;
- scoring déterministe (mots-clés négatifs, source, fraîcheur, bruit sectoriel) ;
- si score faible : classer `pas_de_signal_probant` sans LLM.

**Passe G2 — Qualification approfondie (seulement score moyen/haut)**

- LLM + garde-fous déterministes (portée, polarité, multi-axes, review flag) ;
- production des 4 axes, détails, sources, confiance, période.

#### D.13.4 Stockage systématique des snippets (recommandé)

La partie coûteuse est Serper ; il faut rendre chaque run réutilisable et auditable.

Recommandation : stocker chaque résultat de snippet en base avec granularité fine.

Politique de rétention retenue : **90 jours** (cache d'analyse et d'audit).

Table proposée (exemple) : `finess_signal_v2_snippet`

- `id` (PK)
- `id_gestionnaire`
- `run_id` (identifiant batch/exécution)
- `phase` (`G0|G1|G2|passe_a|passe_b`)
- `query_text`
- `url`, `domain`, `title`, `snippet`
- `published_at` (si détectable)
- `serper_rank`, `retrieved_at`
- `alias_hit_type` (`strong|weak|none`)
- `scope_label` (`gestionnaire_exact|...`)
- `scope_score`, `risk_score`
- `used_for_decision` (bool)

Bénéfices :

- réduction des re-queries inutiles ;
- auditabilité complète des décisions ;
- recalibration offline des règles sans repayer Serper ;
- QA humaine plus rapide (preuve traçable).

#### D.13.5 Sorties métier finales (pour éviter les zones grises)

Ajouter un statut de décision explicite (indépendant des 4 axes) :

- `no_signal_public`
- `signal_public_non_tension`
- `signal_tension_probable`
- `signal_ambigu_review`

Ce statut permet l'exhaustivité opérationnelle : chaque gestionnaire restant obtient une issue
explicite, même en absence de signal.

#### D.13.6 Gouvernance coût / qualité

- fixer un budget max de requêtes/jour et une priorité par segment (taille, secteur, historique) ;
- suivre un tableau de bord hebdomadaire :
  - taux de couverture des restants,
  - taux `web_suspect`,
  - taux de confirmation après G2,
  - part rejetée pour non-imputabilité,
  - précision QA sur échantillon.

#### D.13.7 Décision de cadrage avant implémentation

Le système actuel répond bien à un objectif de **priorisation**. Pour atteindre l'objectif
d'**exhaustivité**, il faut explicitement ajouter la couche G0 universelle et le statut de sortie
`pas_de_signal_probant`, puis n'intensifier les requêtes qu'en cas de signal potentiel.

Cette évolution conserve l'équilibre recherché :

- couverture exhaustive des restants,
- coût Serper piloté,
- réduction des faux positifs de rattachement.

#### D.13.8 Paramètres validés (arbitrages produits)

1. **Couverture** : un dossier est couvert après G0 si aucun signal potentiel n'est détecté.
2. **Rétention snippets** : 90 jours.
3. **Géographie** : le département est un indice faible/complémentaire, jamais une preuve à lui seul.
4. **Imputabilité** : introduire `gestionnaire_probable` vs `gestionnaire_certain` pour maîtriser
  l'équilibre précision/exhaustivité.
5. **Confiance probable** : plafonnée à `moyenne` (pas de `haute`).
6. **Promotion probable -> certain** : nécessite 1 indice fort additionnel.

---

*Fin du document*
