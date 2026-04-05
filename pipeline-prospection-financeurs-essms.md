# Pipeline de prospection et d'enrichissement des contacts financeurs ESSMS

## Objectif

Identifier et enrichir les contacts clés chez les trois catégories de financeurs/tarificateurs ESSMS : les **conseils départementaux** (101), les **directions interrégionales de la PJJ** (9) et les **agences régionales de santé** (18 ARS métropole + outre-mer). Pour chaque entité, on cible un nombre restreint de contacts stratégiques (décideurs et responsables tarification), on retrouve leur nom, leur profil LinkedIn et on reconstitue leur adresse email professionnelle.

---

## 1. Périmètre des entités cibles

### 1.1 Conseils départementaux (101 départements)

**Source de référence pour la liste :** `lannuaire.service-public.fr/navigation/cg`

Pour chaque département, on identifie le site web officiel (`www.{nom-departement}.fr` ou variantes) et on cible les postes suivants dans la DGA Solidarités (ou équivalent) :

| Niveau | Postes cibles (variantes de titre) |
|--------|-----------------------------------|
| DGA | DGA Solidarités, DGA Solidarité et Prévention, DGA Action Sociale, DGA des Solidarités et de la Cohésion Sociale, DGA Solidarités Santé, DGAS |
| Direction Autonomie | Directeur/rice Autonomie, Directeur PA-PH, Directeur Personnes Âgées Personnes Handicapées, Directeur de l'Autonomie et du Handicap |
| Direction Enfance | Directeur/rice Enfance Famille, Directeur ASE, Directeur Enfance et Famille, Directeur Protection de l'Enfance, Directeur Enfance Jeunesse |
| Directeurs adjoints | Adjoint(e) au DGA Solidarités, Directeur adjoint Autonomie, Directeur adjoint Enfance |
| Responsable tarification | Chef de service tarification, Responsable pôle tarification, Chef de service financement des ESSMS, Responsable pilotage de l'offre, Chef du service pilotage budgétaire et tarification, Responsable financement et qualité, Chef de service offre médico-sociale |

**Attention :** certains départements (notamment les métropoles comme Lyon, qui exerce les compétences départementales) ont des organigrammes atypiques. Quelques grands départements peuvent avoir des DGA séparées Autonomie et Enfance plutôt qu'une DGA Solidarités unique.

### 1.2 Directions interrégionales de la PJJ (9 DIRPJJ)

**Liste exhaustive :**

| DIRPJJ | Siège | Domaine email |
|--------|-------|---------------|
| Île-de-France et Outre-Mer | Paris | dirpjj-idf-om@justice.fr |
| Grand-Nord | Lille | dirpjj-grand-nord@justice.fr |
| Grand-Ouest | Rennes | dirpjj-grand-ouest@justice.fr |
| Grand-Est | Nancy | dirpjj-grand-est@justice.fr |
| Grand-Centre | Dijon (anciennement Orléans) | dirpjj-grand-centre@justice.fr |
| Centre-Est | Lyon | dirpjj-centre-est@justice.fr |
| Sud-Ouest | Bordeaux | dirpjj-sud-ouest@justice.fr |
| Sud | Toulouse (Labège) | dirpjj-sud@justice.fr |
| Sud-Est | Marseille | dirpjj-sud-est@justice.fr |

Pour chaque DIRPJJ, on cible :

| Niveau | Postes cibles |
|--------|---------------|
| Direction | Directeur/rice interrégional(e) (DIRA) |
| DEPAFI | Directeur/rice de l'évaluation, de la programmation et des affaires financières (DEPAFI) |
| Tarification SAH | Responsable du bureau du secteur associatif habilité (au sein du pôle Affaires Financières de la DEPAFI), ou Tarificateur/rice SAH |

**Spécificité PJJ :** La tarification ici concerne le **secteur associatif habilité** (SAH). L'organigramme type d'une DIRPJJ comprend trois directions internes : la DPEA (politiques éducatives et audit), la DRH et la DEPAFI. C'est dans la DEPAFI, au sein du pôle Affaires Financières, que se trouve le responsable tarification. Le domaine email est systématiquement `@justice.fr` pour les agents, avec en général un pattern `prenom.nom@justice.fr`.

### 1.3 Agences régionales de santé (18 ARS)

**Liste des 18 ARS :** Auvergne-Rhône-Alpes, Bourgogne-Franche-Comté, Bretagne, Centre-Val de Loire, Corse, Grand Est, Guadeloupe, Guyane, Hauts-de-France, Île-de-France, La Réunion, Martinique, Mayotte, Normandie, Nouvelle-Aquitaine, Occitanie, Pays de la Loire, Provence-Alpes-Côte d'Azur.

Pour chaque ARS, on cible dans la direction en charge du médico-social :

| Niveau | Postes cibles (variantes de titre) |
|--------|-----------------------------------|
| Direction | Directeur/rice de l'offre médico-sociale, Directeur de l'autonomie, Directeur de l'offre de soins et de l'autonomie, Directeur de l'accompagnement et de l'offre médico-sociale |
| Direction adjointe | Directeur adjoint offre médico-sociale, Directeur adjoint autonomie |
| Responsable tarification | Chef de service tarification, Chef du pôle financement / allocation de ressources, Responsable du service pilotage budgétaire médico-social, Chef de service programmation et financement de l'offre médico-sociale, Chef de département efficience |

**Spécificité ARS :** L'intitulé de la direction varie fortement d'une ARS à l'autre. Certaines ARS fusionnent le sanitaire et le médico-social dans une même direction, d'autres ont une direction Autonomie distincte. Le domaine email est systématiquement `@ars.sante.fr` avec un préfixe régional (ex : `prenom.nom@ars.sante.fr`). Les adresses génériques suivent le pattern `ars-{region}-{service}@ars.sante.fr`.

---

## 2. Architecture du pipeline

### 2.1 Vue d'ensemble

```
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  PHASE 1         │    │  PHASE 2         │    │  PHASE 3         │
│  Référentiel     │───>│  Identification  │───>│  Enrichissement  │
│  des entités     │    │  des contacts    │    │  email + LinkedIn │
└──────────────────┘    └──────────────────┘    └──────────────────┘
       │                        │                        │
  Constitution de          Recherche Serper         Reconstitution
  la base entités          + qualification LLM      des emails
  (101 CD + 9 PJJ         des noms et postes       par analyse des
  + 18 ARS)                                         patterns domaine
```

### 2.2 Stack technique

| Composant | Rôle |
|-----------|------|
| **Serper API** | Recherche web (organigrammes, noms, LinkedIn, patterns email) |
| **Mistral Small Latest** | Qualification/extraction structurée des résultats de recherche |
| **Python** | Orchestration, gestion d'état, fichiers de sortie |
| **JSON/CSV** | Stockage intermédiaire et export final |

### 2.3 Gestion du rate limiting et de la robustesse

- Toutes les requêtes Serper et Mistral doivent être encapsulées dans un retry avec backoff exponentiel (3 tentatives, base 2s)
- Sauvegarde d'état après chaque entité traitée (fichier JSON de progression) pour pouvoir reprendre en cas d'interruption
- Log structuré de chaque requête (entité, requête, nb résultats, statut)
- Timeout de 30s par requête Serper, 60s par appel Mistral

---

## 3. Phase 1 — Construction du référentiel des entités

### 3.1 Départements

Constituer un fichier `departements.json` avec pour chaque département :

```json
{
  "code": "77",
  "nom": "Seine-et-Marne",
  "nom_complet": "Conseil départemental de Seine-et-Marne",
  "site_web": "www.seine-et-marne.fr",
  "domaine_email": null,
  "contacts": []
}
```

**Source du site web :** La convention la plus courante est `www.{nom}.fr` (ex : `www.essonne.fr`, `www.dordogne.fr`). Mais il y a des exceptions : `lenord.fr`, `www.haute-marne.fr`, `www.seine-et-marne.fr`. Prévoir un dictionnaire de correspondance pour les cas irréguliers, ou détecter le site officiel par une requête Serper `"conseil départemental" {nom_département} site officiel`.

### 3.2 DIRPJJ

Fichier statique `dirpjj.json` — liste fixe de 9 entités. Le domaine email est toujours `justice.fr`.

### 3.3 ARS

Fichier statique `ars.json` — liste fixe de 18 entités. Le domaine email est toujours `ars.sante.fr`. Le site de chaque ARS suit le pattern `www.{region}.ars.sante.fr`.

---

## 4. Phase 2 — Identification des contacts

### 4.1 Stratégie de recherche Serper

Pour chaque entité et chaque poste cible, lancer une requête Serper construite ainsi :

**Départements :**
```
Requête 1 (organigramme) : "conseil départemental {nom}" organigramme DGA solidarités
Requête 2 (poste ciblé)  : "conseil départemental {nom}" "directeur autonomie" OR "directeur PA-PH"
Requête 3 (LinkedIn)     : site:linkedin.com/in "conseil départemental {nom}" "directeur" "solidarités" OR "autonomie" OR "enfance"
```

**DIRPJJ :**
```
Requête 1 : "DIRPJJ {nom}" OR "direction interrégionale PJJ {siège}" directeur
Requête 2 : "DIRPJJ {nom}" DEPAFI OR "affaires financières"
Requête 3 : site:linkedin.com/in "DIRPJJ" OR "protection judiciaire jeunesse" "{siège}"
```

**ARS :**
```
Requête 1 : "ARS {région}" organigramme "offre médico-sociale" OR "autonomie"
Requête 2 : "ARS {région}" "tarification" OR "financement" médico-social chef service
Requête 3 : site:linkedin.com/in "ARS {région}" "médico-social" OR "autonomie" OR "tarification"
```

**Optimisation clé :** Pour les départements, commencer par la requête organigramme qui donne souvent un PDF ou une page listant tous les noms d'un coup. Beaucoup de départements publient leur organigramme en PDF sur leur site.

### 4.2 Extraction et qualification par LLM (Mistral)

Chaque résultat Serper (snippets + titres de pages) est envoyé à Mistral Small avec un prompt structuré :

```
Tu es un assistant spécialisé dans l'analyse d'organigrammes administratifs français.
À partir des extraits de résultats de recherche ci-dessous, identifie les personnes 
occupant les postes suivants au sein du {type_entité} de {nom_entité} :

Postes recherchés :
- {liste_des_postes_avec_variantes}

Pour chaque personne trouvée, extrais :
- nom_complet : (prénom et nom)
- poste_exact : (intitulé tel qu'il apparaît)
- source : (URL d'où provient l'information)
- confiance : (haute/moyenne/basse selon la fraîcheur et la fiabilité de la source)

Résultats de recherche :
{snippets}

Réponds UNIQUEMENT en JSON valide, sans commentaire.
```

**Niveau de confiance :**
- **Haute** : organigramme officiel du site institutionnel, daté de moins d'un an
- **Moyenne** : LinkedIn, article de presse, annuaire tiers, annonce de nomination
- **Basse** : source ancienne (>2 ans), blog, forum, source indirecte

### 4.3 Recherche LinkedIn dédiée

Pour chaque contact identifié (nom + poste + entité), lancer une requête Serper ciblée :

```
site:linkedin.com/in "{prénom} {nom}" "{entité}"
```

Si pas de résultat :
```
site:linkedin.com/in "{prénom} {nom}" "{poste_simplifié}"
```

Stocker l'URL LinkedIn si trouvée, `null` sinon.

---

## 5. Phase 3 — Reconstitution des emails

### 5.1 Stratégie en deux temps

**Temps 1 : Identifier le pattern email du domaine**

Pour chaque entité, on cherche des exemples d'emails de personnes (pas des adresses génériques) afin de déduire le pattern.

```
Requête Serper : "@{domaine}" -"contact@" -"accueil@" -"dpo@" -"direction@" -"drh@" -"communication@" -"webmaster@"
```

Par exemple pour le CD de l'Essonne : `"@essonne.fr" -"contact@" -"accueil@"`

Cette recherche remonte typiquement des adresses dans des signatures de courriers publics, des avis de marchés publics, des actes administratifs, des convocations, etc.

**Temps 2 : Qualifier le pattern par LLM**

Envoyer les résultats à Mistral avec ce prompt :

```
À partir des adresses email trouvées ci-dessous pour le domaine {domaine}, 
identifie le pattern de construction des adresses email nominatives.

Adresses trouvées :
{liste_emails}

Détermine le pattern parmi :
- prenom.nom@{domaine}
- pnom@{domaine} (première lettre + nom)
- p.nom@{domaine}
- nom.prenom@{domaine}
- prenom-nom@{domaine}
- premiere_lettre_prenom.nom@{domaine}
- autre (préciser)

Indique aussi si les accents sont conservés ou supprimés, 
et si les tirets dans les noms composés sont conservés ou remplacés par un point/supprimés.

Réponds en JSON : { "pattern": "...", "accents": "conservés|supprimés", "tirets_noms": "conservés|point|supprimés", "exemples_trouvés": [...], "confiance": "haute|moyenne|basse" }
```

### 5.2 Reconstruction de l'email du contact

Une fois le pattern connu, reconstruire l'email pour chaque contact. Gérer les cas particuliers :

**Noms composés :** Jean-Pierre Dupont → tester :
- `jean-pierre.dupont@domaine.fr` (tiret conservé)
- `jean.pierre.dupont@domaine.fr` (tiret remplacé par point)
- `jeanpierre.dupont@domaine.fr` (tiret supprimé)
- `jp.dupont@domaine.fr` (initiales)

**Prénoms composés :** Marie-Hélène → variantes similaires

**Accents :** Hélène → `helene` (99% des domaines suppriment les accents)

**Particules :** de, du, le, la → tester avec et sans (ex : `xavier.delaporte@` vs `xavier.de-laporte@`)

**Règle de fallback :** Si le pattern n'a pas pu être identifié avec confiance haute, utiliser `prenom.nom@domaine` comme défaut (c'est de loin le plus courant dans les collectivités et administrations françaises).

### 5.3 Validation croisée de l'email (optionnel mais recommandé)

Pour les contacts prioritaires, vérifier l'email reconstruit par une requête Serper :

```
"{prenom.nom}@{domaine}"
```

Si l'email exact apparaît dans un résultat web, la confiance passe à haute.

### 5.4 Cas particulier des DIRPJJ

Le domaine est `justice.fr` pour tous les agents du ministère de la Justice. Le pattern est quasi systématiquement `prenom.nom@justice.fr`. C'est le cas le plus simple.

### 5.5 Cas particulier des ARS

Le domaine est `ars.sante.fr`. Le pattern est `prenom.nom@ars.sante.fr` (identique pour toutes les ARS, pas de préfixe régional sur les adresses nominatives). Vérifier quand même avec une recherche `"@ars.sante.fr"` pour confirmer.

---

## 6. Structure de données de sortie

### 6.1 Schéma par contact

```json
{
  "entite": {
    "type": "departement|dirpjj|ars",
    "code": "77",
    "nom": "Seine-et-Marne",
    "nom_complet": "Conseil départemental de Seine-et-Marne",
    "site_web": "www.seine-et-marne.fr",
    "domaine_email": "seine-et-marne.fr",
    "pattern_email": "prenom.nom",
    "pattern_confiance": "haute"
  },
  "contact": {
    "nom_complet": "Emmanuel Gagneux",
    "prenom": "Emmanuel",
    "nom": "Gagneux",
    "poste_exact": "Directeur Général Adjoint des Solidarités",
    "niveau": "dga",
    "email_principal": "emmanuel.gagneux@seine-et-marne.fr",
    "email_variantes": [],
    "linkedin_url": "https://www.linkedin.com/in/emmanuel-gagneux-xxxxx",
    "source_nom": "https://www.sanitaire-social.com/...",
    "confiance_nom": "haute",
    "confiance_email": "moyenne",
    "date_extraction": "2026-03-04"
  }
}
```

### 6.2 Niveaux hiérarchiques normalisés

| Code | Description | Applicable à |
|------|-------------|-------------|
| `dga` | DGA Solidarités ou équivalent | Départements |
| `direction` | Directeur thématique (Autonomie, Enfance, Offre MS) ou Directeur interrégional | CD, DIRPJJ, ARS |
| `direction_adjointe` | Directeur adjoint | CD, ARS |
| `responsable_tarification` | Chef de service / responsable tarification ou financement | CD, DIRPJJ, ARS |

### 6.3 Export final

Deux formats :
- **JSON complet** avec toutes les métadonnées et le suivi de confiance
- **CSV aplati** pour exploitation dans un CRM ou outil d'emailing, avec colonnes : `type_entite, code_entite, nom_entite, domaine_email, nom_complet, poste, niveau, email, email_variante_1, linkedin, confiance`

---

## 7. Logique d'orchestration

### 7.1 Séquencement

```
1. Charger/générer le référentiel des entités (Phase 1)
2. Pour chaque entité :
   a. Vérifier si déjà traitée (fichier de progression)
   b. Rechercher l'organigramme général (1 requête Serper)
   c. Extraire les contacts via LLM (1 appel Mistral)
   d. Pour chaque poste non trouvé, recherche ciblée (1 requête Serper par poste manquant)
   e. Recherche LinkedIn pour chaque contact trouvé (1 requête Serper par contact)
   f. Recherche pattern email du domaine (1 requête Serper par entité, mise en cache)
   g. Qualification du pattern (1 appel Mistral par entité, mise en cache)
   h. Reconstruction des emails pour chaque contact
   i. Validation croisée optionnelle (1 requête Serper par contact prioritaire)
   j. Sauvegarde de l'état
3. Export final JSON + CSV
```

### 7.2 Estimation du volume d'appels API

| Étape | Par entité | Total (128 entités) |
|-------|-----------|---------------------|
| Organigramme Serper | 1 | 128 |
| Extraction LLM | 1 | 128 |
| Postes manquants Serper | ~2 | ~256 |
| LinkedIn Serper | ~3 contacts | ~384 |
| Pattern email Serper | 1 | 128 |
| Pattern email LLM | 1 | 128 |
| Validation email Serper | ~2 | ~256 |
| **Total Serper** | | **~1 150 requêtes** |
| **Total Mistral** | | **~256 appels** |

Avec les limites Serper standard (2 500 requêtes/mois sur le plan gratuit, illimité sur les plans payants), le pipeline est exécutable en une seule passe sur un plan payant ou en 2-3 sessions sur le plan gratuit.

### 7.3 Cache et déduplication

- **Cache domaine email** : le pattern email est identique pour tous les contacts d'une même entité → ne le chercher qu'une fois
- **Cache LinkedIn** : éviter les doublons si un contact apparaît dans plusieurs recherches
- **Déduplication des contacts** : normaliser les noms (minuscules, sans accents) pour détecter les doublons provenant de sources différentes

---

## 8. Points de vigilance et pièges connus

### 8.1 Organigrammes en PDF

Beaucoup de départements et ARS publient leur organigramme uniquement en PDF. Serper ne lit pas le contenu des PDF. **Stratégie :** si la recherche organigramme ne donne que des liens PDF, noter l'URL du PDF pour traitement ultérieur (extraction manuelle ou via un parseur PDF séparé). Ne pas compter uniquement sur les snippets Serper.

### 8.2 Turnover des postes

Les DGA et directeurs changent régulièrement. **Mitigation :** privilégier les sources récentes (<1 an), croiser avec LinkedIn (qui est souvent à jour), et marquer le niveau de confiance.

### 8.3 Départements atypiques

- **Métropole de Lyon** : exerce les compétences départementales sur son périmètre, organigramme spécifique (Délégation solidarités, habitat et éducation)
- **Paris** : la Ville de Paris exerce les compétences départementales (Direction de l'Action Sociale, de l'Enfance et de la Santé — DASES)
- **Collectivités d'outre-mer** : Mayotte, Guyane, Martinique ont des collectivités territoriales uniques
- **Alsace** : la Collectivité européenne d'Alsace (CeA) regroupe le Bas-Rhin et le Haut-Rhin

### 8.4 Gestion des noms composés dans les emails

C'est le point le plus fragile du pipeline. Prévoir systématiquement 2 à 4 variantes pour tout contact ayant un prénom ou nom composé, et les tester si possible par recherche web.

### 8.5 RGPD et déontologie

Les données collectées sont des données professionnelles de fonctionnaires et agents publics dans l'exercice de leurs fonctions. Les organigrammes, noms et contacts professionnels des agents publics sont des documents administratifs communicables. L'email professionnel des agents publics n'est pas une donnée à caractère personnel au sens strict dans le contexte de la prospection B2B/B2G. Néanmoins :
- Prévoir un mécanisme de désinscription dans toute communication commerciale
- Ne jamais publier le fichier brut
- Limiter l'usage à la prospection commerciale légitime (intérêt légitime au sens RGPD)

---

## 9. Extensions possibles

### 9.1 Ajout de la CNSA

La CNSA est l'interlocuteur national de la tarification médico-sociale. Cible restreinte mais stratégique : la Direction du financement de l'offre et ses chefs de département. Domaine : `cnsa.fr`.

### 9.2 Ajout des DDETS / DREETS

Les DDETS (directions départementales) et DREETS (directions régionales) du ministère du Travail interviennent sur certains champs ESSMS (hébergement, insertion). Pourrait être un second cercle de prospection.

### 9.3 Scoring des contacts

Ajouter un score de pertinence pondérant : le niveau hiérarchique, la taille du département (budget social), la fraîcheur de l'information, la présence LinkedIn (indicateur d'ouverture au networking).

### 9.4 Détection de signaux d'achat

Enrichir le pipeline avec des recherches sur les marchés publics récents (BOAMP, plateforme des achats de l'État) pour identifier les départements et ARS ayant lancé des consultations sur des thématiques tarification, SI financier, ou analyse budgétaire ESSMS.

---

## 10. Structure de fichiers recommandée pour le projet

```
prospection-financeurs/
├── config/
│   ├── departements.json          # Référentiel des 101 départements
│   ├── dirpjj.json                # Référentiel des 9 DIRPJJ
│   ├── ars.json                   # Référentiel des 18 ARS
│   ├── postes_cibles.json         # Postes et variantes par type d'entité
│   └── settings.py                # Clés API, paramètres rate limiting
├── src/
│   ├── pipeline.py                # Orchestrateur principal
│   ├── serper_client.py           # Client Serper avec retry et cache
│   ├── mistral_client.py          # Client Mistral avec prompts structurés
│   ├── contact_finder.py          # Logique d'identification des contacts
│   ├── email_reconstructor.py     # Reconstitution et variantes email
│   ├── linkedin_finder.py         # Recherche LinkedIn
│   ├── normalizer.py              # Normalisation noms, accents, tirets
│   └── exporter.py                # Export JSON + CSV
├── data/
│   ├── progress.json              # État de progression (reprise après interruption)
│   ├── email_patterns_cache.json  # Cache des patterns email par domaine
│   └── output/
│       ├── contacts_enrichis.json # Export complet
│       └── contacts_enrichis.csv  # Export aplati CRM
├── logs/
│   └── pipeline.log
└── README.md
```
