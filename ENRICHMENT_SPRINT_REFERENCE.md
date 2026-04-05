# Référence sprint — Enrichissement final (Habitat intermédiaire seniors)

Date: 2026-01-31

Objectif
- Servir de **point de départ unique** avant l’implémentation des scripts d’enrichissement.
- Poser le **périmètre**, la **structure DB réelle**, les **champs attendus**, et le **plan de test**.
- Ajouter un volet systématique: **check public cible** + **AVP** (Aide à la Vie Partagée) pour les sous-catégories **Habitat inclusif** et **Habitat intergénérationnel**.

---

## 1) Périmètre du sprint

### 1.1 Ce qu’on enrichit
On vise 2 familles de données:

1) Données “résumé établissement”
- Cohérence éditoriale, complétude, qualité de la description
- Public cible (normalisé)
- Statut AVP (éligibilité) + détails AVP quand vérifié

2) Données “tables d’enrichissement”
- Tarification (min/max, fourchette)
- Logements (types, quelques attributs)
- Restauration (booléens)
- Services (liaisons normalisées)

### 1.2 Contraintes opérationnelles
- **Read-only d’abord**: audits + mesures de couverture avant d’ajouter des scripts write.
- **Modération en base** via `propositions` / `proposition_items` plutôt que des `UPDATE` directs.
- Respect de la contrainte de publication: `public.can_publish(id)` (voir §4).

### 1.3 Politique d’enrichissement (règles d’exécution)
Hypothèse opérationnelle: pas de contrainte bloquante sur les quotas (Serper / ScrapingBee / Gemini), donc on privilégie la qualité et la complétude.

Règles décidées:
- **Tarifications / fourchette prix**: enrichissement **systématique**, même si des données existent déjà.
	- Objectif: rafraîchir/compléter (et corriger) `tarifications`.
- **Services**: enrichissement uniquement si l’établissement a **0 ou 1 service** en base.
	- Objectif: éviter de “dégrader” les établissements déjà bien renseignés.
- **Logements**: sur le **test 2 départements**, enrichissement **systématique**.
	- Ensuite, décision à prendre pour la prod: (A) cibler “vide uniquement” ou (B) refaire systématiquement.
	- Critère: capacité à retrouver les mêmes résultats que la base actuelle + qualité/complétude obtenue.

Règles pour la **description** (si score qualité insuffisant sur l’existant):
- Générer une description en français d’**un seul paragraphe** (≈4–5 lignes), idéalement **400–700 caractères** (espaces compris).
	- Si les sources sont **pauvres** (peu d’infos spécifiques), accepter une version **plus courte** (≈220–450 caractères) plutôt que de généraliser.
- Contenu attendu: ce que l’établissement propose, ses spécificités, son cadre/environnement (ville/quartier/accès si sourcé), et les éléments concrets (logements, services, restauration, animation, public accueilli).
- Contraintes: pas d’affirmation non sourcée, pas de données sensibles, pas de vocabulaire médical inadapté, pas de comparatifs (“le meilleur”), pas d’invention de tarifs.
- Style: phrases courtes, éviter le jargon, pas de liste à puces, ton factuel/bienveillant (pas commercial).

---

## 2) Structure DB (réelle) — tables & champs clés

Ce bloc est basé sur l’audit `information_schema`.

### 2.1 `public.etablissements`
Champs clés (ceux qui impactent nos enrichissements):
- `id` (uuid)
- `nom` (text)
- `presentation` (text)
- `adresse_l1`, `adresse_l2`, `code_postal`, `commune`, `geom`
- `statut_editorial` (enum: `draft|soumis|valide|publie|archive`)
- `habitat_type` (enum: `residence|habitat_partage|logement_independant`)
- `public_cible` (text)
- `eligibilite_statut` (enum: `avp_eligible|non_eligible|a_verifier`)
- `site_web`, `telephone`, `email`, `gestionnaire`

Notes:
- `public_cible` est stocké en **texte**, souvent comme une liste jointe par virgules (ex: `personnes_agees,mixtes`).
- La “fourchette prix” n’est pas sur `etablissements`: elle vient de `tarifications` et est agrégée dans des vues (cf. `v_liste_publication`).

### 2.2 `public.tarifications`
Champs disponibles:
- `etablissement_id` (uuid)
- `fourchette_prix` (enum: `euro|deux_euros|trois_euros`)
- `prix_min`, `prix_max` (numeric)
- `loyer_base`, `charges` (numeric) (actuellement non remplis)
- `periode` (text) (actuellement non rempli)
- `source` (text), `date_observation` (date)

### 2.3 `public.logements_types`
Champs:
- `etablissement_id` (uuid)
- `libelle` (text)
- `surface_min`, `surface_max` (numeric)
- `meuble`, `pmr`, `domotique`, `plain_pied` (boolean)
- `nb_unites` (integer)

### 2.4 `public.restaurations`
Champs (booléens):
- `kitchenette`
- `resto_collectif_midi`
- `resto_collectif`
- `portage_repas`

### 2.5 `public.services` + `public.etablissement_service`
- `services`: référentiel `id`, `libelle`
- `etablissement_service`: table de liaison (`etablissement_id`, `service_id`)

### 2.6 Sous-catégories: `public.sous_categories` + `public.etablissement_sous_categorie`
- `sous_categories`: `libelle`, `alias`, (et `slug` présent en DB)
- `etablissement_sous_categorie`: liaison

### 2.7 AVP détaillé: `public.avp_infos`
C’est la table “spécifique AVP” (plus riche que le simple statut dans `etablissements`).

Champs:
- `etablissement_id` (PK)
- `statut` (enum: `intention|en_projet|ouvert`)
- `date_intention`, `date_en_projet`, `date_ouverture`
- `pvsp_fondamentaux` (jsonb) avec au moins: `animation_vie_sociale`, `gouvernance_partagee`, etc.
- `public_accueilli` (text) (important pour public cible)
- `modalites_admission` (text)
- `partenaires_principaux` (jsonb array), `intervenants` (jsonb array)
- `heures_animation_semaine` (numeric)
- `infos_complementaires` (text)

Contrainte:
- Si `statut='ouvert'`, la DB exige `date_ouverture`, un `pvsp_fondamentaux.animation_vie_sociale` non vide et `intervenants` >= 1.

### 2.8 Modération: `public.propositions` + `public.proposition_items`
- `propositions`: une “enveloppe” (cible, action, statut, payload)
- `proposition_items`: granularité colonne par colonne (`table_name`, `column_name`, `old_value`, `new_value`)

---

## 3) Audit global (baseline) — ce qu’on sait déjà

### 3.1 Couverture globale (tables)
Résultats observés sur la base:
- Total établissements `is_test=false`: **3 386**
- `restaurations`: **3 385** entrées (quasi 100%)
- `services`: **33,9%** des établissements ont au moins 1 service (1 148)
- `logements_types`: **30,7%** des établissements ont au moins 1 type (1 039)
- `tarifications`: **9,1%** des établissements ont au moins 1 ligne (309)

### 3.2 Couverture globale (champs établissement)
- `public_cible` renseigné: **99,9%**
- `habitat_type` renseigné: **98,8%**
- `eligibilite_statut` renseigné: **100%**
- `site_web` renseigné: **89,8%**
- `presentation` renseignée: **98,7%**

### 3.3 AVP (situation actuelle)
- `eligibilite_statut='avp_eligible'`: **22** établissements (faible)
- `avp_infos`: **23** lignes, mais quasi tout est vide (dates/public/intervenants)

Conclusion opérationnelle:
- Aujourd’hui, AVP est surtout un **statut “gating”** (beaucoup de `a_verifier`) mais la donnée AVP riche est quasi inexistante.
- Le sprint doit donc clarifier la méthode pour **confirmer** AVP et remplir `avp_infos` quand c’est solide.

Conséquence sur la stratégie d’enrichissement:
- **Tarifications**: couverture faible et champs détaillés peu remplis → on privilégie un enrichissement systématique (refresh).
- **Services / logements**: couverture partielle → règles “≤1 service” et “logements systématique en test” (cf. §1.3).

---

## 4) Publication & contrainte `can_publish`

La DB impose une contrainte: un établissement `publie` doit satisfaire `public.can_publish(id)`.

En pratique, pour appliquer des corrections/enrichissements sans se faire bloquer:
- Détecter le cas `statut_editorial='publie'` ET `can_publish(id)=false`
- Passer temporairement en `draft`
- Appliquer les changements
- Optionnel: tenter un republish plus tard

Ce mécanisme est critique pour éviter qu’un batch s’arrête à cause d’un seul établissement “publié mais invalide”.

### 4.1 Mécanisme “write en base” (testé) : `propositions` → `approuvee` → apply → republish

Principe
- Le pipeline **n’écrit pas** directement dans les tables métier (`etablissements`, `tarifications`, etc.).
- Il écrit des **propositions de modification** dans `propositions` + `proposition_items`.
- Un workflow applique ensuite ces propositions (après validation/approbation) et tente un republish.

Scripts utilisés (validés sur les tests 49 et 35)
1) Génération des enrichissements + propositions:
	- `scripts/enrich_dept_prototype.py`
	- Sorties fichiers:
	  - `outputs/enrich_proto_<tag>.jsonl` (debug/trace complète)
	  - `outputs/enrich_proto_logements_compare_<tag>.csv`
	  - `outputs/enrich_proto_propositions_<tag>.txt` (liste des ids de propositions créées)

2) Workflow de modération et d’application:
	- `scripts/enrich_propositions_workflow.py`
	- Sous-commandes:
	  - `stats-from-list` : stats sur une liste de propositions
	  - `approve-from-list` : passe les propositions `en_attente → approuvee` (+ items `pending → accepted`)
	  - `apply` : applique en base une sous-partie sûre et idempotente (avec dédup)
	  - `republish-from-list` : tente de repasser en `publie` uniquement si `public.can_publish(id)`
	  - `diagnose-publish-from-list` : écrit un CSV de diagnostic `can_publish` post-apply

Workflow d’exécution (commandes)
1) Créer des propositions (batch)
	- `python scripts/enrich_dept_prototype.py --departements 35 --limit 20 --write-propositions`
	- Récupérer la liste: `outputs/enrich_proto_propositions_<tag>.txt`

2) Inspecter avant application
	- `python scripts/enrich_propositions_workflow.py stats-from-list --input outputs/enrich_proto_propositions_<tag>.txt`

3) Approuver (en test: auto-approve)
	- `python scripts/enrich_propositions_workflow.py approve-from-list --input outputs/enrich_proto_propositions_<tag>.txt`

4) Appliquer
	- `python scripts/enrich_propositions_workflow.py apply --input outputs/enrich_proto_propositions_<tag>.txt`

5) Mettre en `publie` (si possible)
	- `python scripts/enrich_propositions_workflow.py republish-from-list --input outputs/enrich_proto_propositions_<tag>.txt`

6) Diagnostiquer ce qui reste non publiable
	- `python scripts/enrich_propositions_workflow.py diagnose-publish-from-list --input outputs/enrich_proto_propositions_<tag>.txt`
	- Sortie: `outputs/publish_diagnosis_enrich_<tag>.csv`

Règles clés (sécurité / idempotence)
- `apply` est conçu pour être relançable (dédup) et ne doit pas “créer à l’infini”.
- Si un établissement est `publie` mais `public.can_publish(id)=false`, le workflow d’apply le rétrograde en `draft` le temps d’appliquer, pour ne pas échouer sur la contrainte.
- `etablissement_service/create` est supporté si on peut résoudre un `service_id` existant.

---

## 5) Règles métier — Public cible & AVP (systématique sur HI/HIG)

### 5.1 Rappels normalisation public cible
Valeurs attendues (pratiques actuelles):
- `personnes_agees`
- `personnes_handicapees`
- `mixtes`
- `alzheimer_accessible`

Points d’attention:
- `mixtes` est utilisé pour l’intergénérationnel / autres publics, mais “mixte” peut aussi être PA/PH.
- Pour l’habitat inclusif, on accepte PA/PH si c’est clairement du HI, mais il faut éviter les faux positifs médico-sociaux.

Règles normalisées (validées dans le prototype)
- Si le public est **personnes âgées** → `personnes_agees`.
- Si le public est **personnes handicapées** → `personnes_handicapees`.
- Si le public est **les deux** (PA + PH) → stocker **les deux valeurs**: `personnes_agees,personnes_handicapees`.
	- Important: ne pas utiliser `mixtes` pour exprimer “PA + PH”.
- `mixtes` est réservé aux cas où les sources mentionnent explicitement d’autres publics (ex: jeunes, étudiants, publics en difficulté, intergénérationnel).
- `alzheimer_accessible` uniquement si les sources mentionnent explicitement “alzheimer”.

### 5.2 Statut AVP (dans `etablissements.eligibilite_statut`)
- `avp_eligible`: AVP confirmé
- `non_eligible`: hors critères
- `a_verifier`: indéterminé / à qualifier

Règles existantes (résumé):
- RA/RSS/MARPA/Béguinage/Village/Accueil familial: `non_eligible`
- Habitat inclusif: `a_verifier` par défaut, sauf si déjà marqué `avp_eligible`
- Intergénérationnel / colocation avec services: `avp_eligible` seulement si mention AVP explicite

### 5.3 Besoin nouveau: “AVP & public cible par enquête” (HI + intergénérationnel)
Problème: les sites officiels n’affichent pas toujours l’AVP ni le public accueilli.

Stratégie proposée (pistes de sources):
- Délibérations/arrêtés/CR de commission du Conseil Départemental (CD)
- Conférence des financeurs / appels à projets “habitat inclusif”
- PVSP (projet de vie sociale et partagée) quand publié
- Communiqués / dossiers de presse (CD, ARS, opérateurs)
- PDF filetype (brochures, conventions, rapports)

Approche “evidence ladder” (du plus fiable au moins fiable):
1) Document CD/collectivité/ARS mentionnant explicitement AVP et/ou le projet
2) PVSP (même extrait) + mention AVP
3) Page opérateur/gestionnaire explicitant AVP + public accueilli
4) Article de presse (uniquement en complément)

Règle stricte: on ne met `eligibilite_statut='avp_eligible'` que si (1)-(3) sont trouvés.

### 5.4 Mapping vers `avp_infos`
Quand AVP est confirmé:
- Créer/mettre à jour `avp_infos` (statut + dates + public_accueilli + intervenants si disponibles)
- Sinon, ne pas “inventer”: laisser vide / renseigner uniquement ce qui est factuel.

---

## 6) Plan de test (avant industrialisation)

### Phase 0 — Audit global (fait)
- Baseline couverture tables/champs
- Structure réelle confirmée (dont `avp_infos`)

### Phase A — Prototype sur 2 départements (à venir)
✅ Phase A — Prototype sur 2 départements (réalisé en dry-run sur un échantillon)

Run exécuté (échantillon):
- `scripts/enrich_dept_prototype.py --departements 45,76 --limit 5 --dry-run --out-dir outputs --sleep 0.2`
- Option (échantillon plus varié): ajouter `--sample-diverse-sous-categories` (et ajuster `--sample-pool` si besoin).
- Artefacts: `outputs/enrich_proto_<tag>.jsonl` + `outputs/enrich_proto_logements_compare_<tag>.csv`

Résultats observés sur le dernier run (tag `20260131_005834`, 10 établissements):
- **Descriptions**: les descriptions générées tendaient à être **courtes** et/ou **génériques** faute de matière → règle ajustée: cible **400–700 caractères** (1 paragraphe), et **fallback plus court** si sources pauvres.
- **Tarifs**: extraction avec montants mensuels trouvés sur 3/10 (1/5 en 45, 2/5 en 76) ; fourchette calculée en `euro|deux_euros|trois_euros`.
- **Services**: extraction non vide sur 7/10.
- **Logements**: beaucoup de bases vides (7/10) → on ne doit pas compter cela comme un mismatch (voir ci-dessous).
- **AVP HI/HIG**: 6/10 ont un statut proposé (`avp_eligible`/`a_verifier`), mais risque de faux positifs si l’évidence est trop générique (schémas départementaux, etc.) → durcissement ajouté: downgrade automatique de `avp_eligible` vers `a_verifier` si l’évidence ne mentionne pas clairement l’établissement/la commune.

✅ Phase A bis — Tests end-to-end (propositions + apply + republish + diagnostic)

Objectif
- Valider le mécanisme DB complet et la capacité à publier sans second passage manuel.

Résultats récents
- Département 49 (20 établissements): 16/20 publiables après apply+republish.
	- Principaux bloquants: `missing_gestionnaire` (3), `missing_address` (1).
	- L’extraction “publishability precheck” est mutualisée sur le même contexte et propose des `etablissement/update` quand fiable.

- Département 35 (20 établissements): 19/20 publiables après apply+republish.
	- Bloquant restant: `missing_gestionnaire` (1).
	- Les propositions `etablissement_service/create` sont appliquées (liaisons `etablissement_service`) lorsque pertinentes.

Décision (suite):
- On continue demain avec un run plus large (même départements, limit augmenté) puis arbitrage “logements systématique vs vide uniquement”.

Paramétrage attendu du prototype:
- **Tarifs**: exécution systématique sur tous les établissements du périmètre (même si `tarifications` déjà renseigné).
- **Services**: exécution uniquement sur le sous-ensemble avec **0 ou 1 service** en base.
- **Logements**: exécution systématique sur tous les établissements du périmètre, puis comparaison vs base.

Livrable de validation “logements”:
- Un rapport de comparaison (par établissement):
	- types détectés vs types existants
	- taux de match (exact/partiel)
	- divergences (types/attributs)

Critères de match (comparaison logements) — définition opérationnelle:
- Normalisation préalable (avant comparaison):
	- `libelle`: minuscule, trim, suppression ponctuation, accents normalisés, espaces multiples réduits.
	- Mapping synonymes (à figer dans le code): ex. `t1`≈`studio`, `f1`≈`studio`, `t2`≈`2 pieces`, `f2`≈`2 pieces`, `colocation`≈`habitat partage` (si présent dans les libellés).
	- Surfaces: arrondir à 0,5 m² ou 1 m² selon la granularité détectée.
- Match **exact** si:
	- même type normalisé (`libelle` normalisé égal après mapping)
	- ET (si présent) `surface_min` et `surface_max` dans une tolérance de ±2 m²
	- ET booléens (`meuble`, `pmr`, `domotique`, `plain_pied`) identiques quand ils sont renseignés côté base ET côté extraction
	- ET `nb_unites` identique si renseigné des deux côtés
- Match **partiel** si:
	- même type normalisé
	- MAIS au moins une divergence sur surfaces (au-delà tolérance) OU attributs (booléens/nb_unites) OU un champ manquant d’un côté
- **Mismatch** si:
	- aucun type base ne correspond au type extrait après mapping, ou type extrait “trop générique”/ambigu (ex: “appartements” sans précision) → à classer séparément

Sorties attendues du rapport:
- Par établissement: `match_status` (exact/partiel/mismatch/ambigu/**base_empty**), liste des paires appariées, champs divergents, types “en plus” côté extrait et côté base.

Note importante:
- `base_empty` = aucune donnée `logements_types` en base → le rapport sert ici à mesurer ce que l’extraction propose, pas à conclure à un écart.

### Phase B — Industrialisation Google Cloud Run (réalisée)

✅ **Run production complet — 2026-02-01**

Exécution
- **10 Cloud Run Jobs** en parallèle (timeout: 6h)
- Partitionnement: 95 départements répartis en 10 groupes
- Lancés: 2026-02-01 00:15 UTC
- Tous complétés avec succès (COMPLETE 1/1)

Résultats — Propositions créées
- **7695 propositions** écrites en base (statut: `en_attente`)
- **3120 établissements** distincts traités (92% de la base totale)
- Couverture par type:
  - Logements: 2963 propositions (2963 items)
  - Services: 1957 propositions (4172 items de liaison)
  - Tarifications (create): 1814 propositions (6747 items)
  - Établissements (update): 837 propositions (1188 items)
  - Tarifications (update): 124 propositions (457 items)

Workflow de modération et application
- **Commande 1** (stats): `python scripts/enrich_propositions_workflow.py stats-from-list --input outputs/cloudrun_propositions_batch.txt`
- **Commande 2** (approve): `python scripts/enrich_propositions_workflow.py approve-from-list --input outputs/cloudrun_propositions_batch.txt`
  - Résultat: 7695 propositions approuvées
- **Commande 3** (apply): `python scripts/enrich_propositions_workflow.py apply --input outputs/cloudrun_propositions_batch.txt`
  - Résultat: **7650 propositions appliquées**, **15506 items** insérés
  - 21 propositions skipped (déduplication): 16 services, 5 logements
- **Commande 4** (republish): `python scripts/enrich_propositions_workflow.py republish-from-list --input outputs/cloudrun_propositions_batch.txt`
  - Résultat: **2980/3120 établissements republiés** (95,5%)
- **Commande 5** (diagnose): `python scripts/enrich_propositions_workflow.py diagnose-publish-from-list --input outputs/cloudrun_propositions_batch.txt`
  - Sortie: `outputs/publish_diagnosis_enrich_20260201_125958.csv`

Cas non publiables (140 établissements, 4,5%)
- **92** établissements: `missing_address` (adresse non extraite/incomplète)
- **58** établissements: `missing_gestionnaire` (gestionnaire non trouvé dans les sources)
- Ces établissements restent en statut `draft` et nécessitent intervention manuelle

Conclusion opérationnelle
- **Succès massif**: 95,5% des établissements traités sont republiables
- Les 4,5% restants ont des données manquantes critiques (adresse/gestionnaire) non extractibles automatiquement
- Mécanisme de déduplication efficace (21 skips sur 7695)
- Workflow end-to-end validé en production

Contraintes
- ScrapingBee: limiter la concurrence à **≤ 10 requêtes** simultanées (abonnement).

Architecture recommandée (batch)
1) **Cloud Run Jobs** (plutôt qu’un service HTTP) pour exécuter des lots “finis”
	 - Un job = un département (ou un shard de département) + un `--limit` / ou une liste d’ids.
	 - Outputs écrits dans un bucket GCS (JSONL + CSV + liste propositions).

2) Orchestration
	 - Cloud Scheduler (ou déclenchement manuel) → lance un Cloud Run Job par département/shard.
	 - Option scalabilité: Pub/Sub + Cloud Run service “dispatcher” qui découpe en shards et déclenche des Jobs.

3) Rate-limiting ScrapingBee
	 - Le code doit limiter le nombre de fetch pages en parallèle (thread pool/async) à 10.
	 - En pratique: on peut garder `max_pages` modéré (ex: 3) et limiter la concurrence globale.
	 - Recommandation prod: ajouter un paramètre `--scrape-concurrency 10` (à implémenter si on passe en multi-thread).

4) Observabilité
	 - Logs structurés par établissement (id, statut can_publish, propositions créées, erreurs scraping).
	 - Export “résumé” par lot (compteurs + top raisons `can_publish=false`).

Mécanisme de récupération des données enrichies (pour import/publication)
- Artefacts batch (dans GCS ou `outputs/` local):
	- `enrich_proto_<tag>.jsonl`: trace complète (utile debugging / audit)
	- `enrich_proto_logements_compare_<tag>.csv`: rapport comparaison logements
	- `enrich_proto_propositions_<tag>.txt`: ids de propositions à modérer/appliquer

- Chemin “import” en base:
	- Les données “importables” sont les `propositions` + `proposition_items` déjà en DB.
	- La liste `*_propositions_<tag>.txt` sert uniquement à piloter le workflow (approve/apply/republish/diagnose).

Politique de modération
- En test on auto-approve.
- En prod on recommande:
	- auto-approve uniquement les catégories “safe” (ex: publishability address/geom lorsque score fort, services join-table, tarifs si evidence solide)
	- ou garder un mode “approve manuel” avec dashboards/exports.

Retries/backoff
- Serper/ScrapingBee/LLM: retries avec backoff exponentiel + jitter sur erreurs 429/5xx.
- Conserver un statut “a_verifier” quand les sources sont inaccessibles.

Idempotence
- Éviter la recréation infinie:
	- dédup côté `apply` (déjà en place)
	- ajouter un garde-fou côté création de propositions: ne pas créer si la valeur est identique en base, ou si une proposition identique existe déjà sur une fenêtre courte.

Politique de calcul en prod (à décider après Phase A):
- Tarifs: probablement **systématique** (refresh périodique)
- Services: règle **≤1 service** conservée sauf preuve du contraire
- Logements: décision “vide uniquement” vs “refresh systématique” selon le rapport de comparaison

---

## 7) Commandes utiles

Audit global structure/couverture:
- `C:/Users/Lenovo/ingest-habitat/.venv/Scripts/python.exe scripts/audit_enrichment_readonly.py`

Analyse tables enrichissement:
- `C:/Users/Lenovo/ingest-habitat/.venv/Scripts/python.exe check_enrichment_tables.py`

Audit URLs (read-only):
- `C:/Users/Lenovo/ingest-habitat/.venv/Scripts/python.exe scripts/analyze_suspicious_urls.py --limit 50000 --safe-sous-categories ra_rss`

Prototype enrichissement (dry-run):
- `python scripts/enrich_dept_prototype.py --departements 35 --limit 20 --dry-run`

Prototype enrichissement (write propositions):
- `python scripts/enrich_dept_prototype.py --departements 35 --limit 20 --write-propositions`

Workflow propositions (end-to-end):
- `python scripts/enrich_propositions_workflow.py stats-from-list --input outputs/enrich_proto_propositions_<tag>.txt`
- `python scripts/enrich_propositions_workflow.py approve-from-list --input outputs/enrich_proto_propositions_<tag>.txt`
- `python scripts/enrich_propositions_workflow.py apply --input outputs/enrich_proto_propositions_<tag>.txt`
- `python scripts/enrich_propositions_workflow.py republish-from-list --input outputs/enrich_proto_propositions_<tag>.txt`
- `python scripts/enrich_propositions_workflow.py diagnose-publish-from-list --input outputs/enrich_proto_propositions_<tag>.txt`
