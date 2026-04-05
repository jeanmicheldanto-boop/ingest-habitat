# Spec SQL - Interface Next.js ESSMS & Financeurs

## 1. Objectif

Ce document decrit la couche SQL cible pour alimenter l'interface Next.js.

Il ne s'agit pas du schema source complet, mais de la couche de lecture optimisee pour l'UX retenue :

- landing page avec compteurs ;
- navigation ESSMS par gestionnaires, etablissements et contacts ;
- navigation financeurs par entites, contacts et carte departementale ;
- fiches detaillees ;
- exports Excel ;
- assistant IA adosse a une bibliotheque de requetes expertes.

## 2. Principes

### 2.1 Tables sources

Bloc ESSMS :

- `finess_etablissement`
- `finess_gestionnaire`
- `finess_dirigeant`

Bloc financeurs :

- `prospection_entites`
- `prospection_contacts`
- `prospection_signaux`

### 2.2 Regles fonctionnelles structurantes

- Exclure `SAA` des logiques de travail quand on parle de volume utile ESSMS, sauf mention contraire.
- La vue `Gestionnaires` est triee par nombre d'etablissements desc.
- La carte ESSMS doit reutiliser les memes filtres que les listes.
- La carte financeurs doit fonctionner par coloration de departements, pas par points.
- Les vues front doivent eviter les jointures lourdes repetees a chaque requete.

## 3. Vues prioritaires

### 3.1 `v_ui_landing_counts`

Objectif : alimenter la landing page.

Champs attendus :

- `essms_gestionnaires_count`
- `essms_etablissements_count`
- `essms_contacts_count`
- `financeurs_entites_count`
- `financeurs_contacts_count`
- `financeurs_signaux_count` optionnel

Exemple SQL fonctionnel :

```sql
create or replace view public.v_ui_landing_counts as
select
  (select count(*) from public.finess_gestionnaire) as essms_gestionnaires_count,
  (
    select count(*)
    from public.finess_etablissement e
    where e.categorie_normalisee is distinct from 'SAA'
  ) as essms_etablissements_count,
  (select count(*) from public.finess_dirigeant) as essms_contacts_count,
  (select count(*) from public.prospection_entites) as financeurs_entites_count,
  (select count(*) from public.prospection_contacts) as financeurs_contacts_count,
  (select count(*) from public.prospection_signaux) as financeurs_signaux_count;
```

### 3.2 `v_ui_essms_gestionnaires_list`

Objectif : alimenter la liste gestionnaires.

Filtres UX cibles :

- departement du siege
- taille du gestionnaire
- categories d'etablissements du gestionnaire
- financeur principal
- presence de signaux

Champs attendus :

- `id_gestionnaire`
- `raison_sociale`
- `departement_code`
- `categorie_taille`
- `secteur_activite_principal`
- `signal_tension`
- `signal_tension_detail`
- `nb_etablissements`
- `categories_principales`
- `financeurs_principaux`
- `daf_nom`
- `daf_email`

Exemple SQL fonctionnel :

```sql
create or replace view public.v_ui_essms_gestionnaires_list as
with etabs as (
  select
    e.id_gestionnaire,
    count(*) filter (
      where e.categorie_normalisee is not null
        and e.categorie_normalisee != 'SAA'
    ) as nb_etablissements,
    array_remove(array_agg(distinct e.categorie_normalisee), null) as categories_principales,
    array_remove(array_agg(distinct e.financeur_principal), null) as financeurs_principaux
  from public.finess_etablissement e
  group by e.id_gestionnaire
)
select
  g.id_gestionnaire,
  g.raison_sociale,
  g.departement_code,
  g.categorie_taille,
  g.secteur_activite_principal,
  g.signal_tension,
  g.signal_tension_detail,
  g.daf_nom,
  g.daf_email,
  coalesce(et.nb_etablissements, 0) as nb_etablissements,
  coalesce(et.categories_principales, '{}') as categories_principales,
  coalesce(et.financeurs_principaux, '{}') as financeurs_principaux
from public.finess_gestionnaire g
left join etabs et on et.id_gestionnaire = g.id_gestionnaire;
```

Tri conseille :

```sql
order by nb_etablissements desc, raison_sociale asc
```

### 3.3 `v_ui_essms_gestionnaire_detail`

Objectif : alimenter la fiche gestionnaire.

Champs / blocs attendus :

- infos gestionnaire
- compteurs etablissements
- signaux
- DAF
- dirigeants
- panorama financeurs
- panorama categories

Recommendation :

- utiliser une vue detail pour les agregats simples ;
- completer via 2 ou 3 requetes secondaires pour les sous-listes volumineuses (`etablissements`, `dirigeants`).

### 3.4 `v_ui_essms_etablissements_list`

Objectif : alimenter la liste etablissements.

Filtres UX cibles :

- departement
- commune
- categorie
- gestionnaire
- financeur principal
- type de tarification
- presence d'email / site / telephone
- signaux oui/non

Champs attendus :

- `id_finess`
- `raison_sociale`
- `departement_code`
- `commune`
- `categorie_normalisee`
- `id_gestionnaire`
- `gestionnaire_nom`
- `financeur_principal`
- `type_tarification`
- `places_autorisees`
- `email`
- `site_web`
- `telephone`
- `signal_tension_gestionnaire`

Exemple SQL fonctionnel :

```sql
create or replace view public.v_ui_essms_etablissements_list as
select
  e.id_finess,
  e.raison_sociale,
  e.departement_code,
  e.commune,
  e.categorie_normalisee,
  e.id_gestionnaire,
  g.raison_sociale as gestionnaire_nom,
  e.financeur_principal,
  e.type_tarification,
  e.places_autorisees,
  e.email,
  e.site_web,
  e.telephone,
  g.signal_tension as signal_tension_gestionnaire
from public.finess_etablissement e
left join public.finess_gestionnaire g on g.id_gestionnaire = e.id_gestionnaire
where e.categorie_normalisee is not null
  and e.categorie_normalisee != 'SAA';
```

### 3.5 `v_ui_essms_contacts_list`

Objectif : alimenter la recherche de contacts ESSMS.

Filtres UX cibles :

- fonction
- taille du gestionnaire
- presence de signaux sur le gestionnaire
- departement du gestionnaire

Champs attendus :

- `contact_id`
- `nom_prenom`
- `fonction_normalisee`
- `email_reconstitue`
- `email_verifie`
- `linkedin_url`
- `telephone_direct`
- `confiance`
- `id_gestionnaire`
- `gestionnaire_nom`
- `categorie_taille`
- `departement_code`
- `signal_tension`

Note : si `nom_prenom` n'est pas stable en base, exposer un alias construit dans la vue.

### 3.6 `v_ui_financeurs_list`

Objectif : alimenter la liste des entites financeurs.

Filtres UX cibles :

- type de financeur
- presence DGA
- presence responsable tarification
- presence signaux

Champs attendus :

- `entite_id`
- `type_entite`
- `code`
- `nom`
- `nom_complet`
- `site_web`
- `domaine_email`
- `nb_contacts`
- `has_dga`
- `has_responsable_tarification`
- `has_signaux`
- `last_signal_date`

Exemple SQL fonctionnel :

```sql
create or replace view public.v_ui_financeurs_list as
with contacts as (
  select
    c.entite_id,
    count(*) as nb_contacts,
    bool_or(c.niveau = 'dga') as has_dga,
    bool_or(c.niveau = 'responsable_tarification') as has_responsable_tarification
  from public.prospection_contacts c
  group by c.entite_id
),
signaux as (
  select
    s.entite_id,
    count(*) as nb_signaux,
    max(s.created_at) as last_signal_date
  from public.prospection_signaux s
  group by s.entite_id
)
select
  e.id as entite_id,
  e.type_entite,
  e.code,
  e.nom,
  e.nom_complet,
  e.site_web,
  e.domaine_email,
  coalesce(c.nb_contacts, 0) as nb_contacts,
  coalesce(c.has_dga, false) as has_dga,
  coalesce(c.has_responsable_tarification, false) as has_responsable_tarification,
  coalesce(sig.nb_signaux, 0) > 0 as has_signaux,
  sig.last_signal_date
from public.prospection_entites e
left join contacts c on c.entite_id = e.id
left join signaux sig on sig.entite_id = e.id;
```

### 3.7 `v_ui_financeurs_contacts_list`

Objectif : alimenter la recherche de contacts financeurs.

Filtres UX cibles :

- type de financeur
- niveau
- territoire
- confiance
- presence email / LinkedIn

Champs attendus :

- `contact_id`
- `type_entite`
- `code`
- `entite_nom`
- `nom_complet`
- `poste_exact`
- `niveau`
- `email_principal`
- `linkedin_url`
- `confiance_nom`
- `confiance_email`
- `email_valide_web`

### 3.8 `mv_ui_financeurs_dept_coverage`

Objectif : alimenter la carte financeurs par departements colores.

Principe :

- 1 ligne par departement ;
- flags de couverture institutionnelle ;
- compteurs de contacts ;
- flags signaux.

Colonnes proposees :

- `departement_code`
- `has_departement_entity`
- `has_dga`
- `has_responsable_tarification`
- `contacts_count`
- `signaux_count`
- `coverage_score`

## 4. Index recommandes

### 4.1 ESSMS

```sql
create index if not exists idx_ui_finess_etab_dept_statut_cat
  on public.finess_etablissement (departement_code, enrichissement_statut, categorie_normalisee);

create index if not exists idx_ui_finess_etab_gest_statut
  on public.finess_etablissement (id_gestionnaire, enrichissement_statut);

create index if not exists idx_ui_finess_etab_financeur
  on public.finess_etablissement (financeur_principal);

create index if not exists idx_ui_finess_etab_tarif
  on public.finess_etablissement (type_tarification);
```

Si extension `pg_trgm` disponible :

```sql
create index if not exists idx_ui_finess_etab_rs_trgm
  on public.finess_etablissement using gin (raison_sociale gin_trgm_ops);

create index if not exists idx_ui_finess_gest_rs_trgm
  on public.finess_gestionnaire using gin (raison_sociale gin_trgm_ops);
```

Index partiel utile :

```sql
create index if not exists idx_ui_finess_etab_non_saa
  on public.finess_etablissement (departement_code, id_gestionnaire)
  where categorie_normalisee is not null
    and categorie_normalisee != 'SAA';
```

### 4.2 Contacts ESSMS

```sql
create index if not exists idx_ui_finess_dir_gest_fonction
  on public.finess_dirigeant (id_gestionnaire, fonction_normalisee);

create index if not exists idx_ui_finess_dir_fonction_confiance
  on public.finess_dirigeant (fonction_normalisee, confiance);
```

### 4.3 Financeurs

```sql
create index if not exists idx_ui_prospection_contacts_entite_niveau
  on public.prospection_contacts (entite_id, niveau);

create index if not exists idx_ui_prospection_contacts_niveau_confiance
  on public.prospection_contacts (niveau, confiance_nom);

create index if not exists idx_ui_prospection_signaux_entite_created
  on public.prospection_signaux (entite_id, created_at desc);

create index if not exists idx_ui_prospection_signaux_alerte_created
  on public.prospection_signaux (niveau_alerte, created_at desc);
```

Si `pg_trgm` disponible :

```sql
create index if not exists idx_ui_prospection_contacts_nom_trgm
  on public.prospection_contacts using gin (nom_complet gin_trgm_ops);

create index if not exists idx_ui_prospection_contacts_poste_trgm
  on public.prospection_contacts using gin (poste_exact gin_trgm_ops);
```

## 5. Requetes expertes pour l'assistant IA

### 5.1 Landing counts

```sql
select * from public.v_ui_landing_counts;
```

### 5.2 Gestionnaires avec signaux dans un departement

```sql
select *
from public.v_ui_essms_gestionnaires_list
where departement_code = $1
  and signal_tension = true
order by nb_etablissements desc, raison_sociale asc;
```

### 5.3 Contacts ESSMS par taille de gestionnaire

```sql
select *
from public.v_ui_essms_contacts_list
where categorie_taille = $1
order by gestionnaire_nom asc, fonction_normalisee asc;
```

### 5.4 Financeurs avec DGA mais sans responsable tarification

```sql
select *
from public.v_ui_financeurs_list
where type_entite = 'departement'
  and has_dga = true
  and has_responsable_tarification = false
order by nom asc;
```

## 6. Politique de refresh

### 6.1 Vues normales

- refresh immediat par lecture ;
- adaptees aux listes et fiches standards.

### 6.2 Materialized views

Refresh conseille :

- `mv_ui_financeurs_dept_coverage` : refresh horaire ou sur pipeline termine ;
- futures vues de KPI departementaux : refresh periodique ou manuel.

Exemple :

```sql
refresh materialized view concurrently public.mv_ui_financeurs_dept_coverage;
```

## 7. Livrables SQL cibles pour le futur workspace

1. un fichier `00_extensions.sql`
2. un fichier `10_views_essms.sql`
3. un fichier `20_views_financeurs.sql`
4. un fichier `30_indexes_ui.sql`
5. un fichier `40_materialized_views.sql`
6. un fichier `50_seed_queries_for_ai.sql` ou equivalent documentaire

## 8. Decision finale

Pour cette interface, la bonne approche n'est pas `front -> tables brutes`.

La bonne approche est :

- tables sources existantes ;
- vues et materialized views dediees a l'UX ;
- index complementaires ;
- requetes expertes pour l'assistant IA ;
- acces via couche serveur Next.js.
