# Spec Projet - Workspace Next.js ESSMS & Financeurs

## 1. Objectif

Ce document decrit le projet cible a construire dans un autre workspace.

Stack cible :

- Next.js
- React
- TypeScript
- authentification login / mot de passe
- acces base cote serveur Next.js
- aucune dependance a un backend Python applicatif

## 2. Objectif produit

Construire une interface professionnelle permettant :

- une landing page simple avec deux blocs `ESSMS` et `Financeurs` ;
- une navigation ESSMS par gestionnaires, etablissements et contacts ;
- une navigation financeurs par entites, contacts et carte departementale ;
- une cartographie distincte pour ESSMS et financeurs ;
- un assistant IA Gemini adosse a une bibliotheque de requetes SQL experte.

## 3. Arborescence cible

```text
src/
  app/
    login/page.tsx
    (protected)/
      layout.tsx
      page.tsx                       # landing page connectee
      essms/
        page.tsx                     # hub ESSMS
        gestionnaires/page.tsx
        gestionnaires/[id]/page.tsx
        etablissements/page.tsx
        etablissements/[id]/page.tsx
        contacts/page.tsx
        carte/page.tsx
      financeurs/
        page.tsx                     # hub financeurs
        entites/page.tsx
        entites/[id]/page.tsx
        contacts/page.tsx
        carte/page.tsx
      dashboard/page.tsx
      exports/page.tsx
      settings/page.tsx
    api/
      auth/login/route.ts
      auth/logout/route.ts
      landing/counts/route.ts
      essms/gestionnaires/route.ts
      essms/gestionnaires/[id]/route.ts
      essms/etablissements/route.ts
      essms/etablissements/[id]/route.ts
      essms/contacts/route.ts
      essms/carte/route.ts
      financeurs/entites/route.ts
      financeurs/entites/[id]/route.ts
      financeurs/contacts/route.ts
      financeurs/carte/route.ts
      exports/route.ts
      assistant/query/route.ts
  components/
    layout/
    landing/
    filters/
    tables/
    cards/
    maps/
    detail/
    assistant/
    auth/
  features/
    essms/
    financeurs/
    exports/
    assistant/
    auth/
  lib/
    db/
      client.ts
      queries/
        essms.ts
        financeurs.ts
        landing.ts
        exports.ts
        assistant.ts
    auth/
    utils/
    types/
  styles/
```

## 4. Pages et comportements

### 4.1 `/`

Landing page connectee.

Contenu :

- bloc `ESSMS`
- bloc `Financeurs`
- compteurs dynamiques issus de `v_ui_landing_counts`

### 4.2 `/essms/gestionnaires`

Vue principale ESSMS.

Fonctions :

- filtres a gauche ou dans un panneau ;
- table principale ;
- tri par defaut `nb_etablissements desc` ;
- export Excel ;
- bouton `carte`.

### 4.3 `/essms/gestionnaires/[id]`

Fiche gestionnaire.

Sections :

- hero identitaire
- KPIs
- etablissements rattaches
- financeurs
- dirigeants
- signaux
- carte des etablissements

### 4.4 `/essms/etablissements`

Vue liste etablissements.

Fonctions :

- filtres multicriteres
- table resultats
- export Excel
- acces fiche detail
- acces carte

### 4.5 `/essms/contacts`

Vue liste contacts ESSMS.

Fonctions :

- filtres fonction, taille, signal, departement
- table resultats
- export Excel

### 4.6 `/essms/carte`

Carte ESSMS.

Fonctions :

- affiche gestionnaires ou etablissements selon le contexte ;
- reutilise exactement les memes filtres que la liste ;
- clic sur element -> fiche detail.

### 4.7 `/financeurs/entites`

Vue liste financeurs.

Fonctions :

- filtre principal par type d'entite ;
- filtres secondaires optionnels ;
- table simple ;
- acces fiche.

### 4.8 `/financeurs/entites/[id]`

Fiche financeur.

Sections :

- identite
- type
- couverture / territoire
- contacts
- signaux
- liens ESSMS si disponibles

### 4.9 `/financeurs/contacts`

Vue contacts financeurs.

Fonctions :

- filtre type entite
- filtre niveau
- filtre territoire
- filtre confiance
- recherche texte

### 4.10 `/financeurs/carte`

Carte departementale coloree.

Fonctions :

- coloration departements selon presence de DGA, responsable tarification, signal, etc. ;
- legendes ;
- clic sur departement -> filtrage ou fiche.

## 5. Authentification

## 5.1 Parcours

- page `/login`
- saisie identifiant / mot de passe
- creation session securisee
- redirection vers landing page

### 5.2 Contraintes

- toutes les routes applicatives sont protegees ;
- pas de credentials DB exposes au navigateur ;
- les requetes base passent par le serveur Next.js.

## 6. Couche data TypeScript

## 6.1 Organisation

- `lib/db/client.ts` : client serveur PostgreSQL / Supabase
- `lib/db/queries/landing.ts`
- `lib/db/queries/essms.ts`
- `lib/db/queries/financeurs.ts`
- `lib/db/queries/exports.ts`
- `lib/db/queries/assistant.ts`

### 6.2 Regle de conception

Chaque fonction TypeScript doit correspondre a un usage UX clair.

Exemples :

- `getLandingCounts()`
- `getGestionnairesList(filters)`
- `getGestionnaireDetail(id)`
- `getEtablissementsList(filters)`
- `getEssmsContacts(filters)`
- `getFinanceursList(filters)`
- `getFinanceurDetail(id)`
- `getFinanceurContacts(filters)`
- `getFinanceurCoverageMap(filters)`

## 7. Composants UI a construire

### 7.1 Landing

- `LandingMetricCard`
- `UniverseEntryCard`

### 7.2 Listes

- `FilterPanel`
- `ResultsToolbar`
- `DataTable`
- `ExcelExportButton`
- `ActiveFiltersBar`

### 7.3 Fiches

- `HeroSummary`
- `KpiStrip`
- `SectionCard`
- `ContactsTable`
- `SignalsPanel`
- `MapPreview`

### 7.4 Carte

- `EssmsMap`
- `FinanceursCoverageMap`
- `MapLegend`
- `MapDetailSheet`

### 7.5 Assistant

- `AssistantDrawer`
- `AssistantPromptInput`
- `AssistantSuggestionList`
- `AssistantAnswerPanel`

## 8. Types front a prevoir

- `LandingCounts`
- `GestionnaireListItem`
- `GestionnaireDetail`
- `EtablissementListItem`
- `EtablissementDetail`
- `EssmsContactItem`
- `FinanceurListItem`
- `FinanceurDetail`
- `FinanceurContactItem`
- `DepartmentCoverageItem`
- `AssistantQueryResult`

## 9. Exports

Format attendu a minima :

- Excel (`.xlsx`)

Exports prioritaires :

- liste gestionnaires filtree
- liste etablissements filtree
- liste contacts ESSMS filtree
- liste financeurs filtree
- liste contacts financeurs filtree

## 10. Assistant IA Gemini

Le workspace devra integrer un assistant Gemini avec :

- prompts systemes maitres ;
- bibliotheque de requetes expertes ;
- mapping prompt -> requete -> resultat -> explication ;
- garde-fous forts.

Regle cle :

- pas de SQL libre ecrit par l'utilisateur puis execute tel quel.

## 11. Definition of done du futur workspace

Le projet sera considere comme correctement lance lorsque :

1. la page de login fonctionne ;
2. la landing charge les bons compteurs ;
3. la liste gestionnaires fonctionne avec filtres et export ;
4. la fiche gestionnaire affiche panorama complet ;
5. la liste etablissements fonctionne ;
6. la recherche contacts ESSMS fonctionne ;
7. la liste financeurs fonctionne ;
8. la recherche contacts financeurs fonctionne ;
9. la carte ESSMS et la carte financeurs fonctionnent ;
10. l'assistant Gemini est branche a la bibliotheque de requetes expertes.

## 12. Priorite de build

### Sprint 1

- auth
- landing counts
- liste gestionnaires
- fiche gestionnaire

### Sprint 2

- liste etablissements
- contacts ESSMS
- export Excel
- carte ESSMS

### Sprint 3

- liste financeurs
- fiche financeur
- contacts financeurs
- carte departementale financeurs

### Sprint 4

- assistant Gemini
- vues sauvegardees
- raffinement UX / design system
