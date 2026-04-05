# Documentation Complete - Interface Next.js React pour Plateforme ESSMS & Financeurs

## 1. Vision produit

Construire une plateforme web moderne, professionnelle et tres lisible permettant de :

- interroger la base FINESS enrichie sur les ESSMS et gestionnaires ;
- croiser ces informations avec les donnees des financeurs presentes en base ;
- visualiser les signaux utiles a la prospection ;
- produire des listes ciblees, des cartographies, des exports et des fiches de synthese par departement ;
- transformer une base de donnees technique en outil de recherche commerciale et de data intelligence sectorielle.

L'application doit servir a la fois :

- aux equipes commerciales qui cherchent des prospects qualifies ;
- aux analystes qui veulent comprendre le tissu ESSMS d'un territoire ;
- aux dirigeants qui veulent une vision synthetique du marche, des financeurs et des signaux faibles / forts.

Le positionnement n'est pas celui d'un simple annuaire. La plateforme doit ressembler a un cockpit de pilotage commercial et territorial.

## 2. Objectifs metier

### 2.1 Objectifs principaux

- Trouver rapidement des cibles commerciales pertinentes.
- Prioriser les prospects selon des signaux exploitables.
- Cartographier un departement ou une region en quelques secondes.
- Comprendre les relations entre ESSMS, gestionnaires, financeurs et signaux.
- Exporter des listes qualifiees pour action commerciale, analyse ou reporting.

### 2.2 Questions auxquelles l'interface doit repondre

- Quels ESSMS sont actifs dans un departement donne ?
- Quels gestionnaires concentrent le plus d'etablissements ?
- Quels financeurs sont presents sur une zone ou un type de structure ?
- Quels signaux recents indiquent un potentiel commercial ou un besoin ?
- Quels territoires sont sous-denses, sur-denses ou en transformation ?
- Quelle liste de prospects peut etre exportee pour une campagne ciblee ?

## 3. Perimetre fonctionnel

### 3.0 Ancrage dans la base existante

La plateforme doit s'appuyer sur deux blocs de donnees deja presents dans la base :

- le bloc `FINESS enrichi` : `finess_etablissement`, `finess_gestionnaire`, `finess_dirigeant`, `finess_enrichissement_log` ;
- le bloc `prospection financeurs` : `prospection_entites`, `prospection_contacts`, `prospection_signaux`.

Volumes observes en base au 07/03/2026 :

- `finess_etablissement` : 51 715 lignes ;
- `finess_gestionnaire` : 16 498 lignes ;
- `finess_dirigeant` : 70 609 lignes ;
- `finess_cache_serper` : 255 960 lignes ;
- `prospection_entites` : 128 lignes ;
- `prospection_contacts` : 456 lignes ;
- `prospection_signaux` : 1 ligne a date.

Consequence directe :

- l'univers `ESSMS` doit etre pense comme un produit de recherche et d'analyse sur donnees volumineuses ;
- l'univers `financeurs` a un volume plus faible, mais une valeur relationnelle forte ;
- les croisements entre les deux mondes doivent etre pre-structures cote SQL/BFF, pas reconstruits dans le navigateur.

### 3.1 Univers de donnees

La plateforme doit couvrir deux univers relies :

- **ESSMS / FINESS**
- **Financeurs / ecosysteme financeur**

### 3.2 Entites principales

- Etablissement
- Gestionnaire
- Departement
- Financeur
- Signal
- Contact / dirigeant
- Fiche territoriale
- Liste exportable

En pratique, il faut distinguer deux familles d'entites :

- **Entites de marche ESSMS** : etablissements, gestionnaires, dirigeants, indicateurs territoriaux ;
- **Entites institutionnelles financeurs** : conseils departementaux, ARS, DIRPJJ, contacts DGA, directions metier, responsables tarification.

### 3.3 Cas d'usage majeurs

#### Recherche de prospects

- Filtrer les ESSMS par departement, categorie, statut, taille, gestionnaire, signal, niveau de maturite.
- Croiser avec des financeurs presents sur le territoire ou affinitaires d'un segment.
- Construire une shortlist et l'exporter.

#### Cartographie

- Voir les etablissements sur une carte.
- Colorer par categorie, gestionnaire, densite, signal ou financeur associe.
- Comparer deux departements ou un departement avec la moyenne nationale.

#### Intelligence territoriale

- Ouvrir une fiche departement avec volume, repartition, signaux et acteurs dominants.
- Identifier les zones a fort potentiel commercial.

#### Data intelligence sectorielle

- Observer les concentrations, trous de couverture, evolutions et signaux dominants.
- Identifier les gestionnaires influents et les financeurs structurants.

## 4. Experience utilisateur cible

### 4.1 Promesse UX

L'interface doit donner l'impression de manipuler une intelligence de marche, pas une base SQL habillee.

Le produit doit etre :

- rapide ;
- lisible ;
- dense mais jamais brouillon ;
- elegant et contemporain ;
- oriente prise de decision.

### 4.2 Principes UX

- Une action principale claire par ecran.
- Une lecture immediate des KPIs critiques.
- Des filtres persistants et comprehensibles.
- Une profondeur analytique accessible sans surcharge visuelle.
- Une navigation par contexte : national, departement, fiche, liste, financeur.
- Un maximum de valeur visible au premier ecran.

### 4.3 Publics cibles

- Direction commerciale
- Business developers
- Consultants / analysts
- Direction generale
- Operations / equipe data

## 5. Structure generale de l'application

### 5.0 Double lecture produit a assumer

L'interface ne doit pas traiter les financeurs comme un simple filtre secondaire. Il y a en realite deux usages complementaires :

- **Angle 1 - Prospection ESSMS** : identifier des structures, gestionnaires et signaux exploitables commercialement ;
- **Angle 2 - Veille institutionnelle financeurs** : suivre les decideurs publics, responsables tarification, relais PJJ et acteurs ARS qui structurent la decision, le financement ou l'influence sectorielle.

Ces deux angles ne poursuivent pas exactement le meme objectif :

- l'angle ESSMS cherche des opportunites de cibles et de territoires ;
- l'angle financeurs cherche des relais institutionnels, des contacts-clefs et des signaux de politique publique.

### 5.0 bis Principe UX general retenu

L'experience doit etre volontairement simple dans sa structure generale :

- une **landing page tres sobre** ;
- deux **portes d'entree claires** : `ESSMS` et `Financeurs` ;
- pour chaque univers : `liste`, `filtres`, `fiche detail`, `contacts`, `carte`.

Le premier niveau doit repondre a une question simple :

- `Je veux explorer les ESSMS`
- `Je veux explorer les financeurs`

### 5.1 Navigation principale

Le menu principal doit s'articuler autour de 6 entrees :

1. `Dashboard`
2. `Recherche`
3. `Cartographie`
4. `Departements`
5. `Financeurs`
6. `Exports`

Un acces secondaire doit etre prevu pour :

- `Signaux`
- `Gestionnaires`
- `Administration`
- `Historique des exports`

Sous `Financeurs`, il faut prevoir 3 sous-espaces :

- `Institutions` : departements, ARS, DIRPJJ ;
- `Contacts clefs` : DGA, directions, responsables tarification, profils PJJ ;
- `Signaux financeurs` : tensions, politiques de financement, alertes institutionnelles.

### 5.1 bis Landing page attendue

La landing page connectee doit etre tres simple et tres lisible.

Elle doit afficher deux grands blocs cliquables :

1. `ESSMS`
2. `Financeurs`

#### Bloc ESSMS

Doit afficher a minima :

- nombre de gestionnaires references ;
- nombre d'etablissements references ;
- nombre de contacts ESSMS disponibles.

#### Bloc Financeurs

Doit afficher a minima :

- nombre d'entites financeurs referencees ;
- nombre de contacts financeurs disponibles.

#### Logique de clic

- clic sur `ESSMS` -> interface de navigation ESSMS ;
- clic sur `Financeurs` -> interface de navigation financeurs.

### 5.2 Arborescence Next.js recommandee

```text
app/
  (marketing)/
    page.tsx
  (platform)/
    layout.tsx
    dashboard/page.tsx
    recherche/page.tsx
    cartographie/page.tsx
    departements/page.tsx
    departements/[code]/page.tsx
    essms/page.tsx
    essms/[id]/page.tsx
    gestionnaires/page.tsx
    gestionnaires/[id]/page.tsx
    financeurs/page.tsx
    financeurs/[id]/page.tsx
    signaux/page.tsx
    exports/page.tsx
    listes/[id]/page.tsx
    api/
      search/route.ts
      map/route.ts
      exports/route.ts
      departments/[code]/route.ts
      prospects/route.ts
```

## 6. Ecrans clefs

### 6.1 Dashboard

Le `Dashboard` est le cockpit executif.

Il doit afficher :

- le nombre d'ESSMS exploitables ;
- le nombre de gestionnaires exploitables ;
- le nombre de financeurs en base ;
- le volume de signaux actifs / recents ;
- les top departements par potentiel ;
- les top gestionnaires ;
- les top financeurs / partenaires ;
- une carte de chaleur nationale ;
- un fil d'actualite des signaux recents.

#### Blocs recommandés

- `KPI Ribbon` en tete
- `National Opportunity Map`
- `Signals Feed`
- `Top Movers` (territoires ou acteurs qui bougent)
- `Watchlists` sauvegardees

### 6.2 Recherche

L'ecran `Recherche` est le coeur commercial de la plateforme.

En pratique, il faut le decomposer en vues de navigation simples plutot qu'en moteur unique trop abstrait :

- `Gestionnaires`
- `Etablissements`
- `Contacts ESSMS`

Il doit permettre :

- une recherche textuelle globale ;
- une recherche facettee ;
- une recherche guidee par scenario ;
- l'ajout d'elements dans une liste exportable.

#### Filtres principaux

- departement / region
- type d'ESSMS
- categorie / sous-categorie
- statut d'enrichissement / qualite de fiche
- gestionnaire
- presence d'email, telephone, site web
- signaux disponibles
- densite du territoire
- financeur associe ou present
- score de priorite

Ces filtres constituent la bibliotheque generique, mais l'UX retenue doit etre specialisee par sous-espace.

### 6.2 bis Navigation ESSMS

L'univers `ESSMS` doit proposer 3 entrees principales :

1. `Gestionnaires`
2. `Etablissements`
3. `Contacts`

Chaque entree partage la meme logique UX :

- rail de filtres ;
- liste de resultats ;
- export Excel ;
- acces a la fiche detail ;
- icone `carte` utilisant les memes filtres.

### 6.2 ter Vue Gestionnaires

Cette vue doit etre la porte d'entree prioritaire de l'univers ESSMS.

#### Filtres attendus

- departement du siege ;
- taille du gestionnaire ;
- categories d'etablissements rattaches au gestionnaire ;
- financeur principal ;
- presence de signaux : `oui / non`.

#### Liste gestionnaires

La liste doit toujours etre triee par defaut selon le **nombre d'etablissements du gestionnaire**, du plus important au plus petit.

Chaque ligne doit afficher :

- nom du gestionnaire ;
- departement du siege ;
- categorie de taille ;
- nombre d'etablissements ;
- categories principales representees ;
- financeur principal dominant si disponible ;
- presence de signaux ;
- actions : `Voir la fiche`, `Exporter`, `Voir sur carte`.

#### Export

L'utilisateur doit pouvoir exporter la liste au format Excel.

### 6.2 quater Fiche gestionnaire

La fiche gestionnaire doit fournir un panorama complet des informations disponibles en base.

#### Sections de la fiche

- identite et informations generales ;
- etablissements rattaches ;
- financeurs et indicateurs de financement ;
- dirigeants / contacts ;
- signaux presents ;
- informations DAF / contacts fonctionnels si disponibles ;
- vue cartographique des etablissements du gestionnaire.

### 6.2 quinquies Vue Etablissements

La vue `Etablissements` reprend le meme principe que `Gestionnaires` :

- filtres sur les champs disponibles ;
- liste ;
- export Excel ;
- fiche detail ;
- acces carte avec les memes filtres.

#### Filtres etablissements

Les filtres doivent s'appuyer sur ce qui existe reellement en base, notamment :

- departement ;
- commune ;
- categorie / categorie normalisee ;
- gestionnaire ;
- financeur principal ;
- type de tarification ;
- presence de signaux si exposee via gestionnaire ou enrichissement ;
- presence d'email / site / telephone ;
- statut ou completude de fiche.

### 6.2 sexies Fiche etablissement

La fiche etablissement doit montrer :

- identite et localisation ;
- rattachement gestionnaire ;
- informations de contact ;
- elements de financement / tarification ;
- dirigeants relies si disponibles ;
- signaux ;
- contexte institutionnel du territoire ;
- acces aux entites financeurs du territoire.

### 6.2 septies Vue Contacts ESSMS

Un module de recherche de contacts ESSMS doit etre prevu comme un espace a part entiere.

#### Filtres attendus

- fonction ;
- taille du gestionnaire ;
- presence de signaux sur le gestionnaire ;
- departement du gestionnaire ;
- type de gestionnaire ou categorie dominante si pertinent.

#### Resultats

Chaque ligne doit afficher :

- nom du contact ;
- fonction normalisee ;
- gestionnaire ;
- taille du gestionnaire ;
- presence de signaux ;
- email, LinkedIn, telephone si disponibles.

#### Resultat de recherche

Chaque ligne doit proposer :

- nom de l'ESSMS ou du gestionnaire ;
- localisation ;
- categorie ;
- gestionnaire ;
- score ;
- signaux ;
- financeurs lies ou probables ;
- actions rapides : `Voir`, `Ajouter a la liste`, `Exporter`, `Comparer`.

### 6.3 Cartographie

La `Cartographie` doit etre un ecran premium, pas une simple carte technique.

#### Capacites attendues

- affichage carte France / region / departement ;
- clustering dynamique ;
- bascule points / heatmap / choropleth ;
- filtres synchronises avec la recherche ;
- panneau lateral detail ;
- selection multi-zones ;
- dessin d'aire geographique ;
- export de la vue cartographique.

#### Couches cartographiques

- ESSMS
- gestionnaires
- financeurs
- signaux
- densite / penetration
- scores d'opportunite

### 6.3 bis Cartographie ESSMS

La cartographie ESSMS doit etre accessible depuis une icone `carte` dans les vues `Gestionnaires` et `Etablissements`.

#### Regle UX essentielle

La carte doit reutiliser **exactement les memes filtres que la liste**.

#### Affichages attendus

- localisation des gestionnaires ;
- localisation des etablissements ;
- acces a la fiche au clic ;
- clusterisation si necessaire ;
- panneau lateral detail synchronise avec la selection.

### 6.4 Fiche departement

Chaque fiche departement doit resumer un territoire de facon operationnelle.

#### Contenu de la fiche

- volume total d'ESSMS
- repartition par categories
- top gestionnaires du departement
- principaux financeurs presents ou actifs
- carte locale
- signaux recents
- etablissements prioritaires
- comparaison avec region / national
- export rapide du departement

#### Indicateurs utiles

- densite ESSMS / population cible
- part public / prive / associatif si disponible
- part d'etablissements avec site web / email / telephone
- part avec signaux positifs / faibles / alertes
- score de maturite commerciale du departement

### 6.5 Fiche ESSMS

La fiche ESSMS doit etre orientee prise de contact et comprehension rapide.

#### Contenu cle

- identite de la structure
- adresse, contacts, site web
- categorie / statut
- gestionnaire
- dirigeants / contacts associes
- signaux detectes
- financeurs lies ou plausibles
- historique d'export / ciblage si besoin
- recommandations d'action

La fiche ESSMS doit aussi comporter un bloc `Contexte institutionnel` avec :

- conseil departemental de rattachement ;
- ARS de rattachement ;
- relais institutionnels disponibles sur le territoire ;
- signaux financeurs potentiellement relies a la structure ou au territoire.

#### Bloc intelligent

Un panneau `Pourquoi c'est un bon prospect ?` doit synthetiser :

- signaux detectes ;
- completude de la fiche ;
- poids du gestionnaire ;
- contexte territorial ;
- proximite de financeurs ou opportunites.

### 6.6 Fiche financeur

La fiche financeur doit etre un hub d'intelligence relationnelle.

#### Contenu cle

- identite du financeur
- type de financeur
- zones d'intervention
- ESSMS / segments potentiellement lies
- signaux associes
- departements couverts
- liste de structures d'interet
- opportunites commerciales detectees

#### Sous-types de fiche financeur

Il faut prevoir des fiches legerement differentes selon `type_entite` :

- `departement` : angle DGA solidarites, directions autonomie / enfance, responsable tarification ESSMS ;
- `ars` : angle offre medico-sociale, autonomie, financement / allocation de ressources ;
- `dirpjj` : angle direction interregionale, DEPAFI / affaires financieres, secteur associatif habilite.

#### Focus contacts institutionnels

Le schema actuel contient deja des contacts enrichis qui doivent apparaitre nativement dans l'interface :

- `dga` ;
- `direction` ;
- `direction_adjointe` ;
- `responsable_tarification` ;
- `operationnel`.

Etat observe au 07/03/2026 :

- 81 contacts `dga`, uniquement cote departements ;
- 24 contacts `responsable_tarification`, majoritairement cote departements, avec un debut cote ARS ;
- 31 contacts de `direction` cote DIRPJJ ;
- 65 contacts de `direction` cote ARS.

Cela justifie un ecran `Contacts financeurs` a part entiere, et non un simple onglet secondaire.

### 6.6 bis Navigation Financeurs

L'univers `Financeurs` doit etre plus simple que l'univers ESSMS.

Il doit proposer 3 entrees :

1. `Entites financeurs`
2. `Contacts financeurs`
3. `Carte departements`

### 6.6 ter Vue Entites financeurs

#### Filtres attendus

- type de financeur :
  - `departement`
  - `ars`
  - `dirpjj`

Filtres secondaires possibles :

- presence d'un contact DGA ;
- presence d'un responsable tarification ;
- presence de signaux.

#### Liste financeurs

Chaque ligne doit afficher :

- nom de l'entite ;
- type ;
- code ;
- nombre de contacts ;
- presence d'un DGA ;
- presence d'un responsable tarification ;
- dernier signal si disponible.

### 6.6 quater Fiche financeur attendue

La fiche financeur doit afficher :

- identite ;
- type d'entite ;
- site web et domaine email ;
- liste des contacts ;
- repartition des contacts par niveau ;
- signaux ;
- departement ou territoire couvert ;
- liens avec l'univers ESSMS si la jonction est disponible.

### 6.6 quinquies Vue Contacts financeurs

L'utilisateur doit pouvoir rechercher directement dans les contacts institutionnels.

#### Filtres attendus

- type de financeur ;
- niveau de contact (`dga`, `direction`, `responsable_tarification`, etc.) ;
- territoire ;
- confiance ;
- presence email / LinkedIn.

### 6.6 sexies Cartographie financeurs

La cartographie financeurs ne doit pas etre une carte de points comme cote ESSMS.

Elle doit etre une carte des departements colores selon la presence de l'information demandee par les filtres.

Exemples :

- departements avec DGA identifie ;
- departements avec responsable tarification identifie ;
- departements avec signaux financeurs ;
- couverture institutionnelle selon type d'entite.

### 6.7 Exports et listes

Les `Exports` ne doivent pas etre un ecran secondaire pauvre. Ils doivent fonctionner comme un centre de preparation commerciale.

#### Capacites

- creer des listes nommees ;
- ajouter / retirer des ESSMS, gestionnaires, financeurs ;
- enregistrer les filtres comme vue sauvegardee ;
- exporter CSV / XLSX / PDF ;
- generer un export cartographique ;
- generer un pack departement ;
- historiser les exports.

## 7. Moteur de recherche intelligent

### 7.1 Niveaux de recherche

L'interface doit proposer trois modes :

#### Mode 1 - Recherche rapide

Une barre globale capable de chercher :

- un nom d'ESSMS ;
- un gestionnaire ;
- un financeur ;
- un departement ;
- un type de structure ;
- un signal.

#### Mode 2 - Recherche avancee

Des filtres combinables pour les utilisateurs experts.

#### Mode 3 - Recherche assistee

Un panneau de requetes guidees, par exemple :

- `Montre-moi les ESSMS du 69 avec signaux forts et gestionnaire independant`
- `Trouve les departements sous-denses avec financeurs actifs`
- `Liste les structures a fort potentiel exportables pour une campagne`

### 7.2 Scoring prospect

Le moteur doit calculer un `Prospect Score` sur 100.

#### Variables candidates

- completude de la fiche
- presence de contacts exploitables
- presence d'un site web actif
- signaux recents
- poids du gestionnaire
- densite concurrentielle du territoire
- proximite ou presence d'un financeur pertinent
- rarete / attractivite du segment

#### Restitution

- score numerique
- niveau `faible`, `moyen`, `fort`, `prioritaire`
- explication des facteurs

## 8. Module signaux

Le module `Signaux` est indispensable pour la promesse d'intelligence de la donnee.

### 8.1 Types de signaux

- creation / ouverture
- extension / restructuration
- changement de direction / gestionnaire
- subvention / financement
- appel a projets
- recrutement / tension RH
- actualite institutionnelle
- signaux web / contenu site
- signaux faibles deduits de la data

### 8.2 Restitution des signaux

Chaque signal doit avoir :

- un type ;
- une date ;
- une source ;
- un niveau de confiance ;
- un niveau d'impact ;
- un rattachement : ESSMS, gestionnaire, financeur, departement.

### 8.3 Usage UX

- filtrage par signal ;
- timeline sur fiches ;
- classement par intensite ;
- mise en evidence dans les listes ;
- alertes sur watchlists.

## 9. Direction artistique et design system

### 9.1 Intention visuelle

Le produit doit paraitre haut de gamme, analytique et contemporain.

Il faut eviter :

- les dashboards generiques violet/blanc ;
- les composants trop arrondis sans caractere ;
- l'effet back-office banal.

### 9.2 Direction proposee

- univers clair, lumineux, editorial et analytique ;
- base creme froid / bleu nuit / vert sauge / cuivre doux ;
- contrastes propres et serieux ;
- surfaces avec profondeur legere, pas de flat total ;
- accent color pour les signaux et les opportunites.

### 9.3 Tokens CSS recommandes

```css
:root {
  --bg: #f4f1ea;
  --panel: rgba(255, 252, 245, 0.82);
  --panel-strong: #fffaf0;
  --ink: #172033;
  --ink-soft: #51607a;
  --line: rgba(23, 32, 51, 0.10);
  --brand: #0f5c5b;
  --brand-2: #b46a43;
  --accent: #1d7c8c;
  --success: #287a57;
  --warning: #a06a18;
  --danger: #a63f3f;
  --shadow: 0 18px 60px rgba(23, 32, 51, 0.08);
}
```

### 9.4 Typographie

Eviter les piles par defaut.

Recommendation :

- titres : `Fraunces`, `Cormorant Garamond` ou `Canela-like` editorial
- interface : `Suisse Int'l`, `Manrope`, `General Sans` ou `Instrument Sans`
- chiffres / KPIs : variante tabulaire lisible

### 9.5 Composants visuels clefs

- `Hero KPI band`
- `Glass panels` subtils
- `Insight cards`
- `Map side sheet`
- `Signal chips`
- `Score gauge`
- `Comparison strips`
- `Export builder drawer`

### 9.6 Motion

Utiliser peu d'animations, mais significatives :

- reveal progressif au chargement des cartes KPI ;
- apparition echelonnee des lignes dans les listes ;
- transitions douces entre carte et fiche ;
- highlight temporaire des nouveaux signaux.

## 10. Architecture technique Next.js

### 10.1 Stack recommandee

- Next.js 15+ avec App Router
- React 19
- TypeScript
- Tailwind CSS
- `shadcn/ui` ou librairie de design system customisee
- TanStack Query pour la couche data
- Zustand pour l'etat UI local transverse
- Mapbox GL ou MapLibre pour la cartographie
- Recharts / ECharts pour les graphiques
- PostHog ou equivalent pour l'analytics produit

### 10.1 bis Contrainte d'architecture retenue

La cible est une application construite **sans backend Python**.

Le futur workspace devra reposer sur :

- `Next.js + React + TypeScript` ;
- authentification par identifiant / mot de passe ;
- acces base depuis le serveur Next.js uniquement ;
- aucune couche applicative Python intermediaire.

Important : `sans backend Python` ne signifie pas `acces SQL direct depuis le navigateur`.

La bonne architecture reste :

- navigateur -> pages / actions / route handlers Next.js ;
- Next.js serveur -> PostgreSQL / Supabase.

### 10.2 Architecture de donnees

Recommandation : passer par une couche BFF Next.js plutot que de connecter toute l'UI directement a la base.

#### Pourquoi

- unification FINESS + financeurs + signaux ;
- securisation des acces ;
- normalisation des payloads ;
- caching ;
- logique de scoring centralisee ;
- simplification de l'UI.

Dans cette cible, la couche BFF peut etre entierement portee par :

- `Route Handlers` Next.js ;
- `Server Actions` si utiles ;
- une couche `services/queries` TypeScript.

Cette documentation doit donc etre suffisamment precise pour permettre a un autre workspace de reconstruire l'application sans dependance au code Python du repo actuel.

### 10.2 bis Necessite de vues et index dedies

Oui, au vu des volumes et des usages cibles, il est necessaire de construire une couche SQL dediee a l'interface.

Les index existants sont utiles mais insuffisants pour un produit de recherche analytique moderne :

- cote `FINESS`, on a surtout des index simples sur departement, categorie, statut, gestionnaire, secteur et quelques flags ;
- cote `prospection`, on a des index simples sur `entite_id`, `niveau`, `type_entite` ;
- il n'existe pas encore de couche de vues metier pretes pour l'UI hybride ESSMS + financeurs.

Pour un front Next.js avec recherche, cartographie, fiches et exports, il faut ajouter :

- des **vues de lecture metier** ;
- des **materialized views** pour les aggregats lourds ;
- des **index composites** alignes sur les filtres reels de l'interface ;
- des **index textuels** pour la recherche nom / poste / structure.

### 10.2 ter Couche SQL recommandee pour le front

#### Vues de lecture a creer

1. `v_ui_essms_search`

Objectif : fournir une ligne par ESSMS, prete pour la recherche et la liste resultat.

Doit agreger :

- identite ESSMS ;
- departement, commune, categorie, statut ;
- gestionnaire ;
- financeur principal ;
- type de tarification ;
- presence de site, email, telephone ;
- flags signaux ;
- score de completude.

2. `v_ui_gestionnaire_overview`

Objectif : une ligne par gestionnaire avec ses indicateurs clefs.

Doit agreger :

- nb d'etablissements ;
- repartition par categories ;
- signaux ;
- DAF / contacts ;
- secteurs ;
- presence territoriale.

3. `v_ui_department_insights`

Objectif : alimenter les fiches departement et la carte nationale.

Doit contenir :

- nb ESSMS ;
- nb gestionnaires ;
- nb dirigeants ;
- financeurs rattaches ;
- top categories ;
- top gestionnaires ;
- score d'opportunite ;
- couverture data ;
- synthese signaux.

4. `v_ui_financeur_overview`

Objectif : une ligne par entite institutionnelle (`departement`, `ars`, `dirpjj`).

Doit agreger :

- type entite ;
- code, nom, domaine email, site web ;
- nb contacts par niveau ;
- presence DGA ;
- presence responsable tarification ;
- dernier signal connu ;
- zone / couverture ;
- volume d'ESSMS rattaches au territoire si applicable.

5. `v_ui_financeur_contacts`

Objectif : alimenter la recherche de contacts institutionnels.

Doit exposer :

- entite ;
- type d'entite ;
- niveau ;
- nom ;
- poste exact ;
- email ;
- linkedin ;
- confiance nom ;
- confiance email ;
- email valide web.

6. `mv_ui_department_kpis`

Materialized view pour dashboard et cartes.

Doit pre-calculer :

- stats par departement ;
- scores ;
- densites ;
- volumes par categorie ;
- ratios de completude ;
- indicateurs financeurs.

#### Index recommandes

Sur `finess_etablissement` :

- `(departement_code, enrichissement_statut, categorie_normalisee)` ;
- `(id_gestionnaire, enrichissement_statut)` ;
- `(financeur_principal)` si ce filtre devient central ;
- `(type_tarification)` si exploite massivement ;
- index trigram sur `raison_sociale` si recherche nominative ESSMS ;
- index partiel sur les lignes `categorie_normalisee != 'SAA'` si cette regle reste structurante.

Sur `finess_gestionnaire` :

- `(departement_code, enrichissement_statut, categorie_taille)` ;
- `(signal_tension, enrichissement_statut)` ;
- trigram sur `raison_sociale`.

Sur `finess_dirigeant` :

- `(id_gestionnaire, fonction_normalisee)` ;
- `(fonction_normalisee, confiance)` ;
- trigram sur le nom complet si expose en recherche contact.

Sur `prospection_contacts` :

- `(entite_id, niveau)` ;
- `(niveau, confiance_nom)` ;
- `(email_principal)` si verification / recherche email ;
- trigram sur `nom_complet` et `poste_exact` pour recherche institutionnelle.

Sur `prospection_signaux` :

- `(entite_id, created_at DESC)` ;
- `(niveau_alerte, created_at DESC)`.

### 10.3 Couche API/BFF

Le front doit interroger des endpoints metier, pas des tables brutes.

#### Endpoints logiques

- `GET /api/landing/counts`
- `GET /api/dashboard/overview`
- `GET /api/search`
- `GET /api/essms/gestionnaires`
- `GET /api/essms/gestionnaires/:id`
- `GET /api/essms/etablissements`
- `GET /api/essms/etablissements/:id`
- `GET /api/essms/contacts`
- `GET /api/essms/:id`
- `GET /api/gestionnaires/:id`
- `GET /api/departments/:code`
- `GET /api/financeurs/:id`
- `GET /api/financeurs/contacts`
- `GET /api/financeurs/signaux`
- `GET /api/map/layers`
- `POST /api/exports`
- `POST /api/lists`
- `POST /api/prospects/score`

Il faut aussi prevoir des endpoints de jonction entre les deux mondes :

- `GET /api/departments/:code/financeurs`
- `GET /api/departments/:code/prospects`
- `GET /api/essms/:id/institutional-context`
- `GET /api/financeurs/:id/territory-insights`

### 10.4 Couches de services front

```text
UI Components
  -> Feature Modules
    -> Query Hooks
      -> API Client
        -> Next.js Route Handlers / BFF
          -> Services metier
            -> DB / views / materialized views
```

## 11. Modele de donnees front logique

### 11.0 Schema de base a expliquer au futur workspace

Comme le code sera construit dans un autre workspace, cette documentation doit servir de passerelle de comprehension du schema reel.

#### Bloc 1 - Tables ESSMS

`finess_etablissement`

- une ligne par etablissement FINESS ;
- contient les champs de recherche territoriale et de caracterisation ;
- contient notamment : `id_finess`, `id_gestionnaire`, `raison_sociale`, `categorie_normalisee`, `departement_code`, `financeur_principal`, `type_tarification`, `places_autorisees`, `enrichissement_statut`.

`finess_gestionnaire`

- une ligne par gestionnaire ;
- table pivot pour la navigation gestionnaires ;
- contient notamment : `id_gestionnaire`, `raison_sociale`, `departement_code`, `categorie_taille`, `secteur_activite_principal`, `signal_tension`, `signal_tension_detail`, `daf_nom`, `daf_email`, `enrichissement_statut`.

`finess_dirigeant`

- contacts / dirigeants relies aux gestionnaires et parfois aux etablissements ;
- utile pour la recherche de contacts ESSMS ;
- contient notamment : `id_gestionnaire`, `fonction_normalisee`, `email_reconstitue`, `email_verifie`, `telephone_direct`, `linkedin_url`, `source_type`, `confiance`.

#### Bloc 2 - Tables financeurs

`prospection_entites`

- referentiel des entites institutionnelles ;
- types observes : `departement`, `ars`, `dirpjj` ;
- contient : `code`, `nom`, `nom_complet`, `site_web`, `domaine_email`.

`prospection_contacts`

- contacts institutionnels enrichis ;
- utile pour les vues financeurs et la recherche directe de contacts ;
- contient : `entite_id`, `nom_complet`, `poste_exact`, `niveau`, `email_principal`, `linkedin_url`, `confiance_nom`, `confiance_email`, `email_valide_web`.

`prospection_signaux`

- signaux de tension tarification / financement ;
- actuellement peu fournie, mais a integrer nativement dans le produit ;
- contient : `entite_id`, `resume`, `tags`, `niveau_alerte`, `sources`, `confiance`, `periode_couverte`, `created_at`.

#### Ce que l'application doit considerer comme disponible

- navigation gestionnaires par taille, departement, signaux ;
- navigation etablissements par categorie, territoire, financeur principal, tarification ;
- recherche de contacts ESSMS via dirigeants ;
- navigation financeurs par type d'entite ;
- recherche de contacts institutionnels ;
- cartographie ESSMS en points et cartographie financeurs par departements colores.

### 11.1 Objet ESSMS

```ts
type EssmsCard = {
  id: string;
  finess: string;
  nom: string;
  categorie: string;
  sousCategorie?: string;
  departement: string;
  commune: string;
  gestionnaire?: {
    id: string;
    nom: string;
  };
  contacts: {
    email?: string;
    telephone?: string;
    siteWeb?: string;
  };
  scores: {
    prospect: number;
    dataQuality: number;
    signalIntensity: number;
  };
  signaux: SignalSummary[];
  financeurs: FinanceurSummary[];
  geo?: {
    lat: number;
    lng: number;
  };
};
```

### 11.2 Objet financeur

```ts
type FinanceurCard = {
  id: string;
  nom: string;
  type: string;
  code: string;
  zones: string[];
  segments: string[];
  scorePresence: number;
  signaux: SignalSummary[];
  opportunitesLiees: number;
  contacts: Array<{
    id: string;
    niveau: 'dga' | 'direction' | 'direction_adjointe' | 'responsable_tarification' | 'operationnel';
    nomComplet: string;
    posteExact?: string;
    email?: string;
    linkedinUrl?: string;
    confianceNom?: string;
    confianceEmail?: string;
    emailValideWeb?: boolean;
  }>;
};
```

### 11.3 Objet fiche departement

```ts
type DepartmentInsight = {
  code: string;
  nom: string;
  stats: {
    essms: number;
    gestionnaires: number;
    financeurs: number;
    signaux: number;
  };
  repartitions: {
    categories: Array<{ label: string; value: number }>;
    densite: number;
  };
  topProspects: EssmsCard[];
  topGestionnaires: Array<{ id: string; nom: string; total: number }>;
  topFinanceurs: Array<{ id: string; nom: string; total: number }>;
};
```

## 12. Vues et composants metier

### 12.1 Composants transverses

- `GlobalSearchBar`
- `FilterRail`
- `SavedViewPicker`
- `ProspectScoreBadge`
- `SignalStack`
- `ExportDrawer`
- `MapLegend`
- `DepartmentHero`
- `FinanceurPresenceMatrix`
- `ComparePanel`

### 12.2 Modules fonctionnels

- `dashboard/`
- `search/`
- `map/`
- `departments/`
- `essms/`
- `financeurs/`
- `signals/`
- `exports/`

## 13. Strategie de performance

### 13.1 Points de vigilance

- gros volumes sur la recherche ;
- rendu carte ;
- croisements multi-entites ;
- exports lourds ;
- filtres multiples.

### 13.2 Recommandations

- pagination ou virtualisation des listes ;
- pre-aggregation pour dashboard et fiches departement ;
- materialized views SQL pour stats territoriales ;
- cache serveur court sur endpoints stables ;
- cache client avec TanStack Query ;
- lazy loading des cartes et graphes lourds ;
- web workers si traitements front lourds.

### 13.3 Vues a adapter aux parcours UX retenus

Au vu du parcours final demande, les vues SQL les plus prioritaires deviennent :

1. `v_ui_landing_counts`

Doit retourner les decompteurs de landing page :

- nb gestionnaires ;
- nb etablissements ;
- nb contacts ESSMS ;
- nb entites financeurs ;
- nb contacts financeurs.

2. `v_ui_essms_gestionnaires_list`

Doit alimenter la liste gestionnaires avec tri par `nb_etablissements DESC`.

3. `v_ui_essms_gestionnaire_detail`

Doit alimenter la fiche gestionnaire complete.

4. `v_ui_essms_etablissements_list`

Doit alimenter la liste etablissements avec tous les filtres disponibles.

5. `v_ui_essms_contacts_list`

Doit alimenter la recherche de contacts ESSMS.

6. `v_ui_financeurs_list`

Doit alimenter la liste simple des entites financeurs.

7. `v_ui_financeurs_contacts_list`

Doit alimenter la recherche de contacts financeurs.

8. `mv_ui_financeurs_dept_coverage`

Materialized view pour la carte departementale coloree cote financeurs.

## 14. Securite et permissions

L'acces a l'espace doit fonctionner avec identifiant et mot de passe.

La documentation de build du futur workspace devra donc prevoir :

- page de login ;
- session utilisateur ;
- protection des routes applicatives ;
- acces aux donnees uniquement cote serveur Next.js.

### 14.1 Roles suggérés

- `admin`
- `analyst`
- `sales`
- `read_only`

### 14.2 Regles d'acces

- certaines fiches peuvent masquer des donnees sensibles ;
- l'export massif doit etre trace ;
- les listes partagees doivent etre journalisees ;
- les requetes a forte volumetrie doivent etre limitees.

### 14.3 Auditabilite

Tracer :

- recherches executees ;
- exports generes ;
- listes creees ;
- fiches consultees ;
- signaux les plus utilises.

## 15. Accessibilite et qualite d'usage

Le produit doit rester premium sans sacrifier l'accessibilite.

Exigences :

- contrastes AA minimum ;
- navigation clavier ;
- focus visibles ;
- graphiques legendees ;
- lecture correcte mobile et desktop ;
- textes de resume au-dessus des visualisations.

## 16. Responsive design

### 16.1 Desktop

Le desktop reste la surface principale de production.

Priorites :

- double colonne sur recherche et fiches ;
- panneaux latéraux ;
- cartes larges ;
- comparaisons simultanees.

### 16.2 Mobile

Le mobile doit rester consultable et utile.

Priorites :

- consultation des fiches ;
- suivi des signaux ;
- lecture des KPIs ;
- ajout rapide a une liste ;
- export simple.

## 17. Parcours utilisateurs types

### 17.1 Parcours commercial

1. Ouvrir `Recherche`.
2. Filtrer un departement et un type d'ESSMS.
3. Trier par `Prospect Score`.
4. Ouvrir 5 fiches prioritaires.
5. Ajouter les meilleures cibles a une liste.
6. Exporter en CSV ou XLSX.

### 17.2 Parcours analyste territorial

1. Ouvrir `Departements`.
2. Selectionner un departement.
3. Lire la fiche, la carte et les repartitions.
4. Comparer avec la moyenne regionale.
5. Identifier les gestionnaires et financeurs dominants.
6. Exporter un pack d'analyse.

### 17.3 Parcours direction

1. Ouvrir `Dashboard`.
2. Voir les KPIs et la heatmap nationale.
3. Explorer les top opportunites.
4. Suivre l'evolution des signaux.
5. Acceder a une synthese exportable.

## 18. Fonctionnalites intelligentes a forte valeur

### 18.0 Fonctionnalites financeurs a part entiere

L'angle financeurs ne doit pas etre limite a `financeur principal` sur les fiches ESSMS. Il faut un vrai sous-produit de veille institutionnelle.

Exemples de fonctionnalites a prevoir :

- annuaire intelligent des decideurs departementaux, ARS et PJJ ;
- vue `ou sont nos relais institutionnels par territoire ?` ;
- vue `ou manque-t-il un responsable tarification identifie ?` ;
- alertes sur les signaux de tension tarification / financement ;
- cartographie des territoires avec forte exposition institutionnelle.

Exemples de questions produit :

- Dans quels departements avons-nous un DGA identifie mais pas de responsable tarification ?
- Quelles ARS ont des contacts direction mais pas de relais tarification ?
- Quels territoires cumulent beaucoup d'ESSMS et peu de contacts institutionnels fiables ?
- Quels espaces PJJ meritent une veille ou une campagne specifique ?

### 18.1 Recommandations automatiques

- `Prospects similaires`
- `Departements voisins a fort potentiel`
- `Financeurs a surveiller`
- `Gestionnaires sous-exploites`

### 18.2 Requetes en langage naturel

L'interface peut embarquer un assistant de requetes guidees, sans exposer directement un chatbot envahissant.

Dans la cible deployee, cet assistant doit etre pense comme un **assistant IA configure avec Gemini** dans l'interface.

Il ne doit pas etre un agent libre sans garde-fous. Il doit s'appuyer sur :

- une bibliotheque de requetes SQL experte ;
- une connaissance du schema reel ;
- des templates de requetes par usage ;
- des reponses expliquees en langage metier.

#### Role attendu de l'assistant

- aider a formuler une recherche complexe ;
- transformer une intention metier en requete filtrable ;
- proposer une requete SQL ou une interpretation des donnees ;
- expliquer pourquoi un resultat remonte.

#### Contraintes importantes

- l'assistant ne doit pas executer des requetes SQL arbitraires cote navigateur ;
- il doit s'appuyer sur des requetes ou patrons valides, maintenus dans l'application ;
- il doit connaitre les limites du schema disponible ;
- il doit prioriser les parcours ESSMS et financeurs decrits dans cette documentation.

#### Bibliotheque de requetes expertes a prevoir

- requetes `landing counts` ;
- requetes `liste gestionnaires` ;
- requetes `fiche gestionnaire` ;
- requetes `liste etablissements` ;
- requetes `contacts ESSMS` ;
- requetes `liste financeurs` ;
- requetes `contacts financeurs` ;
- requetes `cartographie departementale financeurs`.

Exemples :

- `Donne-moi les ESSMS du 75 avec signaux recents et email disponible`
- `Quels departements ont beaucoup de structures mais peu de financeurs identifies ?`
- `Construis-moi une liste de 100 prospects prioritaires sur l'Arc atlantique`

### 18.3 Explication de la data

Chaque score important doit etre explicable.

L'utilisateur doit comprendre :

- pourquoi un ESSMS est prioritaire ;
- pourquoi un departement remonte ;
- pourquoi un financeur est relie a un segment.

## 19. Roadmap de livraison recommandee

### Phase 1 - MVP metier

- login et layout plateforme
- dashboard simple
- recherche facettee ESSMS
- fiches ESSMS et departement
- export CSV/XLSX

### Phase 2 - Valeur commerciale forte

- scoring prospect
- listes sauvegardees
- fiches financeurs
- signaux visibles dans recherche et fiches
- cartographie interactive

### Phase 3 - Data intelligence avancee

- comparaison multi-territoires
- recommandations automatiques
- requetes en langage naturel guidees
- exports premium PDF / cartographie

## 20. Criteres de succes produit

### KPI usage

- temps moyen pour produire une liste exportable
- taux d'usage de la recherche avancee
- nombre de listes sauvegardees
- nombre d'exports par utilisateur
- consultation des fiches departement

### KPI valeur metier

- taux d'utilisation commerciale des exports
- conversion de listes en prises de contact
- reduction du temps d'analyse territoriale
- usage recurrent des signaux et scores

## 21. Recommandation de mise en oeuvre

### 21.1 Positionnement technique

Construire une application Next.js BFF-first, en TypeScript, avec un design system proprietaire legerement editorial, une cartographie premium et une forte couche de scoring / signaux.

### 21.2 Positionnement produit

Le produit doit se situer entre :

- un outil de prospection qualifiee ;
- un observatoire territorial ESSMS ;
- un moteur d'intelligence de la donnee sectorielle.

### 21.3 Phrase directrice

`Une plateforme qui transforme la base ESSMS + financeurs + signaux en decisions commerciales et territoriales actionnables.`

## 22. Annexes - liste des livrables UX/UI a produire ensuite

Pour passer a l'implementation, les livrables suivants sont recommandes :

1. sitemap detaille
2. wireframes low-fi des ecrans clefs
3. UI kit / design tokens
4. maquettes high-fi desktop et mobile
5. spec des endpoints BFF
6. mapping exact des tables SQL -> modeles front
7. spec du scoring prospect
8. spec du module signaux
