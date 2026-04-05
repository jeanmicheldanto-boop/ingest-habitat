# Handoff Dev - Application Next.js ESSMS & Financeurs

## 1. A quoi sert ce document

Ce document est la version de transmission rapide pour l'equipe ou l'agent qui construira l'application dans un autre workspace.

Il indique :

- ce qu'il faut construire ;
- sur quelles donnees s'appuyer ;
- quelles sont les priorites ;
- quels sont les pieges a eviter.

## 2. Ce qu'il faut construire

Construire une application Next.js protegee par login / mot de passe avec deux univers d'exploration :

- `ESSMS`
- `Financeurs`

L'application ne doit pas etre un dashboard generique. Elle doit privilegier :

- landing page simple ;
- listes filtrees ;
- fiches detaillees ;
- exports Excel ;
- cartographie ;
- assistant IA Gemini.

## 3. Experience utilisateur cible

### 3.1 Landing

Deux gros blocs :

- `ESSMS`
- `Financeurs`

Avec compteurs :

- ESSMS : gestionnaires, etablissements, contacts ESSMS
- Financeurs : entites, contacts financeurs

### 3.2 Univers ESSMS

Trois modules :

- `Gestionnaires`
- `Etablissements`
- `Contacts`

#### Gestionnaires

Filtres :

- departement du siege
- taille
- categories d'etablissements
- financeur principal
- signaux oui/non

Tri par defaut :

- `nombre d'etablissements desc`

Fiche :

- infos generales
- etablissements
- financeurs
- dirigeants
- signaux
- DAF si disponible

#### Etablissements

Filtres sur les champs disponibles en base.

Fiche :

- infos structure
- gestionnaire
- financement / tarification
- signaux
- contexte institutionnel

#### Contacts ESSMS

Filtres :

- fonction
- taille du gestionnaire
- signal du gestionnaire
- departement

### 3.3 Univers Financeurs

Trois modules :

- `Entites financeurs`
- `Contacts financeurs`
- `Carte departements`

#### Entites financeurs

Filtre principal :

- type (`departement`, `ars`, `dirpjj`)

Fiche :

- identite
- type
- contacts
- signaux
- couverture territoriale

#### Contacts financeurs

Recherche directe avec filtres :

- type entite
- niveau
- territoire
- confiance
- email / LinkedIn

#### Carte financeurs

- carte de departements colores
- pas une carte de points

## 4. Contraintes techniques non negociables

1. pas de backend Python
2. pas d'acces SQL depuis le navigateur
3. acces DB uniquement cote serveur Next.js
4. pas de SQL libre execute brut depuis le module IA
5. utiliser les vues SQL dediees au front

## 5. Schema utile a connaitre

### ESSMS

- `finess_etablissement`
- `finess_gestionnaire`
- `finess_dirigeant`

### Financeurs

- `prospection_entites`
- `prospection_contacts`
- `prospection_signaux`

## 6. Volumes observes

- `finess_etablissement` : 51 715
- `finess_gestionnaire` : 16 498
- `finess_dirigeant` : 70 609
- `prospection_entites` : 128
- `prospection_contacts` : 456

Conclusion :

- ESSMS = navigation volumique, besoin de vues / index serieux
- Financeurs = volume plus faible, mais forte valeur relationnelle

## 7. Vues SQL a demander / utiliser

Priorite absolue :

- `v_ui_landing_counts`
- `v_ui_essms_gestionnaires_list`
- `v_ui_essms_gestionnaire_detail`
- `v_ui_essms_etablissements_list`
- `v_ui_essms_contacts_list`
- `v_ui_financeurs_list`
- `v_ui_financeurs_contacts_list`
- `mv_ui_financeurs_dept_coverage`

## 8. Index a surveiller

Ne pas supposer que les index actuels suffisent.

Verifier / ajouter :

- composites ESSMS sur departement / statut / categorie
- indexes gestionnaire / signal / taille
- indexes contacts financeurs sur entite + niveau
- trigram sur noms / raison sociale / poste si disponible

## 9. Assistant IA

Provider cible :

- `Gemini`

Principe :

- l'utilisateur formule une intention ;
- l'assistant choisit une requete experte ou un template ;
- l'application execute cote serveur ;
- l'assistant explique le resultat.

Ne jamais faire :

- champ texte libre -> SQL arbitraire execute tel quel

## 10. Ordre recommande de build

### Etape 1

- auth
- layout protegee
- landing counts

### Etape 2

- gestionnaires list
- gestionnaire detail
- export Excel gestionnaires

### Etape 3

- etablissements list
- etablissements detail
- contacts ESSMS
- carte ESSMS

### Etape 4

- financeurs list
- financeur detail
- contacts financeurs
- carte financeurs

### Etape 5

- assistant Gemini
- polish UX
- performances

## 11. Pieges a eviter

- vouloir tout faire en une seule page monolithique
- reconstruire les agregats cote front
- confondre carte ESSMS et carte financeurs
- oublier le tri obligatoire des gestionnaires par nombre d'etablissements
- traiter les financeurs comme un simple champ secondaire
- ne pas documenter les vues SQL dans le nouveau workspace

## 12. Documents de reference a utiliser ensemble

- `DOCUMENTATION_INTERFACE_NEXTJS_ESSMS.md`
- `SPEC_SQL_INTERFACE_NEXTJS_ESSMS.md`
- `SPEC_WORKSPACE_NEXTJS_ESSMS.md`

## 13. Question directrice a garder

Si une decision produit ou technique est floue, revenir a cette question :

`Est-ce que cela aide vraiment un utilisateur a explorer rapidement les ESSMS ou les financeurs, filtrer, comprendre, cartographier et exporter ?`
