# 🔍 DIAGNOSTIC QUALITÉ DES DONNÉES ESSMS

**Date:** 7 mars 2026  
**Contexte:** Audit complet des données contacts/dirigeants ESSMS  
**Échantillons analysés:** 1000+ contacts, 500+ domaines email

---

## 📊 SYNTHÈSE EXÉCUTIVE

### Problèmes majeurs identifiés

| Problème | Volumétrie | Impact | Priorité |
|----------|------------|--------|----------|
| **Domaines email inexacts** | **84.4%** (422/500) | 🔴 CRITIQUE | P0 |
| **Biais pattern `prenom.nom`** | **96.6%** (28 976/30 000) | 🔴 CRITIQUE | P0 |
| **Doublons DG/Président** | 259 gestionnaires | 🟠 ÉLEVÉ | P1 |
| **Directeurs sans précision** | 34.2% (342/1000) | 🟡 MOYEN | P2 |
| **Fonctions suspectes** | 0% détecté | 🟢 FAIBLE | P3 |

**Conclusion:** ~85% des emails reconstruits sont probablement **INVALIDES** car basés sur des domaines incorrects.

---

## 🚨 PROBLÈME 1: DOMAINES EMAIL INEXACTS (CRITIQUE)

### Constats

#### Analyse échantillon 500 contacts:
- ✅ **Domaines pertinents:** 78 (15.6%)
- ❌ **Domaines sans lien sémantique:** 422 (84.4%)
- ❌ **Domaines génériques:** 0% (non détectés dans cet échantillon)
- ❌ **Domaines institutionnels:** 0% (non détectés dans cet échantillon)

### Exemples concrets

#### Cas FONDATION SAVART
**Gestionnaire:** FONDATION SAVART  
**Domaine reconstruit:** `@adsea02.org` (ADSEA 02 = autre association)  

**Contacts concernés:**
- Juliette DIEUSAERT → `juliette.dieusaert@adsea02.org`
- Eric Denicourt → `eric.denicourt@adsea02.org`
- Audrey Briquet → `audrey.briquet@adsea02.org`
- +7 autres contacts

❌ **Incohérence:** ADSEA ≠ FONDATION SAVART → emails invalides

#### Domaines aberrants détectés
```
- jorfsearch.steinertriples.ch   (site technique sans rapport)
- zones-humides.org               (site écologique générique)
- adsea02.org                     (autre association)
```

### Problème additionnel: Emails malformés (reconstruction défectueuse)

#### Analyse échantillon 1000 contacts:
- ❌ **Emails avec "null":** 22 (2.2%)
- ❌ **Emails avec noms génériques:** 6 (0.6%) - "dupont@", "martin@"
- ❌ **Emails avec parties manquantes:** 0 (0.0%)
- ✅ **Structure valide:** 972 (97.2%)

**Total emails malformés:** 28 (2.8%)

#### Exemples concrets d'emails malformés

**Cas 1: Contact sans nom/prénom (null.null@)**
```
- null null @ MAISON DE RETRAITE DE COUCY-LE CHATEAU
  → null.null@ccomptes.fr

- null null @ FEDERATION ADMR DE L'AISNE
  → null.null@aisne.com

- null null @ MAISON DE RETRAITE DE BUIRONFOSSE
  → null.null@lesmaisonsderetraite.fr
```
❌ **Cause:** Extraction NER n'a pas détecté de nom → champs `prenom` et `nom` = NULL → reconstruction = "null.null@domain"

**Cas 2: Noms génériques placeholder (Dupont, Martin)**
```
- Olivier DUPONT @ CENTRE HOSPITALIER DE CHATEAU THIERRY
  → olivier.dupont@journals.openedition.org

- Pierre DUPONT @ ASSOCIATION TUTELAIRE DEPARTEMENTALE
  → pierre.dupont@archivesdepartementales.aude.fr

- Patrice MARTIN @ ASSOCIATION ITINERANCE
  → patrice.martin@psmigrants.org
```
❌ **Cause:** Scraping a capturé des "Dupont" et "Martin" utilisés comme exemples/placeholders dans les sources → pas de vraies personnes

#### Impact cumulé domaines + malformés

**Emails totalement inutilisables:**
- Domaines incorrects: 84.4%
- Emails malformés: 2.8%
- **Cumul:** ~87% d'emails non exploitables

**Correction requise:** Validation domaine + suppression contacts "null.null" + flagging noms génériques

### Problème additionnel: Biais systémique vers `prenom.nom`

#### Vérification empirique (base réelle)
- `finess_dirigeant` (n=30 000):
  - `prenom.nom`: **28 976** (**96.6%**)
  - `p.nom`: 533 (1.8%)
  - `prenom-nom`: 491 (1.6%)
- `finess_gestionnaire.structure_mail` (n=1 000):
  - `prenom.nom`: **975** (**97.5%**)
  - `p.nom`: 15 (1.5%)
  - `prenom-nom`: 8 (0.8%)
  - `prenom_nom`: 2 (0.2%)

#### Diagnostic
Le pipeline n'infère pas suffisamment le schéma réel: en cas d'ambiguïté ou d'absence d'exemples fiables, il retombe majoritairement sur `prenom.nom`. Ce fallback devient de facto la règle et écrase l'objectif initial (dériver le pattern réel à partir d'exemples trouvés via Serper/LLM).

#### Impact
1. Génération d'adresses plausibles mais incorrectes pour les domaines qui utilisent d'autres conventions.
2. Hausse mécanique du taux de bounce malgré un domaine valide.
3. Fausse impression de couverture email élevée alors que la précision est faible.

#### Solution proposée
1. Supprimer le fallback systématique `prenom.nom`.
2. Reconstruire uniquement si `structure_mail` est explicitement inférée et fiable.
3. Introduire un statut `email_reconstruction_status` (`done`, `skipped_low_confidence`, `skipped_no_pattern`).
4. Si pattern inconnu: ne pas reconstruire et pousser le gestionnaire en file de qualification (Serper + LLM) au lieu de générer une adresse par défaut.
5. N'autoriser `prenom.nom` que s'il est détecté par exemples concordants (pas comme valeur par défaut).

### Impact

1. **Emails invalides** → Impossible de contacter les dirigeants
2. **Bounce rate élevé** → Détérioration réputation email
3. **Perte de leads qualifiés** → ROI campagnes = 0
4. **Fiabilité données = 15.6%** → Base inexploitable en l'état

### Solution requise

#### Phase 1: Validation domaine (URGENT)
```javascript
Pour chaque gestionnaire:
  1. Identifier website_officiel depuis FINESS/Sirene
  2. Si website_officiel absent:
     → Recherche Serper: "[raison_sociale] site officiel"
     → LLM validation: site correspond au gestionnaire? (Oui/Non)
  3. Extraire domaine du site officiel validé
  4. Reconstruire emails avec nouveau domaine
```

**Coût estimé:**
- 5000 gestionnaires uniques × 1 recherche Serper = 5000 appels
- Validation LLM: seulement si ambiguïté (estim. 20%) = 1000 appels
- **Total:** ~$50-100 pour clean complet

#### Phase 1.5: Nettoyage contacts malformés (AVANT reconstruction)
```sql
-- Supprimer contacts "null null"
DELETE FROM finess_dirigeant
WHERE nom IS NULL 
   OR prenom IS NULL
   OR nom = 'null'
   OR prenom = 'null'
   OR LOWER(CONCAT(prenom, ' ', nom)) = 'null null';

-- Flagging noms génériques suspects (à valider manuellement)
UPDATE finess_dirigeant
SET to_review = true, 
    review_reason = 'Nom générique suspect'
WHERE LOWER(nom) IN ('dupont', 'martin', 'durand', 'bernard', 'thomas', 'robert', 'richard', 'petit')
  AND prenom IS NOT NULL;
```

**Impact:**
- Suppression ~22 contacts "null null" (2.2%)
- Flagging ~6 noms génériques pour validation manuelle (0.6%)
- Base nettoyée avant reconstruction emails

#### Phase 2: Reconstruction emails
```sql
-- Nouvelle colonne domain_valide
ALTER TABLE finess_gestionnaire 
ADD COLUMN domain_valide TEXT;

-- (Recommandé) colonne de confiance pattern pour gate de reconstruction
ALTER TABLE finess_gestionnaire
ADD COLUMN IF NOT EXISTS structure_mail_confiance TEXT;

-- Suppression anciens emails invalides
UPDATE finess_dirigeant
SET email_reconstitue = NULL
WHERE email_reconstitue IS NOT NULL;

-- Reconstruction avec pattern détecté (PAS de fallback prenom.nom)
UPDATE finess_dirigeant fd
SET email_reconstitue = CASE
  WHEN g.structure_mail = 'prenom.nom' THEN
    LOWER(REPLACE(fd.prenom, ' ', '.')) || '.' || LOWER(REPLACE(fd.nom, ' ', '')) || '@' || g.domain_valide
  WHEN g.structure_mail = 'p.nom' THEN
    LOWER(LEFT(REPLACE(fd.prenom, ' ', ''), 1)) || '.' || LOWER(REPLACE(fd.nom, ' ', '')) || '@' || g.domain_valide
  WHEN g.structure_mail = 'nom.prenom' THEN
    LOWER(REPLACE(fd.nom, ' ', '')) || '.' || LOWER(REPLACE(fd.prenom, ' ', '.')) || '@' || g.domain_valide
  WHEN g.structure_mail = 'prenom-nom' THEN
    LOWER(REPLACE(fd.prenom, ' ', '-')) || '-' || LOWER(REPLACE(fd.nom, ' ', '-')) || '@' || g.domain_valide
  WHEN g.structure_mail = 'prenom_nom' THEN
    LOWER(REPLACE(fd.prenom, ' ', '_')) || '_' || LOWER(REPLACE(fd.nom, ' ', '_')) || '@' || g.domain_valide
  ELSE NULL
END
FROM finess_gestionnaire g
WHERE fd.id_gestionnaire = g.id_gestionnaire
  AND g.domain_valide IS NOT NULL
  AND fd.prenom IS NOT NULL
  AND fd.nom IS NOT NULL
  AND fd.to_review IS NOT true
  AND g.structure_mail IN ('prenom.nom', 'p.nom', 'nom.prenom', 'prenom-nom', 'prenom_nom')
  AND COALESCE(g.structure_mail_confiance, 'basse') IN ('haute', 'moyenne');
```

---

## 👥 PROBLÈME 2: DOUBLONS DG/PRÉSIDENT (ÉLEVÉ)

### Constats

**Total DG/Présidents:** 1000  
**Gestionnaires affectés:**
- Multiples DG: **99** (certains avec 3 DG!)
- Multiples Présidents: **160** (jusqu'à 4 présidents)

### Exemples

#### Cas: ASSOCIATION ECLAT
```
Laurent CASTAING       → DG
Haliki CHOUA          → DG
Emmanuelle LACAILLE   → DG
```
❌ 3 DG pour 1 gestionnaire = incohérent

#### Cas: EHPAD LA MAISON A SOIE
```
Gaël ALLAIN           → Président
Florian MALARD        → Président
Nelly BOUTEAUD        → Président
```
❌ 3 Présidents simultanés = impossible

#### Cas particulier: Présidents "fantômes"
```
MAISON DE RETRAITE DE PONT D'AIN:
  → Conseil départemental de l'Ain       (Président)
  → Université Savoie Mont Blanc         (Président)
  → Université Paris-IV - Paris-Sorbonne (Président)
```
❌ Entités organisationnelles détectées comme personnes

### Causes racines

1. **Données historiques non nettoyées** (anciens + actuels)
2. **Scraping multiple sources** sans dédoublonnage
3. **Absence de date de mandat** → impossible de distinguer actuel/passé
4. **Erreurs d'extraction NER** (entités vs personnes)

### Solution requise

#### Règle métier
```
Un gestionnaire ne peut avoir que:
  - 1 seul DG/Directeur Général
  - 1 seul Président
```

#### Stratégie de dédoublonnage

**Critères de sélection (ordre de priorité):**
1. **Date la plus récente** (si available)
2. **Score de confiance le plus élevé** (champ `confiance`)
3. **Email vérifié** (si `email_verifie = true`)
4. **LinkedIn URL présent** (plus de chances d'être actuel)
5. **Nom complet** (exclure "Dupont", "null null")

**Implémentation:**
```sql
-- Script de dédoublonnage
WITH ranked_dgs AS (
  SELECT 
    id,
    id_gestionnaire,
    fonction_normalisee,
    ROW_NUMBER() OVER (
      PARTITION BY id_gestionnaire, 
      CASE 
        WHEN fonction_normalisee ILIKE '%directeur général%' THEN 'DG'
        WHEN fonction_normalisee ILIKE '%président%' THEN 'PRESIDENT'
      END
      ORDER BY 
        confiance DESC NULLS LAST,
        CASE WHEN email_verifie THEN 1 ELSE 0 END DESC,
        CASE WHEN linkedin_url IS NOT NULL THEN 1 ELSE 0 END DESC,
        CASE WHEN nom IS NOT NULL AND nom != 'Dupont' THEN 1 ELSE 0 END DESC
    ) as rn
  FROM finess_dirigeant
  WHERE fonction_normalisee ILIKE '%directeur général%'
     OR fonction_normalisee ILIKE '%président%'
)
-- Marquer comme "à supprimer" les doublons (rn > 1)
UPDATE finess_dirigeant
SET to_delete = true
FROM ranked_dgs
WHERE finess_dirigeant.id = ranked_dgs.id
  AND ranked_dgs.rn > 1;
```

**Validation manuelle requise:** Top 50 gestionnaires (vérifier avant suppression)

---

## 👔 PROBLÈME 3: DIRECTEURS SANS PRÉCISION (MOYEN)

### Constats

**Contacts affectés:** 342/1000 (34.2%)

**Fonctions vagues détectées:**
- `Directeur` (sans précision)
- `Directrice` (sans précision)

**Exemples:**
```
- Patrick CRÉTINON → "Directeur" @ RESIDENCE FONTELUNE
- Claude MARECHAL → "Directeur" @ LA RESIDENCE D'URFE
- null null → "Directeur" @ EHPAD L'ALBIZIA
```

### Fonctions précises souhaitées

```
✅ Directeur d'Établissement
✅ Directeur de Pôle
✅ Directeur de Site
✅ Directeur Qualité
✅ Directeur des Ressources Humaines
```

### Impact

- **Manque de ciblage** pour campagnes (impossible de filtrer par spécialité)
- **Confusion lors du contact** (quel directeur?)
- **Perte de contexte** pour approche commerciale

### Solution requise

#### Enrichissement par LLM (contextuel)

**Prompt enrichissement:**
```
Contexte:
- Gestionnaire: {raison_sociale}
- Catégorie: {categorie_etablissement}
- Secteur: {secteur_activite}
- Contact: {prenom} {nom}
- Fonction actuelle: "Directeur"

Question: Dans ce contexte ESSMS, quelle est la fonction précise 
la plus probable pour ce directeur?

Réponse (choisir parmi):
- Directeur d'Établissement
- Directeur de Pôle
- Directeur de Site
- Directeur Général Adjoint
- Autre: [préciser]
```

**Coût estimé:**
- 342 contacts × 1 appel LLM = $1-3

**Alternative low-cost:**
```sql
-- Règle métier par défaut
UPDATE finess_dirigeant
SET fonction_normalisee = 'Directeur d''Établissement'
WHERE fonction_normalisee IN ('Directeur', 'Directrice')
  AND id_gestionnaire IN (
    SELECT id_gestionnaire 
    FROM finess_gestionnaire 
    WHERE categorie_taille IN ('Petit', 'Moyen')
  );

-- Pour les grands groupes: garder "Directeur" (peut être Directeur de Région, etc.)
```

---

## 🎭 PROBLÈME 4: FONCTIONS SUSPECTES (FAIBLE)

### Constats

**Contacts affectés:** 0/1000 (0%)

**Fonctions recherchées (non détectées):**
- Commissaire aux Comptes
- Expert-Comptable
- Avocat
- Consultant externe

### Conclusion

✅ **Pas de problème identifié** sur cet échantillon.

**Recommandation:** Surveillance continue lors des prochains scraping (filtre à l'ingestion).

---

## 📋 PLAN D'ACTION PRIORISÉ

### Phase 0: Préparation (Immédiat)

**Objectif:** Créer infrastructure de quality check

- [ ] Script validation domaine (Serper + LLM minimal)
- [ ] Script dédoublonnage DG/Président avec ranking
- [ ] Script enrichissement directeurs (règle métier + LLM optionnel)
- [ ] Dashboard monitoring qualité données

**Durée:** 2-3 jours  
**Coût:** 0€ (dev interne)

---

### Phase 1: Validation domaines (P0 - CRITIQUE)

**Objectif:** Nettoyer les 84.4% de domaines invalides

#### Étapes:
1. **Extraction gestionnaires uniques** (~5000)
2. **Recherche domaine officiel:**
   ```
   - Source 1: Champs existants (website_url, site_web)
   - Source 2: Scraping FINESS API (si dispo)
   - Source 3: Serper API (fallback)
   ```
3. **Validation LLM** (si multiples résultats ou ambiguïté)
4. **Stockage** dans `finess_gestionnaire.domain_valide`
5. **Reconstruction emails** en masse

#### Estimations:
- **Durée:** 3-5 jours (run automatisé)
- **Coût API:**
  - Serper: 3000 appels × $0.01 = $30
  - LLM validation: 1000 appels × $0.002 = $2
  - **Total:** ~$35-50
- **Gain:** Base emails passant de 15.6% → 85%+ validité

#### Métriques de succès:
- ✅ 90%+ gestionnaires avec `domain_valide` renseigné
- ✅ Cohérence sémantique domaine/raison_sociale > 80%
- ✅ Réduction bounce rate emails < 5%

### Phase 1.5: Nettoyage emails malformés (P0 - CRITIQUE)

**Objectif:** Supprimer contacts "null.null" et flagging noms génériques avant reconstruction

#### Étapes:
1. **Suppression contacts null.null** (SQL DELETE)
2. **Flagging noms génériques** (Dupont, Martin, etc.)
3. **Export CSV** noms génériques pour validation manuelle
4. **Validation + décision** (garder/supprimer)
5. **Application décisions** (suppression si confirmé invalide)

#### Estimations:
- **Durée:** 0.5 jour (1/2 journée)
 - **Gain:** Élimination 2.8% d'emails malformés + amélioration qualité base

#### Métriques de succès:
- ✅ 0 contact avec nom/prénom = NULL
- ✅ 0 email "null.null@domain.com"
- ✅ <0.1% noms génériques non validés

---

---

### Phase 2: Dédoublonnage dirigeants (P1 - ÉLEVÉ)

**Objectif:** 1 seul DG + 1 seul Président par gestionnaire

#### Étapes:
1. **Exécution script ranking** (voir section Problème 2)
2. **Export CSV top 100** gestionnaires pour validation manuelle
3. **Validation + ajustements** si nécessaire
4. **Application dédoublonnage** en masse (soft delete)
5. **Refresh vues matérialisées**

#### Estimations:
- **Durée:** 1-2 jours
- **Coût:** 0€ (SQL pur)
- **Gain:** Cohérence gouvernance + fiabilité fiches gestionnaires

#### Métriques de succès:
- ✅ 0 gestionnaire avec >1 DG
- ✅ 0 gestionnaire avec >1 Président
- ✅ Conservation du contact le plus fiable (confiance max)

---

### Phase 3: Enrichissement directeurs (P2 - MOYEN)

**Objectif:** Préciser les 342 "Directeur" vagues

#### Étapes:
1. **Application règle métier** (Petit/Moyen gestionnaire → "Directeur d'Établissement")
2. **LLM enrichissement** pour grands groupes (optionnel, si budget)
3. **Update base** + refresh vues

#### Estimations:
- **Durée:** 1 jour
- **Coût:** $0-3 (selon stratégie LLM)
- **Gain:** Ciblage campagnes + contexte commercial

#### Métriques de succès:
- ✅ <5% "Directeur" sans précision
- ✅ Fonctions cohérentes avec catégorie_gestionnaire

---

### Phase 4: Monitoring continu (P3 - PRÉVENTIF)

**Objectif:** Éviter régression lors des futurs scraping

#### Outils:
- **Quality check pipeline** (pre-ingestion):
  ```
  - Validation domaine avant reconstruction email
  - Détection doublons DG/Président
  - Flagging fonctions suspectes (CAC, Avocat, etc.)
  ```
- **Dashboard qualité temps réel:**
  ```
  - % domaines validés
  - Nb doublons DG/Président
  - % fonctions précises
  ```

#### Estimations:
- **Durée setup:** 2-3 jours
- **Coût run:** marginal (checks automatisés)
- **Gain:** Propreté données maintenue dans le temps

---

## 💰 BUDGET & ROI

### Coûts totaux

| Phase | Durée | Coût API | Coût dev |
|-------|-------|----------|----------|
| Phase 0: Infrastructure | 2-3j | $0 | Interne |
| Phase 1: Domaines | 3-5j | $35-50 | Interne |
| Phase 1.5: Emails malformés | 0.5j | $0 | Interne |
| Phase 2: Dédoublonnage | 1-2j | $0 | Interne |
| Phase 3: Enrichissement | 1j | $0-3 | Interne |
| Phase 4: Monitoring | 2-3j | $0 | Interne |
| **TOTAL** | **9.5-14.5j** | **$35-53** | **Interne** |

### ROI attendu

**Avant nettoyage:**
- Validité emails: **~13%** (84.4% domaines incorrects + 2.8% malformés)
- Bounce rate: **>85%**
- Coût lead qualifié: **INFINI** (emails ne passent pas)

**Après nettoyage:**
- Validité emails: **85-90%**
- Bounce rate: **<10%**
- Coût lead qualifié: **Normal** (campagnes fonctionnelles)

**Gain business:**
```
Campagne 10,000 contacts:
  - Avant: 1,560 emails valides → 156 ouvertures (10%) → ~15 leads
  - Après: 8,500 emails valides → 850 ouvertures (10%) → ~85 leads
  
ROI = 5.6x amélioration taux de lead
```

---

## 🎯 RECOMMANDATIONS FINALES

### Priorités immédiates (Semaine 1)

1. ✅ **VALIDER ce diagnostic** avec équipe data
2. ✅ **BLOQUER nouvelles campagnes email** (bounce rate actuel catastrophique)
3. ✅ **LANCER Phase 1** (validation domaines) immédiatement

### Quick wins (< 48h)

- Appliquer règle dédoublonnage DG/Président (SQL pur)
- Appliquer règle métier "Directeur" → "Directeur d'Établissement" pour petits/moyens
- Flagging contacts avec domaines suspects dans l'UI

### Success metrics (1 mois)

- [ ] **90%+** gestionnaires avec domaine validé
- [ ] **0** doublon DG/Président
- [ ] **<5%** "Directeur" sans précision
- [ ] **Bounce rate email < 10%**
- [ ] **Campagne test** avec 100 contacts nettoyés → validation terrain

---

## 📞 CONTACTS & SUPPORT

**Responsable data quality:** [À définir]  
**Repo scripts:** `ingest-habitat/scripts/quality-checks/`  
**Dashboard:** [À créer]

---

**Document généré le:** 7 mars 2026  
**Prochaine revue:** Après Phase 1 (validation domaines)  
**Version:** 1.0
