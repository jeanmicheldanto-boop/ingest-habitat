# Check & nettoyage des URLs + plan d’enrichissement (handoff)

Date: 2026-01-30

Mise à jour (2026-01-31)
- Le prototype d’enrichissement (tarifs/services/logements + AVP HI/HIG) a été exécuté en **dry-run** sur **45 + 76** (échantillon `--limit 5`).
- La référence à jour (règles + état + prochaines étapes) est dans [ENRICHMENT_SPRINT_REFERENCE.md](ENRICHMENT_SPRINT_REFERENCE.md).

Objectif de ce document
- Résumer de façon synthétique la méthode mise en place pour auditer/nettoyer `etablissements.site_web`.
- Décrire le workflow “modération en base” via `propositions` / `proposition_items` (batch, traçabilité).
- Expliquer la contrainte `can_publish` (check SQL) et la stratégie d’application robuste.
- Donner une méthode proposée pour les prochaines actions d’enrichissement (tarification, tables, description) + un plan de test (76 & 64) et industrialisation (Cloud Run).

---

## 1) Périmètre & conventions

### Périmètre de sélection (URL)
Le périmètre “URL éligibles” correspond aux établissements:
- `is_test = false`
- `site_web` non NULL et non vide
- **exclusion par sous-catégorie** (configurable):
  - par défaut: Résidence autonomie (RA) + Résidence services seniors (RSS) + MARPA
  - pour certains comptages: exclusion RA/RSS uniquement (MARPA incluse)

### Pourquoi une modération via `propositions`
On évite les updates “en direct” sur `etablissements`:
- trace complète des changements (qui/quoi/pourquoi)
- possibilité d’approbation en masse
- application en batch (avec retries)
- compatible avec les contraintes de publication

---

## 2) Méthode: audit des URLs (`site_web`)

### Script principal (read-only)
Le script d’audit ne modifie jamais la base.
- Script: `scripts/analyze_suspicious_urls.py`
- Sorties:
  - `outputs/url_audit_<tag>.md` (rapport humain)
  - `outputs/url_audit_<tag>.csv` (détail machine)
  - optionnel: `outputs/url_review_auto_<tag>.csv` (format de “review” pour import automatique)

### Catégories produites
Le script normalise l’URL, extrait le domaine, puis attribue une catégorie:
- `allowlisted`: domaine explicitement accepté (règle métier)
- `excluded_domain`: domaines explicitement interdits (annuaires, plateformes, social, etc.)
- `document_host`: hébergeurs de documents (PDF, drive, etc.)
- `real_estate`: sites d’immobilier
- `social`: réseaux sociaux
- `directory_map`: annuaires/maps génériques
- `invalid`: URL invalide
- `likely_ok`: semble plausible mais non prouvé
- `ambiguous`: douteux / nécessite vérification

Décisions auto (avant LLM):
- `allowlisted` => `KEEP`
- `excluded_domain|document_host|real_estate|social|directory_map|invalid` => `DROP`
- `likely_ok|ambiguous` => décision vide (sauf si LLM activé)

### Allowlist (domaines “OK sans vérif”)
Une allowlist “par token dans le hostname” a été ajoutée.
- Avantage: réduit drastiquement le coût (LLM + scraping) et les faux positifs.
- Comportement: catégorie `allowlisted` => `KEEP` direct, **aucune vérification LLM**.

Exemples de tokens allowlist:
- `agesetvie`, `cettefamille`, `monsenior`, `udaf`, `habitat-humanisme`, `domani`, etc.

### Vérification LLM (optionnelle)
Objectif: confirmer que le site est bien:
1) “officiel” (établissement / gestionnaire / commune / gouv)
2) bien lié à l’habitat senior attendu (pas hors-sujet)

Chaîne de vérification:
- (A) scraping pour récupérer du texte
- (B) classification LLM sur le texte

Scraping:
- Par défaut: requêtes HTTP directes
- Optionnel: ScrapingBee si la page bloque les robots
- Robustesse: les timeouts / erreurs scraping ne doivent pas casser le batch (le script continue et marque un verdict `ERROR`).

LLM:
- Modèle: Gemini (par défaut `gemini-2.0-flash`)
- Clés:
  - `GEMINI_API_KEY` (prioritaire)
  - sinon `GOOGLE_MAPS_API_KEY`

Modes:
- `--verify-mode ambiguous`: ne vérifie que `ambiguous`
- `--verify-mode all`: vérifie `ambiguous` + `likely_ok`

Décision après LLM:
- si le verdict est “officiel” ET que le site parle bien de l’établissement senior: `KEEP`
- si le site est “annuaire / pdf / hors sujet”: `DROP`
- sinon: laisse vide (revue humaine possible)

### Options CLI clés
Exemples:
- Audit simple (read-only):
  - `python scripts/analyze_suspicious_urls.py --limit 500`
- Audit avec LLM (ambiguous):
  - `python scripts/analyze_suspicious_urls.py --limit 200 --verify-llm --verify-mode ambiguous --export-review-csv`
- Audit avec LLM (all):
  - `python scripts/analyze_suspicious_urls.py --limit 500 --verify-llm --verify-mode all --export-review-csv`
- Périmètre “hors RA/RSS” (MARPA incluse):
  - `python scripts/analyze_suspicious_urls.py --limit 50000 --safe-sous-categories ra_rss`

Exclusions (éviter de retraiter les mêmes établissements):
- `--exclude-review outputs/url_review_auto_YYYYMMDD_HHMMSS.csv`
- `--exclude-ids outputs/url_exclude_ids_processed.txt`

---

## 3) Workflow DB: proposer → accepter → appliquer

### Script workflow propositions
- Script: `scripts/url_propositions_workflow.py`
- Sous-commandes:
  - `import-auto-review --input <csv>` : crée des propositions uniquement pour les lignes actionnables (`DROP`/`REPLACE`)
  - `approve-from-list --input <txt>` : approuve en masse
  - `apply --mode approved --input <txt>` : applique en base
  - (optionnel) `republish-from-list` / `diagnose-publish-from-list`

### Séquençage batch recommandé
1) Audit read-only => produire `url_review_auto_*.csv`
2) Import => produire `url_proposition_ids_*.txt`
3) Approve => approuver les propositions
4) Apply => appliquer en base
5) Stats => re-run report / comptage

### Statistiques / contrôle (read-only)
- Script: `scripts/url_cleanup_report.py`
- Permet de:
  - compter les décisions (KEEP / DROP / empty)
  - vérifier le résultat DB (site_web vide après DROP, statut publie/draft, can_publish)

---

## 4) Contrainte `can_publish` et stratégie d’application

### Le problème
Certaines lignes sont `statut_editorial='publie'` mais ne satisfont pas les champs requis: `public.can_publish(id)=false`.
Dans ce cas, une contrainte SQL (check) peut empêcher un `UPDATE` sur l’établissement publié.

Effet:
- un simple `UPDATE etablissements SET site_web=NULL ... WHERE id=...` peut échouer même si le changement est légitime.

### La stratégie robuste
Lors de l’apply:
- on détecte le cas `publie AND NOT can_publish(id)`
- on force temporairement `statut_editorial='draft'`
- on applique le changement (DROP/REPLACE)
- (optionnel plus tard) on tente un republish si `can_publish` est redevenu true

Objectif prioritaire validé:
- **garantir que les DROP s’appliquent**, même sur des établissements publiés mais non publiables.

Note opérationnelle:
- On a choisi de “traiter tous les draft à la fin” (petit volume), plutôt que bloquer le nettoyage d’URL.

---

## 5) Résultat: comptage “reste-t-il des URLs suspectes?”

Pour répondre à la question “reste-t-il des URLs suspectes” sur le périmètre hors RA/RSS:
- on relance `analyze_suspicious_urls.py` (sans apply) avec `--safe-sous-categories ra_rss`
- on compte les catégories

Interprétation:
- “suspicious certain” = `excluded_domain|document_host|real_estate|social|directory_map|invalid`
- `ambiguous` et `likely_ok` ne sont pas “certainement mauvaises”, mais candidates à vérification LLM si on veut augmenter la précision.

---

## 6) Prochaines actions: enrichissement tarification & qualité des descriptions

### Objectifs de contenu
1) Enrichir `etablissements`:
- fourchette de tarification (min/max ou équivalent)
- informations “résumées” cohérentes avec les tables détaillées

2) Enrichir les tables:
- `tarifications`
- `logements`
- `restauration`
- `services`

3) Évaluer et améliorer la qualité des descriptions
Constat:
- certaines descriptions ont été générées avec des modèles hétérogènes (ex: GPT-5, Llama)
- symptômes: texte trop court, trop générique, ton mécanique

Cible:
- descriptions plus spécifiques, informatives, lisibles, non “template”
- réécriture à partir de signaux factuels (site officiel si possible, sinon recherche)

### Méthode proposée (pipeline d’enrichissement)

#### 6.1 Acquisition des sources
On combine:
- Serper (recherche web) pour trouver:
  - page tarifs / brochure / PDF
  - page “services” / “restauration” / “hébergement”
  - page du gestionnaire si pertinent
- ScrapingBee (si possible) pour scraper:
  - plusieurs pages (pas seulement la home)
  - pages profondes: `/tarifs`, `/services`, `/restauration`, `/hebergement`, `/residence`, etc.

Stratégie multi-pages:
- partir de l’URL officielle si disponible
- crawler léger: extraire quelques liens internes (même domaine) avec mots-clés
- limiter le nombre de pages (budget + latence) et prioriser par score (keywords “tarif”, “prix”, “prestations”, “restauration”…)

#### 6.2 Extraction structurée
Utiliser Gemini (au minimum `gemini-2.0-flash`, ou modèle plus puissant si nécessaire) pour:
- extraire des champs structurés à partir de textes scrapés:
  - tarification: montant(s), unité (€/mois, €/jour), conditions, entrée de gamme / haut de gamme
  - services: liste normalisée, éléments différenciants
  - restauration: types (sur place, livrée), options
  - logements: types (T1/T2…), surfaces, équipements

Modules ajoutés (briques réutilisables):
- `scripts/llm_cascade.py`: cascade flash → pro si la sortie est incomplète
- `scripts/crawl_priority.py`: priorisation des URLs internes à crawler (même domaine)

#### 6.3 Contrôles de cohérence
Exemples de contrôles:
- cohérence `etablissements.prix_min/prix_max` vs lignes dans `tarifications`
  - si `tarifications` contient un min/max => la fourchette de `etablissements` doit englober
  - si `etablissements` a une fourchette, mais `tarifications` vide => marquer “à compléter”
- unités:
  - éviter de mélanger €/jour et €/mois sans conversion / précision
- valeurs aberrantes:
  - prix trop bas/haut hors distribution attendue => flag “à vérifier”

Module ajouté:
- `scripts/coherence_validator.py`: checks PASS/WARN/FAIL + score 0–100, basé sur `etablissements`, `tarifications` et `etablissement_service`

#### 6.4 Réécriture de description (qualité)
Étapes:
1) Scorer la description existante (heuristiques):
- longueur minimale
- densité de faits (chiffres, services, localisation)
- répétitions / phrases vides
- “style template” (phrases génériques)

Module ajouté:
- `scripts/enrich_quality_scorer.py`: scoring heuristique (et optionnellement Gemini en fallback) + `needs_rewrite`

2) Si score faible:
- re-générer une description à partir:
  - du scraping du site (prioritaire)
  - sinon Serper + extraits
- modèle:
  - Gemini 2 Flash par défaut
  - escalade vers un modèle plus puissant si manque de précision

3) Output:
- une description:
  - factuelle, spécifique, sans superlatifs non sourcés
  - structurée (2–3 paragraphes) + éventuellement liste courte de services

---

## 7) Plan d’exécution recommandé

### Phase A — Prototype sur 2 départements
Départements cibles:
- 76
- 64

Objectifs:
- valider extraction “tarifs” sur vrais sites
- calibrer la navigation multi-pages
- calibrer les prompts et le schéma de sortie
- mesurer qualité / taux de réussite / coût

Livrables:
- un script CLI “enrichissement complet” sur un département
- exports CSV/MD de contrôle
- création de propositions prêtes à appliquer

### Phase B — Passage à l’échelle (industrialisation)
Approche:
- traiter les établissements en parallèle (par lots) via Google Cloud Run
- orchestration:
  - découpage en lots (ex: 50–100 établissements)
  - retry avec backoff (Serper/ScrapingBee/LLM)
  - journalisation structurée

Précautions:
- limiter concurrence ScrapingBee
- budget LLM contrôlé (gating: n’appeler le modèle “cher” qu’en fallback)
- idempotence: ne pas recréer 10 fois la même proposition

---

## 8) Notes pratiques & variables d’environnement

Clés utiles:
- `GEMINI_API_KEY` (ou `GOOGLE_MAPS_API_KEY`)
- `SCRAPINGBEE_API_KEY` (optionnel)
- `SERPER_API_KEY` (pour la suite enrichissement)
- `GEMINI_MODEL` (par défaut gemini-2.0-flash)

Bonnes pratiques:
- conserver les CSV d’audit et les listes d’IDs de propositions (traçabilité)
- batcher petit d’abord, mesurer, puis augmenter

---

## 9) “Starter prompt” pour une nouvelle conversation

Contexte:
- Nous avons un pipeline URL + workflow propositions robuste.
- Nous voulons maintenant enrichir tarification/logements/restauration/services + améliorer les descriptions.

Demande type:
- Implémenter un script `scripts/enrich_department.py` qui:
  1) prend un département
  2) récupère les établissements éligibles
  3) trouve sources (Serper)
  4) scrape plusieurs pages (ScrapingBee si besoin)
  5) extrait JSON structuré (Gemini)
  6) fait des contrôles de cohérence (tarifications vs fourchette)
  7) génère des propositions (update + insert tables)
  8) exporte un rapport + stats

Critères de succès:
- taux d’extraction “tarifs” acceptable
- descriptions améliorées (score qualité > seuil)
- propositions applicables sans casser `publie` (gestion `can_publish`)
