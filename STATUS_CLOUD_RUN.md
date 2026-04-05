# Statut – Industrialisation Cloud Run (Habitat enrich)

Dernière mise à jour : 2026-01-31

## Objectif

Industrialiser un enrichissement “Habitat intermédiaire seniors” en batch sur **Google Cloud Run Jobs** :

- D’abord valider un run **read-only / dry-run** sur un échantillon.
- Ensuite écrire via tables de **modération** (workflow `propositions` / `proposition_items`) plutôt que modifier les tables finales directement.
- Respecter le garde-fou métier côté DB (`public.can_publish(id)`), puis “apply/republish” via un workflow dédié.

## Ce qui est en place

### 1) Image Docker minimaliste pour Cloud Run

- Image basée sur `python:3.11-slim`.
- Dépendances minimales pour exécuter les scripts batch (dont `psycopg2`).
- Copie uniquement ce qui est nécessaire dans l’image.

Fichiers :
- [cloudrun_ref/Dockerfile](cloudrun_ref/Dockerfile)
- [cloudrun_ref/requirements.txt](cloudrun_ref/requirements.txt)

### 2) Runbook PowerShell (Windows)

Un runbook reproductible décrit :
- APIs GCP à activer,
- création Artifact Registry,
- service account + rôles,
- création/exec du Cloud Run Job,
- commandes pour vérifier exécutions/logs,
- suite workflow “approve/apply/republish”.

Fichier :
- [cloudrun_ref/RUNBOOK_CLOUD_RUN.md](cloudrun_ref/RUNBOOK_CLOUD_RUN.md)

### 3) Gestion des secrets (pour éviter le copier/coller)

Script PowerShell qui lit un `.env` et pousse des **nouvelles versions** de secrets dans Secret Manager.

- Compatible Windows PowerShell 5.1.
- Secrets gérés (par défaut) : `DB_PASSWORD`, `SERPER_API_KEY`, `SCRAPINGBEE_API_KEY`, `GEMINI_API_KEY`.

Fichier :
- [cloudrun_ref/sync_gcp_secrets_from_env.ps1](cloudrun_ref/sync_gcp_secrets_from_env.ps1)

### 4) Ajustements code pour exécution batch

But : ne pas dépendre de Streamlit / pandas en batch.

- [database.py](database.py)
  - imports `streamlit` / `pandas` rendus optionnels (batch-friendly)
  - fallback config depuis variables d’environnement si besoin
- [config.py](config.py)
  - suppression du mot de passe DB par défaut (éviter “fallback dangereux”)
  - suppression de la dépendance obligatoire à pandas pour certains helpers

## Ce qui a été validé

### Exécution Cloud Run Job réussie (dry-run)

- Le job Cloud Run `habitat-enrich-test` a été exécuté avec succès sur un échantillon (département 45, limit 5) en mode `--dry-run`.
- Sorties générées **dans le conteneur** (ex: `/tmp/outputs/*.jsonl`, `/tmp/outputs/*.csv`).

Remarque : `/tmp/outputs` est éphémère sur Cloud Run (non persistant entre exécutions).

## Incidents rencontrés et corrections

### 1) Args Cloud Run passés en une seule chaîne

Symptôme :
- `python: can't open file '/app/scripts/enrich_dept_prototype.py --departements ...'`

Cause :
- mauvaise configuration `--args` (tout concaténé en 1 string au lieu d’une liste d’arguments).

Correction :
- passer `--command python` et `--args` comme une liste (ex: `"scripts/enrich_dept_prototype.py","--departements","45",...`).

### 2) Auth DB Supabase en échec

Symptôme :
- `FATAL: password authentication failed for user "postgres"`

Cause :
- `DB_PASSWORD` injecté dans Cloud Run ≠ valeur réelle.

Correction :
- mise à jour du secret `DB_PASSWORD` via Secret Manager (script de sync).

## Point bloquant / point d’attention

### Logs “propres” par exécution

- `gcloud run jobs logs read` mélange les logs historiques (anciens runs) et le dernier run.
- La stratégie la plus fiable est de filtrer via Cloud Logging sur `labels."run.googleapis.com/execution_name"`.

Note : selon le contexte/quoting PowerShell, certaines requêtes `gcloud logging read ... --format=json` ont pu retourner `[]` alors qu’un filtre texte (ex: `textPayload:"Département 45"`) retrouvait bien une entrée. Dans ce cas, l’approche la plus robuste est d’utiliser `status.logUri` d’une exécution (lien console) et de reprendre le filtre “Advanced logs filter” exact.

## Prochaines étapes (recommandées)

### 1) Passer à `--write-propositions` (petit échantillon)

- Re-exécuter le job sur `dep 45 limit 5` avec `--write-propositions`.
- Vérifier en DB que des lignes sont créées dans `propositions` / `proposition_items`.

### 2) Appliquer le workflow (approve/apply/republish)

- Utiliser `scripts/enrich_propositions_workflow.py` pour :
  - stats,
  - approve,
  - apply,
  - republish,
  - diagnose-publish.

### 3) Persister les artefacts de sortie

Choix à faire :
- Ajouter un upload vers un bucket GCS (recommandé) : écrire dans `/tmp/outputs`, puis upload en fin de run.
- Alternative rapide : intégrer `gsutil cp` si on installe `google-cloud-sdk` dans l’image (moins propre / plus lourd).

### 4) Sécurité (fortement recommandé)

- Les clés API + mots de passe ne doivent pas transiter dans des logs ou du chat.
- Rotation des secrets si exposition accidentelle.

## Commandes clés (rappel)

- Construire/push l’image :
  - `docker build -f cloudrun_ref/Dockerfile -t <IMAGE> .`
  - `docker push <IMAGE>`

- Créer le job (extrait) :
  - `gcloud run jobs create habitat-enrich-test ... --command "python" --args "scripts/enrich_dept_prototype.py","--departements","45",...`

- Exécuter le job :
  - `gcloud run jobs execute habitat-enrich-test --region europe-west1 --wait`

- Trouver un filtre logs fiable :
  - `gcloud run jobs executions describe <EXECUTION_ID> --region europe-west1 --format="value(status.logUri)"`

