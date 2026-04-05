# Industrialisation Google Cloud Run (Jobs) — Habitat Enrichissement

Date: 2026-01-31

Objectif
- Exécuter l’enrichissement en **batch** (Cloud Run Jobs) par département (ou shard), en créant des **propositions** en base.
- Produire des **artefacts** (JSONL / CSV / liste de propositions) pour audit + pilotage du workflow `approve/apply/republish/diagnose`.
- Garder une **concurrence globale ScrapingBee ≤ 10**.

---

## 1) Point d’entrée (repo)

Batch enrichissement (création de propositions):
- `scripts/enrich_dept_prototype.py`
  - Arguments utiles:
    - `--departements` (ex: `35,49`)
    - `--limit` (`0` = pas de limite)
    - `--write-propositions` (écrit dans `propositions`/`proposition_items`)
    - `--out-dir` (dossier artefacts)
    - `--sleep` (petite pause entre établissements si besoin)

Workflow d’application (après modération/approve):
- `scripts/enrich_propositions_workflow.py`
  - `stats-from-list`, `approve-from-list`, `apply`, `republish-from-list`, `diagnose-publish-from-list`

---

## 2) Pré-requis GCP

### 2.1 Ressources recommandées
- 1 bucket GCS pour artefacts: `gs://<BUCKET>/enrichment/`
- 1 Artifact Registry repo Docker: `gcr.io` ou `REGION-docker.pkg.dev/<PROJECT>/<REPO>`
- 1 service account Cloud Run Job (principe du moindre privilège)

### 2.2 Secrets / variables d’environnement
Ce repo charge la DB via `config.py` + `.env` (dotenv). En Cloud Run, il faut **tout passer en variables/Secrets**.

DB (psycopg2):
- `DB_HOST`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_PORT`

APIs (selon usage):
- `SERPER_API_KEY`
- `SCRAPINGBEE_API_KEY`
- `GEMINI_API_KEY` (ou `GOOGLE_MAPS_API_KEY` selon config) + `GEMINI_MODEL` (optionnel)

Recommandation: stocker les valeurs sensibles dans Secret Manager et les injecter dans le job.

---

## 3) Stratégie de batch / sharding

### 3.1 Version simple (recommandée pour démarrer)
- **1 job = 1 département**, sans sharding.
- Commande:
  - `python scripts/enrich_dept_prototype.py --departements <DEP> --limit 0 --write-propositions --out-dir /tmp/outputs`

Avantages
- Moins de mécanique, robuste.

Inconvénients
- Temps d’exécution potentiellement long si beaucoup d’établissements.

### 3.2 Sharding (à prévoir si nécessaire)
État actuel: `scripts/enrich_dept_prototype.py` ne supporte pas encore un `--offset/--page/--ids-file` déterministe.

Options (ordre recommandé):
1) Ajouter un argument `--etablissement-ids-file` (CSV/TXT) et faire 1 shard = 1 liste.
2) Ajouter un mode `--seed` + `--shard-index/--shard-count` basé sur un tri stable (ex: `id` ou `nom`) + découpe.

---

## 4) Contrainte ScrapingBee (≤ 10 concurrents)

Le prototype est principalement **séquentiel**: à l’instant t, chaque job fait peu de requêtes en parallèle.

Donc la contrainte “≤10 simultanés” se pilote surtout côté orchestration:
- Lancer **≤ 10 Cloud Run Jobs** en parallèle.
- (Optionnel) Ajouter `--sleep 0.2` ou `--sleep 0.5` si on observe des 429.

---

## 5) Artefacts de sortie

Le prototype génère (dans `--out-dir`):
- `enrich_proto_<tag>.jsonl`
- `enrich_proto_logements_compare_<tag>.csv`
- `enrich_proto_propositions_<tag>.txt`

En Cloud Run, écrire dans un dossier writable:
- recommandé: `--out-dir /tmp/outputs`

### 5.1 Stockage dans GCS (2 options)
Option A (plus simple, sans code):
- Installer `gsutil` dans l’image, puis copier en fin de job.

Option B (propre, Python):
- Ajouter une étape d’upload via `google-cloud-storage`.
- Avantage: pas besoin de SDK complet, plus léger.

Note: même sans artefacts, la “vérité importable” reste en base via `propositions`.

---

## 6) Runbook gcloud (squelette)

Variables (exemple)
```bash
PROJECT_ID="..."
REGION="europe-west1"
JOB_NAME="habitat-enrich"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/habitat/enrich:2026-01-31"
SA_EMAIL="habitat-enrich-sa@${PROJECT_ID}.iam.gserviceaccount.com"
BUCKET="gs://..."
```

### 6.1 Build & push de l’image
(à adapter selon votre setup Docker/Artifact Registry)
```bash
gcloud auth configure-docker ${REGION}-docker.pkg.dev

docker build -t "${IMAGE}" .
docker push "${IMAGE}"
```

### 6.2 Créer le job
Commande “job” typique:
- utilise `/tmp/outputs`
- exécute 1 département

```bash
gcloud run jobs create "${JOB_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --service-account "${SA_EMAIL}" \
  --task-timeout 3600 \
  --max-retries 0 \
  --set-env-vars GEMINI_MODEL=gemini-2.0-flash \
  --set-env-vars PYTHONUNBUFFERED=1 \
  --set-env-vars OUT_DIR=/tmp/outputs \
  --command "python" \
  --args "scripts/enrich_dept_prototype.py","--departements","35","--limit","0","--write-propositions","--out-dir","/tmp/outputs"
```

Injection secrets (exemples, à ajuster)
```bash
# Exemple: Secret Manager
# gcloud secrets create DB_PASSWORD --replication-policy="automatic"
# echo -n "..." | gcloud secrets versions add DB_PASSWORD --data-file=-

gcloud run jobs update "${JOB_NAME}" \
  --region "${REGION}" \
  --set-secrets DB_PASSWORD=DB_PASSWORD:latest \
  --set-env-vars DB_HOST=...,DB_NAME=...,DB_USER=...,DB_PORT=5432 \
  --set-secrets SERPER_API_KEY=SERPER_API_KEY:latest \
  --set-secrets SCRAPINGBEE_API_KEY=SCRAPINGBEE_API_KEY:latest \
  --set-secrets GEMINI_API_KEY=GEMINI_API_KEY:latest
```

### 6.3 Exécuter le job
```bash
gcloud run jobs execute "${JOB_NAME}" --region "${REGION}" --wait
```

### 6.4 Lire les logs
```bash
gcloud run jobs executions list --job "${JOB_NAME}" --region "${REGION}"
# puis
# gcloud run jobs executions describe <EXECUTION_ID> --region "${REGION}"
```

---

## 7) Récupération & application (workflow DB)

Cas standard (post-batch)
1) Récupérer la liste `enrich_proto_propositions_<tag>.txt` (depuis GCS ou logs)
2) Appliquer:
```bash
python scripts/enrich_propositions_workflow.py stats-from-list --input outputs/enrich_proto_propositions_<tag>.txt
python scripts/enrich_propositions_workflow.py approve-from-list --input outputs/enrich_proto_propositions_<tag>.txt
python scripts/enrich_propositions_workflow.py apply --input outputs/enrich_proto_propositions_<tag>.txt
python scripts/enrich_propositions_workflow.py republish-from-list --input outputs/enrich_proto_propositions_<tag>.txt
python scripts/enrich_propositions_workflow.py diagnose-publish-from-list --input outputs/enrich_proto_propositions_<tag>.txt
```

---

## 8) Observabilité / garde-fous

- Logs structurés: au minimum `departement`, `etablissement_id`, compteurs propositions créées, raisons `can_publish=false`.
- Relançabilité:
  - `apply` est idempotent (dédup).
  - éviter de recréer des propositions identiques si on relance la même journée (amélioration possible: dédup côté création).

---

## 9) Checklist “prod-ready”

- [ ] Secrets DB et API dans Secret Manager (pas de valeurs en dur)
- [ ] Job test sur 1 département avec `--limit 20`
- [ ] Validation end-to-end: `approve/apply/republish/diagnose`
- [ ] Définir cadence (Cloud Scheduler) + max parallélisme (=10)
- [ ] Décider “logements refresh vs vide uniquement” après rapport de comparaison
- [ ] (Optionnel) implémenter sharding déterministe avant un run national
