# Runbook Cloud Run Jobs (Windows / PowerShell)

Ce runbook sert à lancer **un premier batch** de test (échantillon) avec `scripts/enrich_dept_prototype.py` dans Google Cloud Run Jobs.

## 0) Pré-requis (local)

- Installer **Google Cloud SDK** (gcloud)
- Installer **Docker Desktop**
- Avoir un projet GCP + droits (Cloud Run Admin, Artifact Registry Writer, Secret Manager Admin, Service Account User)

Vérif rapide (PowerShell) :

```powershell
gcloud --version

docker version
```

## 1) Variables à définir

Dans PowerShell :

```powershell
$PROJECT_ID = "<PROJECT_ID>"
$REGION = "europe-west1"
$AR_REPO = "habitat"
$IMAGE_NAME = "enrich"
$TAG = (Get-Date -Format "yyyyMMdd_HHmmss")
$IMAGE = "$REGION-docker.pkg.dev/$PROJECT_ID/$AR_REPO/$IMAGE_NAME`:$TAG"

$JOB_NAME = "habitat-enrich-test"
$SA_NAME = "habitat-enrich-sa"
$SA_EMAIL = "$SA_NAME@$PROJECT_ID.iam.gserviceaccount.com"
```

## 2) Activer les APIs

```powershell
gcloud config set project $PROJECT_ID

gcloud services enable run.googleapis.com artifactregistry.googleapis.com secretmanager.googleapis.com
```

## 3) Artifact Registry (repo Docker)

Créer le repo (si pas déjà fait) :

```powershell
gcloud artifacts repositories create $AR_REPO `
  --repository-format=docker `
  --location=$REGION `
  --description="Habitat enrichment images"
```

Configurer Docker pour push :

```powershell
gcloud auth configure-docker "$REGION-docker.pkg.dev"
```

## 4) Service Account (exécution du Job)

Créer le SA :

```powershell
gcloud iam service-accounts create $SA_NAME --display-name "Habitat enrich job"
```

Donner les rôles minimum (à ajuster selon politique) :

```powershell
# Cloud Run Job exécution
gcloud projects add-iam-policy-binding $PROJECT_ID --member "serviceAccount:$SA_EMAIL" --role "roles/run.invoker"

# Lire secrets
gcloud projects add-iam-policy-binding $PROJECT_ID --member "serviceAccount:$SA_EMAIL" --role "roles/secretmanager.secretAccessor"

# (Optionnel) écrire logs (souvent inclus)
gcloud projects add-iam-policy-binding $PROJECT_ID --member "serviceAccount:$SA_EMAIL" --role "roles/logging.logWriter"
```

## 5) Secrets (DB + APIs)

Créer/charger les secrets (exemples) :

```powershell
# DB_PASSWORD
# gcloud secrets create DB_PASSWORD --replication-policy="automatic"
# Get-Content -Raw .\.env | ... (ne pas faire) -> mieux: saisir la valeur

# Exemple ajout version:
# "<VALUE>" | gcloud secrets versions add DB_PASSWORD --data-file=-

# Idem pour SERPER_API_KEY / SCRAPINGBEE_API_KEY / GEMINI_API_KEY
```

## 6) Build & push de l'image

Depuis la racine du repo :

```powershell
docker build -f cloudrun_ref/Dockerfile -t $IMAGE .

docker push $IMAGE
```

## 7) Créer un Cloud Run Job (test échantillon)

Exemple: département 45, limit 5, création **propositions** (write) + artefacts dans `/tmp/outputs`.

```powershell
$DEP = "45"
$LIMIT = "5"

gcloud run jobs create $JOB_NAME `
  --region $REGION `
  --image $IMAGE `
  --service-account $SA_EMAIL `
  --task-timeout 3600 `
  --max-retries 0 `
  --set-env-vars PYTHONUNBUFFERED=1 `
  --set-env-vars GEMINI_MODEL=gemini-2.0-flash `
  --set-env-vars DB_HOST="<DB_HOST>",DB_NAME="<DB_NAME>",DB_USER="<DB_USER>",DB_PORT="5432" `
  --set-secrets DB_PASSWORD=DB_PASSWORD:latest `
  --set-secrets SERPER_API_KEY=SERPER_API_KEY:latest `
  --set-secrets SCRAPINGBEE_API_KEY=SCRAPINGBEE_API_KEY:latest `
  --set-secrets GEMINI_API_KEY=GEMINI_API_KEY:latest `
  --command "python" `
  --args "scripts/enrich_dept_prototype.py","--departements",$DEP,"--limit",$LIMIT,"--write-propositions","--out-dir","/tmp/outputs"
```

Notes:
- Pour un premier test, tu peux remplacer `--write-propositions` par `--dry-run`.
- Les artefacts restent dans le container: pour les récupérer, on ajoutera ensuite une étape d'upload GCS (option propre) ou un `gsutil cp` (option rapide).

## 8) Exécuter le Job

```powershell
gcloud run jobs execute $JOB_NAME --region $REGION --wait
```

## 9) Lire les logs

```powershell
gcloud run jobs executions list --job $JOB_NAME --region $REGION

# Puis inspecter une exécution
# gcloud run jobs executions describe <EXECUTION_ID> --region $REGION
```

## 10) Workflow DB après batch

En local (si tu as la liste des propositions) :

```powershell
python scripts/enrich_propositions_workflow.py stats-from-list --input outputs/enrich_proto_propositions_<tag>.txt
python scripts/enrich_propositions_workflow.py approve-from-list --input outputs/enrich_proto_propositions_<tag>.txt
python scripts/enrich_propositions_workflow.py apply --input outputs/enrich_proto_propositions_<tag>.txt
python scripts/enrich_propositions_workflow.py republish-from-list --input outputs/enrich_proto_propositions_<tag>.txt
python scripts/enrich_propositions_workflow.py diagnose-publish-from-list --input outputs/enrich_proto_propositions_<tag>.txt
```
