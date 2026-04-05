# FINESS Cloud Run Deployment Guide

## Vue d'ensemble

Le pipeline d'enrichissement FINESS peut tourner sur Google Cloud Run Jobs pour des runs de production à large échelle. Cette configuration utilise **Mistral API** comme provider LLM par défaut.

## Architecture

- **Image Docker** : `europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest`
- **Script** : `scripts/enrich_finess_dept.py`
- **Base de données** : Supabase PostgreSQL (lecture/écriture directe)
- **Secrets** : Google Secret Manager (DB_PASSWORD, SERPER_API_KEY, MISTRAL_API_KEY)
- **Ressources** : 2 CPU, 1 Gi RAM, timeout 3600s

## Configuration

### Jobs disponibles

1. **`finess_job_65_test.yaml`** (test)
   - Département 65 avec `--limit 15`
   - Timeout 1800s (30 min)
   - Usage : validation rapide avant full run

2. **`finess_job_65.yaml`** (production)
   - Département 65 complet (83 gestionnaires, 239 établissements)
   - Timeout 3600s (1h)
   - Usage : enrichissement production

### Variables d'environnement

```yaml
LLM_PROVIDER: "mistral"
MISTRAL_MODEL: "mistral-small-latest"
MISTRAL_API_KEY: <secret>
SERPER_API_KEY: <secret>
DB_HOST: db.minwoumfgutampcgrcbr.supabase.co
DB_NAME: postgres
DB_USER: postgres
DB_PASSWORD: <secret>
DB_PORT: "5432"
PYTHONUNBUFFERED: "1"
```

## Déploiement

### Prérequis

1. **Authentification GCP**
   ```powershell
   gcloud auth login
   gcloud config set project gen-lang-client-0230548399
   ```

2. **Docker configuré pour Artifact Registry**
   ```powershell
   gcloud auth configure-docker europe-west1-docker.pkg.dev
   ```

3. **Secrets synchronisés**
   ```powershell
   # Sync MISTRAL_API_KEY
   .\cloudrun_jobs\sync_mistral_secret.ps1
   
   # Sync autres secrets (si nécessaire)
   .\cloudrun_ref\sync_gcp_secrets_from_env.ps1
   ```

### Déploiement du job

#### Option 1 : Script automatisé (recommandé)

```powershell
# Test (--limit 15)
.\cloudrun_jobs\deploy_finess_job.ps1

# Full production
.\cloudrun_jobs\deploy_finess_job.ps1 -Full

# Skip rebuild (si image déjà à jour)
.\cloudrun_jobs\deploy_finess_job.ps1 -SkipBuild
```

#### Option 2 : Manuel

```powershell
# 1. Build + push image
docker build -f cloudrun_ref/Dockerfile -t europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest .
docker push europe-west1-docker.pkg.dev/gen-lang-client-0230548399/habitat/enrich:latest

# 2. Deploy job
gcloud run jobs replace cloudrun_jobs/finess_job_65_test.yaml --region=europe-west1
```

## Exécution

### Lancer un job

```powershell
# Test job
gcloud run jobs execute finess-enrich-65-test --region=europe-west1 --wait

# Production job
gcloud run jobs execute finess-enrich-65 --region=europe-west1 --wait
```

### Monitorer les logs

#### Logs récents (simples)
```powershell
gcloud run jobs logs read finess-enrich-65-test --region=europe-west1 --limit=100
```

#### Logs structurés (filtrage avancé)
```powershell
# Filtrer par run_id pour isoler un run spécifique
gcloud logging read 'resource.type=cloud_run_job AND jsonPayload.run_id="20260303_143000"' --limit=50 --format=json

# Filtrer les checkpoints de progression
gcloud logging read 'resource.type=cloud_run_job AND jsonPayload.phase="PASSE_1_GESTIONNAIRES"' --limit=20 --format=json

# Filtrer les erreurs uniquement
gcloud logging read 'resource.type=cloud_run_job AND jsonPayload.severity="ERROR"' --limit=50 --format=json
```

#### Live streaming des logs
```powershell
gcloud run jobs logs tail finess-enrich-65-test --region=europe-west1
```

## Logging structuré

Le pipeline émet des logs JSON structurés pour faciliter le monitoring :

### Format du log
```json
{
  "severity": "INFO",
  "message": "PASSE_1_GESTIONNAIRES checkpoint: 10/83",
  "run_id": "20260303_143000",
  "timestamp": "2026-03-03T14:35:22.123456",
  "phase": "PASSE_1_GESTIONNAIRES",
  "progress_pct": 12.0,
  "entity_id": "650123456"
}
```

### Types de logs

- **Checkpoints** : Émis toutes les 10 entités (gestionnaires/établissements)
- **Start/End** : Début et fin du pipeline avec métadonnées
- **Errors** : Exceptions avec stack trace

### Avantage du `run_id`

Chaque exécution génère un `run_id` unique (format : `YYYYMMDD_HHMMSS`). Utilisez-le pour :
- Isoler les logs d'un run spécifique
- Débugger un job qui a échoué
- Analyser les performances d'un run

Exemple :
```powershell
# Récupérer tous les logs d'un run spécifique
gcloud logging read 'resource.type=cloud_run_job AND jsonPayload.run_id="20260303_143000"' --format=json
```

## Problèmes courants

### Secret MISTRAL_API_KEY manquant

**Symptôme** : Job fail avec "MISTRAL_API_KEY not set"

**Solution** :
```powershell
.\cloudrun_jobs\sync_mistral_secret.ps1
```

### Timeout dépassé

**Symptôme** : Job killed après 1800s/3600s

**Solution** : Augmenter `timeoutSeconds` dans le YAML, ou utiliser `--limit` pour tester

### Logs mélangés

**Symptôme** : Difficulté à isoler les logs d'un run spécifique

**Solution** : Utiliser le filtre `jsonPayload.run_id` dans Cloud Logging

### Rate limit Mistral

**Symptôme** : Erreurs 429 dans les logs

**Solution** : Le script a un backoff de 10s pour 429. Si persistant, réduire la concurrence ou passer à un model plus rapide

## Scalabilité

### Parallélisation multi-départements

Pour traiter plusieurs départements en parallèle :

1. Créer un job par département (copier `finess_job_65.yaml` → `finess_job_31.yaml`, etc.)
2. Exécuter en parallèle :
   ```powershell
   gcloud run jobs execute finess-enrich-65 --region=europe-west1 --async
   gcloud run jobs execute finess-enrich-31 --region=europe-west1 --async
   gcloud run jobs execute finess-enrich-64 --region=europe-west1 --async
   ```

3. Monitorer avec des requêtes groupées :
   ```powershell
   gcloud run jobs list --region=europe-west1 --filter="metadata.name:finess-enrich-*"
   ```

### Coûts estimés

- **Cloud Run** : ~0.10€/h (2 CPU, 1 Gi)
- **Mistral Small** : ~0.20€/M tokens
- **Serper** : 1€ / 1000 queries

Pour dept 65 (83 G + 239 ET) :
- Durée : ~1-2h
- Coût Mistral : ~2-3€ (estimation avec Serper, LLM, scraping)
- Coût Cloud Run : ~0.20€

## Maintenance

### Mise à jour du code

```powershell
# Rebuild et redeploy
.\cloudrun_jobs\deploy_finess_job.ps1 -Full
```

### Changer de model LLM

Modifier dans le YAML :
```yaml
env:
- name: MISTRAL_MODEL
  value: "mistral-large-latest"  # Plus puissant mais plus cher
```

### Revenir à Gemini

```yaml
env:
- name: LLM_PROVIDER
  value: "gemini"
- name: GEMINI_MODEL
  value: "gemini-2.0-flash"
- name: GEMINI_API_KEY
  valueFrom:
    secretKeyRef:
      name: GEMINI_API_KEY
      key: latest
```

## Next steps

- [ ] Tester job avec `--limit 15`
- [ ] Valider qualité LLM en cloud
- [ ] Valider géocodage (pas de SSL issues en cloud)
- [ ] Run full dept 65 en production
- [ ] Généraliser à tous les départements
- [ ] Automatiser via Cloud Scheduler (cron)
