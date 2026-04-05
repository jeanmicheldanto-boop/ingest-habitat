# Script de deploiement et debug Cloud Run
# Executer depuis la racine du projet: .\deploy_debug_cloudrun.ps1

$ErrorActionPreference = "Stop"

Write-Host "=" * 80 -ForegroundColor Cyan
Write-Host "DEPLOIEMENT DEBUG CLOUD RUN" -ForegroundColor Cyan
Write-Host "=" * 80 -ForegroundColor Cyan

$PROJECT = "gen-lang-client-0230548399"
$REGION = "europe-west1"
$IMAGE_TAG = "debug"
$IMAGE_PATH = "europe-west1-docker.pkg.dev/$PROJECT/habitat/enrich:$IMAGE_TAG"

# Etape 1: Build de l'image Docker
Write-Host "`n[1/5] Build de l'image Docker..." -ForegroundColor Yellow
docker build -f cloudrun_ref/Dockerfile -t "habitat-enrich:$IMAGE_TAG" .
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERREUR: Build Docker echoue!" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Image buildee avec succes" -ForegroundColor Green

# Etape 2: Tag pour GCP Artifact Registry
Write-Host "`n[2/5] Tag de l'image pour GCP..." -ForegroundColor Yellow
docker tag "habitat-enrich:$IMAGE_TAG" $IMAGE_PATH
Write-Host "[OK] Image taguee: $IMAGE_PATH" -ForegroundColor Green

# Etape 3: Push vers Artifact Registry
Write-Host "`n[3/5] Push vers Artifact Registry..." -ForegroundColor Yellow
docker push $IMAGE_PATH
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERREUR: Push echoue! Verifiez: gcloud auth configure-docker europe-west1-docker.pkg.dev" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Image pushee avec succes" -ForegroundColor Green

# Etape 4: Deployer le job de debug
Write-Host "`n[4/5] Deploiement du job de debug..." -ForegroundColor Yellow
gcloud run jobs replace cloudrun_job_debug.yaml --region $REGION --project $PROJECT
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERREUR: Deploiement echoue!" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Job deploye avec succes" -ForegroundColor Green

# Etape 5: Executer le job de debug
Write-Host "`n[5/5] Execution du job de debug..." -ForegroundColor Yellow
$executionOutput = gcloud run jobs execute habitat-enrich-debug --region $REGION --project $PROJECT --wait 2>&1 | Out-String
Write-Host $executionOutput

# Extraire le nom de l'execution
if ($executionOutput -match "habitat-enrich-debug-[a-z0-9]+") {
    $executionName = $matches[0]
    Write-Host "`n[EXECUTION] Nom: $executionName" -ForegroundColor Cyan
    
    # Attendre un peu que les logs arrivent
    Write-Host "Attente 10s pour les logs..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
    
    # Recuperer les logs
    Write-Host "`n[LOGS] Recuperation des logs..." -ForegroundColor Yellow
    $logsCmd = @"
gcloud logging read "resource.type=cloud_run_job AND labels.`"run.googleapis.com/execution_name`"=$executionName" --limit 200 --format="value(textPayload)" --project $PROJECT
"@
    Write-Host "Commande: $logsCmd" -ForegroundColor Gray
    
    gcloud logging read "resource.type=cloud_run_job AND labels.`"run.googleapis.com/execution_name`"=$executionName" --limit 200 --format="value(textPayload)" --project $PROJECT
}

Write-Host "`n" + "=" * 80 -ForegroundColor Cyan
Write-Host "FIN DEPLOIEMENT DEBUG" -ForegroundColor Cyan
Write-Host "=" * 80 -ForegroundColor Cyan
