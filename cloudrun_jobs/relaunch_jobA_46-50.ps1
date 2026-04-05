# Lancement robuste du batch Job A: depts 46-50
# Script simple et sûr

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$JOB_A   = "finess-enrich-national"

Write-Host ""
Write-Host "════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  LANCEMENT JOB A: depts 46-50" -ForegroundColor Cyan
Write-Host "════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Vérification prérequis
Write-Host "[CHECK] Vérification gcloud auth..." -ForegroundColor Yellow
$auth = gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>&1
if (-not $auth) {
    Write-Host "ERROR: Pas d'authentification gcloud active!" -ForegroundColor Red
    Write-Host "Exécutez: gcloud auth login"
    exit 1
}
Write-Host "[OK] Authentifié: $auth" -ForegroundColor Green
Write-Host ""

# Lancement du batch
Write-Host "[LAUNCH] Démarrage depts 46,47,48,49,50..." -ForegroundColor Yellow
$exec = gcloud run jobs execute $JOB_A `
    --region=$REGION `
    --project=$PROJECT `
    --args="--departements=46,47,48,49,50" `
    --format="value(metadata.name)" 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCCESS]" -ForegroundColor Green
    Write-Host "Execution ID: $exec" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Monitorer avec:" -ForegroundColor Magenta
    Write-Host "gcloud run jobs executions describe $exec --job=$JOB_A --region=$REGION --project=$PROJECT"
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[ERROR] Erreur au lancement" -ForegroundColor Red
    Write-Host $exec
    exit 1
}
