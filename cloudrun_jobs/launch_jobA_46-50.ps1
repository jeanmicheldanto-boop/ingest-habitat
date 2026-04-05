# Lancement robuste Job A: depts 46-50

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$JOB_A   = "finess-enrich-national"

Write-Host ""
Write-Host "==== LANCEMENT JOB A: depts 46-50 ====" -ForegroundColor Cyan
Write-Host ""

# Verification
Write-Host "[CHECK] Verification gcloud auth..." -ForegroundColor Yellow
$auth = gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>&1
if (-not $auth) {
    Write-Host "[ERROR] Pas d'authentification gcloud!" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Authentifie: $auth" -ForegroundColor Green
Write-Host ""

# Update job args (script path + departements + out-dir)
$argsStr = "scripts/enrich_finess_dept.py,--departements,46,47,48,49,50,--out-dir,/tmp/outputs"
Write-Host "[UPDATE] Mise a jour args du job..." -ForegroundColor Yellow
gcloud run jobs update $JOB_A `
    --region=$REGION --project=$PROJECT `
    --args="$argsStr" --quiet 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Echec update job" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Args mis a jour" -ForegroundColor Green

# Launch (without --args, uses updated spec)
Write-Host "[LAUNCH] Demarrage depts 46,47,48,49,50..." -ForegroundColor Yellow
$exec = gcloud run jobs execute $JOB_A `
    --region=$REGION `
    --project=$PROJECT `
    --format="value(metadata.name)" 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCCESS] Batch lance!" -ForegroundColor Green
    Write-Host "Execution ID: $exec" -ForegroundColor Cyan
    Write-Host ""
} else {
    Write-Host ""
    Write-Host "[ERROR] Erreur au lancement" -ForegroundColor Red
    Write-Host $exec
    exit 1
}
