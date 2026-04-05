# Lancement robuste Job B: depts 61-65

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$JOB_B   = "finess-enrich-national-b"

Write-Host ""
Write-Host "==== LANCEMENT JOB B: depts 61-65 ====" -ForegroundColor Cyan
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
$argsStr = "scripts/enrich_finess_dept.py,--departements,61,62,63,64,65,--out-dir,/tmp/outputs"
Write-Host "[UPDATE] Mise a jour args du job..." -ForegroundColor Yellow
gcloud run jobs update $JOB_B `
    --region=$REGION --project=$PROJECT `
    --args="$argsStr" --quiet 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Echec update job" -ForegroundColor Red
    exit 1
}
Write-Host "[OK] Args mis a jour" -ForegroundColor Green

# Launch (without --args, uses updated spec)
Write-Host "[LAUNCH] Demarrage depts 61,62,63,64,65..." -ForegroundColor Yellow
$exec = gcloud run jobs execute $JOB_B `
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
