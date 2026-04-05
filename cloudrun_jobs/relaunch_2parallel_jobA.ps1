# Lancement de 2 batches Job A en parallele
# Batch 1: depts 36-38
# Batch 2: depts 41-43

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$JOB_A   = "finess-enrich-national"

Write-Host "Lancement de 2 batches Job A en parallele..." -ForegroundColor Cyan

# Batch 1: 36-38
Write-Host ""
Write-Host "[1/2] Lancement depts 36-38..." -ForegroundColor Yellow
$exec1 = gcloud run jobs execute $JOB_A --region=$REGION --project=$PROJECT --args="--departements=36,37,38" --format="value(metadata.name)" 2>&1

Start-Sleep -Seconds 5

# Batch 2: 41-43
Write-Host ""
Write-Host "[2/2] Lancement depts 41-43..." -ForegroundColor Yellow
$exec2 = gcloud run jobs execute $JOB_A --region=$REGION --project=$PROJECT --args="--departements=41,42,43" --format="value(metadata.name)" 2>&1

Write-Host ""
Write-Host "2 batches lances!" -ForegroundColor Green
Write-Host "Execution 1 (36-38): $exec1" -ForegroundColor Cyan
Write-Host "Execution 2 (41-43): $exec2" -ForegroundColor Cyan

Write-Host ""
Write-Host "Verification:" -ForegroundColor Magenta
Write-Host "gcloud run jobs executions list --job=$JOB_A --region=$REGION --project=$PROJECT --limit=10"
