# launch_quality_pass_2jobs.ps1
# Lance la passe qualite ciblee en 2 jobs Cloud Run paralleles
# Job 1 : depts 01-50 + 2A, 2B  (51 depts)  -> finess-quality-pass-1
# Job 2 : depts 51-95 + 9A-9F   (51 depts)  -> finess-quality-pass-2
# Les DEPTS_FILTER et PHASE_FILTER sont bakes dans les YAMLs (pas de --args)

param(
    [switch]$SkipBuild,
    [switch]$SkipDeploy
)

$PROJECT   = "gen-lang-client-0230548399"
$REGION    = "europe-west1"
$REGISTRY  = "europe-west1-docker.pkg.dev/$PROJECT/habitat/enrich:quality"
$JOB_NAME1 = "finess-quality-pass-1"
$JOB_NAME2 = "finess-quality-pass-2"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  FINESS Quality Pass -- 2 jobs paralleles" -ForegroundColor Cyan
Write-Host "  Job 1: finess-quality-pass-1 (depts 01-50 + 2A,2B)" -ForegroundColor Cyan
Write-Host "  Job 2: finess-quality-pass-2 (depts 51-95 + 9A-9F)" -ForegroundColor Cyan
Write-Host "  Phases A,B,C -- via env var PHASE_FILTER dans les YAMLs" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

if (-not $SkipBuild) {
    Write-Host ""
    Write-Host "[1/3] Build de l image Docker tag :quality..." -ForegroundColor Yellow
    docker build -t $REGISTRY .
    if ($LASTEXITCODE -ne 0) { Write-Error "Build echoue"; exit 1 }
    Write-Host "      Push vers Artifact Registry..." -ForegroundColor Yellow
    docker push $REGISTRY
    if ($LASTEXITCODE -ne 0) { Write-Error "Push echoue"; exit 1 }
    Write-Host "      Image poussee : $REGISTRY" -ForegroundColor Green
} else {
    Write-Host "[1/3] Build ignore (-SkipBuild)" -ForegroundColor DarkGray
}

if (-not $SkipDeploy) {
    Write-Host ""
    Write-Host "[2/3] Deploiement des 2 jobs Cloud Run depuis les YAMLs..." -ForegroundColor Yellow
    gcloud run jobs replace cloudrun_jobs/quality_pass_job1.yaml --region=$REGION --project=$PROJECT
    if ($LASTEXITCODE -ne 0) { Write-Error "Deploiement job1 echoue"; exit 1 }
    Write-Host "      Job 1 deploye : $JOB_NAME1" -ForegroundColor Green
    gcloud run jobs replace cloudrun_jobs/quality_pass_job2.yaml --region=$REGION --project=$PROJECT
    if ($LASTEXITCODE -ne 0) { Write-Error "Deploiement job2 echoue"; exit 1 }
    Write-Host "      Job 2 deploye : $JOB_NAME2" -ForegroundColor Green
} else {
    Write-Host "[2/3] Deploiement ignore (-SkipDeploy)" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "[3/3] Lancement des 2 jobs en parallele (sans --args)..." -ForegroundColor Yellow

Write-Host "  Job 1 (depts 01-50 + 2A,2B)..." -ForegroundColor Cyan
$exec1 = gcloud run jobs execute $JOB_NAME1 --region=$REGION --project=$PROJECT --format="value(metadata.name)" 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "Lancement job1 echoue: $exec1"; exit 1 }
Write-Host "  -> Execution : $exec1" -ForegroundColor Green

Start-Sleep -Seconds 3

Write-Host "  Job 2 (depts 51-95 + 9A-9F)..." -ForegroundColor Cyan
$exec2 = gcloud run jobs execute $JOB_NAME2 --region=$REGION --project=$PROJECT --format="value(metadata.name)" 2>&1
if ($LASTEXITCODE -ne 0) { Write-Error "Lancement job2 echoue: $exec2"; exit 1 }
Write-Host "  -> Execution : $exec2" -ForegroundColor Green

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  2 jobs lances ! Email envoye a la fin de chaque job." -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Suivi Job 1 :" -ForegroundColor Magenta
Write-Host "  gcloud run jobs executions list --job=$JOB_NAME1 --region=$REGION --project=$PROJECT --limit=3"
Write-Host "Suivi Job 2 :" -ForegroundColor Magenta
Write-Host "  gcloud run jobs executions list --job=$JOB_NAME2 --region=$REGION --project=$PROJECT --limit=3"