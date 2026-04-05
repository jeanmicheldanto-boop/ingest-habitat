# launch_signaux_v2_passe_b.ps1
# Deploy + run Cloud Run Job for Signaux V2 Passe B (Serper + LLM)

param(
    [int]$BatchOffset = 0,
    [int]$BatchSize = 100,
    [string]$Depts = "",
    [switch]$DryRun,
    [switch]$ScopeFilterLLM,
    [switch]$SkipBuild,
    [switch]$SkipDeploy,
    [string]$Project = "gen-lang-client-0230548399",
    [string]$Region = "europe-west1",
    [string]$JobName = "finess-signaux-v2-passb",
    [string]$ImageTag = "signaux-v2-passb"
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$Image = "europe-west1-docker.pkg.dev/$Project/habitat/enrich:$ImageTag"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Signaux V2 Passe B - Cloud Run" -ForegroundColor Cyan
Write-Host "Project=$Project Region=$Region Job=$JobName" -ForegroundColor Cyan
Write-Host "BatchOffset=$BatchOffset BatchSize=$BatchSize DryRun=$DryRun" -ForegroundColor Cyan
Write-Host "ScopeFilterLLM=$ScopeFilterLLM" -ForegroundColor Cyan
if ($Depts) { Write-Host "Depts=$Depts" -ForegroundColor Cyan }
Write-Host "============================================================" -ForegroundColor Cyan

if (-not $SkipBuild) {
    Write-Host "[1/3] Build + push image: $Image" -ForegroundColor Yellow
    docker build -t $Image .
    if ($LASTEXITCODE -ne 0) { Write-Error "docker build failed (exit $LASTEXITCODE)"; exit 1 }
    docker push $Image
    if ($LASTEXITCODE -ne 0) { Write-Error "docker push failed (exit $LASTEXITCODE)"; exit 1 }
} else {
    Write-Host "[1/3] Build skipped" -ForegroundColor DarkGray
}

$jobParamCsv = "scripts/signaux_v2_passe_b.py,--batch-offset,$BatchOffset,--batch-size,$BatchSize"
if ($Depts) {
    $jobParamCsv += ",--dept,$Depts"
}
if ($DryRun) {
    $jobParamCsv += ",--dry-run"
}
if ($ScopeFilterLLM) {
    $jobParamCsv += ",--scope-filter-llm"
}

if (-not $SkipDeploy) {
    Write-Host "[2/3] Deploy/update Cloud Run job" -ForegroundColor Yellow
    $envVars = "PYTHONUNBUFFERED=1,LLM_PROVIDER=gemini,GEMINI_MODEL=gemini-2.0-flash,DB_HOST=db.minwoumfgutampcgrcbr.supabase.co,DB_NAME=postgres,DB_USER=postgres,DB_PORT=5432"
    $secrets  = "DB_PASSWORD=DB_PASSWORD:latest,SERPER_API_KEY=SERPER_API_KEY:latest,GEMINI_API_KEY=GEMINI_API_KEY:latest,MISTRAL_API_KEY=MISTRAL_API_KEY:latest"
    gcloud run jobs deploy $JobName `
        --region=$Region `
        --project=$Project `
        --image=$Image `
        --command=python `
        --args=$jobParamCsv `
        --max-retries=0 `
        --tasks=1 `
        --task-timeout=3600 `
        --cpu=1 `
        --memory=1Gi `
        --set-env-vars=$envVars `
        --set-secrets=$secrets
    if ($LASTEXITCODE -ne 0) { Write-Error "gcloud run jobs deploy failed (exit $LASTEXITCODE)"; exit 1 }
} else {
    Write-Host "[2/3] Deploy skipped" -ForegroundColor DarkGray
}

Write-Host "[3/3] Execute job" -ForegroundColor Yellow
$execName = gcloud run jobs execute $JobName --region=$Region --project=$Project --format="value(metadata.name)"
if ($LASTEXITCODE -ne 0) { Write-Error "gcloud run jobs execute failed (exit $LASTEXITCODE)"; exit 1 }
Write-Host "Execution started: $execName" -ForegroundColor Green

Write-Host ""
Write-Host "Follow logs:" -ForegroundColor Magenta
Write-Host "gcloud run jobs executions describe $execName --region=$Region --project=$Project"
Write-Host "gcloud run jobs executions list --job=$JobName --region=$Region --project=$Project --limit=5"
