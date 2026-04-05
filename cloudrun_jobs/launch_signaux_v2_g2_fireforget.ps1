# launch_signaux_v2_g2_fireforget.ps1
# Deploy + execute Signaux V2 G2 in fire-and-forget mode.
# Completion email is sent from Cloud Run execution (Python script), so local PC can be shut down.

param(
    [int]$BatchOffset = 0,
    [int]$BatchSize = 1500,
    [string]$Depts = "",
    [int]$MaxSerperResults = 8,
    [switch]$ScopeFilterLLM,
    [switch]$SkipSerperLlm,
    [switch]$ForceRerun,
    [switch]$DryRun,
    [switch]$SkipBuild,
    [switch]$SkipDeploy,
    [int]$TaskTimeoutSeconds = 86400,
    [string]$NotificationEmail = "patrick.danto@confidensia.fr",
    [string]$SenderEmail = "patrick.danto@bmse.fr",
    [string]$Project = "gen-lang-client-0230548399",
    [string]$Region = "europe-west1",
    [string]$JobName = "finess-signaux-v2-g2",
    [string]$ImageTag = "signaux-v2-g2"
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if ($TaskTimeoutSeconds -lt 900 -or $TaskTimeoutSeconds -gt 86400) {
    throw "TaskTimeoutSeconds doit etre entre 900 et 86400"
}

$Image = "europe-west1-docker.pkg.dev/$Project/habitat/enrich:$ImageTag"

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Signaux V2 G2 - Cloud Run fire-and-forget" -ForegroundColor Cyan
Write-Host "Project=$Project Region=$Region Job=$JobName" -ForegroundColor Cyan
Write-Host "BatchOffset=$BatchOffset BatchSize=$BatchSize MaxSerperResults=$MaxSerperResults" -ForegroundColor Cyan
Write-Host "ScopeFilterLLM=$ScopeFilterLLM SkipSerperLlm=$SkipSerperLlm ForceRerun=$ForceRerun" -ForegroundColor Cyan
Write-Host "DryRun=$DryRun SkipBuild=$SkipBuild SkipDeploy=$SkipDeploy" -ForegroundColor Cyan
Write-Host "NotificationEmail=$NotificationEmail SenderEmail=$SenderEmail" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

if (-not $SkipBuild) {
    Write-Host "[1/3] Build + push image: $Image" -ForegroundColor Yellow
    docker build -t $Image .
    if ($LASTEXITCODE -ne 0) { throw "docker build failed (exit $LASTEXITCODE)" }
    docker push $Image
    if ($LASTEXITCODE -ne 0) { throw "docker push failed (exit $LASTEXITCODE)" }
} else {
    Write-Host "[1/3] Build skipped" -ForegroundColor DarkGray
}

$jobArgs = "scripts/signaux_v2_g2_deep.py,--batch-offset=$BatchOffset,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults"
if ($Depts) { $jobArgs += ",--dept=$Depts" }
if ($ScopeFilterLLM) { $jobArgs += ",--scope-filter-llm" }
if ($SkipSerperLlm) { $jobArgs += ",--skip-serper-llm" }
if ($ForceRerun) { $jobArgs += ",--force-rerun" }
if ($DryRun) { $jobArgs += ",--dry-run" }

if (-not $SkipDeploy) {
    Write-Host "[2/3] Deploy/update Cloud Run job" -ForegroundColor Yellow
    $envVars = "PYTHONUNBUFFERED=1,LLM_PROVIDER=gemini,GEMINI_MODEL=gemini-2.0-flash,DB_HOST=db.minwoumfgutampcgrcbr.supabase.co,DB_NAME=postgres,DB_USER=postgres,DB_PORT=5432,ENABLE_COMPLETION_EMAIL=1,NOTIFICATION_EMAIL=$NotificationEmail,SENDER_EMAIL=$SenderEmail"
    $secrets = "DB_PASSWORD=DB_PASSWORD:latest,SERPER_API_KEY=SERPER_API_KEY:latest,GEMINI_API_KEY=GEMINI_API_KEY:latest,MISTRAL_API_KEY=MISTRAL_API_KEY:latest,ELASTICMAIL_API_KEY=ELASTICMAIL_API_KEY:latest"

    gcloud run jobs deploy $JobName `
        --region=$Region `
        --project=$Project `
        --image=$Image `
        --command=python `
        --args=$jobArgs `
        --max-retries=0 `
        --tasks=1 `
        --task-timeout=$TaskTimeoutSeconds `
        --cpu=2 `
        --memory=2Gi `
        --set-env-vars=$envVars `
        --set-secrets=$secrets

    if ($LASTEXITCODE -ne 0) { throw "gcloud run jobs deploy failed (exit $LASTEXITCODE)" }
} else {
    Write-Host "[2/3] Deploy skipped" -ForegroundColor DarkGray
}

Write-Host "[3/3] Execute job (fire-and-forget)" -ForegroundColor Yellow
$execName = gcloud run jobs execute $JobName --region=$Region --project=$Project --format="value(metadata.name)"
if ($LASTEXITCODE -ne 0) { throw "gcloud run jobs execute failed (exit $LASTEXITCODE)" }

Write-Host "Execution started: $execName" -ForegroundColor Green
Write-Host "Le mail de fin sera envoye depuis Cloud Run (si secret ElasticMail valide)." -ForegroundColor Green
Write-Host "Suivi (optionnel):" -ForegroundColor Magenta
Write-Host "  gcloud run jobs executions describe $execName --region=$Region --project=$Project"
Write-Host "  gcloud run jobs executions list --job=$JobName --region=$Region --project=$Project --limit=10"
