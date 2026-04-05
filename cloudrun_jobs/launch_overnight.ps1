#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Lance en parallele toutes les executions restantes pour la nuit.

    Etat au 04/03/2026 23h :
      - Completement faits : 01-27 (sauf 17 exclu), 9A-9F (sauf 9E exclu)
      - En cours           : 28,29,2A,2B,30 (Job A batch 6)
                             91,92,94,95 (Job B batch 2) + 93 failed
      - A faire ce soir    : 31-90 (60 depts) + retry 93

    Job A (finess-enrich-national)   : 31-60 = 6 batches
    Job B (finess-enrich-national-b) : 61-90 + retry 93 = 7 batches
    Total : 13 executions async
#>

$PROJECT  = "gen-lang-client-0230548399"
$REGION   = "europe-west1"
$OUTDIR   = "/tmp/outputs"
$YAML_A   = "cloudrun_jobs/finess_job_national.yaml"
$YAML_B   = "cloudrun_jobs/finess_job_national_b.yaml"

# ─── Déploiement des YAMLs (image mise à jour) ────────────────────────────────
Write-Host "============================================" -ForegroundColor Green
Write-Host "  DEPLOY YAMLs (mise a jour image)"          -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Set-Location $PSScriptRoot\..
gcloud run jobs replace $YAML_A --region=$REGION --project=$PROJECT --quiet
if ($LASTEXITCODE -ne 0) { Write-Error "Echec deploy Job A"; exit 1 }
Write-Host "  Job A deploye OK" -ForegroundColor Green
gcloud run jobs replace $YAML_B --region=$REGION --project=$PROJECT --quiet
if ($LASTEXITCODE -ne 0) { Write-Error "Echec deploy Job B"; exit 1 }
Write-Host "  Job B deploye OK`n" -ForegroundColor Green

# ─── JOB A : 31 → 60 ─────────────────────────────────────────────────────────
$batchesA = @(
    @("31","32","33","34","35"),
    @("36","37","38","39","40"),
    @("41","42","43","44","45"),
    @("46","47","48","49","50"),
    @("51","52","53","54","55"),
    @("56","57","58","59","60")
)

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  LANCEMENT OVERNIGHT - JOB A (31-60)"      -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

foreach ($batch in $batchesA) {
    $depts = $batch -join ","
    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $batch + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","
    Write-Host "  -> Exec A : $depts" -ForegroundColor Cyan
    gcloud run jobs update finess-enrich-national `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --quiet 2>&1 | Out-Null
    gcloud run jobs execute finess-enrich-national `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1
    Start-Sleep -Seconds 30  # laisse le temps au job precedent de reserver ses rows en DB
}

# ─── JOB B : 61 → 90 + retry 93 ─────────────────────────────────────────────
$batchesB = @(
    @("61","62","63","64","65"),
    @("66","67","68","69","70"),
    @("71","72","73","74","75"),
    @("76","77","78","79","80"),
    @("81","82","83","84","85"),
    @("86","87","88","89","90"),
    @("93")                        # retry dept 93 (failed)
)

Write-Host "`n============================================"     -ForegroundColor Magenta
Write-Host "  LANCEMENT OVERNIGHT - JOB B (61-90 + 93)"       -ForegroundColor Magenta
Write-Host "============================================`n"    -ForegroundColor Magenta

foreach ($batch in $batchesB) {
    $depts = $batch -join ","
    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $batch + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","
    Write-Host "  -> Exec B : $depts" -ForegroundColor Magenta
    gcloud run jobs update finess-enrich-national-b `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --quiet 2>&1 | Out-Null
    gcloud run jobs execute finess-enrich-national-b `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1
    Start-Sleep -Seconds 30  # laisse le temps au job precedent de reserver ses rows en DB
}

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "  13 executions lancees en parallele !"      -ForegroundColor Yellow
Write-Host "  Verifie demain matin avec :"               -ForegroundColor Yellow
Write-Host "  .venv\Scripts\python.exe _check_all_depts.py" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Yellow
