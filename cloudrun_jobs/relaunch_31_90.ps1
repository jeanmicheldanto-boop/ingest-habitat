#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Relance les departements 31-90 + 93 qui ont echoue (exit code 2 = argparse bug).

    FIX : --departements accepte maintenant nargs='+' (args separés par espace).
          L'image a ete reconstruite et les jobs mis a jour.

    Etat : depts 31-90 TOUS en brut (7000+ lignes a enrichir).
           dept 31 : 33 enrichis deja, les brut restants seront traites.
           dept 93 : retry (avait echoue lors du batch precedent).
#>

$PROJECT  = "gen-lang-client-0230548399"
$REGION   = "europe-west1"
$OUTDIR   = "/tmp/outputs"

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
Write-Host "  RELAUNCH - JOB A (31-60) — FIX argparse"  -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

foreach ($batch in $batchesA) {
    $depts = $batch -join ","
    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $batch + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","
    Write-Host "  -> Exec A : $depts" -ForegroundColor Cyan
    gcloud run jobs execute finess-enrich-national `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --async --quiet 2>&1
}

# ─── JOB B : 61 → 90 + retry 93 ─────────────────────────────────────────────
$batchesB = @(
    @("61","62","63","64","65"),
    @("66","67","68","69","70"),
    @("71","72","73","74","75"),
    @("76","77","78","79","80"),
    @("81","82","83","84","85"),
    @("86","87","88","89","90"),
    @("93")
)

Write-Host "`n============================================"     -ForegroundColor Magenta
Write-Host "  RELAUNCH - JOB B (61-90 + 93) — FIX argparse"  -ForegroundColor Magenta
Write-Host "============================================`n"    -ForegroundColor Magenta

foreach ($batch in $batchesB) {
    $depts = $batch -join ","
    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $batch + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","
    Write-Host "  -> Exec B : $depts" -ForegroundColor Magenta
    gcloud run jobs execute finess-enrich-national-b `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --async --quiet 2>&1
}

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "  13 executions relancees !"                  -ForegroundColor Yellow
Write-Host "  Verification dans ~2h via :"                -ForegroundColor Yellow
Write-Host "  .venv\Scripts\python.exe _check_all_depts.py" -ForegroundColor White
Write-Host "  gcloud run jobs executions list --job=finess-enrich-national --region=europe-west1 --project=$PROJECT" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Yellow
