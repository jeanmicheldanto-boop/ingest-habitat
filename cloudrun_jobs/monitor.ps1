#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Monitore l'état des exécutions Cloud Run FINESS.
    Peut être lancé à tout moment, même après avoir fermé VS Code.

.DESCRIPTION
    Affiche :
    - Les exécutions en cours (RUNNING)
    - Les exécutions terminées OK / FAILED
    - L'état d'enrichissement dans la base de données
    - Le % de progression global

.PARAMETER Since
    Filtre les exécutions créées depuis N heures (défaut: 12)

.PARAMETER DbCheck
    Inclut le check base de données (défaut: true)
#>
param(
    [int]$Since = 12,
    [switch]$NoDb
)

$PROJECT_ID = "gen-lang-client-0230548399"
$REGION = "europe-west1"
$JOB_NAME = "finess-enrich-national"

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "   FINESS ENRICHMENT - MONITORING" -ForegroundColor Yellow
Write-Host "   $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor DarkGray
Write-Host "============================================`n" -ForegroundColor Yellow

# ── Cloud Run Executions ──
Write-Host "[CLOUD RUN] Fetching executions (last ${Since}h)..." -ForegroundColor Cyan
$executions = gcloud run jobs executions list --job=$JOB_NAME `
    --region=$REGION --project=$PROJECT_ID --limit=200 `
    --format="csv[no-heading](name.basename(),status.succeededCount,status.failedCount,status.runningCount,createTime)" 2>&1

$running = @()
$success = @()
$failed = @()
$cutoff = (Get-Date).AddHours(-$Since).ToUniversalTime()

foreach ($line in ($executions | Out-String) -split "`n") {
    $line = $line.Trim()
    if (-not $line) { continue }
    $parts = $line -split ","
    if ($parts.Count -lt 5) { continue }
    
    $name = $parts[0]
    $succeeded = $parts[1]
    $failedCount = $parts[2]
    $runningCount = $parts[3]
    $created = $parts[4]

    # Filter by time
    try {
        $createdTime = [DateTime]::Parse($created).ToUniversalTime()
        if ($createdTime -lt $cutoff) { continue }
    } catch { continue }

    if ($runningCount -eq "1") {
        $elapsed = [Math]::Round(((Get-Date).ToUniversalTime() - $createdTime).TotalMinutes, 0)
        $running += "$name (${elapsed}min)"
    } elseif ($succeeded -eq "1") {
        $success += $name
    } elseif ($failedCount -eq "1") {
        $failed += $name
    }
}

$total_execs = $running.Count + $success.Count + $failed.Count

Write-Host ""
Write-Host "  RUNNING : $($running.Count)" -ForegroundColor Yellow
foreach ($r in $running) { Write-Host "    - $r" -ForegroundColor Yellow }
Write-Host "  SUCCESS : $($success.Count)" -ForegroundColor Green
Write-Host "  FAILED  : $($failed.Count)" -ForegroundColor Red
Write-Host "  TOTAL   : $total_execs executions" -ForegroundColor Cyan
Write-Host ""

# ── Database Check ──
if (-not $NoDb) {
    Write-Host "[DATABASE] Checking enrichment status..." -ForegroundColor Cyan
    $scriptRoot = Split-Path $PSScriptRoot -Parent
    $pythonExe = Join-Path $scriptRoot ".venv\Scripts\python.exe"
    $checkScript = Join-Path $scriptRoot "_check_all_depts.py"
    
    if (Test-Path $pythonExe) {
        Push-Location $scriptRoot
        & $pythonExe $checkScript 2>&1
        Pop-Location
    } else {
        Write-Host "  Python venv not found at: $pythonExe" -ForegroundColor Red
        Write-Host "  Run manually: .venv\Scripts\python.exe _check_all_depts.py" -ForegroundColor Yellow
    }
}

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "To re-check later: cd cloudrun_jobs; .\monitor.ps1" -ForegroundColor DarkGray
Write-Host "============================================`n" -ForegroundColor Yellow
