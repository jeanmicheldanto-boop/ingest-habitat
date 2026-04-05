#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Lance l'enrichissement FINESS national en batches sur Cloud Run.
    Chaque département prend 1 à 3 heures. Le script attend la fin de
    chaque batch avant de lancer le suivant.

    LANCER DEPUIS UNE FENETRE POWERSHELL EXTERNE (pas VS Code) :
      powershell -File C:\Users\Lenovo\ingest-habitat\cloudrun_jobs\launch_all.ps1

.PARAMETER BatchSize
    Nombre de départements par batch (défaut: 5)

.PARAMETER Exclude
    Départements à exclure (virgules), ex: "17,02,65"

.PARAMETER PollIntervalSecs
    Intervalle de polling en secondes (défaut: 300 = 5 min)

.PARAMETER MaxBatchWaitMin
    Temps max d'attente par batch en minutes (défaut: 240 = 4h)
#>
param(
    [int]$BatchSize = 5,
    [string]$Exclude = "17",
    [int]$PollIntervalSecs = 300,
    [int]$MaxBatchWaitMin = 240
)

$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot\..

$PROJECT_ID = "gen-lang-client-0230548399"
$REGION = "europe-west1"
$IMAGE_NAME = "europe-west1-docker.pkg.dev/$PROJECT_ID/habitat/enrich@sha256:6e02f609e49c2967f42bcc11b3a5ecd76ff086e87002fe6832f114425b5e24a3"
$JOB_NAME = "finess-enrich-national"
$YAML_FILE = "cloudrun_jobs/finess_job_national.yaml"

$ALL_DEPTS = @(
    "01","02","03","04","05","06","07","08","09","10",
    "11","12","13","14","15","16","17","18","19","21",
    "22","23","24","25","26","27","28","29","2A","2B",
    "30","31","32","33","34","35","36","37","38","39",
    "40","41","42","43","44","45","46","47","48","49",
    "50","51","52","53","54","55","56","57","58","59",
    "60","61","62","63","64","65","66","67","68","69",
    "70","71","72","73","74","75","76","77","78","79",
    "80","81","82","83","84","85","86","87","88","89",
    "90","91","92","93","94","95","9A","9B","9C","9D",
    "9E","9F"
)

$EXCLUDE_LIST = @($Exclude -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$DEPTS = @($ALL_DEPTS | Where-Object { $_ -notin $EXCLUDE_LIST })

# Build batches
$batches = @()
for ($i = 0; $i -lt $DEPTS.Count; $i += $BatchSize) {
    $end = [Math]::Min($i + $BatchSize - 1, $DEPTS.Count - 1)
    $batches += ,@($DEPTS[$i..$end])
}

$totalEstHours = [Math]::Round($batches.Count * 2, 0)  # ~2h avg per batch

Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  FINESS BATCH LAUNCHER" -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  Departements : $($DEPTS.Count)  (excl: $($EXCLUDE_LIST -join ', '))"
Write-Host "  Batch size   : $BatchSize"
Write-Host "  Nb batches   : $($batches.Count)"
Write-Host "  Poll interval: ${PollIntervalSecs}s (5 min)"
Write-Host "  Max wait/batch: ${MaxBatchWaitMin} min (4h)"
Write-Host "  Estimated total: ~${totalEstHours}h"
Write-Host "============================================`n" -ForegroundColor Yellow

# ── Step 1: Deploy YAML ──
Write-Host "[1/3] Deploying job definition..." -ForegroundColor Green
gcloud run jobs replace $YAML_FILE --region=$REGION --project=$PROJECT_ID --quiet 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to deploy job YAML"
    exit 1
}
Write-Host "[1/3] Job deployed OK`n" -ForegroundColor Green

# ── Helpers ──
function Launch-Dept {
    param([string]$Dept)
    $dept_args = "scripts/enrich_finess_dept.py,--departements,$Dept,--out-dir,/tmp/outputs"
    gcloud run jobs update $JOB_NAME `
        --region=$REGION --project=$PROJECT_ID `
        --image=$IMAGE_NAME `
        --args=$dept_args --quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { return @{ Status = "FAIL"; ExecName = $null } }

    $exec_output = gcloud run jobs execute $JOB_NAME `
        --region=$REGION --project=$PROJECT_ID --quiet 2>&1
    $exec_str = $exec_output | Out-String
    if ($exec_str -match '\[([^\]]+)\]') {
        return @{ Status = "OK"; ExecName = $Matches[1] }
    }
    return @{ Status = "OK"; ExecName = $null }
}

function Wait-BatchDone {
    param([array]$Executions)  # array of @{Dept; ExecName}
    $trackable = @($Executions | Where-Object { $_.ExecName })
    if ($trackable.Count -eq 0) { return }

    $maxPolls = [Math]::Ceiling(($MaxBatchWaitMin * 60) / $PollIntervalSecs)
    $startTime = Get-Date

    for ($poll = 1; $poll -le $maxPolls; $poll++) {
        Start-Sleep -Seconds $PollIntervalSecs

        $stillRunning = @()
        $justFinished = @()

        foreach ($ex in $trackable) {
            if ($ex.Done) { continue }
            $completion = gcloud run jobs executions describe $ex.ExecName `
                --region=$REGION --project=$PROJECT_ID `
                --format="value(status.completionTime)" --quiet 2>&1
            $c = ($completion | Out-String).Trim()
            if ($c) {
                # Check success vs failure
                $succeeded = gcloud run jobs executions describe $ex.ExecName `
                    --region=$REGION --project=$PROJECT_ID `
                    --format="value(status.succeededCount)" --quiet 2>&1
                $s = ($succeeded | Out-String).Trim()
                $ex.Done = $true
                if ($s -eq "1") {
                    $ex.Result = "SUCCESS"
                    $justFinished += $ex
                } else {
                    $ex.Result = "FAILED"
                    $justFinished += $ex
                }
            } else {
                $stillRunning += $ex
            }
        }

        $elapsed = [Math]::Round(((Get-Date) - $startTime).TotalMinutes, 0)

        foreach ($f in $justFinished) {
            if ($f.Result -eq "SUCCESS") { $color = "Green" } else { $color = "Red" }
            Write-Host "    [$($f.Result)] dept $($f.Dept) (${elapsed}min)" -ForegroundColor $color
        }

        if ($stillRunning.Count -eq 0) {
            Write-Host "    Batch complete (${elapsed}min total)" -ForegroundColor Green
            return
        }
        $runningDepts = ($stillRunning | ForEach-Object { $_.Dept }) -join ","
        Write-Host "    ... $($stillRunning.Count) running: $runningDepts (${elapsed}min)" -ForegroundColor DarkGray
    }

    Write-Host "    [TIMEOUT] Some depts still running after ${MaxBatchWaitMin}min -- moving to next batch" -ForegroundColor Red
}

# ── Step 2: Launch by batches ──
Write-Host "[2/3] Launching batches...`n" -ForegroundColor Green
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = "outputs/launch_log_$timestamp.txt"
New-Item -Path "outputs" -ItemType Directory -Force | Out-Null
$allLogs = @()
$globalStart = Get-Date
$totalOK = 0
$totalFail = 0

$batchNum = 1
foreach ($batch in $batches) {
    $batchStart = Get-Date
    Write-Host "== BATCH $batchNum/$($batches.Count) : $($batch -join ', ') ==" -ForegroundColor Cyan

    $batchExecs = @()
    foreach ($dept in $batch) {
        $result = Launch-Dept $dept
        if ($result.Status -eq "OK") {
            $execName = $result.ExecName
            if ($execName) { $label = $execName } else { $label = "(no name)" }
            Write-Host "  -> dept $dept : $label" -ForegroundColor DarkGreen
            $batchExecs += @{ Dept = $dept; ExecName = $execName; Done = $false; Result = $null }
            $allLogs += "$dept|LAUNCHED|$execName"
        } else {
            Write-Host "  -> dept $dept : LAUNCH FAILED" -ForegroundColor Red
            $allLogs += "$dept|LAUNCH_FAILED|N/A"
            $totalFail++
        }
        Start-Sleep -Seconds 3
    }

    # Wait for this batch to complete
    if ($batchExecs.Count -gt 0) {
        Write-Host "  Waiting (poll every $([Math]::Round($PollIntervalSecs/60,0))min, max ${MaxBatchWaitMin}min)..." -ForegroundColor Yellow
        Wait-BatchDone $batchExecs

        # Count results
        foreach ($ex in $batchExecs) {
            if ($ex.Result -eq "SUCCESS") { $totalOK++ } else { $totalFail++ }
        }
    }

    $batchElapsed = [Math]::Round(((Get-Date) - $batchStart).TotalMinutes, 0)
    $globalElapsed = [Math]::Round(((Get-Date) - $globalStart).TotalHours, 1)
    Write-Host "  Batch $batchNum done (${batchElapsed}min). Total: ${totalOK} OK, ${totalFail} fail. Elapsed: ${globalElapsed}h`n" -ForegroundColor Cyan

    $batchNum++
}

# ── Step 3: Summary ──
$allLogs | Out-File $logFile -Encoding UTF8
$totalH = [Math]::Round(((Get-Date) - $globalStart).TotalHours, 1)

Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  ALL BATCHES COMPLETE" -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  Success : $totalOK" -ForegroundColor Green
Write-Host "  Failed  : $totalFail" -ForegroundColor $(if ($totalFail -gt 0) { "Red" } else { "Green" })
Write-Host "  Duration: ${totalH}h"
Write-Host "  Log     : $logFile" -ForegroundColor Cyan
Write-Host "============================================`n" -ForegroundColor Yellow
Write-Host "Check DB: .venv\Scripts\python.exe _check_all_depts.py" -ForegroundColor Yellow
