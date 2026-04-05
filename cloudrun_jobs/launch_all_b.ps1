#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Job B : Lance l'enrichissement FINESS en sens INVERSE (9F -> 01).
    Tourne en parallele du job A (01 -> 9F) sans risque de collision
    car les departements traites simultanement sont differents.

    LANCER DEPUIS UNE FENETRE POWERSHELL EXTERNE :
      powershell -File C:\Users\Lenovo\ingest-habitat\cloudrun_jobs\launch_all_b.ps1

.PARAMETER BatchSize
    Nombre de departements par batch (defaut: 5)

.PARAMETER Exclude
    Departements a exclure (virgules), ex: "17,9E"

.PARAMETER PollIntervalSecs
    Intervalle de polling en secondes (defaut: 300 = 5 min)

.PARAMETER MaxBatchWaitMin
    Temps max d'attente par batch en minutes (defaut: 240 = 4h)
#>
param(
    [int]$BatchSize = 5,
    [string]$Exclude = "17,9E",
    [int]$PollIntervalSecs = 300,
    [int]$MaxBatchWaitMin = 240
)

$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot\..

$PROJECT_ID = "gen-lang-client-0230548399"
$REGION = "europe-west1"
$IMAGE_NAME = "europe-west1-docker.pkg.dev/$PROJECT_ID/habitat/enrich@sha256:6e02f609e49c2967f42bcc11b3a5ecd76ff086e87002fe6832f114425b5e24a3"
$JOB_NAME = "finess-enrich-national-b"
$YAML_FILE = "cloudrun_jobs/finess_job_national_b.yaml"

# Sens INVERSE : 9F -> 55 (job A couvre le reste depuis 01)
$ALL_DEPTS_REVERSED = @(
    "9F","9E","9D","9C","9B","9A","95","94","93","92",
    "91","90","89","88","87","86","85","84","83","82",
    "81","80","79","78","77","76","75","74","73","72",
    "71","70","69","68","67","66","65","64","63","62",
    "61","60","59","58","57","56","55"
)

$EXCLUDE_LIST = @($Exclude -split "," | ForEach-Object { $_.Trim() } | Where-Object { $_ })
$DEPTS = @($ALL_DEPTS_REVERSED | Where-Object { $_ -notin $EXCLUDE_LIST })

# Build batches
$batches = @()
for ($i = 0; $i -lt $DEPTS.Count; $i += $BatchSize) {
    $end = [Math]::Min($i + $BatchSize - 1, $DEPTS.Count - 1)
    $batches += ,@($DEPTS[$i..$end])
}

$totalEstHours = [Math]::Round($batches.Count * 2, 0)

Write-Host "============================================" -ForegroundColor Magenta
Write-Host "  FINESS BATCH LAUNCHER B (sens inverse)" -ForegroundColor Magenta
Write-Host "============================================" -ForegroundColor Magenta
Write-Host "  Departements : $($DEPTS.Count)  (excl: $($EXCLUDE_LIST -join ', '))"
Write-Host "  Batch size   : $BatchSize"
Write-Host "  Nb batches   : $($batches.Count)"
Write-Host "  Poll interval: ${PollIntervalSecs}s"
Write-Host "  Max wait/batch: ${MaxBatchWaitMin}min"
Write-Host "  Estimated total: ~${totalEstHours}h"
Write-Host "============================================`n" -ForegroundColor Magenta

# Step 1: Deploy YAML (job B)
Write-Host "[1/3] Deploying job B definition..." -ForegroundColor Green
gcloud run jobs replace $YAML_FILE --region=$REGION --project=$PROJECT_ID --quiet 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Failed to deploy job B YAML"
    exit 1
}
Write-Host "[1/3] Job B deployed OK`n" -ForegroundColor Green

# Helpers
function Launch-Dept-B {
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

function Wait-BatchDone-B {
    param([array]$Executions)
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
            Write-Host "    Batch B complete (${elapsed}min total)" -ForegroundColor Green
            return
        }
        $runningDepts = ($stillRunning | ForEach-Object { $_.Dept }) -join ","
        Write-Host "    ... $($stillRunning.Count) running: $runningDepts (${elapsed}min)" -ForegroundColor DarkGray
    }

    Write-Host "    [TIMEOUT] Some depts still running after ${MaxBatchWaitMin}min -- moving to next batch" -ForegroundColor Red
}

# Step 2: Launch batches
Write-Host "[2/3] Launching B batches...`n" -ForegroundColor Green
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$logFile = "outputs/launch_log_B_$timestamp.txt"
New-Item -Path "outputs" -ItemType Directory -Force | Out-Null
$allLogs = @()
$globalStart = Get-Date
$totalOK = 0
$totalFail = 0

$batchNum = 1
foreach ($batch in $batches) {
    $batchStart = Get-Date
    Write-Host "== BATCH B $batchNum/$($batches.Count) : $($batch -join ', ') ==" -ForegroundColor Magenta

    $batchExecs = @()
    foreach ($dept in $batch) {
        $result = Launch-Dept-B $dept
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

    if ($batchExecs.Count -gt 0) {
        Write-Host "  Waiting (poll every $([Math]::Round($PollIntervalSecs/60,0))min, max ${MaxBatchWaitMin}min)..." -ForegroundColor Yellow
        Wait-BatchDone-B $batchExecs

        foreach ($ex in $batchExecs) {
            if ($ex.Result -eq "SUCCESS") { $totalOK++ } else { $totalFail++ }
        }
    }

    $batchElapsed = [Math]::Round(((Get-Date) - $batchStart).TotalMinutes, 0)
    $globalElapsed = [Math]::Round(((Get-Date) - $globalStart).TotalHours, 1)
    Write-Host "  Batch B $batchNum done (${batchElapsed}min). Total: ${totalOK} OK, ${totalFail} fail. Elapsed: ${globalElapsed}h`n" -ForegroundColor Magenta

    $batchNum++
}

# Step 3: Summary
$allLogs | Out-File $logFile -Encoding UTF8
$totalH = [Math]::Round(((Get-Date) - $globalStart).TotalHours, 1)

Write-Host "============================================" -ForegroundColor Magenta
Write-Host "  JOB B COMPLETE" -ForegroundColor Magenta
Write-Host "============================================" -ForegroundColor Magenta
Write-Host "  Success : $totalOK" -ForegroundColor Green
if ($totalFail -gt 0) { $fc = "Red" } else { $fc = "Green" }
Write-Host "  Failed  : $totalFail" -ForegroundColor $fc
Write-Host "  Duration: ${totalH}h"
Write-Host "  Log     : $logFile" -ForegroundColor Cyan
Write-Host "============================================`n" -ForegroundColor Magenta
Write-Host "Check DB: .venv\Scripts\python.exe _check_all_depts.py" -ForegroundColor Yellow
