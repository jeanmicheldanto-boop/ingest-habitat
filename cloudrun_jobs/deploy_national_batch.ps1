#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Déploie et lance l'enrichissement FINESS national (102 départements) sur Cloud Run.

.DESCRIPTION
    Ce script :
    1. Vérifie le job Cloud Run finess-enrich-national
    2. Lance les départements en batches
    3. Pour chaque département : met à jour le job (--args) puis exécute (async)
    4. Poll le statut de toutes les exécutions du batch jusqu'à complétion
    5. Affiche un résumé

    IMPORTANT : les départements sont lancés SÉQUENTIELLEMENT dans chaque batch
    (update job args → execute → next), mais les exécutions Cloud Run tournent
    EN PARALLÈLE. On poll ensuite le statut de toutes les exécutions du batch.

.PARAMETER SkipBuild
    Skip Docker build & push (utilise l'image :latest existante)

.PARAMETER BatchSize
    Nombre de départements par batch (défaut: 10)

.PARAMETER DepartmentList
    Liste spécifique de départements (ex: "01,02,03"), sinon tous les 102

.PARAMETER PollIntervalSecs
    Intervalle de polling en secondes (défaut: 60)

.EXAMPLE
    .\deploy_national_batch.ps1
    # Déploie et lance tous les 102 départements

.EXAMPLE
    .\deploy_national_batch.ps1 -SkipBuild -BatchSize 5
    # Skip build, batches de 5

.EXAMPLE
    .\deploy_national_batch.ps1 -DepartmentList "75,69,13,59"
    # Lance uniquement Paris, Lyon, Marseille, Lille
#>

param(
    [switch]$SkipBuild,
    [int]$BatchSize = 5,
    [string]$DepartmentList = "",
    [int]$PollIntervalSecs = 60
)

$ErrorActionPreference = "Continue"
Set-Location $PSScriptRoot\..

$PROJECT_ID = "gen-lang-client-0230548399"
$REGION = "europe-west1"
$IMAGE_NAME = "europe-west1-docker.pkg.dev/$PROJECT_ID/habitat/enrich@sha256:6e02f609e49c2967f42bcc11b3a5ecd76ff086e87002fe6832f114425b5e24a3"
$JOB_NAME = "finess-enrich-national"
$YAML_FILE = "cloudrun_jobs/finess_job_national.yaml"

# Liste complète des 102 départements
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

# Départements déjà enrichis à 100% (à exclure)
$ALREADY_DONE = @("17")

# Parse department list
if ($DepartmentList) {
    $DEPTS_TO_PROCESS = @(($DepartmentList -split ",") | ForEach-Object { $_.Trim() } | Where-Object { $_ -notin $ALREADY_DONE })
    Write-Host "Departements specifies: $($DEPTS_TO_PROCESS.Count) (excl: $($ALREADY_DONE -join ','))" -ForegroundColor Cyan
} else {
    $DEPTS_TO_PROCESS = @($ALL_DEPTS | Where-Object { $_ -notin $ALREADY_DONE })
    Write-Host "Mode NATIONAL: deploiement de $($DEPTS_TO_PROCESS.Count) departements (excl: $($ALREADY_DONE -join ','))" -ForegroundColor Cyan
}

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "FINESS ENRICHMENT - BATCH NATIONAL" -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "Departements: $($DEPTS_TO_PROCESS.Count)"
Write-Host "Batch size: $BatchSize"
Write-Host "Poll interval: ${PollIntervalSecs}s"
Write-Host "Skip build: $SkipBuild"
Write-Host "============================================`n" -ForegroundColor Yellow

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Docker Build & Push
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[STEP 1/4] Build skipped — using pinned digest in YAML`n" -ForegroundColor Yellow

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Verify Cloud Run Job
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[STEP 2/4] Verifying Cloud Run job: $JOB_NAME..." -ForegroundColor Green
$jobCheck = gcloud run jobs describe $JOB_NAME --region=$REGION --project=$PROJECT_ID --format="value(metadata.name)" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "[STEP 2/4] Job not found, deploying..." -ForegroundColor Yellow
    gcloud run jobs replace $YAML_FILE --region=$REGION --project=$PROJECT_ID
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Job deployment failed"
        exit 1
    }
}
Write-Host "[STEP 2/4] Job ready: $JOB_NAME`n" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────────────────────
# Helper: Extract execution name from gcloud output
# ─────────────────────────────────────────────────────────────────────────────
function Get-ExecutionName {
    param([string]$GcloudOutput)
    # gcloud output: "Execution [finess-enrich-national-xxxxx] has successfully started running."
    if ($GcloudOutput -match '\[([^\]]+)\]') {
        return $Matches[1]
    }
    return $null
}

# ─────────────────────────────────────────────────────────────────────────────
# Helper: Check if execution is done + succeeded
# ─────────────────────────────────────────────────────────────────────────────
function Get-ExecutionResult {
    <#
    .SYNOPSIS
        Returns "running", "success", "failed", or "error"
    .DESCRIPTION
        Uses completionTime to determine if done (reliable, unlike conditions[0]
        which can be "True" for both Running and Completed conditions).
        Then checks succeededCount to determine success vs failure.
    #>
    param([string]$ExecName)

    # 1. Check if execution is done (completionTime is set)
    $completion = gcloud run jobs executions describe $ExecName `
        --region=$REGION --project=$PROJECT_ID `
        --format="value(status.completionTime)" 2>&1
    if ($LASTEXITCODE -ne 0) { return "error" }
    $completion_str = ($completion | Out-String).Trim()
    if (-not $completion_str) {
        return "running"
    }

    # 2. Done — check if it succeeded
    $succeeded = gcloud run jobs executions describe $ExecName `
        --region=$REGION --project=$PROJECT_ID `
        --format="value(status.succeededCount)" 2>&1
    $succeeded_str = ($succeeded | Out-String).Trim()
    if ($succeeded_str -eq "1") {
        return "success"
    }
    return "failed"
}

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Launch Jobs in Batches (sequential launch, parallel execution)
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "[STEP 3/4] Launching $($DEPTS_TO_PROCESS.Count) jobs in batches of $BatchSize..." -ForegroundColor Green
Write-Host "  Strategy: sequential launch (update args -> execute) then poll all" -ForegroundColor DarkGray

$batches = @()
for ($i = 0; $i -lt $DEPTS_TO_PROCESS.Count; $i += $BatchSize) {
    $end = [Math]::Min($i + $BatchSize - 1, $DEPTS_TO_PROCESS.Count - 1)
    $batch = @($DEPTS_TO_PROCESS[$i..$end])
    $batches += ,@($batch)
}

Write-Host "Total batches: $($batches.Count)`n" -ForegroundColor Cyan

$all_results = @()
$batch_num = 1

foreach ($batch in $batches) {
    Write-Host "----------------------------------------" -ForegroundColor Cyan
    Write-Host "BATCH $batch_num / $($batches.Count) : $($batch -join ', ')" -ForegroundColor Cyan
    Write-Host "----------------------------------------" -ForegroundColor Cyan

    # ── Phase 1: Launch all departments in this batch (sequentially) ──
    $batch_executions = @()

    foreach ($dept in $batch) {
        Write-Host "  Launching dept $dept..." -ForegroundColor White

        # 1a. Update job definition with this department's args (keep pinned image)
        $dept_args = "scripts/enrich_finess_dept.py,--departements,$dept,--out-dir,/tmp/outputs"
        $update_output = gcloud run jobs update $JOB_NAME `
            --region=$REGION --project=$PROJECT_ID `
            --image=$IMAGE_NAME `
            --args=$dept_args --quiet 2>&1

        if ($LASTEXITCODE -ne 0) {
            Write-Host "  [ERROR] Failed to update job for dept $dept" -ForegroundColor Red
            Write-Host "    $($update_output | Out-String)" -ForegroundColor DarkRed
            $batch_executions += @{ Dept = $dept; ExecName = $null; Status = "launch_failed" }
            continue
        }

        # 1b. Execute (fire-and-forget, NO --wait)
        $exec_output = gcloud run jobs execute $JOB_NAME `
            --region=$REGION --project=$PROJECT_ID 2>&1
        $exec_output_str = $exec_output | Out-String

        if ($LASTEXITCODE -ne 0) {
            Write-Host "  [ERROR] Failed to execute job for dept $dept" -ForegroundColor Red
            $batch_executions += @{ Dept = $dept; ExecName = $null; Status = "launch_failed" }
            continue
        }

        # 1c. Parse execution name
        $exec_name = Get-ExecutionName $exec_output_str
        if (-not $exec_name) {
            Write-Host "  [WARN] Could not parse execution name for dept $dept" -ForegroundColor Yellow
            Write-Host "    Output: $exec_output_str" -ForegroundColor DarkYellow
            $batch_executions += @{ Dept = $dept; ExecName = $null; Status = "launch_unknown" }
        } else {
            Write-Host "  [LAUNCHED] dept $dept -> $exec_name" -ForegroundColor DarkGreen
            $batch_executions += @{ Dept = $dept; ExecName = $exec_name; Status = "running" }
        }

        # 1d. Small delay to ensure the execution is fully registered before
        #     we update the job args for the next department.
        Start-Sleep -Seconds 3
    }

    # ── Phase 2: Poll all executions until all are done ──
    $running = @($batch_executions | Where-Object { $_.Status -eq "running" })
    $poll_count = 0
    $max_polls = [Math]::Ceiling(7200 / $PollIntervalSecs)  # 2 hours max per batch

    if ($running.Count -gt 0) {
        Write-Host "`n  Waiting for $($running.Count) executions to complete (poll every ${PollIntervalSecs}s, max 2h)..." -ForegroundColor Yellow
    }

    while ($running.Count -gt 0 -and $poll_count -lt $max_polls) {
        Start-Sleep -Seconds $PollIntervalSecs
        $poll_count++

        $still_running = @()
        foreach ($exec in $running) {
            $result = Get-ExecutionResult $exec.ExecName
            switch ($result) {
                "running" {
                    $still_running += $exec
                }
                "success" {
                    $exec.Status = "success"
                    Write-Host "  [OK] Dept $($exec.Dept) completed" -ForegroundColor Green
                }
                "failed" {
                    $exec.Status = "failed"
                    Write-Host "  [FAIL] Dept $($exec.Dept) FAILED" -ForegroundColor Red
                }
                default {
                    $exec.Status = "error"
                    Write-Host "  [ERROR] Dept $($exec.Dept) - polling error" -ForegroundColor Red
                }
            }
        }
        $running = $still_running

        if ($running.Count -gt 0) {
            $elapsed_min = [Math]::Round(($poll_count * $PollIntervalSecs) / 60, 1)
            Write-Host "  ... $($running.Count) still running (${elapsed_min}min elapsed, poll #$poll_count)" -ForegroundColor DarkGray
        }
    }

    if ($running.Count -gt 0) {
        Write-Host "  [TIMEOUT] $($running.Count) executions still running after max poll time" -ForegroundColor Red
        foreach ($exec in $running) {
            $exec.Status = "timeout"
        }
    }

    # ── Collect results ──
    foreach ($exec in $batch_executions) {
        $all_results += @{
            Dept = $exec.Dept
            ExecName = $exec.ExecName
            Status = $exec.Status
        }
    }

    $batch_ok = ($batch_executions | Where-Object { $_.Status -eq "success" }).Count
    $batch_fail = ($batch_executions | Where-Object { $_.Status -ne "success" }).Count
    Write-Host "`nBatch $batch_num done: $batch_ok OK, $batch_fail failed/other`n" -ForegroundColor Cyan

    if ($batch_num -lt $batches.Count) {
        Write-Host "  Waiting 15s before next batch...`n" -ForegroundColor DarkGray
        Start-Sleep -Seconds 15
    }

    $batch_num++
}

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Results Summary
# ─────────────────────────────────────────────────────────────────────────────
Write-Host "`n[STEP 4/4] Results Summary" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Yellow

$success = @($all_results | Where-Object { $_.Status -eq "success" }).Count
$failed = @($all_results | Where-Object { $_.Status -ne "success" }).Count

Write-Host "Total: $($all_results.Count) departments" -ForegroundColor Cyan
Write-Host "Success: $success" -ForegroundColor Green
Write-Host "Failed/Other: $failed" -ForegroundColor Red

if ($failed -gt 0) {
    Write-Host "`nFailed departments:" -ForegroundColor Red
    $all_results | Where-Object { $_.Status -ne "success" } | ForEach-Object {
        Write-Host "  - Dept $($_.Dept) : $($_.Status) ($($_.ExecName))" -ForegroundColor Red
    }
}

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "BATCH NATIONAL COMPLETE" -ForegroundColor Yellow
Write-Host "============================================`n" -ForegroundColor Yellow

# Export results to JSON
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
New-Item -Path "outputs" -ItemType Directory -Force | Out-Null
$results_file = "outputs/batch_national_results_$timestamp.json"
$all_results | ConvertTo-Json -Depth 10 | Out-File $results_file -Encoding UTF8
Write-Host "Results exported to: $results_file" -ForegroundColor Cyan

# Check email notifications
Write-Host "`nCheck your email (patrick.danto@confidensia.fr) for completion notifications from all departments.`n" -ForegroundColor Magenta

exit 0
