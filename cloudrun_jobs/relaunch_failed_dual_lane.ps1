#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Relance securisee avec parallelisme controle (2 lanes max):
    - Lane A: 1 execution du job finess-enrich-national
    - Lane B: 1 execution du job finess-enrich-national-b

    Pourquoi ce mode:
    - Evite le carnage de locks observe avec 13 executions simultanees
    - Garde un peu de parallelisme pour reduire la duree totale
    - Evite les races de --args en gardant l'ordre par job

    IMPORTANT:
    - A lancer uniquement quand les 6 executions en cours sont terminees.
#>

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$OUTDIR  = "/tmp/outputs"
$JOB_A   = "finess-enrich-national"
$JOB_B   = "finess-enrich-national-b"

function Get-RunningExecutions {
    param([string]$JobName)
    $rows = gcloud run jobs executions list --job=$JobName `
        --region=$REGION --project=$PROJECT --limit=20 `
        --format="value(name.basename(),status.conditions[0].status)" 2>&1
    $running = @()
    foreach ($r in $rows) {
        if ($r -match "Unknown") { $running += $r }
    }
    return $running
}

function Start-Batch {
    param(
        [string]$JobName,
        [string[]]$Depts
    )

    $deptStr = $Depts -join ","
    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $Depts + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","

    Write-Host "  -> START $JobName [$deptStr]" -ForegroundColor Cyan

    gcloud run jobs update $JobName `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Echec update $JobName [$deptStr]"
    }

    $execOut = gcloud run jobs execute $JobName `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Echec execute $JobName [$deptStr]"
    }

    Start-Sleep -Seconds 3
    $execName = gcloud run jobs executions list --job=$JobName `
        --region=$REGION --project=$PROJECT --limit=1 `
        --format="value(name.basename())" 2>&1

    return @{ Job = $JobName; Depts = $deptStr; Exec = ($execName | Select-Object -First 1) }
}

function Wait-Execution {
    param(
        [string]$JobName,
        [string]$ExecName,
        [int]$PollSeconds = 120
    )

    $start = Get-Date
    while ($true) {
        Start-Sleep -Seconds $PollSeconds

        $condStatus = gcloud run jobs executions describe $ExecName `
            --region=$REGION --project=$PROJECT `
            --format="value(status.conditions[0].status)" 2>&1

        if (($condStatus | Select-Object -First 1) -eq "True") {
            $succ = gcloud run jobs executions describe $ExecName `
                --region=$REGION --project=$PROJECT `
                --format="value(status.succeededCount)" 2>&1
            $elapsed = [math]::Round(((Get-Date) - $start).TotalMinutes, 1)
            if (($succ | Select-Object -First 1) -eq "1") {
                Write-Host "  [OK] $ExecName (${elapsed} min)" -ForegroundColor Green
                return $true
            }
            Write-Host "  [FAIL] $ExecName (${elapsed} min)" -ForegroundColor Red
            return $false
        }

        $elapsed2 = [math]::Round(((Get-Date) - $start).TotalMinutes, 0)
        Write-Host "  [WAIT] $ExecName en cours (${elapsed2} min)" -ForegroundColor DarkGray
    }
}

# Failed scope decoupe en petits batches pour rester sous 24h
$batchesA = @(
    @("36","37","38"),
    @("39","40"),
    @("41","42","43"),
    @("44","45"),
    @("46","47","48"),
    @("49","50"),
    @("56","57","58"),
    @("59","60")
)

$batchesB = @(
    @("61","62","63"),
    @("64","65"),
    @("66","67","68"),
    @("69","70"),
    @("93")
)

Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  RELANCE DUAL-LANE (A+B en parallele)" -ForegroundColor Yellow
Write-Host "============================================" -ForegroundColor Yellow

$runningA = Get-RunningExecutions -JobName $JOB_A
$runningB = Get-RunningExecutions -JobName $JOB_B
if ($runningA.Count -gt 0 -or $runningB.Count -gt 0) {
    Write-Host "Des executions sont encore en cours:" -ForegroundColor Red
    foreach ($r in $runningA) { Write-Host "  A: $r" -ForegroundColor Red }
    foreach ($r in $runningB) { Write-Host "  B: $r" -ForegroundColor Red }
    Write-Host "Attendre la fin avant de relancer." -ForegroundColor Red
    exit 1
}

$ixA = 0
$ixB = 0
$ok = 0
$ko = 0
$wave = 0

while ($ixA -lt $batchesA.Count -or $ixB -lt $batchesB.Count) {
    $wave++
    Write-Host "`n--- WAVE $wave ---" -ForegroundColor Magenta

    $runA = $null
    $runB = $null

    try {
        if ($ixA -lt $batchesA.Count) {
            $runA = Start-Batch -JobName $JOB_A -Depts $batchesA[$ixA]
            $ixA++
        }
        if ($ixB -lt $batchesB.Count) {
            $runB = Start-Batch -JobName $JOB_B -Depts $batchesB[$ixB]
            $ixB++
        }
    } catch {
        Write-Host "[ERROR] $($_.Exception.Message)" -ForegroundColor Red
        $ko++
    }

    # Les deux tournent en parallele; on attend qu'ils finissent.
    if ($runA -ne $null) {
        if (Wait-Execution -JobName $runA.Job -ExecName $runA.Exec) { $ok++ } else { $ko++ }
    }
    if ($runB -ne $null) {
        if (Wait-Execution -JobName $runB.Job -ExecName $runB.Exec) { $ok++ } else { $ko++ }
    }

    Write-Host "Pause de 20s avant la wave suivante..." -ForegroundColor DarkGray
    Start-Sleep -Seconds 20
}

Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "  FIN RELANCE DUAL-LANE" -ForegroundColor Yellow
Write-Host "  OK: $ok | KO: $ko" -ForegroundColor Yellow
Write-Host "  Verif finale: .venv\Scripts\python.exe _check_all_depts.py" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Yellow
