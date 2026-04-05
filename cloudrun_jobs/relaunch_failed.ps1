#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Relance SEQUENTIELLE des batches qui ont crashé à cause du LockNotAvailable.
    
    Attendre que les 6 jobs en cours (984vz, rpvrn, jtrfw, pzsbh, cwnk9, 9xvh6)
    soient terminés AVANT de lancer ce script !

    Stratégie :
    - 1 seul batch à la fois (pas de parallélisme = pas de contention DDL)
    - Attend la FIN de chaque batch avant de lancer le suivant
    - Batches de 2-3 depts au lieu de 5 (pour rester sous 24h)
    - Vérifie le status de chaque execution via polling

    Batches à relancer (crashés sur LockNotAvailable) :
    - 36-37, 38-39-40, 41-42-43, 44-45, 46-47-48, 49-50
    - 55 (seul, rpvrn fait 51-54)
    - 56-57-58, 59-60
    - 61-62-63, 64-65, 66-67-68, 69-70
    - 93 (déjà enrichi OK précédemment, skip si tout est enrichi)

    Estimation : ~2-3h par batch de 3 depts, total ~30-40h séquentiel
#>

$PROJECT  = "gen-lang-client-0230548399"
$REGION   = "europe-west1"
$OUTDIR   = "/tmp/outputs"
$JOB_A    = "finess-enrich-national"
$JOB_B    = "finess-enrich-national-b"

# ─── Helper : attendre la fin d'une execution ────────────────────────────────
function Wait-Execution {
    param(
        [string]$JobName,
        [string]$ExecName,
        [int]$PollSeconds = 120    # check toutes les 2 min
    )
    Write-Host "  [WAIT] Polling $ExecName toutes les ${PollSeconds}s..." -ForegroundColor DarkGray
    $startTime = Get-Date
    while ($true) {
        Start-Sleep -Seconds $PollSeconds
        $elapsed = [math]::Round(((Get-Date) - $startTime).TotalMinutes, 0)
        
        $status = gcloud run jobs executions describe $ExecName `
            --region=$REGION --project=$PROJECT `
            --format="value(status.conditions[0].status)" 2>&1
        
        if ($status -eq "True") {
            # Check if succeeded or failed
            $succeeded = gcloud run jobs executions describe $ExecName `
                --region=$REGION --project=$PROJECT `
                --format="value(status.succeededCount)" 2>&1
            if ($succeeded -eq "1") {
                Write-Host "  [OK] $ExecName terminé avec succès (${elapsed}min)" -ForegroundColor Green
                return $true
            } else {
                Write-Host "  [FAIL] $ExecName échoué après ${elapsed}min" -ForegroundColor Red
                return $false
            }
        }
        Write-Host "  [WAIT] $ExecName en cours... (${elapsed}min)" -ForegroundColor DarkGray
    }
}

# ─── Helper : lancer un batch et attendre ────────────────────────────────────
function Run-Batch {
    param(
        [string]$JobName,
        [string[]]$Depts
    )
    $deptStr = $Depts -join ","
    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $Depts + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","
    
    Write-Host "`n========================================" -ForegroundColor Yellow
    Write-Host "  BATCH : $deptStr (job: $JobName)" -ForegroundColor Yellow
    Write-Host "  Heure : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Yellow
    Write-Host "========================================" -ForegroundColor Yellow
    
    # Update args
    gcloud run jobs update $JobName `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Error "  Echec update args pour $deptStr"
        return $false
    }
    
    # Execute
    $output = gcloud run jobs execute $JobName `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1
    
    # Extract execution name from output
    $execName = ($output | Select-String "executions/(\S+)" | ForEach-Object { $_.Matches[0].Groups[1].Value })
    if (-not $execName) {
        # Fallback: get the latest execution
        Start-Sleep -Seconds 5
        $execName = gcloud run jobs executions list --job=$JobName `
            --region=$REGION --project=$PROJECT `
            --limit=1 --format="value(name.basename())" 2>&1
    }
    
    Write-Host "  Execution: $execName" -ForegroundColor Cyan
    
    # Wait for completion
    $result = Wait-Execution -JobName $JobName -ExecName $execName
    return $result
}

# ─── Vérification : s'assurer qu'aucun job n'est en cours ────────────────────
Write-Host "============================================" -ForegroundColor Red
Write-Host "  VERIFICATION PRE-LANCEMENT" -ForegroundColor Red
Write-Host "============================================" -ForegroundColor Red

# Check running executions on both jobs
$runningA = gcloud run jobs executions list --job=$JOB_A `
    --region=$REGION --project=$PROJECT --limit=5 `
    --format="value(name.basename(),status.conditions[0].status)" 2>&1
$runningB = gcloud run jobs executions list --job=$JOB_B `
    --region=$REGION --project=$PROJECT --limit=5 `
    --format="value(name.basename(),status.conditions[0].status)" 2>&1

$hasRunning = $false
foreach ($line in ($runningA + $runningB)) {
    if ($line -match "Unknown") {
        Write-Host "  !! Execution encore en cours: $line" -ForegroundColor Red
        $hasRunning = $true
    }
}

if ($hasRunning) {
    Write-Host "`n  ATTENTION : des executions sont encore en cours !" -ForegroundColor Red
    Write-Host "  Attendez qu'elles finissent avant de lancer ce script." -ForegroundColor Red
    Write-Host "  Pour forcer le lancement quand meme, ajoutez -Force" -ForegroundColor DarkGray
    if (-not $args.Contains("-Force")) {
        exit 1
    }
    Write-Host "  -Force detecte, on continue..." -ForegroundColor Yellow
}

Write-Host "  Aucune execution en cours. Lancement OK.`n" -ForegroundColor Green

# ─── Batches à relancer (découpés en 2-3 depts pour rester sous 24h) ────────
# Job A : depts 36-60 (les batchs 31-35 et 51-55 étaient OK via 984vz/rpvrn)
$relaunchBatches = @(
    @{ Job = $JOB_A; Depts = @("36","37","38") },
    @{ Job = $JOB_A; Depts = @("39","40") },
    @{ Job = $JOB_A; Depts = @("41","42","43") },
    @{ Job = $JOB_A; Depts = @("44","45") },
    @{ Job = $JOB_A; Depts = @("46","47","48") },
    @{ Job = $JOB_A; Depts = @("49","50") },
    @{ Job = $JOB_A; Depts = @("56","57","58") },
    @{ Job = $JOB_A; Depts = @("59","60") },
    # Job B : depts 61-70 (batchs 71-90 étaient OK via jtrfw/pzsbh/cwnk9/9xvh6)
    @{ Job = $JOB_B; Depts = @("61","62","63") },
    @{ Job = $JOB_B; Depts = @("64","65") },
    @{ Job = $JOB_B; Depts = @("66","67","68") },
    @{ Job = $JOB_B; Depts = @("69","70") },
    @{ Job = $JOB_B; Depts = @("93") }
)

$total = $relaunchBatches.Count
$succeeded = 0
$failed = 0

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  RELANCE SEQUENTIELLE : $total batches" -ForegroundColor Cyan
Write-Host "  Debut : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan
Write-Host "============================================`n" -ForegroundColor Cyan

foreach ($i in 0..($total - 1)) {
    $batch = $relaunchBatches[$i]
    $batchNum = $i + 1
    Write-Host "[$batchNum/$total]" -ForegroundColor White -NoNewline
    
    $result = Run-Batch -JobName $batch.Job -Depts $batch.Depts
    
    if ($result) {
        $succeeded++
    } else {
        $failed++
        Write-Host "  [WARN] Batch échoué, on continue avec le suivant..." -ForegroundColor Yellow
    }
    
    # Petite pause entre batches pour libérer les locks DB
    if ($i -lt ($total - 1)) {
        Write-Host "  Pause 10s avant le prochain batch..." -ForegroundColor DarkGray
        Start-Sleep -Seconds 10
    }
}

# ─── Résumé ──────────────────────────────────────────────────────────────────
Write-Host "`n============================================" -ForegroundColor Yellow
Write-Host "  RELANCE TERMINEE" -ForegroundColor Yellow
Write-Host "  Fin : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Yellow
Write-Host "  Reussis : $succeeded / $total" -ForegroundColor $(if ($succeeded -eq $total) { "Green" } else { "Yellow" })
if ($failed -gt 0) {
    Write-Host "  Echoues : $failed / $total" -ForegroundColor Red
}
Write-Host "  Verifie avec :" -ForegroundColor Yellow
Write-Host "  .venv\Scripts\python.exe _check_all_depts.py" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Yellow
