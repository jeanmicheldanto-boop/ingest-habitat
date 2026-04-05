# launch_signaux_v2_passb_full.ps1
# Déclenche tous les batches Passe B sur un job Cloud Run déjà déployé.
# Prérequis : avoir lancé launch_signaux_v2_passe_b.ps1 une fois pour le build+deploy.
#
# Usage:
#   .\cloudrun_jobs\launch_signaux_v2_passb_full.ps1                              # 10 batches de 100, parallèle 2 à la fois
#   .\cloudrun_jobs\launch_signaux_v2_passb_full.ps1 -TotalCandidates 500         # 5 batches
#   .\cloudrun_jobs\launch_signaux_v2_passb_full.ps1 -Sequential                  # séquentiel (plus sûr, plus lent)
#   .\cloudrun_jobs\launch_signaux_v2_passb_full.ps1 -DryRun                      # affiche les commandes sans exécuter

param(
    [int]$TotalCandidates = 1000,   # estimation haute — le script s'arrête naturellement si plus de candidats
    [int]$BatchSize       = 100,
    [int]$StartOffset     = 0,
    [switch]$ScopeFilterLLM,
    [switch]$Sequential,            # attend chaque batch avant le suivant (--wait)
    [switch]$DryRun,
    [string]$Project = "gen-lang-client-0230548399",
    [string]$Region  = "europe-west1",
    [string]$JobName = "finess-signaux-v2-passb"
)

$ErrorActionPreference = "Stop"

function Get-ExecutionState {
    param(
        [string]$ExecutionName,
        [string]$Region,
        [string]$Project
    )

    $json = gcloud run jobs executions describe $ExecutionName `
        --region=$Region --project=$Project `
        --format=json 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($json)) {
        return "Unknown"
    }

    try {
        $obj = $json | ConvertFrom-Json
        $completed = $obj.status.conditions | Where-Object { $_.type -eq "Completed" } | Select-Object -First 1
        if ($null -eq $completed) { return "Unknown" }
        return [string]$completed.status
    } catch {
        return "Unknown"
    }
}

function Wait-Execution {
    param(
        [string]$ExecutionName,
        [string]$Region,
        [string]$Project,
        [int]$MaxTries = 80,
        [int]$SleepSeconds = 30
    )

    $tries = 0
    while ($tries -lt $MaxTries) {
        Start-Sleep -Seconds $SleepSeconds
        $tries++
        $state = Get-ExecutionState -ExecutionName $ExecutionName -Region $Region -Project $Project
        Write-Host "    $ExecutionName : Completed.status=$state (${tries}x${SleepSeconds}s)" -ForegroundColor DarkGray
        if ($state -in @("True", "False")) {
            return $state
        }
    }
    Write-Warning "Timeout attente $ExecutionName"
    return "Timeout"
}

function Send-CompletionEmail {
    param(
        [string]$Project,
        [string]$JobName,
        [int]$TotalLaunched,
        [int]$Succeeded,
        [int]$Failed,
        [int]$TimedOut,
        [double]$DurationMin
    )

    $apiKey = $env:ELASTICMAIL_API_KEY
    $recipient = $env:NOTIFICATION_EMAIL
    $sender = if ($env:SENDER_EMAIL) { $env:SENDER_EMAIL } else { "noreply@bmse.fr" }

    if ([string]::IsNullOrWhiteSpace($recipient)) {
        $recipient = "patrick.danto@confidensia.fr"
    }

    if ([string]::IsNullOrWhiteSpace($apiKey)) {
        try {
            $apiKey = (gcloud secrets versions access latest --secret=ELASTICMAIL_API_KEY --project=$Project 2>$null | Out-String).Trim()
        } catch {
            $apiKey = ""
        }
    }

    if ([string]::IsNullOrWhiteSpace($apiKey)) {
        Write-Warning "Email non envoyé: ELASTICMAIL_API_KEY indisponible"
        return $false
    }

    $subjectPrefix = if ($Failed -eq 0 -and $TimedOut -eq 0) { "✅" } else { "⚠️" }
    $subject = "$subjectPrefix Signaux V2 Passe B terminé - $Succeeded OK / $Failed KO / $TimedOut timeout"
    $body = @"
Campagne Signaux V2 Passe B terminée.

Projet: $Project
Job: $JobName
Durée: $([Math]::Round($DurationMin, 1)) min

Executions lancées: $TotalLaunched
Succès: $Succeeded
Échecs: $Failed
Timeout: $TimedOut

Suivi:
gcloud run jobs executions list --job=$JobName --region=europe-west1 --project=$Project --limit=30
"@

    try {
        $resp = Invoke-RestMethod -Method Post -Uri "https://api.elasticemail.com/v2/email/send" -Body @{
            apikey = $apiKey
            from = $sender
            to = $recipient
            subject = $subject
            bodyText = $body
        }
        Write-Host "Email de notification envoyé à $recipient" -ForegroundColor Green
        return $true
    } catch {
        Write-Warning "Erreur envoi email: $($_.Exception.Message)"
        return $false
    }
}

$startedAt = Get-Date

$NumBatches = [math]::Ceiling($TotalCandidates / $BatchSize)
$offsets = 0..($NumBatches - 1) | ForEach-Object { $StartOffset + ($_ * $BatchSize) }

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Signaux V2 Passe B - Lancement complet" -ForegroundColor Cyan
Write-Host "Batches: $NumBatches x $BatchSize  |  Mode: $(if ($Sequential) {'Séquentiel'} else {'Parallèle 2 à la fois'})" -ForegroundColor Cyan
Write-Host "StartOffset: $StartOffset" -ForegroundColor Cyan
Write-Host "ScopeFilterLLM: $ScopeFilterLLM" -ForegroundColor Cyan
Write-Host "Offsets: $($offsets -join ', ')" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

if ($DryRun) {
    Write-Host "[DryRun] Commandes qui seraient lancées :" -ForegroundColor DarkGray
    foreach ($offset in $offsets) {
        $argsPreview = "scripts/signaux_v2_passe_b.py,--batch-offset=$offset,--batch-size=$BatchSize"
        if ($ScopeFilterLLM) { $argsPreview += ",--scope-filter-llm" }
        Write-Host "  gcloud run jobs execute $JobName --args=$argsPreview" -ForegroundColor DarkGray
    }
    exit 0
}

$executions = @()
$executionStates = @{}

if ($Sequential) {
    foreach ($offset in $offsets) {
        Write-Host ""
        Write-Host "Batch offset=$offset ..." -ForegroundColor Yellow
        $jobArgs = "scripts/signaux_v2_passe_b.py,--batch-offset=$offset,--batch-size=$BatchSize"
        if ($ScopeFilterLLM) { $jobArgs += ",--scope-filter-llm" }
        $execName = gcloud run jobs execute $JobName `
            --region=$Region --project=$Project `
            --args="$jobArgs" `
            --wait `
            --format="value(metadata.name)"
        if ($LASTEXITCODE -ne 0) { throw "Echec lancement execution offset=$offset" }
        $executions += $execName
        $executionStates[$execName] = "Completed"
        Write-Host "  Terminé: $execName" -ForegroundColor Green
    }
} else {
    # Parallèle 2 à la fois — attend la paire avant de lancer la suivante
    for ($i = 0; $i -lt $offsets.Count; $i += 2) {
        $pair = $offsets[$i..([math]::Min($i + 1, $offsets.Count - 1))]
        $pairExecs = @()

        foreach ($offset in $pair) {
            Write-Host ""
            Write-Host "Lancement offset=$offset ..." -ForegroundColor Yellow
            $jobArgs = "scripts/signaux_v2_passe_b.py,--batch-offset=$offset,--batch-size=$BatchSize"
            if ($ScopeFilterLLM) { $jobArgs += ",--scope-filter-llm" }
            $execName = gcloud run jobs execute $JobName `
                --region=$Region --project=$Project `
                --args="$jobArgs" `
                --format="value(metadata.name)"
            if ($LASTEXITCODE -ne 0) { throw "Echec lancement execution offset=$offset" }
            $pairExecs += $execName
            $executions += $execName
            Write-Host "  Démarré: $execName" -ForegroundColor Green
            if ($pair.Count -gt 1) { Start-Sleep -Seconds 3 }
        }

        # Attendre la fin de la paire avant de continuer (y compris la dernière paire)
        Write-Host "  Attente de la fin des jobs courants..." -ForegroundColor DarkGray
        foreach ($ex in $pairExecs) {
            $executionStates[$ex] = Wait-Execution -ExecutionName $ex -Region $Region -Project $Project
        }
    }
}

if ($Sequential) {
    foreach ($ex in $executions) {
        if (-not $executionStates.ContainsKey($ex)) {
            $executionStates[$ex] = Get-ExecutionState -ExecutionName $ex -Region $Region -Project $Project
        }
    }
}

$succeeded = 0
$failed = 0
$timedOut = 0
foreach ($state in $executionStates.Values) {
    if ($state -eq "True") { $succeeded++ }
    elseif ($state -eq "False") { $failed++ }
    else { $timedOut++ }
}

$elapsed = ((Get-Date) - $startedAt).TotalMinutes
Send-CompletionEmail -Project $Project -JobName $JobName -TotalLaunched $executions.Count -Succeeded $succeeded -Failed $failed -TimedOut $timedOut -DurationMin $elapsed

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "$($executions.Count) batch(es) terminés/lancés | OK=$succeeded KO=$failed Timeout=$timedOut" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Suivi global:" -ForegroundColor Magenta
Write-Host "  gcloud run jobs executions list --job=$JobName --region=$Region --project=$Project --limit=20"
