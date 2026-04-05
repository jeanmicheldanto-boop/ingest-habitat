# launch_signaux_v2_g0_full.ps1
# Deploy + execute Signaux V2 G0 on Cloud Run over full base in batches.

param(
    [int]$TotalCandidates = 9169,
    [int]$BatchSize = 500,
    [int]$StartOffset = 0,
    [int]$MaxParallel = 2,
    [int]$MaxSerperResults = 8,
    [int]$MaxLaunchRetries = 3,
    [int]$MaxExecutionRetries = 1,
    [switch]$FireAndForget,
    [switch]$FireAndForgetEmail,
    [int]$SubmissionDelaySeconds = 2,
    [switch]$ForceRerun,
    [switch]$DisableLlmSnippetGate,
    [switch]$RobustMode,
    [switch]$Sequential,
    [switch]$DryRun,
    [switch]$SkipBuild,
    [switch]$SkipDeploy,
    [int]$TaskTimeoutSeconds = 86400,
    [string]$Project = "gen-lang-client-0230548399",
    [string]$Region = "europe-west1",
    [string]$JobName = "finess-signaux-v2-g0",
    [string]$ImageTag = "signaux-v2-g0",
    [switch]$MonitorOnly,
    [string]$MonitorPayloadPath = ""
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
$startedAt = Get-Date

if ($TaskTimeoutSeconds -lt 900 -or $TaskTimeoutSeconds -gt 86400) {
    throw "TaskTimeoutSeconds doit etre entre 900 et 86400"
}

$Image = "europe-west1-docker.pkg.dev/$Project/habitat/enrich:$ImageTag"

function Send-CompletionEmail {
    param(
        [string]$Project,
        [string]$JobName,
        [int]$TotalLaunched,
        [int]$Succeeded,
        [int]$Failed,
        [int]$TimedOut,
        [double]$DurationMin,
        [int]$MaxMailRetries = 3
    )

    $apiKey = $env:ELASTICMAIL_API_KEY
    $recipient = $env:NOTIFICATION_EMAIL
    $fromAddress = if ($env:SENDER_EMAIL) { $env:SENDER_EMAIL } else { "patrick.danto@bmse.fr" }

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
        Write-Warning "Email non envoye: ELASTICMAIL_API_KEY indisponible"
        return $false
    }

    $subjectPrefix = if ($Failed -eq 0 -and $TimedOut -eq 0) { "OK" } else { "ALERTE" }
    $subject = "[$subjectPrefix] Signaux V2 G0 termine - $Succeeded OK / $Failed KO / $TimedOut timeout"
    $body = @"
Campagne Signaux V2 G0 terminee.

Projet: $Project
Job: $JobName
Duree: $([Math]::Round($DurationMin, 1)) min

Executions lancees: $TotalLaunched
Succes: $Succeeded
Echecs: $Failed
Timeout: $TimedOut

Suivi:
gcloud run jobs executions list --job=$JobName --region=$Region --project=$Project --limit=30
"@

    for ($attempt = 1; $attempt -le $MaxMailRetries; $attempt++) {
        try {
            $resp = Invoke-RestMethod -Method Post -Uri "https://api.elasticemail.com/v2/email/send" -Body @{
                apikey   = $apiKey
                from     = $fromAddress
                fromName = "FINESS Pipeline"
                to       = $recipient
                subject  = $subject
                bodyText = $body
            }

            if ($resp -and ($resp.success -eq $true -or $resp.success -eq "true")) {
                Write-Host "Email de notification envoye a $recipient" -ForegroundColor Green
                return $true
            }

            Write-Warning "Tentative email ${attempt}/${MaxMailRetries}: reponse sans succes explicite"
        } catch {
            Write-Warning "Tentative email ${attempt}/${MaxMailRetries} en erreur: $($_.Exception.Message)"
        }

        if ($attempt -lt $MaxMailRetries) {
            Start-Sleep -Seconds ([Math]::Min(60, 5 * $attempt))
        }
    }

    # Fallback: API v4 with header auth (already used elsewhere in this workspace).
    for ($attempt = 1; $attempt -le $MaxMailRetries; $attempt++) {
        try {
            $payload = @{
                Recipients = @{ To = @($recipient) }
                Content = @{
                    From = $fromAddress
                    Subject = $subject
                    Body = @(
                        @{
                            ContentType = "PlainText"
                            Content = $body
                        }
                    )
                }
            } | ConvertTo-Json -Depth 8

            $resp = Invoke-RestMethod -Method Post -Uri "https://api.elasticemail.com/v4/emails/transactional" `
                -Headers @{ "X-ElasticEmail-ApiKey" = $apiKey; "Content-Type" = "application/json" } `
                -Body $payload

            if ($resp) {
                Write-Host "Email de notification envoye a $recipient (fallback v4)" -ForegroundColor Green
                return $true
            }

            Write-Warning "Tentative email v4 ${attempt}/${MaxMailRetries}: reponse vide"
        } catch {
            Write-Warning "Tentative email v4 ${attempt}/${MaxMailRetries} en erreur: $($_.Exception.Message)"
        }

        if ($attempt -lt $MaxMailRetries) {
            Start-Sleep -Seconds ([Math]::Min(60, 5 * $attempt))
        }
    }

    Write-Warning "Email non envoye apres $MaxMailRetries tentatives"
    return $false
}

function Get-RemainingG0Candidates {
    param(
        [string]$PythonExe,
        [switch]$ForceRerun
    )

    $rerunSql = "AND COALESCE(g.signal_v2_phase, '') NOT IN ('G0', 'G1', 'G2')"
    if ($ForceRerun) {
        $rerunSql = ""
    }

    $code = @"
import psycopg2.extras
from database import DatabaseManager

db = DatabaseManager()
with db.get_connection() as conn:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute('''
            SELECT COUNT(*) AS n
            FROM public.finess_gestionnaire g
            WHERE EXISTS (
                SELECT 1
                FROM public.finess_etablissement e
                WHERE e.id_gestionnaire = g.id_gestionnaire
                  AND e.categorie_normalisee IS DISTINCT FROM 'SAA'
            )
              AND COALESCE(g.signal_v2_methode, '') <> 'serper_passe_b'
              AND NOT (COALESCE(g.signal_financier, FALSE) OR COALESCE(g.signal_rh, FALSE)
                       OR COALESCE(g.signal_qualite, FALSE) OR COALESCE(g.signal_juridique, FALSE))
              $rerunSql
        ''')
        print(int((cur.fetchone() or {}).get('n', 0)))
"@

    $result = & $PythonExe -c $code 2>$null | Out-String
    if ($LASTEXITCODE -ne 0) {
        throw "Impossible de calculer le restant G0 en base"
    }

    $txt = ($result | Out-String).Trim()
    $n = 0
    if (-not [int]::TryParse($txt, [ref]$n)) {
        throw "Valeur restante invalide: '$txt'"
    }
    return $n
}

function Start-ExecutionWithRetry {
    param(
        [int]$Offset,
        [string]$JobArgs,
        [int]$MaxRetries,
        [string]$Region,
        [string]$Project,
        [string]$JobName
    )

    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        Write-Host "Lancement offset=$Offset tentative=$attempt/$MaxRetries" -ForegroundColor Yellow
        $execName = gcloud run jobs execute $JobName `
            --region=$Region --project=$Project `
            --args=$JobArgs `
            --format="value(metadata.name)"

        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($execName)) {
            Write-Host "  Demarre: $execName" -ForegroundColor Green
            return $execName.Trim()
        }

        Write-Warning "Echec lancement offset=$Offset tentative=$attempt/$MaxRetries"
        if ($attempt -lt $MaxRetries) {
            Start-Sleep -Seconds ([Math]::Min(45, 5 * $attempt))
        }
    }

    throw "Echec lancement execution offset=$Offset apres $MaxRetries tentatives"
}

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
        [int]$MaxTries = 240,
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

function Start-FireAndForgetMonitor {
    param(
        [string]$PayloadPath
    )

    if ([string]::IsNullOrWhiteSpace($PayloadPath)) {
        throw "Payload monitor manquant"
    }

    $scriptPath = $PSCommandPath
    if ([string]::IsNullOrWhiteSpace($scriptPath)) {
        throw "Impossible de determiner le chemin du launcher"
    }

    $argList = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $scriptPath,
        "-MonitorOnly",
        "-MonitorPayloadPath", $PayloadPath
    )

    $proc = Start-Process -FilePath "powershell.exe" -ArgumentList $argList -PassThru -WindowStyle Hidden
    Write-Host "Monitor detache demarre (PID=$($proc.Id))" -ForegroundColor Green
}

if ($MonitorOnly) {
    if ([string]::IsNullOrWhiteSpace($MonitorPayloadPath) -or -not (Test-Path $MonitorPayloadPath)) {
        throw "MonitorPayloadPath invalide: $MonitorPayloadPath"
    }

    $payload = Get-Content -Raw $MonitorPayloadPath | ConvertFrom-Json
    $mProject = [string]$payload.project
    $mRegion = [string]$payload.region
    $mJobName = [string]$payload.job_name
    $mStartedAt = Get-Date ([string]$payload.started_at)
    $mExecutionNames = @($payload.execution_names)
    $mMaxTries = [int]$payload.max_tries
    if ($mMaxTries -lt 1) { $mMaxTries = 2880 }

    Write-Host "[Monitor] Start | project=$mProject region=$mRegion job=$mJobName executions=$($mExecutionNames.Count)" -ForegroundColor Cyan

    $succeeded = 0
    $failed = 0
    $timedOut = 0
    foreach ($ex in $mExecutionNames) {
        $state = Wait-Execution -ExecutionName ([string]$ex) -Region $mRegion -Project $mProject -MaxTries $mMaxTries -SleepSeconds 30
        if ($state -eq "True") { $succeeded++ }
        elseif ($state -eq "False") { $failed++ }
        else { $timedOut++ }
    }

    $durationMin = ((Get-Date) - $mStartedAt).TotalMinutes
    Send-CompletionEmail -Project $mProject -JobName $mJobName -TotalLaunched $mExecutionNames.Count -Succeeded $succeeded -Failed $failed -TimedOut $timedOut -DurationMin $durationMin | Out-Null

    Write-Host "[Monitor] Termine | OK=$succeeded KO=$failed Timeout=$timedOut" -ForegroundColor Green
    exit 0
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "Signaux V2 G0 - Cloud Run full base" -ForegroundColor Cyan
Write-Host "Project=$Project Region=$Region Job=$JobName" -ForegroundColor Cyan
Write-Host "TotalCandidates=$TotalCandidates BatchSize=$BatchSize StartOffset=$StartOffset" -ForegroundColor Cyan
Write-Host "MaxParallel=$MaxParallel Sequential=$Sequential" -ForegroundColor Cyan
Write-Host "MaxLaunchRetries=$MaxLaunchRetries MaxExecutionRetries=$MaxExecutionRetries" -ForegroundColor Cyan
Write-Host "FireAndForget=$FireAndForget SubmissionDelaySeconds=$SubmissionDelaySeconds" -ForegroundColor Cyan
Write-Host "FireAndForgetEmail=$FireAndForgetEmail" -ForegroundColor Cyan
Write-Host "TaskTimeoutSeconds=$TaskTimeoutSeconds ForceRerun=$ForceRerun DisableLlmSnippetGate=$DisableLlmSnippetGate" -ForegroundColor Cyan
Write-Host "RobustMode=$RobustMode" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

if ($RobustMode -and $ForceRerun) {
    throw "RobustMode n'est pas compatible avec ForceRerun (risque de boucle infinie)"
}

if (-not $SkipBuild) {
    Write-Host "[1/3] Build + push image: $Image" -ForegroundColor Yellow
    docker build -t $Image .
    if ($LASTEXITCODE -ne 0) { throw "docker build failed (exit $LASTEXITCODE)" }
    docker push $Image
    if ($LASTEXITCODE -ne 0) { throw "docker push failed (exit $LASTEXITCODE)" }
} else {
    Write-Host "[1/3] Build skipped" -ForegroundColor DarkGray
}

$bootstrapArgs = "scripts/signaux_v2_g0_discovery.py,--batch-offset=0,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults"
if ($ForceRerun) { $bootstrapArgs += ",--force-rerun" }
if ($DisableLlmSnippetGate) { $bootstrapArgs += ",--disable-llm-snippet-gate" }
if ($DryRun) { $bootstrapArgs += ",--dry-run" }

if (-not $SkipDeploy) {
    Write-Host "[2/3] Deploy/update Cloud Run job" -ForegroundColor Yellow
    $envVars = "PYTHONUNBUFFERED=1,LLM_PROVIDER=gemini,GEMINI_MODEL=gemini-2.0-flash,DB_HOST=db.minwoumfgutampcgrcbr.supabase.co,DB_NAME=postgres,DB_USER=postgres,DB_PORT=5432"
    $secrets = "DB_PASSWORD=DB_PASSWORD:latest,SERPER_API_KEY=SERPER_API_KEY:latest,GEMINI_API_KEY=GEMINI_API_KEY:latest,MISTRAL_API_KEY=MISTRAL_API_KEY:latest"

    gcloud run jobs deploy $JobName `
        --region=$Region `
        --project=$Project `
        --image=$Image `
        --command=python `
        --args=$bootstrapArgs `
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

$NumBatches = [math]::Ceiling($TotalCandidates / $BatchSize)
$offsets = 0..($NumBatches - 1) | ForEach-Object { $StartOffset + ($_ * $BatchSize) }
$pythonExe = "c:/Users/Lenovo/ingest-habitat/.venv/Scripts/python.exe"

if ($DryRun) {
    Write-Host "[3/3] DryRun mode - commandes prévues:" -ForegroundColor DarkGray
    if ($RobustMode) {
        Write-Host "  RobustMode: boucle dynamique en --batch-offset=0 jusqu'a epuisement" -ForegroundColor DarkGray
        $cmd = "gcloud run jobs execute $JobName --region=$Region --project=$Project --args=scripts/signaux_v2_g0_discovery.py,--batch-offset=0,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults"
        Write-Host "  $cmd" -ForegroundColor DarkGray
    }
    foreach ($offset in $offsets) {
        $runId = "g0_full_{0}_{1}" -f (Get-Date -Format "yyyyMMddHHmmss"), $offset
        $jobArgs = "scripts/signaux_v2_g0_discovery.py,--batch-offset=$offset,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults,--run-id=$runId"
        if ($ForceRerun) { $jobArgs += ",--force-rerun" }
        if ($DisableLlmSnippetGate) { $jobArgs += ",--disable-llm-snippet-gate" }
        $cmd = "gcloud run jobs execute $JobName --region=$Region --project=$Project --args=$jobArgs"
        Write-Host "  $cmd" -ForegroundColor DarkGray
    }
    exit 0
}

if ($FireAndForget) {
    Write-Host "[3/3] Fire-and-forget: soumission des batches sans attente de fin" -ForegroundColor Yellow
    $submitted = @()

    if ($Sequential -or $MaxParallel -le 1) {
        foreach ($offset in $offsets) {
            $runId = "g0_full_{0}_{1}_ff" -f (Get-Date -Format "yyyyMMddHHmmss"), $offset
            $jobArgs = "scripts/signaux_v2_g0_discovery.py,--batch-offset=$offset,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults,--run-id=$runId"
            if ($ForceRerun) { $jobArgs += ",--force-rerun" }
            if ($DisableLlmSnippetGate) { $jobArgs += ",--disable-llm-snippet-gate" }

            $execName = Start-ExecutionWithRetry -Offset $offset -JobArgs $jobArgs -MaxRetries $MaxLaunchRetries -Region $Region -Project $Project -JobName $JobName
            $submitted += [pscustomobject]@{ offset = $offset; execution = $execName; run_id = $runId }
            if ($SubmissionDelaySeconds -gt 0) {
                Start-Sleep -Seconds $SubmissionDelaySeconds
            }
        }
    } else {
        foreach ($offset in $offsets) {
            $runId = "g0_full_{0}_{1}_ff" -f (Get-Date -Format "yyyyMMddHHmmss"), $offset
            $jobArgs = "scripts/signaux_v2_g0_discovery.py,--batch-offset=$offset,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults,--run-id=$runId"
            if ($ForceRerun) { $jobArgs += ",--force-rerun" }
            if ($DisableLlmSnippetGate) { $jobArgs += ",--disable-llm-snippet-gate" }

            $execName = Start-ExecutionWithRetry -Offset $offset -JobArgs $jobArgs -MaxRetries $MaxLaunchRetries -Region $Region -Project $Project -JobName $JobName
            $submitted += [pscustomobject]@{ offset = $offset; execution = $execName; run_id = $runId }
            if ($SubmissionDelaySeconds -gt 0) {
                Start-Sleep -Seconds $SubmissionDelaySeconds
            }
        }
    }

    $elapsed = ((Get-Date) - $startedAt).TotalMinutes
    Write-Host "" 
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host "Soumission terminee (fire-and-forget): $($submitted.Count) execution(s) lancee(s) en $([Math]::Round($elapsed,1)) min" -ForegroundColor Green
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host "Suivi:" -ForegroundColor Magenta
    Write-Host "  gcloud run jobs executions list --job=$JobName --region=$Region --project=$Project --limit=30"
    Write-Host "" 

    if ($FireAndForgetEmail) {
        $logsDir = Join-Path (Get-Location) "logs"
        if (-not (Test-Path $logsDir)) {
            New-Item -ItemType Directory -Path $logsDir | Out-Null
        }

        $payloadPath = Join-Path $logsDir ("ff_monitor_payload_{0}.json" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
        $maxTries = [int][Math]::Ceiling(($TaskTimeoutSeconds + 600) / 30.0)
        $payload = @{
            project = $Project
            region = $Region
            job_name = $JobName
            started_at = $startedAt.ToString("o")
            execution_names = @($submitted | ForEach-Object { $_.execution })
            max_tries = $maxTries
        }
        $payload | ConvertTo-Json -Depth 5 | Set-Content -Path $payloadPath -Encoding UTF8

        Start-FireAndForgetMonitor -PayloadPath $payloadPath
        Write-Host "Mode fire-and-forget + email actif (monitor detache)." -ForegroundColor Green
        Write-Host "Payload monitor: $payloadPath" -ForegroundColor DarkGray
    } else {
        Write-Host "Note: en mode fire-and-forget sans FireAndForgetEmail, pas d'email de fin." -ForegroundColor Yellow
    }

    exit 0
}

Write-Host "[3/3] Execute batches" -ForegroundColor Yellow
$executions = @()
$executionStates = @{}
$offsetFinalState = @{}

if ($RobustMode) {
    $iteration = 0
    $succeeded = 0
    $failed = 0
    $timedOut = 0
    $stagnationCount = 0
    $lastRemaining = -1

    while ($true) {
        $remaining = Get-RemainingG0Candidates -PythonExe $pythonExe -ForceRerun:$ForceRerun
        Write-Host "[Robust] Restant G0 eligible: $remaining" -ForegroundColor Yellow
        if ($remaining -le 0) { break }

        if ($remaining -eq $lastRemaining) {
            $stagnationCount++
        } else {
            $stagnationCount = 0
        }
        $lastRemaining = $remaining

        if ($stagnationCount -ge 3) {
            Write-Warning "[Robust] Stagnation detectee ($remaining restants), arret de securite"
            break
        }

        $iteration++
        $runId = "g0_robust_{0}_{1}" -f (Get-Date -Format "yyyyMMddHHmmss"), $iteration
        $jobArgs = "scripts/signaux_v2_g0_discovery.py,--batch-offset=0,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults,--run-id=$runId"
        if ($DisableLlmSnippetGate) { $jobArgs += ",--disable-llm-snippet-gate" }

        $finalState = "False"
        for ($attempt = 0; $attempt -le $MaxExecutionRetries; $attempt++) {
            $execName = Start-ExecutionWithRetry -Offset 0 -JobArgs $jobArgs -MaxRetries $MaxLaunchRetries -Region $Region -Project $Project -JobName $JobName
            $executions += $execName
            Write-Host "  [Robust] Attente execution=$execName" -ForegroundColor DarkGray
            $state = Wait-Execution -ExecutionName $execName -Region $Region -Project $Project
            $executionStates[$execName] = $state

            if ($state -eq "True") {
                $finalState = "True"
                break
            }

            if ($attempt -lt $MaxExecutionRetries) {
                Write-Warning "[Robust] execution en echec ($state), relance tentative $(($attempt + 2))/$($MaxExecutionRetries + 1)"
            }
        }

        if ($finalState -eq "True") { $succeeded++ }
        elseif ($finalState -eq "False") { $failed++ }
        else { $timedOut++ }
    }

    $elapsed = ((Get-Date) - $startedAt).TotalMinutes
    Send-CompletionEmail -Project $Project -JobName $JobName -TotalLaunched $executions.Count -Succeeded $succeeded -Failed $failed -TimedOut $timedOut -DurationMin $elapsed

    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host "Robust mode termine | executions lancees=$($executions.Count) | OK=$succeeded KO=$failed Timeout=$timedOut" -ForegroundColor Green
    Write-Host "============================================================" -ForegroundColor Green
    Write-Host "Suivi:" -ForegroundColor Magenta
    Write-Host "  gcloud run jobs executions list --job=$JobName --region=$Region --project=$Project --limit=30"
    exit 0
}

if ($Sequential) {
    foreach ($offset in $offsets) {
        $finalState = "False"
        for ($attempt = 0; $attempt -le $MaxExecutionRetries; $attempt++) {
            $runId = "g0_full_{0}_{1}_r{2}" -f (Get-Date -Format "yyyyMMddHHmmss"), $offset, $attempt
            $jobArgs = "scripts/signaux_v2_g0_discovery.py,--batch-offset=$offset,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults,--run-id=$runId"
            if ($ForceRerun) { $jobArgs += ",--force-rerun" }
            if ($DisableLlmSnippetGate) { $jobArgs += ",--disable-llm-snippet-gate" }

            $execName = Start-ExecutionWithRetry -Offset $offset -JobArgs $jobArgs -MaxRetries $MaxLaunchRetries -Region $Region -Project $Project -JobName $JobName
            $executions += $execName

            Write-Host "  Attente execution=$execName" -ForegroundColor DarkGray
            $state = Wait-Execution -ExecutionName $execName -Region $Region -Project $Project
            $executionStates[$execName] = $state
            if ($state -eq "True") {
                $finalState = "True"
                break
            }

            if ($attempt -lt $MaxExecutionRetries) {
                Write-Warning "offset=$offset en echec ($state), relance tentative $(($attempt + 2))/$($MaxExecutionRetries + 1)"
            }
        }
        $offsetFinalState[$offset] = $finalState
    }
} else {
    if ($MaxParallel -lt 1) { $MaxParallel = 1 }
    for ($i = 0; $i -lt $offsets.Count; $i += $MaxParallel) {
        $last = [math]::Min($i + $MaxParallel - 1, $offsets.Count - 1)
        $group = $offsets[$i..$last]
        $groupExecByOffset = @{}

        foreach ($offset in $group) {
            $runId = "g0_full_{0}_{1}_r0" -f (Get-Date -Format "yyyyMMddHHmmss"), $offset
            $jobArgs = "scripts/signaux_v2_g0_discovery.py,--batch-offset=$offset,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults,--run-id=$runId"
            if ($ForceRerun) { $jobArgs += ",--force-rerun" }
            if ($DisableLlmSnippetGate) { $jobArgs += ",--disable-llm-snippet-gate" }

            $execName = Start-ExecutionWithRetry -Offset $offset -JobArgs $jobArgs -MaxRetries $MaxLaunchRetries -Region $Region -Project $Project -JobName $JobName
            $groupExecByOffset[$offset] = $execName
            $executions += $execName
            Start-Sleep -Seconds 2
        }

        Write-Host "  Attente des executions du groupe..." -ForegroundColor DarkGray
        foreach ($offset in $group) {
            $ex = $groupExecByOffset[$offset]
            $state = Wait-Execution -ExecutionName $ex -Region $Region -Project $Project
            $executionStates[$ex] = $state
            $offsetFinalState[$offset] = $state
        }

        # Retry failed/timeouts for this group before launching next group.
        for ($retry = 1; $retry -le $MaxExecutionRetries; $retry++) {
            $retryOffsets = @()
            foreach ($offset in $group) {
                if (-not $offsetFinalState.ContainsKey($offset) -or $offsetFinalState[$offset] -ne "True") {
                    $retryOffsets += $offset
                }
            }

            if ($retryOffsets.Count -eq 0) {
                break
            }

            Write-Warning "Relance groupe: tentative=$retry offsets=$($retryOffsets -join ',')"
            foreach ($offset in $retryOffsets) {
                $runId = "g0_full_{0}_{1}_r{2}" -f (Get-Date -Format "yyyyMMddHHmmss"), $offset, $retry
                $jobArgs = "scripts/signaux_v2_g0_discovery.py,--batch-offset=$offset,--batch-size=$BatchSize,--max-serper-results=$MaxSerperResults,--run-id=$runId"
                if ($ForceRerun) { $jobArgs += ",--force-rerun" }
                if ($DisableLlmSnippetGate) { $jobArgs += ",--disable-llm-snippet-gate" }

                $execName = Start-ExecutionWithRetry -Offset $offset -JobArgs $jobArgs -MaxRetries $MaxLaunchRetries -Region $Region -Project $Project -JobName $JobName
                $executions += $execName
                $state = Wait-Execution -ExecutionName $execName -Region $Region -Project $Project
                $executionStates[$execName] = $state
                $offsetFinalState[$offset] = $state
            }
        }

        foreach ($offset in $group) {
            if (-not $offsetFinalState.ContainsKey($offset)) {
                $offsetFinalState[$offset] = "False"
            }
        }
    }
}

$succeeded = 0
$failed = 0
$timedOut = 0
foreach ($offset in $offsets) {
    if (-not $offsetFinalState.ContainsKey($offset)) {
        $offsetFinalState[$offset] = "Timeout"
    }
    $state = $offsetFinalState[$offset]
    if ($state -eq "True") { $succeeded++ }
    elseif ($state -eq "False") { $failed++ }
    else { $timedOut++ }
}

$elapsed = ((Get-Date) - $startedAt).TotalMinutes
Send-CompletionEmail -Project $Project -JobName $JobName -TotalLaunched $executions.Count -Succeeded $succeeded -Failed $failed -TimedOut $timedOut -DurationMin $elapsed

Write-Host "" 
Write-Host "============================================================" -ForegroundColor Green
Write-Host "Batches cibles=$($offsets.Count) | executions lancees=$($executions.Count) | OK=$succeeded KO=$failed Timeout=$timedOut" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host "Suivi:" -ForegroundColor Magenta
Write-Host "  gcloud run jobs executions list --job=$JobName --region=$Region --project=$Project --limit=30"
