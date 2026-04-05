# Relance finale en parallele (5 jobs) avec depts restants non-SAA
# Source de la liste: _check_status_reel.py
# Depts restants: 23,42,43,44,45,50,56,57,58,59,60,62,63,64,66,69,70
#
# Important: on garde EXACTEMENT le format d'args de l'overnight:
# scripts/enrich_finess_dept.py,--departements,<d1>,<d2>,...,--out-dir,/tmp/outputs

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$OUTDIR  = "/tmp/outputs"
$STAGGER_SECONDS = 45

# 5 batches equilibres en charge (todo etablissements)
# B1 ~1590 : 59,62,23
# B2 ~1453 : 44,57,50
# B3 ~1156 : 56,60,70
# B4  ~912 : 64,63
# B5 ~1500 : 45,66,43,58,69,42
$batches = @(
    @{ job = "finess-enrich-national";   depts = @("59","62","23") },
    @{ job = "finess-enrich-national-b"; depts = @("44","57","50") },
    @{ job = "finess-enrich-national";   depts = @("56","60","70") },
    @{ job = "finess-enrich-national-b"; depts = @("64","63") },
    @{ job = "finess-enrich-national";   depts = @("45","66","43","58","69","42") }
)

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  RELANCE FINALE PARALLELE (5 JOBS)"        -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

$idx = 0
foreach ($b in $batches) {
    $idx += 1
    $job = $b.job
    $batch = $b.depts
    $depts = $batch -join ","

    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $batch + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","

    Write-Host ""
    Write-Host "[$idx/5] [UPDATE] $job -> depts $depts" -ForegroundColor Yellow
    gcloud run jobs update $job `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Echec update $job pour $depts" -ForegroundColor Red
        exit 1
    }

    Write-Host "[$idx/5] [LAUNCH] $job async -> $depts" -ForegroundColor Green
    gcloud run jobs execute $job `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Echec execution $job pour $depts" -ForegroundColor Red
        exit 1
    }

    if ($idx -lt $batches.Count) {
        Write-Host "[WAIT] $STAGGER_SECONDS s pour limiter la contention DB au demarrage" -ForegroundColor DarkGray
        Start-Sleep -Seconds $STAGGER_SECONDS
    }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  5 jobs lances (departements restants)"    -ForegroundColor Yellow
Write-Host "  Verifier avec :"                           -ForegroundColor Yellow
Write-Host "  .venv\Scripts\python.exe _check_status_reel.py" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Yellow
