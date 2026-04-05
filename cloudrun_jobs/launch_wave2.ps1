# WAVE 2 : 14 depts non couverts par les jobs actifs
# Jobs actifs (ne pas toucher) :
#   Job A : 34-38, 40,42-45, 46-50, 56-60
#   Job B : 61-66, 67-70
#
# Non couverts avec todo etablissements > 0 :
#   06 (47), 13 (988), 17 (267), 23 (23), 24 (22), 28 (3), 29 (4)
#   33 (457), 39 (131), 41 (160), 91 (45), 92 (268), 94 (606), 95 (14)
#
# 4 batches, alternance Job A / Job B, equitable en charge :
#   Batch 1 Job A : 13, 23, 24         (~1033 etab)
#   Batch 2 Job B : 94, 91, 95         (~665 etab)
#   Batch 3 Job A : 33, 39, 41         (~748 etab)
#   Batch 4 Job B : 92, 17, 06, 28, 29 (~799 etab)
# => 4 jobs tournent en parallele

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$OUTDIR  = "/tmp/outputs"

# --- Batch 1 : Job A depts 13, 23, 24 ---
$batchesA = @(
    @("13","23","24"),
    @("33","39","41")
)

# --- Batch 2 : Job B depts 94, 91, 95 ---
$batchesB = @(
    @("94","91","95"),
    @("92","17","06","28","29")
)

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  WAVE 2 - JOB A (2 batches)"               -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

foreach ($batch in $batchesA) {
    $depts = $batch -join ","
    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $batch + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","

    Write-Host ""
    Write-Host "[UPDATE] Args -> depts $depts" -ForegroundColor Yellow
    gcloud run jobs update finess-enrich-national `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Echec update Job A pour $depts" -ForegroundColor Red
        exit 1
    }

    Write-Host "[LAUNCH] Execution async depts $depts" -ForegroundColor Cyan
    gcloud run jobs execute finess-enrich-national `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Echec lancement Job A pour $depts" -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK] Batch lance : $depts" -ForegroundColor Green
    Start-Sleep -Seconds 30
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Magenta
Write-Host "  WAVE 2 - JOB B (2 batches)"               -ForegroundColor Magenta
Write-Host "============================================" -ForegroundColor Magenta

foreach ($batch in $batchesB) {
    $depts = $batch -join ","
    $argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $batch + @("--out-dir", $OUTDIR)
    $argsStr = $argTokens -join ","

    Write-Host ""
    Write-Host "[UPDATE] Args -> depts $depts" -ForegroundColor Yellow
    gcloud run jobs update finess-enrich-national-b `
        --region=$REGION --project=$PROJECT `
        --args="$argsStr" --quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Echec update Job B pour $depts" -ForegroundColor Red
        exit 1
    }

    Write-Host "[LAUNCH] Execution async depts $depts" -ForegroundColor Magenta
    gcloud run jobs execute finess-enrich-national-b `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Echec lancement Job B pour $depts" -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK] Batch lance : $depts" -ForegroundColor Green
    Start-Sleep -Seconds 30
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  4 batches lances (14 depts)"              -ForegroundColor Yellow
Write-Host "  => 4 jobs tournent en parallele"          -ForegroundColor Yellow
Write-Host "  Verifier avec : python _check_status_reel.py" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Yellow
