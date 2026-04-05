# Relance des 29 departements sans donnees etablissements
# Cause de l'echec : --args passait --departements directement a python au lieu du script
# Fix : gcloud run jobs update --args (avec chemin script) puis execute (sans --args)
#
# Departements manquants (0 etablissements enrichis) :
#   Job A : 34-38, 40,42-45, 46-50, 56-60
#   Job B : 61-64,66, 67-70

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$OUTDIR  = "/tmp/outputs"

# --- JOB A : 4 batches (20 depts) ---
$batchesA = @(
    @("34","35","36","37","38"),
    @("40","42","43","44","45"),
    @("46","47","48","49","50"),
    @("56","57","58","59","60")
)

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  RELANCE JOB A (20 depts manquants)"       -ForegroundColor Cyan
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

    Write-Host "[LAUNCH] Execution depts $depts" -ForegroundColor Cyan
    gcloud run jobs execute finess-enrich-national `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1

    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Echec lancement Job A pour $depts" -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK] Batch $depts lance" -ForegroundColor Green
    Start-Sleep -Seconds 30
}

# --- JOB B : 2 batches (9 depts) ---
$batchesB = @(
    @("61","62","63","64","66"),
    @("67","68","69","70")
)

Write-Host ""
Write-Host "============================================" -ForegroundColor Magenta
Write-Host "  RELANCE JOB B (9 depts manquants)"        -ForegroundColor Magenta
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

    Write-Host "[LAUNCH] Execution depts $depts" -ForegroundColor Magenta
    gcloud run jobs execute finess-enrich-national-b `
        --region=$REGION --project=$PROJECT `
        --async --quiet 2>&1

    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Echec lancement Job B pour $depts" -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK] Batch $depts lance" -ForegroundColor Green
    Start-Sleep -Seconds 30
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Yellow
Write-Host "  6 batches lances (29 departements)"       -ForegroundColor Yellow
Write-Host "  Verifier avec : python _check_all_depts.py" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Yellow
