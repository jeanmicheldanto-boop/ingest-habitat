$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$OUTDIR  = "/tmp/outputs"

# Union des departements avec reste (etablissements ou gestionnaires)
$DEPTS = @(
    "04","06","07","13","18","21","25","26","27","2B",
    "30","31","33","34","35","36","37","38","40","42",
    "44","45","47","49","51","54","56","57","59","61",
    "63","65","66","67","68","69","75","76","77","78",
    "79","81","83","84","86","90","91","92","93","94",
    "95","9D"
)

$argTokens = @("scripts/enrich_finess_dept.py", "--departements") + $DEPTS + @("--out-dir", $OUTDIR)
$argsStr = $argTokens -join ","

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  LANCEMENT BATCH RESTANTS (UNIQUE)" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ("Departements: " + ($DEPTS -join ",")) -ForegroundColor Yellow

# Update args du job

gcloud run jobs update finess-enrich-national `
    --region=$REGION --project=$PROJECT `
    --args="$argsStr" --quiet
if ($LASTEXITCODE -ne 0) {
    Write-Error "Echec update du job finess-enrich-national"
    exit 1
}

# Execute async + retourne execution ID
$execName = gcloud run jobs execute finess-enrich-national `
    --region=$REGION --project=$PROJECT `
    --async --format="value(metadata.name)"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Echec execution du job finess-enrich-national"
    exit 1
}

Write-Host "Execution lancee: $execName" -ForegroundColor Green
