# Deployer et executer les 10 jobs
$jobs = @()
for ($i = 1; $i -le 10; $i++) {
    $idx = $i.ToString().PadLeft(2, '0')
    Write-Host "Deploiement job $idx..." -NoNewline
    gcloud run jobs replace cloudrun_jobs/job_$idx.yaml --region europe-west1 --project gen-lang-client-0230548399 2>$null
    Write-Host " OK" -ForegroundColor Green
}

Write-Host "`nLancement des 10 jobs en parallele..."
for ($i = 1; $i -le 10; $i++) {
    $idx = $i.ToString().PadLeft(2, '0')
    gcloud run jobs execute habitat-enrich-$idx --region europe-west1 --project gen-lang-client-0230548399 --async 2>$null
    Write-Host "Job $idx lance" -ForegroundColor Cyan
}
