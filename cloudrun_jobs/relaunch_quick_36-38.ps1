#!/usr/bin/env pwsh
# Lancement rapide du premier batch de relance
# Départements 36-37-38 (Job A)

$PROJECT = "gen-lang-client-0230548399"
$REGION  = "europe-west1"
$JOB_A   = "finess-enrich-national"

Write-Host "🚀 Lancement batch depts 36-37-38..." -ForegroundColor Cyan

gcloud run jobs execute $JOB_A `
    --region=$REGION `
    --project=$PROJECT `
    --args="--departements=36,37,38"

Write-Host "✓ Batch lancé!" -ForegroundColor Green
Write-Host "Vérifier l'exécution: gcloud run jobs executions list --job=$JOB_A --region=$REGION --project=$PROJECT --limit=5"
