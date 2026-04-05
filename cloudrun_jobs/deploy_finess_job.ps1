# Deploy FINESS enrichment job to Cloud Run
# Usage:
#   .\deploy_finess_job.ps1                    # Deploy test job (--limit 15)
#   .\deploy_finess_job.ps1 -Full              # Deploy full production job

param(
  [switch]$Full,
  [switch]$SkipBuild,
  [string]$Project = "gen-lang-client-0230548399",
  [string]$Region = "europe-west1"
)

$ErrorActionPreference = 'Stop'

Write-Output "==================================================================="
Write-Output "FINESS Cloud Run Job Deployment"
Write-Output "==================================================================="
Write-Output "  Project: $Project"
Write-Output "  Region: $Region"
Write-Output "  Mode: $(if ($Full) {'FULL (all dept 65)'} else {'TEST (limit 15)'})"
Write-Output "  Skip build: $SkipBuild"
Write-Output "==================================================================="
Write-Output ""

# Step 1: Build and push Docker image (unless skipped)
if (-not $SkipBuild) {
  Write-Output "[1/4] Building Docker image..."
  $imageName = "$Region-docker.pkg.dev/$Project/habitat/enrich:latest"
  
  docker build -f cloudrun_ref/Dockerfile -t $imageName .
  if ($LASTEXITCODE -ne 0) {
    throw "Docker build failed"
  }
  
  Write-Output "[2/4] Pushing image to Artifact Registry..."
  docker push $imageName
  if ($LASTEXITCODE -ne 0) {
    throw "Docker push failed"
  }
} else {
  Write-Output "[1-2/4] Skipping build (using existing image)"
}

# Step 2: Ensure MISTRAL_API_KEY secret exists
Write-Output "[3/4] Checking MISTRAL_API_KEY secret..."
gcloud secrets describe MISTRAL_API_KEY --project=$Project 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
  Write-Output "  WARNING: MISTRAL_API_KEY secret not found. Run: .\cloudrun_jobs\sync_mistral_secret.ps1"
  $continue = Read-Host "  Continue anyway? (y/n)"
  if ($continue -ne "y") {
    exit 1
  }
} else {
  Write-Output "  OK: MISTRAL_API_KEY secret exists"
}

# Step 3: Deploy job
Write-Output "[4/4] Deploying Cloud Run Job..."

if ($Full) {
  $jobFile = "cloudrun_jobs\finess_job_65.yaml"
  $jobName = "finess-enrich-65"
} else {
  $jobFile = "cloudrun_jobs\finess_job_65_test.yaml"
  $jobName = "finess-enrich-65-test"
}

gcloud run jobs replace $jobFile --region=$Region --project=$Project
if ($LASTEXITCODE -ne 0) {
  throw "Job deployment failed"
}

Write-Output ""
Write-Output "==================================================================="
Write-Output "OK: Deployment successful!"
Write-Output "==================================================================="
Write-Output ""
Write-Output "To execute the job:"
Write-Output "  gcloud run jobs execute $jobName --region=$Region --project=$Project --wait"
Write-Output ""
Write-Output "To view logs:"
Write-Output "  gcloud run jobs logs read $jobName --region=$Region --project=$Project --limit=100"
Write-Output ""
Write-Output "To filter structured logs by run_id:"
Write-Output '  gcloud logging read "resource.type=cloud_run_job AND jsonPayload.run_id=YYYYMMDD_HHMMSS" --limit=50 --format=json'
Write-Output ""
