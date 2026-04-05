param(
    [string]$RepoPath = "."
)

$ErrorActionPreference = "Stop"

function Write-Section($title) {
    Write-Host ""
    Write-Host "=== $title ==="
}

$repo = Resolve-Path $RepoPath

Write-Section "Repository"
$inside = git -C $repo rev-parse --is-inside-work-tree 2>$null
if ($LASTEXITCODE -ne 0 -or $inside -ne "true") {
    Write-Error "Not a git repository: $repo"
    exit 2
}
Write-Host "Repo: $repo"

$failed = $false

Write-Section "Tracked secret files"
$trackedEnv = git -C $repo ls-files ".env" ".env.local" ".env.*"
$trackedEnv = $trackedEnv | Where-Object { $_ -and $_ -notmatch "\.env\.example$" }
if ($trackedEnv) {
    $failed = $true
    Write-Host "ERROR: Secret env files are tracked:" -ForegroundColor Red
    $trackedEnv | ForEach-Object { Write-Host " - $_" }
} else {
    Write-Host "OK: no tracked secret env files"
}

Write-Section "Tracked cache/build artifacts"
$trackedArtifacts = @()
$trackedArtifacts += git -C $repo ls-files "*.pyc"
$trackedArtifacts += git -C $repo ls-files "pipeline_*.csv" "pipeline_complet_*.csv" "data_*.csv" "test_*.csv"
$trackedArtifacts += git -C $repo ls-files "latest_check.txt" "temp_*.json" "temp_*.txt"
$trackedArtifacts += git -C $repo ls-files "data/_tmp*" "data/cache_*/*" "data/cache_*/*/*"
$trackedArtifacts = $trackedArtifacts | Where-Object { $_ }
if ($trackedArtifacts) {
    $failed = $true
    Write-Host "ERROR: generated artifacts are tracked:" -ForegroundColor Red
    $trackedArtifacts | Select-Object -First 50 | ForEach-Object { Write-Host " - $_" }
    if ($trackedArtifacts.Count -gt 50) {
        Write-Host " ... and $($trackedArtifacts.Count - 50) more"
    }
} else {
    Write-Host "OK: no tracked generated artifacts"
}

Write-Section "Token pattern scan (tracked files)"
$patterns = @(
    "ghp_[A-Za-z0-9]{20,}",
    "AIza[0-9A-Za-z_-]{20,}",
    "sk-proj-[A-Za-z0-9_-]{20,}",
    "sk-[A-Za-z0-9]{20,}",
    "gsk_[A-Za-z0-9]{20,}",
    "xox[baprs]-[A-Za-z0-9-]{10,}"
)
$hits = @()
foreach ($p in $patterns) {
    $r = git -C $repo grep -nE $p 2>$null
    if ($LASTEXITCODE -eq 0 -and $r) {
        $hits += $r
    }
}
if ($hits) {
    $failed = $true
    Write-Host "ERROR: potential tokens detected:" -ForegroundColor Red
    $hits | Select-Object -Unique | ForEach-Object { Write-Host " - $_" }
} else {
    Write-Host "OK: no obvious token pattern found"
}

Write-Section "Result"
if ($failed) {
    Write-Host "FAILED: fix findings before push." -ForegroundColor Red
    exit 1
}

Write-Host "PASS: repository looks safe to push." -ForegroundColor Green
exit 0
