param(
    [string]$InputCsv = "data/communes_idf.csv",
    [string]$OutputCsv = "data/mails_collectivites_FINAL.csv",
    [int]$Limit = 0,
    [int]$Offset = 0,
    [int]$MinPopulation = 2000,
    [switch]$EnableGenericRhSerper,
    [string]$LogLevel = "INFO"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $root

$python = Join-Path $root ".venv/Scripts/python.exe"
if (-not (Test-Path $python)) {
    throw "Python introuvable: $python"
}

$args = @(
    "scripts/enrich_communes_idf_contacts.py",
    "--input", $InputCsv,
    "--output", $OutputCsv,
    "--limit", "$Limit",
    "--offset", "$Offset",
    "--flush-every", "20",
    "--progress-every", "1",
    "--min-population", "$MinPopulation",
    "--log-level", $LogLevel
)

if ($EnableGenericRhSerper) {
    $args += "--enable-generic-rh-serper"
}

Write-Host "[collectivites] Lancement enrich_communes_idf_contacts.py"
& $python @args

Write-Host "[collectivites] Termine: $OutputCsv"
