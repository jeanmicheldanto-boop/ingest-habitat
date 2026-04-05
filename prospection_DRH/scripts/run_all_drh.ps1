param(
    [string]$CollectivitesInputCsv = "data/communes_idf.csv",
    [string]$CollectivitesOutputCsv = "data/mails_collectivites_FINAL.csv",
    [string]$PmeInputCsv = "data/pme_idf_50_500_pappers_large_nodup_v2.csv",
    [string]$PmeFinalOutputCsv = "data/mails_pme_FINAL.csv"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $root

Write-Host "[all] Pipeline collectivites DRH/DGS"
& ".\prospection_DRH\scripts\run_collectivites_drh_dgs.ps1" `
    -InputCsv $CollectivitesInputCsv `
    -OutputCsv $CollectivitesOutputCsv

Write-Host "[all] Pipeline PME DRH"
& ".\prospection_DRH\scripts\run_pme_drh.ps1" `
    -PmeInputCsv $PmeInputCsv `
    -FinalOutputCsv $PmeFinalOutputCsv

Write-Host "[all] Termine"
