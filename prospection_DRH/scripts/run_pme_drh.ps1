param(
    [string]$PmeInputCsv = "data/pme_idf_50_500_pappers_large_nodup_v2.csv",
    [string]$SerperOutputCsv = "data/pme_idf_50_500_drh_serper_nodup1630.csv",
    [string]$SubsetWithEmailCsv = "data/pme_idf_50_500_drh_serper_nodup_with_email.csv",
    [string]$FinalOutputCsv = "data/mails_pme_FINAL.csv",
    [int]$Limit = 0,
    [int]$BatchSize = 100,
    [int]$PollSeconds = 8,
    [int]$MaxWaitSeconds = 900
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $root

$python = Join-Path $root ".venv/Scripts/python.exe"
if (-not (Test-Path $python)) {
    throw "Python introuvable: $python"
}

Write-Host "[pme] Etape 1/3 - Enrichissement DRH via Serper"
& $python "scripts/enrich_pme_drh_serper.py" `
    --input $PmeInputCsv `
    --output $SerperOutputCsv `
    --limit $Limit `
    --progress-every 20 `
    --flush-every 10

Write-Host "[pme] Etape 2/3 - Filtrage des lignes avec email"
& $python -c "import csv, pathlib; p=pathlib.Path(r'$SerperOutputCsv'); rows=list(csv.DictReader(open(p,encoding='utf-8-sig'))); keep=[r for r in rows if (r.get('drh_email_public') or '').strip() or (r.get('drh_email_reconstitue') or '').strip()]; out=pathlib.Path(r'$SubsetWithEmailCsv'); out.parent.mkdir(parents=True,exist_ok=True); f=open(out,'w',encoding='utf-8-sig',newline=''); w=csv.DictWriter(f,fieldnames=rows[0].keys() if rows else []); w.writeheader(); w.writerows(keep); f.close(); print(f'subset_with_email={len(keep)} output={out}')"

Write-Host "[pme] Etape 3/3 - Qualification Dropcontact"
& $python "scripts/enrich_pme_dropcontact.py" `
    --input $SubsetWithEmailCsv `
    --output $FinalOutputCsv `
    --limit $Limit `
    --batch-size $BatchSize `
    --poll-seconds $PollSeconds `
    --max-wait-seconds $MaxWaitSeconds

Write-Host "[pme] Termine: $FinalOutputCsv"
