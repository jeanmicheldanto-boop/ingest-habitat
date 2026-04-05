# Wrapper to run merge_complement_pdfs.py using the venv_pdf python
$venv = Join-Path $PSScriptRoot 'venv_pdf\Scripts\python.exe'
if (-Not (Test-Path $venv)) {
    Write-Error "venv_pdf python introuvable : $venv"
    exit 1
}
$script = Join-Path $PSScriptRoot 'merge_complement_pdfs.py'
& "$venv" "$script"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }