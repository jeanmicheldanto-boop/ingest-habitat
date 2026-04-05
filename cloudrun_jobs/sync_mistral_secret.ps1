# Sync MISTRAL_API_KEY secret to Google Secret Manager
# Usage: .\sync_mistral_secret.ps1

param(
  [string]$EnvFile = ".\.env"
)

$ErrorActionPreference = 'Stop'

function Get-DotenvValue {
  param(
    [string]$Path,
    [string]$Name
  )

  if (-not (Test-Path $Path)) {
    throw "Env file not found: $Path"
  }

  $pattern = "^\s*" + [regex]::Escape($Name) + "\s*=\s*(.*)\s*$"
  $lines = Get-Content -LiteralPath $Path -ErrorAction Stop

  foreach ($line in $lines) {
    $lineStr = ""
    if ($null -ne $line) { $lineStr = [string]$line }
    $trimmed = $lineStr.Trim()
    if (-not $trimmed) { continue }
    if ($trimmed.StartsWith('#')) { continue }

    $m = [regex]::Match($lineStr, $pattern)
    if ($m.Success) {
      $v = ""
      if ($null -ne $m.Groups[1] -and $null -ne $m.Groups[1].Value) { $v = [string]$m.Groups[1].Value }
      $v = $v.Trim()

      # Remove surrounding quotes if present
      if (($v.StartsWith('"') -and $v.EndsWith('"')) -or ($v.StartsWith("'") -and $v.EndsWith("'"))) {
        $v = $v.Substring(1, $v.Length - 2)
      }

      # Remove BOM and other problematic characters
      $v = $v -replace '\ufeff', '' -replace '\x00', ''

      return $v
    }
  }

  return $null
}

Write-Output "Syncing MISTRAL_API_KEY secret from $EnvFile"

$name = "MISTRAL_API_KEY"
$value = Get-DotenvValue -Path $EnvFile -Name $name

if ([string]::IsNullOrWhiteSpace($value)) {
  throw "Missing or empty $name in $EnvFile"
}

# Ensure secret exists
gcloud secrets describe $name 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
  Write-Output "Creating secret $name..."
  gcloud secrets create $name --replication-policy="automatic" | Out-Null
}

# Add version
$tmp = New-TemporaryFile
try {
  Set-Content -LiteralPath $tmp -Value $value -NoNewline -Encoding utf8
  gcloud secrets versions add $name --data-file=$tmp | Out-Null
  Write-Output "OK: Secret $name updated successfully"
} finally {
  Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
}

Write-Output "Done."
