# Script pour lancer 10 executions paralleles du job Cloud Run
# Toutes sur le departement 60, limit 5 chacune

$region = "europe-west1"
$jobName = "habitat-enrich-test"
$parallelCount = 10

Write-Host "Lancement de $parallelCount executions Cloud Run en parallele..." -ForegroundColor Green
Write-Host "   Departement: 60, Limite: 5 etablissements par execution" -ForegroundColor Cyan
Write-Host ""

$executionNames = @()

# Lancer les executions en parallele (sans --wait)
for ($i = 1; $i -le $parallelCount; $i++) {
    Write-Host "Demarrage de l'execution #$i..." -ForegroundColor Cyan
    
    $output = gcloud run jobs execute $jobName --region $region 2>&1 | Out-String
    
    # Extraire le nom de l'execution depuis la sortie
    if ($output -match "habitat-enrich-test-[a-z0-9]+") {
        $executionName = $matches[0]
        $executionNames += $executionName
        Write-Host "   Execution lancee: $executionName" -ForegroundColor Gray
    }
    
    # Petite pause entre les lancements
    Start-Sleep -Milliseconds 300
}

Write-Host ""
Write-Host "$parallelCount executions lancees. Attente de leur fin..." -ForegroundColor Yellow
Write-Host ""

# Attendre que toutes les executions se terminent
$allCompleted = $false
$checkCount = 0
$maxChecks = 120  # 120 * 5s = 10 minutes max

while (-not $allCompleted -and $checkCount -lt $maxChecks) {
    Start-Sleep -Seconds 5
    $checkCount++
    
    $completed = 0
    $failed = 0
    $running = 0
    
    foreach ($execName in $executionNames) {
        $status = gcloud run jobs executions describe $execName --region $region --format="value(status.conditions[0].status)" 2>$null
        
        if ($status -eq "True") {
            $completed++
        } elseif ($status -eq "False") {
            $failed++
        } else {
            $running++
        }
    }
    
    $total = $completed + $failed
    if ($total -eq $parallelCount) {
        $allCompleted = $true
    }
    
    if ($checkCount % 6 -eq 0) {  # Afficher toutes les 30 secondes
        Write-Host "   En cours: $running | Terminees: $completed | Echecs: $failed" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "Resultats finaux:" -ForegroundColor Green
Write-Host ""

$successCount = 0
$failCount = 0

foreach ($execName in $executionNames) {
    $conditions = gcloud run jobs executions describe $execName --region $region --format="value(status.conditions[0].status,status.conditions[0].reason)" 2>$null
    
    if ($conditions -match "True") {
        Write-Host "$execName : SUCCESS" -ForegroundColor Green
        $successCount++
    } else {
        Write-Host "$execName : FAILED" -ForegroundColor Red
        $failCount++
    }
}

Write-Host ""
Write-Host "Total: $successCount succes, $failCount echecs sur $parallelCount executions" -ForegroundColor Cyan
Write-Host ""
Write-Host "Pour verifier les propositions creees, lancez:" -ForegroundColor Yellow
Write-Host "   python get_pending_propositions.py" -ForegroundColor White
