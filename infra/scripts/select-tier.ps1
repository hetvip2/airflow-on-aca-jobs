$ErrorActionPreference = "Stop"

$existingTier = azd env get-value DEPLOYMENT_TIER 2>$null
if ($LASTEXITCODE -eq 0 -and $existingTier) {
  Write-Host "DEPLOYMENT_TIER already set to '$existingTier'."
  return
}

Write-Host "Select deployment tier:"
Write-Host "1) try"
Write-Host "2) small"
Write-Host "3) production"

$choice = Read-Host "Enter 1, 2, or 3"

switch ($choice) {
  "1" { $tier = "try" }
  "2" { $tier = "small" }
  "3" { $tier = "production" }
  default { throw "Invalid selection '$choice'. Choose 1, 2, or 3." }
}

azd env set DEPLOYMENT_TIER $tier | Out-Null
Write-Host "DEPLOYMENT_TIER set to '$tier'."
