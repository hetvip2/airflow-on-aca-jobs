#requires -Version 5.1
<#
.SYNOPSIS
  One-command local demo: runs Apache Airflow in Docker and triggers a DAG
  that starts (and waits on) your deployed Azure Container Apps Job.

.EXAMPLE
  ./demo/run-demo.ps1 -ResourceGroup rg-airflow-aca -JobName my-aca-job

.EXAMPLE
  # Pass your own workload arguments to the job for this run:
  ./demo/run-demo.ps1 -ResourceGroup rg-airflow-aca -JobName my-aca-job `
    -Conf '{"command":["python","main.py"],"env":{"BATCH_SIZE":"100"}}'
#>
param(
  [Parameter(Mandatory = $true)] [string]$ResourceGroup,
  [Parameter(Mandatory = $true)] [string]$JobName,
  [string]$SubscriptionId,
  [string]$Conf,
  [string]$Image = "apache/airflow:2.10.2"
)

$ErrorActionPreference = "Stop"

function Find-Docker {
  $cmd = Get-Command docker -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  $fallback = "C:\Program Files\Docker\Docker\resources\bin\docker.exe"
  if (Test-Path $fallback) { return $fallback }
  throw "Docker was not found. Install Docker Desktop and start it, then re-run."
}

$docker = Find-Docker
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$dagsPath = Join-Path $repoRoot "airflow\dags"
$pluginsPath = Join-Path $repoRoot "airflow\plugins"
$container = "airflow-aca-demo"

if (-not $SubscriptionId) {
  $SubscriptionId = az account show --query id -o tsv
}
$jobResourceId = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.App/jobs/$JobName"

Write-Host "==> Getting a short-lived Azure token (so the demo needs no secrets)..."
$token = az account get-access-token --resource https://management.azure.com --query accessToken -o tsv
if (-not $token) { throw "Could not get an Azure token. Run 'az login' first." }

Write-Host "==> Starting Airflow in Docker (first run pulls the image, ~2-3 min)..."
& $docker rm -f $container 2>$null | Out-Null
& $docker run -d --name $container -p 8080:8080 `
  -e "AZURE_ACCESS_TOKEN=$token" `
  -e "AIRFLOW_VAR_AZURE_SUBSCRIPTION_ID=$SubscriptionId" `
  -e "AIRFLOW_VAR_ACA_RESOURCE_GROUP=$ResourceGroup" `
  -e "AIRFLOW_VAR_ACA_JOB_NAME=$JobName" `
  -e "AIRFLOW_VAR_ACA_JOB_RESOURCE_ID=$jobResourceId" `
  -e "AIRFLOW__CORE__LOAD_EXAMPLES=false" `
  -e "_PIP_ADDITIONAL_REQUIREMENTS=requests" `
  -v "${dagsPath}:/opt/airflow/dags" `
  -v "${pluginsPath}:/opt/airflow/plugins" `
  $Image airflow standalone | Out-Null

Write-Host "==> Waiting for Airflow to be ready..."
$ready = $false
for ($i = 0; $i -lt 60; $i++) {
  Start-Sleep -Seconds 10
  $dags = & $docker exec $container airflow dags list 2>$null
  if ($LASTEXITCODE -eq 0 -and ($dags -match "aca_jobs_example")) { $ready = $true; break }
  Write-Host "   ...still starting ($($i * 10)s)"
}
if (-not $ready) { & $docker logs --tail 40 $container; throw "Airflow did not start in time." }

$pw = & $docker exec $container cat /opt/airflow/standalone_admin_password.txt 2>$null
Write-Host ""
Write-Host "Airflow UI : http://localhost:8080"
Write-Host "Username   : admin"
Write-Host "Password   : $pw"
Write-Host ""

Write-Host "==> Triggering DAG 'aca_jobs_example'..."
$runId = "demo__" + (Get-Date -Format "yyyyMMddHHmmss")
& $docker exec $container airflow dags unpause aca_jobs_example | Out-Null
if ($Conf) {
  Write-Host "    with config: $Conf"
  & $docker exec $container airflow dags trigger aca_jobs_example -r $runId --conf $Conf | Out-Null
} else {
  & $docker exec $container airflow dags trigger aca_jobs_example -r $runId | Out-Null
}

Write-Host "==> Waiting for the run to finish..."
$state = "running"
for ($i = 0; $i -lt 60; $i++) {
  Start-Sleep -Seconds 10
  $runs = & $docker exec $container airflow dags list-runs -d aca_jobs_example 2>$null
  $line = $runs | Select-String $runId
  if ($line -match "success") { $state = "success"; break }
  if ($line -match "failed")  { $state = "failed";  break }
  Write-Host "   ...running ($($i * 10)s)"
}

Write-Host ""
Write-Host "=== Airflow task -> ACA execution log ==="
& $docker exec $container bash -lc "find /opt/airflow/logs -path '*aca_jobs_example*$runId*' -name '*.log' -exec grep -E 'Started ACA|current state|succeeded' {} +" 2>$null

Write-Host ""
if ($state -eq "success") {
  Write-Host "SUCCESS: Airflow orchestrated your ACA Job end-to-end."
} else {
  throw "DAG run state: $state. See logs above."
}

Write-Host ""
Write-Host "Open http://localhost:8080 to view the run visually."
Write-Host "Stop the demo with:  docker rm -f $container"
