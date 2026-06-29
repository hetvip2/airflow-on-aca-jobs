#!/usr/bin/env bash
# One-command local demo: runs Apache Airflow in Docker and triggers a DAG
# that starts (and waits on) your deployed Azure Container Apps Job.
#
# Usage: ./demo/run-demo.sh <resource-group> <job-name> [subscription-id] [conf-json]
#   conf-json (optional) passes your own workload arguments, e.g.
#   '{"command":["python","main.py"],"env":{"BATCH_SIZE":"100"}}'
set -euo pipefail

RESOURCE_GROUP="${1:?Usage: run-demo.sh <resource-group> <job-name> [subscription-id] [conf-json]}"
JOB_NAME="${2:?Usage: run-demo.sh <resource-group> <job-name> [subscription-id] [conf-json]}"
SUBSCRIPTION_ID="${3:-$(az account show --query id -o tsv)}"
CONF="${4:-}"
IMAGE="apache/airflow:2.10.2"
CONTAINER="airflow-aca-demo"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DAGS_PATH="${REPO_ROOT}/airflow/dags"
PLUGINS_PATH="${REPO_ROOT}/airflow/plugins"
JOB_RESOURCE_ID="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.App/jobs/${JOB_NAME}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker was not found. Install Docker and start it, then re-run." >&2
  exit 1
fi

echo "==> Getting a short-lived Azure token (so the demo needs no secrets)..."
TOKEN="$(az account get-access-token --resource https://management.azure.com --query accessToken -o tsv)"
[ -n "${TOKEN}" ] || { echo "Could not get an Azure token. Run 'az login' first." >&2; exit 1; }

echo "==> Starting Airflow in Docker (first run pulls the image, ~2-3 min)..."
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER}" -p 8080:8080 \
  -e "AZURE_ACCESS_TOKEN=${TOKEN}" \
  -e "AIRFLOW_VAR_AZURE_SUBSCRIPTION_ID=${SUBSCRIPTION_ID}" \
  -e "AIRFLOW_VAR_ACA_RESOURCE_GROUP=${RESOURCE_GROUP}" \
  -e "AIRFLOW_VAR_ACA_JOB_NAME=${JOB_NAME}" \
  -e "AIRFLOW_VAR_ACA_JOB_RESOURCE_ID=${JOB_RESOURCE_ID}" \
  -e "AIRFLOW__CORE__LOAD_EXAMPLES=false" \
  -e "_PIP_ADDITIONAL_REQUIREMENTS=requests" \
  -v "${DAGS_PATH}:/opt/airflow/dags" \
  -v "${PLUGINS_PATH}:/opt/airflow/plugins" \
  "${IMAGE}" airflow standalone >/dev/null

echo "==> Waiting for Airflow to be ready..."
ready=0
for i in $(seq 1 60); do
  sleep 10
  if docker exec "${CONTAINER}" airflow dags list 2>/dev/null | grep -q aca_jobs_example; then ready=1; break; fi
  echo "   ...still starting ($((i * 10))s)"
done
[ "${ready}" -eq 1 ] || { docker logs --tail 40 "${CONTAINER}"; echo "Airflow did not start in time." >&2; exit 1; }

PW="$(docker exec "${CONTAINER}" cat /opt/airflow/standalone_admin_password.txt 2>/dev/null || true)"
echo ""
echo "Airflow UI : http://localhost:8080"
echo "Username   : admin"
echo "Password   : ${PW}"
echo ""

echo "==> Triggering DAG 'aca_jobs_example'..."
RUN_ID="demo__$(date +%Y%m%d%H%M%S)"
docker exec "${CONTAINER}" airflow dags unpause aca_jobs_example >/dev/null
if [ -n "${CONF}" ]; then
  echo "    with config: ${CONF}"
  docker exec "${CONTAINER}" airflow dags trigger aca_jobs_example -r "${RUN_ID}" --conf "${CONF}" >/dev/null
else
  docker exec "${CONTAINER}" airflow dags trigger aca_jobs_example -r "${RUN_ID}" >/dev/null
fi

echo "==> Waiting for the run to finish..."
state="running"
for i in $(seq 1 60); do
  sleep 10
  line="$(docker exec "${CONTAINER}" airflow dags list-runs -d aca_jobs_example 2>/dev/null | grep "${RUN_ID}" || true)"
  case "${line}" in
    *success*) state="success"; break ;;
    *failed*)  state="failed";  break ;;
    *) echo "   ...running ($((i * 10))s)" ;;
  esac
done

echo ""
echo "=== Airflow task -> ACA execution log ==="
docker exec "${CONTAINER}" bash -lc "find /opt/airflow/logs -path '*aca_jobs_example*${RUN_ID}*' -name '*.log' -exec grep -E 'Started ACA|current state|succeeded' {} +" 2>/dev/null || true

echo ""
if [ "${state}" = "success" ]; then
  echo "SUCCESS: Airflow orchestrated your ACA Job end-to-end."
else
  echo "DAG run state: ${state}. See logs above." >&2
  exit 1
fi

echo ""
echo "Open http://localhost:8080 to view the run visually."
echo "Stop the demo with:  docker rm -f ${CONTAINER}"
