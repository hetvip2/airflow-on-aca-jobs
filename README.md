# Airflow on Azure Container Apps Jobs

Run your existing **Apache Airflow** DAGs against **Azure Container Apps (ACA) Jobs**.
Airflow stays where it already lives (your laptop, your cluster, Managed Airflow, Astronomer, …);
a small operator tells ACA Jobs to run, then waits for the result.

```
Airflow DAG task  ──►  AzureContainerAppsJobOperator  ──►  ACA Jobs (runs your container)
        ▲                                                          │
        └──────────────────  waits for success / failure  ◄────────┘
```

**Nothing new to host.** This template ships an Azure deployment for the ACA Job
plus the Airflow operator + example DAG. There is no extra server, scheduler, or
database to run — Airflow is the brain, ACA Jobs is the worker.

---

## What you get

| Tier | Best for | Adds on top of `try` |
|------|----------|----------------------|
| **try** | A quick proof of concept | ACA environment + one ACA Job |
| **small** | Jobs that read/write files | + Storage account & Azure Files share |
| **production** | Jobs that need a database | + secure Postgres connection string |

You pick the tier when you run `azd up`.

---

## Prerequisites

1. An **Azure subscription**.
2. **[Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)** — then run `az login`.
3. **[Azure Developer CLI (azd)](https://learn.microsoft.com/azure/developer/azure-developer-cli/install-azd)** — then run `azd auth login`.
4. *(Only for the local demo in Step 3)* **[Docker Desktop](https://www.docker.com/products/docker-desktop/)**, running.

> **On Windows?** Docker Desktop needs WSL2. If you don't have it: open PowerShell **as Administrator**, run `wsl --install`, and reboot once. Do **not** try to run Airflow directly on Windows Python — it isn't supported. The demo in Step 3 runs Airflow inside Docker for you, so this is handled.

---

## Step 1 — Deploy the ACA Job to Azure

```bash
# from this folder
azd auth login
azd env new my-airflow-aca
azd up
```

`azd up` asks you to choose **try / small / production**, then provisions everything.
When it finishes it prints your **resource group** and **job name** — you'll need those next.

> **Capacity tip:** if `azd up` fails with `AKSCapacityHeavyUsage`, that region is
> temporarily full. Pick another region and retry:
> `azd env set AZURE_LOCATION northcentralus` then `azd up`.

---

## Step 2 — Point the operator at your job

The operator needs three values, supplied as Airflow **Variables**:

| Airflow Variable | Value |
|------------------|-------|
| `azure_subscription_id` | your subscription id |
| `aca_resource_group` | resource group from Step 1 |
| `aca_job_name` | job name from Step 1 |

In a real Airflow environment you'd set these once (UI → Admin → Variables, or
`airflow variables set ...`) and copy `airflow/plugins/` and `airflow/dags/` into
your Airflow. The local demo below does all of this for you.

These are just the **defaults** — at trigger time a user can override the job name and
pass their own workload arguments without touching code (see Step 4).

---

## Step 3 — See it work (local demo)

This starts Airflow in Docker, wires in your job, runs the DAG, and shows the result.
No secrets needed — it uses a short-lived token from your `az login` session.

**macOS / Linux**
```bash
./demo/run-demo.sh <resource-group> <job-name>
```

**Windows (PowerShell)**
```powershell
./demo/run-demo.ps1 -ResourceGroup <resource-group> -JobName <job-name>
```

When it finishes you'll see `SUCCESS: Airflow orchestrated your ACA Job end-to-end.`
Open **http://localhost:8080** (user `admin`, password printed by the script) to view
the run in Airflow's Graph/Grid. The matching execution appears in the Azure Portal
under your job's **Execution history**.

Stop the demo when done:
```bash
docker rm -f airflow-aca-demo
```

To pass your own workload arguments in the demo, add a config JSON:

**macOS / Linux**
```bash
./demo/run-demo.sh <resource-group> <job-name> "" '{"env":{"BATCH_SIZE":"100"}}'
```

**Windows (PowerShell)**
```powershell
./demo/run-demo.ps1 -ResourceGroup <resource-group> -JobName <job-name> `
  -Conf '{"env":{"BATCH_SIZE":"100"}}'
```

---

## Step 4 — Run any workload, no code edits

The DAG is reusable: anyone can trigger it with a small config form and tell it **which
job to run and how**. Nothing in the Python needs to change.

**From the Airflow UI:** open the `aca_jobs_example` DAG → **Trigger DAG w/ config** →
fill in the form:

| Field | What it does | Example |
|-------|--------------|---------|
| **ACA Job name** | Which job to run (blank = the `aca_job_name` Variable) | `nightly-etl` |
| **Command override** | Replace the image entrypoint | `["python", "main.py"]` |
| **Args override** | Arguments for the command | `["--batch-size", "100"]` |
| **Environment variables** | Extra env vars for this run | `{"MODE": "nightly"}` |

**From the CLI / REST API** (for automation and scheduling), pass the same fields as
the run config:

```bash
airflow dags trigger aca_jobs_example \
  --conf '{"job_name":"nightly-etl","command":["python","main.py"],"env":{"MODE":"nightly"}}'
```

You only specify what you want to change. The operator reads the job's existing
definition and applies your overrides on top, so blank fields keep the job's defaults.
To run the same workload on a schedule, set `schedule=` on the DAG (e.g. `"0 2 * * *"`)
and the config above becomes its `params` defaults.

---

## Step 5 — Production scale: pipelines, deferral & retries

ACA cron is per-job only — it can't express *"run B after A succeeds"*, fan-out, backfill,
or dependency-aware retries. That orchestration layer is exactly what Airflow adds. This
template ships a production pipeline DAG, `aca_jobs_pipeline`, that does it:

```
start ─► make_shards ─► run_shard[0..N-1]  (parallel fan-out)  ─► finalize
                        each shard = one real ACA Job execution
```

Trigger it with the number of parallel shards:

```bash
airflow dags trigger aca_jobs_pipeline --conf '{"shards": 50}'
```

### Deferrable operator (the scaling unlock)

Set `deferrable=True` (the pipeline DAG already does). Instead of holding an Airflow
worker slot for the whole job, the operator **starts** the ACA execution, then hands
polling to Airflow's async **triggerer** and releases the slot. One triggerer can watch
thousands of in-flight executions, so fan-out width is bounded by **ACA**, not by your
Airflow worker count.

```python
AzureContainerAppsJobOperator(
    task_id="run_job",
    job_name="{{ var.value.aca_job_name }}",
    deferrable=True,           # release the worker slot while ACA runs
    poll_interval_seconds=15,
)
```

You can make every operator deferrable by default with an env var:
```bash
AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE=true
```

### What your Airflow needs for scale

| Requirement | Why |
|-------------|-----|
| A running **triggerer** (`airflow triggerer`) | Required for `deferrable=True`. Without it, deferred tasks never resume. |
| A concurrent **executor** — Local, Celery, or Kubernetes (**not** Sequential) | Sequential runs one task at a time, so no parallel fan-out. The Docker demo in Step 3 is Sequential and is for proof-of-concept only. |
| **Managed Identity / Service Principal** auth (not a static token) | Deferred polls can outlive a token's ~1h lifetime. `DefaultAzureCredential` auto-refreshes; the `AZURE_ACCESS_TOKEN` env var does **not** and will stall long-running deferred tasks. |

### Built-in throttling & failure handling

- **Retries with exponential backoff** on ARM throttling (`429`) and transient `5xx`,
  honoring the `Retry-After` header — so large fan-outs don't hammer the control plane.
- **Token refresh** on `401` mid-poll (with managed identity).
- **Dependency-aware retries**: set `retries`/`retry_delay` in the DAG's `default_args`
  (the pipeline DAG uses `retries=2`). A failed ACA execution surfaces as a task failure,
  Airflow retries it, and downstream tasks are skipped on permanent failure.

### Validated at scale

This was tested end-to-end against a live ACA Job with a real Airflow stack
(LocalExecutor + Postgres + triggerer):

- **50 concurrent** deferrable executions: 50/50 succeeded, **0 throttling**.
- **8-shard pipeline**: all shards deferred, ran in parallel, and succeeded on Azure.
- **Retry recovery**: a deliberately failing execution was caught and automatically retried.

---

## Use it in your own Airflow

1. Copy `airflow/plugins/` into your Airflow `plugins/` folder (gives you the operator
   **and the async trigger**).
2. Copy the DAGs you want from `airflow/dags/` into your `dags/` folder
   (`aca_jobs_example_dag.py` for single jobs, `aca_jobs_pipeline_dag.py` for pipelines).
3. Install the requirements on your Airflow workers: `pip install -r airflow/requirements.txt`.
4. Make sure Airflow can reach Azure — either an attached **Managed Identity / Service
   Principal** (the operator uses `DefaultAzureCredential`) or the `AZURE_ACCESS_TOKEN`
   env var. Grant that identity access to the job:
   ```bash
   az role assignment create --assignee <identity-id> --role Contributor \
     --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.App/jobs/<job>
   ```
5. For `deferrable=True` (recommended at scale), make sure a **triggerer** is running and
   you use a concurrent executor — see [Step 5](#step-5--production-scale-pipelines-deferral--retries).
6. Set the three Airflow Variables from Step 2 and trigger the DAG.

---

## Clean up

```bash
azd down
```

---

## Project structure

```
airflow-on-aca-jobs/
├── azure.yaml                 # Azure Developer CLI (azd) config
├── infra/                     # Bicep: ACA environment + Job (+ tier add-ons)
│   ├── main.bicep
│   ├── main.parameters.json
│   └── scripts/               # interactive tier picker
├── airflow/                   # what you copy into your Airflow
│   ├── dags/                  # example DAG + production pipeline DAG
│   ├── plugins/
│   │   ├── operators/         # AzureContainerAppsJobOperator (+ shared HTTP/auth helpers)
│   │   └── triggers/          # async trigger for deferrable mode
│   └── requirements.txt
└── demo/                      # one-command local demo (Docker)
    ├── run-demo.ps1
    └── run-demo.sh
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `azd up` fails with `AKSCapacityHeavyUsage` | Region is temporarily full. `azd env set AZURE_LOCATION <other-region>` and retry. |
| `401`/`403` when the DAG runs | The Azure identity can't reach the job. Re-check the `az role assignment` in "Use it in your own Airflow". |
| Demo says Docker not found | Install Docker Desktop and make sure it's running. |
| Airflow fails with `ModuleNotFoundError: fcntl` | You're running Airflow on native Windows. Use the Docker demo (Step 3) or WSL2. |
| Task times out | Your job runs longer than the operator waits. Raise `execution_timeout_seconds` in the DAG. |
| Deferrable task never resumes / stuck in `deferred` | No **triggerer** is running (`airflow triggerer`), or auth is a static `AZURE_ACCESS_TOKEN` that expired mid-poll. Run a triggerer and use Managed Identity for long jobs. |
| Fan-out tasks run one at a time | You're on the Sequential executor (the demo). Use Local, Celery, or Kubernetes executor for parallelism. |

## License

Apache-2.0 — see [LICENSE](LICENSE).
