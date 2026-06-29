# Airflow on Azure Container Apps Jobs

[![CI](https://github.com/hetvip2/airflow-on-aca-jobs/actions/workflows/ci.yml/badge.svg)](https://github.com/hetvip2/airflow-on-aca-jobs/actions/workflows/ci.yml)

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

## Quickstart

Get from zero to "Airflow ran my ACA Job" in three commands. Assumes you've
installed the [prerequisites](#prerequisites) (`az`, `azd`, Docker) and run
`az login` + `azd auth login`.

```bash
# 1. Deploy the ACA Job to Azure (pick the "try" tier when prompted)
azd env new my-airflow-aca && azd up

# 2. Run the local demo — starts Airflow in Docker, wires in your job, runs the DAG.
#    Use the resource group + job name that `azd up` printed.
#    macOS / Linux:
./demo/run-demo.sh <resource-group> <job-name>
#    Windows (PowerShell):
./demo/run-demo.ps1 -ResourceGroup <resource-group> -JobName <job-name>

# 3. Done — you'll see "SUCCESS: Airflow orchestrated your ACA Job end-to-end."
#    Open http://localhost:8080 (user: admin) to view the run, then clean up:
docker rm -f airflow-aca-demo
```

**No Azure account yet, or just want to see the code run?** The offline test
suite needs neither Azure nor network:

```bash
pip install -r airflow/requirements.txt pytest ruff && pytest
```

**Already run Airflow?** Skip the demo — copy `airflow/plugins/` and
`airflow/dags/` into your Airflow (or `pip install "git+https://github.com/hetvip2/airflow-on-aca-jobs"`),
set three Variables, and trigger the DAG. See [Use it in your own Airflow](#use-it-in-your-own-airflow).

> Each step is explained in full below (deploy → wire up → demo → run any
> workload → scale). The Quickstart is just the fast path.

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

> Prefer not to use Variables? You can pass `subscription_id` / `resource_group` directly to
> the operator, or store them in an Airflow **Connection** so DAGs need almost no inline
> config — see [Authentication](#authentication).

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

### Validated end-to-end

Tested against a live ACA Job with a real Airflow stack (LocalExecutor + Postgres + triggerer):

- **50 concurrent** deferrable executions: 50/50 succeeded, **0 throttling** observed.
- **8-shard pipeline**: all shards deferred (worker slots freed), ran in parallel, and
  succeeded on the Azure side.
- **Retry recovery**: a deliberately failing execution was caught and automatically retried.

> Scope of testing, stated honestly: the above used a single job in one subscription/region.
> The architecture (deferrable + triggerer) is what enables much larger fan-out, but
> throughput beyond ~50 concurrent and subscription-level ARM throttling limits haven't been
> independently benchmarked. Tune `poll_interval_seconds`, executor parallelism, and DAG
> `max_active_tasks` for your workload, and the operator's backoff will absorb throttling if
> you hit it.

---

## Testing

The repo ships an **offline** test suite (mocked ARM — no Azure account or network needed)
plus a GitHub Actions workflow that lints and runs it on every push/PR.

```bash
pip install -r airflow/requirements.txt pytest ruff
pytest          # 35 tests: auth resolution, retry/backoff, body building, trigger events
ruff check airflow tests
```

> Airflow doesn't run on native Windows — run the tests in WSL2, Linux, macOS, or the
> `apache/airflow` Docker image.

---

## Use it in your own Airflow

**1. Install the operator** — two interchangeable options:

- *Copy the plugin* (simplest): copy `airflow/plugins/` into your Airflow `plugins/` folder
  (gives you the operator **and the async trigger**).
- *Or pip-install it* into your Airflow image:
  ```bash
  pip install "git+https://github.com/hetvip2/airflow-on-aca-jobs"
  ```
  Both expose the same `operators` / `triggers` modules the DAGs import.

**2. Add the DAGs** you want from `airflow/dags/` into your `dags/` folder
   (`aca_jobs_example_dag.py` for single jobs, `aca_jobs_pipeline_dag.py` for pipelines).

**3. Install the requirements** on your workers (skip if you pip-installed above):
   `pip install -r airflow/requirements.txt`.

**4. Connect Airflow to Azure** — see [Authentication](#authentication) below. The easy,
   portable path is a single Airflow **Connection**; it works the same whether your Airflow
   runs on Azure, AWS, or on-prem.

**5. For scale**, run a **triggerer** and a concurrent executor so `deferrable=True` works —
   see [Step 5](#step-5--production-scale-pipelines-deferral--retries).

---

## Authentication

The operator needs an Azure AD identity that can start your job. Pick whichever fits your
Airflow — the operator tries them in this order:

### Option A — Airflow Connection (recommended, works anywhere)

Create one Connection and point the operator at it with `azure_conn_id`. This is the easiest
path for **any** Airflow, including off-Azure (AWS, on-prem, Astronomer).

```bash
airflow connections add azure_default \
  --conn-type generic \
  --conn-login "<client-id>" \
  --conn-password "<client-secret>" \
  --conn-extra '{"tenant_id":"<tenant-id>","subscription_id":"<sub>","resource_group":"<rg>"}'
```

```python
AzureContainerAppsJobOperator(
    task_id="run_job",
    azure_conn_id="azure_default",   # auth comes from the connection
    job_name="my-job",               # sub + rg can live in the connection extra
    deferrable=True,
)
```

The connection's `extra` understands: `tenant_id`, `client_id`/`client_secret` (or use
login/password), `access_token` (a pre-fetched ARM token), `managed_identity_client_id`,
and optional `subscription_id` / `resource_group` defaults so your DAGs stay tiny.

### Option B — Managed identity (best on Azure-hosted Airflow)

If Airflow runs on Azure with a managed identity attached, set **nothing** — the operator
uses `DefaultAzureCredential` automatically. For a **user-assigned** identity, pass its
client id:

```python
AzureContainerAppsJobOperator(task_id="run_job", job_name="my-job",
                              managed_identity_client_id="<user-assigned-mi-client-id>")
```

### Option C — Pre-fetched token (demo / quick tests only)

Set `AZURE_ACCESS_TOKEN` to a short-lived ARM token (this is what the local demo uses).
Not for production: it doesn't auto-refresh, so long deferred polls will stall when it expires.

**Grant the identity access to the job** (any option):
```bash
az role assignment create --assignee <identity-or-client-id> --role Contributor \
  --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.App/jobs/<job>
```

> You can scope this more tightly than `Contributor` to just the job's start/read actions
> with a custom role if your security posture requires least privilege.

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
├── pyproject.toml             # packaging + pytest/ruff config (pip-installable)
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
├── tests/                     # offline test suite (mocked ARM, no Azure needed)
├── .github/workflows/ci.yml   # lint + tests on every push / PR
└── demo/                      # one-command local demo (Docker)
    ├── run-demo.ps1
    └── run-demo.sh
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `azd up` fails with `AKSCapacityHeavyUsage` | Region is temporarily full. `azd env set AZURE_LOCATION <other-region>` and retry. |
| `401`/`403` when the DAG runs | The Azure identity can't reach the job. Re-check the `az role assignment` in [Authentication](#authentication). |
| `DefaultAzureCredential failed to retrieve a token` | No ambient identity. Use an Airflow **Connection** (Option A) or attach a managed identity. |
| Demo says Docker not found | Install Docker Desktop and make sure it's running. |
| Airflow fails with `ModuleNotFoundError: fcntl` | You're running Airflow on native Windows. Use the Docker demo (Step 3) or WSL2. |
| Task times out | Your job runs longer than the operator waits. Raise `execution_timeout_seconds` in the DAG. |
| Deferrable task never resumes / stuck in `deferred` | No **triggerer** is running (`airflow triggerer`), or auth is a static `AZURE_ACCESS_TOKEN` that expired mid-poll. Run a triggerer and use a Connection or Managed Identity for long jobs. |
| Fan-out tasks run one at a time | You're on the Sequential executor (the demo). Use Local, Celery, or Kubernetes executor for parallelism. |

## License

Apache-2.0 — see [LICENSE](LICENSE).
