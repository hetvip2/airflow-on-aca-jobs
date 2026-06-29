from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator

try:
    from operators.azure_container_apps_job_operator import (
        AzureContainerAppsJobOperator,
    )
except ModuleNotFoundError:
    from plugins.operators.azure_container_apps_job_operator import (
        AzureContainerAppsJobOperator,
    )


# Production-style pipeline:
#   start -> make_shards -> [shard 0 .. shard N-1] -> finalize
#
# This is the orchestration pattern ACA Jobs can't express on its own:
# dependency-aware ordering, parallel fan-out, and per-task retries. Each shard
# runs as a real ACA Job execution. `deferrable=True` frees the Airflow worker
# slot while each job runs, so the fan-out width is bounded by ACA, not by the
# number of Airflow workers. The number of shards is set per run via config.
with DAG(
    dag_id="aca_jobs_pipeline",
    description="Dependency + parallel fan-out + retries over ACA Jobs (deferrable).",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    render_template_as_native_obj=True,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["aca", "jobs", "pipeline"],
    params={
        "shards": Param(
            default=5,
            type="integer",
            minimum=1,
            maximum=200,
            title="Number of parallel shards",
            description="How many ACA Job executions to fan out in parallel.",
        ),
    },
) as dag:
    start = EmptyOperator(task_id="start")
    finalize = EmptyOperator(task_id="finalize")

    @task
    def make_shards(params: dict | None = None) -> list[dict[str, str]]:
        n = int((params or {}).get("shards", 5))
        return [{"SHARD_INDEX": str(i), "SHARD_TOTAL": str(n)} for i in range(n)]

    shards = make_shards()

    run_shards = AzureContainerAppsJobOperator.partial(
        task_id="run_shard",
        subscription_id="{{ var.value.azure_subscription_id }}",
        resource_group="{{ var.value.aca_resource_group }}",
        job_name="{{ var.value.aca_job_name }}",
        azure_conn_id="{{ var.value.get('azure_conn_id', '') }}",
        deferrable=True,
        poll_interval_seconds=15,
        execution_timeout_seconds=3600,
    ).expand(env_vars=shards)

    start >> shards >> run_shards >> finalize
