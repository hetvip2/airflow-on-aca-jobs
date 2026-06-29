"""Offline tests for the operator's request-building and job-ref logic.

These import Airflow's ``BaseOperator``; CI installs apache-airflow. No network.
"""
import pytest
from airflow.exceptions import AirflowException
from operators.azure_container_apps_job_operator import (
    AzureContainerAppsJobOperator,
)


def _op(**kwargs):
    return AzureContainerAppsJobOperator(task_id="t", **kwargs)


def test_merge_env_overrides_and_adds():
    base = [{"name": "A", "value": "1"}, {"name": "B", "value": "2"}]
    merged = AzureContainerAppsJobOperator._merge_env(base, {"B": "X", "C": "3"})
    as_map = {e["name"]: e["value"] for e in merged}
    assert as_map == {"A": "1", "B": "X", "C": "3"}


def test_resolve_job_ref_from_parts():
    op = _op(subscription_id="s", resource_group="r", job_name="j")
    ref = op._resolve_job_ref()
    assert (ref.subscription_id, ref.resource_group, ref.job_name) == ("s", "r", "j")


def test_resolve_job_ref_from_resource_id():
    rid = "/subscriptions/s/resourceGroups/r/providers/Microsoft.App/jobs/j"
    op = _op(job_resource_id=rid)
    ref = op._resolve_job_ref()
    assert ref.job_name == "j"


def test_resolve_job_ref_missing_raises():
    op = _op(job_name="j")  # no sub/rg, no conn
    with pytest.raises(AirflowException):
        op._resolve_job_ref()


def test_build_start_body_empty_when_no_overrides():
    op = _op(subscription_id="s", resource_group="r", job_name="j")
    ref = op._resolve_job_ref()
    assert op._build_start_body(ref, "tok") == {}


def test_build_start_body_passthrough():
    body = {"containers": [{"name": "c", "image": "img"}]}
    op = _op(subscription_id="s", resource_group="r", job_name="j", job_start_body=body)
    ref = op._resolve_job_ref()
    assert op._build_start_body(ref, "tok") is body


def test_build_start_body_applies_overrides(monkeypatch):
    op = _op(
        subscription_id="s",
        resource_group="r",
        job_name="j",
        args=["--batch", "100"],
        env_vars={"MODE": "nightly"},
    )
    ref = op._resolve_job_ref()
    monkeypatch.setattr(
        op, "_get_base_container",
        lambda *a, **k: {"name": "c", "image": "img:1", "env": [{"name": "KEEP", "value": "1"}]},
    )
    body = op._build_start_body(ref, "tok")
    container = body["containers"][0]
    assert container["image"] == "img:1"          # preserved
    assert container["args"] == ["--batch", "100"]  # overridden
    env = {e["name"]: e["value"] for e in container["env"]}
    assert env == {"KEEP": "1", "MODE": "nightly"}  # merged


def test_auth_dict():
    op = _op(
        subscription_id="s", resource_group="r", job_name="j",
        azure_conn_id="azure_default", managed_identity_client_id="mi-1",
    )
    assert op._auth() == {
        "conn_id": "azure_default",
        "managed_identity_client_id": "mi-1",
    }

