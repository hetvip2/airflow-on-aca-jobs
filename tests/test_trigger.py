"""Offline tests for the async trigger (serialize + event emission, no network)."""
import asyncio

from operators import aca_jobs_common as aca
from triggers.azure_container_apps_job_trigger import (
    AzureContainerAppsJobTrigger,
)


def _make(**kwargs):
    base = dict(
        job_ref={"subscription_id": "s", "resource_group": "r", "job_name": "j"},
        execution_name="exec-1",
        poll_interval_seconds=0,
    )
    base.update(kwargs)
    return AzureContainerAppsJobTrigger(**base)


def _first_event(trigger):
    async def _run():
        async for event in trigger.run():
            return event

    return asyncio.run(_run())


def test_serialize_roundtrip():
    trig = _make(auth={"conn_id": "azure_default"})
    classpath, kwargs = trig.serialize()
    assert classpath.endswith("AzureContainerAppsJobTrigger")
    assert kwargs["execution_name"] == "exec-1"
    assert kwargs["auth"] == {"conn_id": "azure_default"}
    # kwargs must be enough to reconstruct the trigger
    AzureContainerAppsJobTrigger(**kwargs)


def test_run_emits_success(monkeypatch):
    monkeypatch.setattr(aca, "get_token", lambda *a, **k: "tok")
    monkeypatch.setattr(aca, "get_execution_state", lambda *a, **k: "Succeeded")
    event = _first_event(_make())
    assert event.payload["status"] == "success"
    assert event.payload["execution_name"] == "exec-1"


def test_run_emits_failed(monkeypatch):
    monkeypatch.setattr(aca, "get_token", lambda *a, **k: "tok")
    monkeypatch.setattr(aca, "get_execution_state", lambda *a, **k: "Failed")
    event = _first_event(_make())
    assert event.payload["status"] == "failed"


def test_run_refreshes_token_then_succeeds(monkeypatch):
    tokens = ["tok-1", "tok-2"]
    monkeypatch.setattr(aca, "get_token", lambda *a, **k: tokens.pop(0))
    calls = {"n": 0}

    def state(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise aca.ACATokenExpired("expired")
        return "Succeeded"

    monkeypatch.setattr(aca, "get_execution_state", state)
    event = _first_event(_make())
    assert event.payload["status"] == "success"
    assert calls["n"] == 2  # retried after refresh
