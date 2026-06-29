"""Shared helpers for the Azure Container Apps Jobs integration.

Used by both the operator (worker side) and the trigger (triggerer side) so the
HTTP/auth/poll logic lives in one place. Hardened for production fan-out:
retries with exponential backoff on throttling (429) and transient 5xx errors,
honoring the ARM `Retry-After` header.
"""
from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

import requests
from airflow.exceptions import AirflowException


DEFAULT_API_VERSION = "2024-03-01"

# Identifies executions started by this operator on the ARM Jobs API, so the
# ACA platform can attribute job executions to Airflow and adoption of this
# integration can be measured (see docs/why-airflow-on-aca-jobs.md).
OPERATOR_USER_AGENT = "airflow-on-aca-jobs-operator/0.1"

# Transient statuses worth retrying: ARM throttling (429) and gateway/5xx.
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

TERMINAL_SUCCESS = {"Succeeded"}
TERMINAL_FAILURE = {"Failed", "Canceled"}


class ACATokenExpired(Exception):
    """Raised on HTTP 401 so callers can refresh the ARM token and retry."""


@dataclass(frozen=True)
class ACAJobRef:
    subscription_id: str
    resource_group: str
    job_name: str

    @property
    def base_url(self) -> str:
        return (
            "https://management.azure.com/subscriptions/"
            f"{self.subscription_id}/resourceGroups/{self.resource_group}"
            f"/providers/Microsoft.App/jobs/{self.job_name}"
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "subscription_id": self.subscription_id,
            "resource_group": self.resource_group,
            "job_name": self.job_name,
        }

    @staticmethod
    def from_dict(data: Mapping[str, str]) -> "ACAJobRef":
        return ACAJobRef(
            subscription_id=data["subscription_id"],
            resource_group=data["resource_group"],
            job_name=data["job_name"],
        )

    @staticmethod
    def from_resource_id(resource_id: str) -> "ACAJobRef":
        pattern = (
            r"^/subscriptions/(?P<subscription_id>[^/]+)/resourceGroups/"
            r"(?P<resource_group>[^/]+)/providers/Microsoft\.App/jobs/"
            r"(?P<job_name>[^/]+)$"
        )
        match = re.match(pattern, resource_id, flags=re.IGNORECASE)
        if not match:
            raise AirflowException(
                "job_resource_id must match "
                "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.App/jobs/<job>"
            )
        parts = match.groupdict()
        return ACAJobRef(
            subscription_id=parts["subscription_id"],
            resource_group=parts["resource_group"],
            job_name=parts["job_name"],
        )


def headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": OPERATOR_USER_AGENT,
    }


def get_token(log: Any = None) -> str:
    # Demo/dev fallback: use a pre-fetched ARM token if provided. Avoids needing
    # azure-identity in constrained environments. The token is short-lived (~1h).
    env_token = os.environ.get("AZURE_ACCESS_TOKEN")
    if env_token:
        if log:
            log.info("Using ARM token from AZURE_ACCESS_TOKEN environment variable.")
        return env_token

    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential()
    access_token = credential.get_token("https://management.azure.com/.default")
    return access_token.token


def request_with_retry(
    method: str,
    url: str,
    token: str,
    *,
    json_body: dict[str, Any] | None = None,
    timeout: int = 30,
    max_retries: int = 6,
    backoff_base: float = 1.0,
    backoff_cap: float = 60.0,
    log: Any = None,
    sleep: Callable[[float], None] = time.sleep,
) -> requests.Response:
    """Issue an ARM request, retrying transient throttling/5xx with backoff.

    Honors the `Retry-After` response header when present. Raises
    ACATokenExpired on 401 so the caller can refresh the token.
    """
    last_response: requests.Response | None = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.request(
                method,
                url,
                headers=headers(token),
                json=json_body,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt >= max_retries:
                raise AirflowException(f"ARM request failed after retries: {exc}") from exc
            delay = min(backoff_base * (2 ** attempt), backoff_cap)
            if log:
                log.warning("ARM request error (%s); retry in %.1fs", exc, delay)
            sleep(delay)
            continue

        if response.status_code == 401:
            raise ACATokenExpired("ARM returned 401 (token expired or invalid).")

        if response.status_code in RETRYABLE_STATUS and attempt < max_retries:
            delay = _retry_delay(response, attempt, backoff_base, backoff_cap)
            if log:
                log.warning(
                    "ARM %s on %s; retry %d/%d in %.1fs",
                    response.status_code, url, attempt + 1, max_retries, delay,
                )
            last_response = response
            sleep(delay)
            continue

        return response

    return last_response  # type: ignore[return-value]


def _retry_delay(
    response: requests.Response,
    attempt: int,
    backoff_base: float,
    backoff_cap: float,
) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return min(float(retry_after), backoff_cap)
        except ValueError:
            pass
    return min(backoff_base * (2 ** attempt), backoff_cap)


def start_execution(
    job_ref: ACAJobRef,
    token: str,
    start_body: dict[str, Any],
    api_version: str,
    log: Any = None,
) -> str:
    url = f"{job_ref.base_url}/start?api-version={api_version}"
    response = request_with_retry(
        "POST", url, token, json_body=start_body, log=log
    )
    if response.status_code >= 300:
        raise AirflowException(
            "Failed to start ACA Job execution: "
            f"status={response.status_code}, body={response.text}"
        )
    payload = safe_json(response)
    execution_name = _extract_execution_name(payload, response.headers)
    if not execution_name:
        raise AirflowException(
            "ACA start response did not include execution name in body or headers."
        )
    if log:
        log.info("Started ACA Job execution '%s'", execution_name)
    return execution_name


def get_execution_state(
    job_ref: ACAJobRef,
    execution_name: str,
    token: str,
    api_version: str,
    log: Any = None,
) -> str:
    url = (
        f"{job_ref.base_url}/executions/{execution_name}"
        f"?api-version={api_version}"
    )
    response = request_with_retry("GET", url, token, log=log)
    if response.status_code >= 300:
        raise AirflowException(
            "Failed to fetch ACA Job execution state: "
            f"status={response.status_code}, body={response.text}"
        )
    payload = safe_json(response)
    state = payload.get("properties", {}).get("status")
    if not state:
        raise AirflowException(f"Execution status missing from response: {payload}")
    return state


def safe_json(response: requests.Response) -> dict[str, Any]:
    try:
        data = response.json()
        if not isinstance(data, dict):
            raise AirflowException(
                f"Expected JSON object response, got: {type(data).__name__}"
            )
        return data
    except (json.JSONDecodeError, ValueError) as exc:
        raise AirflowException(f"Invalid JSON response: {response.text}") from exc


def _extract_execution_name(
    payload: Mapping[str, Any],
    response_headers: Mapping[str, str],
) -> str | None:
    execution_name = payload.get("name")
    if isinstance(execution_name, str) and execution_name:
        return execution_name

    location = response_headers.get("Location") or response_headers.get("location")
    if not location:
        return None

    match = re.search(r"/executions/([^/?]+)", location)
    if not match:
        return None
    return match.group(1)
