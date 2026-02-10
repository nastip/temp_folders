# non_kraken_etl_test/powerbi_refresh.py

import time
from typing import Callable, Tuple

import requests

from .non_kraken_etl_setup import logger


def get_pbi_access_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
) -> str:
    """
    Get an Azure AD access token for the Power BI REST API
    using client credentials (service principal).
    """
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
    }

    resp = requests.post(token_url, data=data, timeout=30)
    resp.raise_for_status()

    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Could not obtain Power BI access token.")
    return token


def _poll_until_complete(
    get_status_fn: Callable[[], Tuple[str, str]],
    timeout_seconds: int = 1800,
    poll_interval_seconds: int = 60,
) -> None:
    """
    Generic poller: repeatedly call get_status_fn() until
    the refresh completes, fails, or we hit timeout.

    get_status_fn must return (status, detail).
    """
    start = time.monotonic()
    last_status_logged = None

    while True:
        status, detail = get_status_fn()
        raw_status = status or "Unknown"
        norm_status = raw_status.lower()

        # Only log when the status changes to avoid spamming the log
        if raw_status != last_status_logged:
            logger.info("Power BI refresh status='%s'.", raw_status)
            last_status_logged = raw_status

        # ---- Success states ----
        if norm_status in ("completed", "succeeded", "success"):
            logger.info("Power BI refresh completed with status '%s'.", raw_status)
            return

        # ---- Hard failure states ----
        if norm_status in ("failed", "cancelled", "disabled"):
            raise RuntimeError(
                f"Power BI refresh ended with status '{raw_status}': {detail}"
            )

        # ---- In-progress / unknown states ----
        # 'Unknown' is common while the refresh is still starting.
        elapsed = time.monotonic() - start
        if elapsed >= timeout_seconds:
            raise TimeoutError(
                f"Timed out after {timeout_seconds}s waiting for Power BI refresh. "
                f"Last status='{raw_status}'. Detail: {detail}"
            )

        logger.info(
            "Power BI refresh not finished yet (status='%s'). "
            "Waiting %ds before next check...",
            raw_status,
            poll_interval_seconds,
        )
        time.sleep(poll_interval_seconds)


def trigger_powerbi_dataset_refresh(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    workspace_id: str,
    dataset_id: str,
    *,
    wait_for_completion: bool = False,
    timeout_seconds: int = 1800,
    poll_interval_seconds: int = 60,
) -> None:
    """
    Trigger a Power BI dataset refresh.
    If wait_for_completion=True, poll until the refresh finishes or times out.
    """
    token = get_pbi_access_token(tenant_id, client_id, client_secret)

    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/"
        f"{workspace_id}/datasets/{dataset_id}/refreshes"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"notifyOption": "NoNotification"}

    logger.info(
        "Triggering Power BI dataset refresh (workspace_id=%s, dataset_id=%s)",
        workspace_id,
        dataset_id,
    )

    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        logger.error(
            "Power BI refresh trigger failed: HTTP %s – %s",
            resp.status_code,
            resp.text[:500],
        )
        resp.raise_for_status()

    logger.info("Power BI refresh triggered (HTTP %s).", resp.status_code)

    if not wait_for_completion:
        return

    # Prefer the per-refresh operation URL from the Location header if present
    operation_url = resp.headers.get("Location")

    if operation_url:
        logger.info("Polling refresh status via operation URL.")

        def _get_status_by_operation_url() -> Tuple[str, str]:
            s_resp = requests.get(
                operation_url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            s_resp.raise_for_status()
            data = s_resp.json()
            status = data.get("status") or "Unknown"
            detail = data.get("serviceExceptionJson") or ""
            return status, detail

        _poll_until_complete(
            _get_status_by_operation_url,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )
        return

    # Fallback: poll the latest entry from the refresh history
    logger.info(
        "No Location header returned from trigger. "
        "Falling back to polling dataset refresh history."
    )

    def _get_status_from_latest_refresh() -> Tuple[str, str]:
        list_url = (
            f"https://api.powerbi.com/v1.0/myorg/groups/"
            f"{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
        )
        l_resp = requests.get(
            list_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        l_resp.raise_for_status()
        data = l_resp.json()
        values = data.get("value") or []
        if not values:
            # Treat as still in progress if history hasn't updated yet
            return "Unknown", "No refresh history entries yet."
        latest = values[0]
        status = latest.get("status") or "Unknown"
        detail = latest.get("serviceExceptionJson") or ""
        return status, detail

    _poll_until_complete(
        _get_status_from_latest_refresh,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
