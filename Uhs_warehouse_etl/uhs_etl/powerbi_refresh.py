# uhs_etl/powerbi_refresh.py

import time  # Sleep between polls and track elapsed time.
from typing import Callable, Tuple  # Type hints for callback signatures and return values.

import requests  # HTTP client used to call Azure AD and Power BI REST APIs.

from .etl_setup import logger  # Shared ETL logger for consistent logging output.


def get_pbi_access_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
) -> str:
    """
    Get an Azure AD access token for the Power BI REST API
    using client credentials (service principal).

    Inputs:
      tenant_id: Azure AD tenant ID where the app is registered.
      client_id: Service principal (app) client ID.
      client_secret: Service principal secret.

    Outputs:
      Access token string to use in the Authorization header.

    Example:
      token = get_pbi_access_token(tenant_id, client_id, client_secret)
    """
    # This function performs the OAuth "client credentials" flow:
    # it uses the app ID + secret to request a short-lived bearer token.
    # Build the OAuth2 client-credentials token endpoint for the tenant.
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    # Standard client-credentials payload scoped to the Power BI API resource.
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
    }

    # Exchange the service principal credentials for a short-lived bearer token.
    resp = requests.post(token_url, data=data, timeout=30)
    resp.raise_for_status()

    # Token is required for every subsequent Power BI REST call.
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

    Inputs:
      get_status_fn: Callback that returns a (immutable) tuple of (status, detail).
      timeout_seconds: Max time to wait before raising TimeoutError.
      poll_interval_seconds: Seconds to sleep between polls.

    Outputs:
      None. Raises RuntimeError on failure states and TimeoutError on timeout.

    Example:
      def status_fn():
          return "Completed", ""
      _poll_until_complete(status_fn, timeout_seconds=600, poll_interval_seconds=30)
    """
    # Use monotonic time to avoid issues if the system clock changes.
    # It's a measure of time that is guaranteed to move forward continuously and never jump backward
    # (e.g., daylight saving time or manual clock adjustments).
    start = time.monotonic()
    # Track the last logged status to keep logs concise.
    last_status_logged = None

    while True:
        # This loop keeps polling until it hits a success, failure, or timeout.
        # The callback encapsulates the actual API call to get refresh state.
        status, detail = get_status_fn()
        raw_status = status or "Unknown"
        # Normalize for simple state comparisons.
        norm_status = raw_status.lower()

        # Only log when the status changes to avoid spamming the log
        if raw_status != last_status_logged:
            logger.info("Power BI refresh status='%s'.", raw_status)
            last_status_logged = raw_status

        # ---- Success states ----
        if norm_status in ("completed", "succeeded", "success"):
            # Any of these strings means Power BI considers the refresh done.
            logger.info("Power BI refresh completed with status '%s'.", raw_status)
            return

        # ---- Hard failure states ----
        if norm_status in ("failed", "cancelled", "disabled"):
            # These statuses indicate the refresh will not complete successfully.
            raise RuntimeError(
                f"Power BI refresh ended with status '{raw_status}': {detail}"
            )

        # ---- In-progress / unknown states ----
        # 'Unknown' is common while the refresh is still starting.
        # Enforce the overall timeout to prevent an infinite poll loop.
        elapsed = time.monotonic() - start
        if elapsed >= timeout_seconds:
            # Timeout is treated as a hard failure to avoid hanging forever.
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
        # Sleep between polls to avoid hammering the API.
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
    # This function does two things:
    # 1) Sends the REST API request to start a dataset refresh.
    # 2) Optionally polls Power BI until the refresh completes.
    Trigger a Power BI dataset refresh.
    If wait_for_completion=True, poll until the refresh finishes or times out.
    If wait_for_completion=False, then fire and forget -- proceed with next code block.

    Inputs:
      tenant_id: Azure AD tenant ID for token acquisition.
      client_id: Service principal (app) client ID.
      client_secret: Service principal secret.
      workspace_id: Power BI workspace (group) ID.
      dataset_id: Power BI dataset ID to refresh.
      wait_for_completion: If True, block until refresh completes or fails.
      timeout_seconds: Max time to wait when polling for completion.
      poll_interval_seconds: Seconds to sleep between status checks.

    Outputs:
      None. Raises RuntimeError on refresh failure and TimeoutError on timeout.

    Example:
      trigger_powerbi_dataset_refresh(
          tenant_id,
          client_id,
          client_secret,
          workspace_id,
          dataset_id,
          wait_for_completion=True,
          timeout_seconds=1800,
          poll_interval_seconds=60,
      )
    """
    # Acquire a bearer token used by both the trigger request and polling calls.
    # The token is short-lived and must be included in all Power BI API requests.
    token = get_pbi_access_token(tenant_id, client_id, client_secret)

    # Power BI REST endpoint to kick off a refresh for the dataset.
    # This is the official "refreshes" endpoint scoped to a workspace and dataset.
    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/"
        f"{workspace_id}/datasets/{dataset_id}/refreshes"
    )
    # Attach the bearer token and specify JSON payload for the request body.
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    # Suppress email notifications; orchestration handles logging.
    # Keeps notifications centralized in the ETL logs instead of Power BI emails.
    payload = {"notifyOption": "NoNotification"}

    logger.info(
        "Triggering Power BI dataset refresh (workspace_id=%s, dataset_id=%s)",
        workspace_id,
        dataset_id,
    )

    # Trigger the refresh; no body is returned on success.
    # A successful response means Power BI accepted the request, not that it completed.
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if not resp.ok:
        logger.error(
            "Power BI refresh trigger failed: HTTP %s %s",
            resp.status_code,
            resp.text[:500],
        )
        resp.raise_for_status()

    logger.info("Power BI refresh triggered (HTTP %s).", resp.status_code)

    # Exit early when caller only wants to fire-and-forget.
    # In this mode we do not poll Power BI for completion.
    if not wait_for_completion:
        return

    # Prefer the per-refresh operation URL from the Location header if present
    # (this is the most precise way to follow a single refresh instance).
    # If missing, we fall back to querying the refresh history list.
    operation_url = resp.headers.get("Location")

    if operation_url:
        logger.info("Polling refresh status via operation URL.")

        def _get_status_by_operation_url() -> Tuple[str, str]:
            # Use the operation URL to fetch the status of this exact refresh.
            # The operation endpoint returns a single refresh instance status.
            s_resp = requests.get(
                operation_url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            s_resp.raise_for_status()
            data = s_resp.json()
            status = data.get("status") or "Unknown"
            # serviceExceptionJson captures backend error details on failures.
            detail = data.get("serviceExceptionJson") or ""
            return status, detail

        _poll_until_complete(
            _get_status_by_operation_url,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )
        return

    # Fallback: poll the latest entry from the refresh history
    # This is less precise but works when Location is not provided.
    logger.info(
        "No Location header returned from trigger. "
        "Falling back to polling dataset refresh history."
    )

    def _get_status_from_latest_refresh() -> Tuple[str, str]:
        # Query the refresh history and use the most recent entry.
        # Query refresh history; $top=1 returns the most recent attempt.
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
        # serviceExceptionJson captures backend error details on failures.
        detail = latest.get("serviceExceptionJson") or ""
        return status, detail

    _poll_until_complete(
        _get_status_from_latest_refresh,
        timeout_seconds=timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
    )
