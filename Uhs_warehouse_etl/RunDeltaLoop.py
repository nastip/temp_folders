#!/usr/bin/env python
# coding: utf-8

"""
Continuously run an ETL pipeline + Power BI refresh in a loop.

Flow per iteration:
  1. Run ETL pipeline (e.g. 'delta', 'full', 'adhoc')
  2. Trigger + poll Power BI refresh (success or failure both count as 'completed')
  3. Sleep for the configured interval
  4. Repeat

This script is designed for scheduler-based execution where the process
remains alive and repeats work at a fixed cadence.
"""

import argparse  # Parse CLI arguments that control pipeline name, interval, and flags.
import atexit  # Register cleanup hooks to release the lock file on process exit.
import json  # Read/write lock file payloads as JSON.
import logging  # Build module-level logger and emit runtime messages.
import os  # Access environment variables and process metadata.
import re  # Sanitize folder/user names for safe lock file naming.
import socket  # Get hostname for the lock payload (who is holding the lock).
import sys  # Exit with a status code and access argv when needed.
import time  # Sleep between iterations and track timing.
import uuid  # Generate unique cycle ids per run.
from datetime import datetime  # Timestamp loop start/end in a readable format.
from getpass import getuser  # Get the OS user for lock ownership and logs.
from pathlib import Path  # Build OS-agnostic paths to logs and lock files.
from typing import NoReturn  # Explicit return type for non-returning methods.

# It declares the name and sets it to None as a placeholder so the script can later assign the real function in
# _init_etl(). That lazy setup avoids importing the ETL package until it’s needed and lets the script swap in the correct profile‑aware objects.
run_etl_pipeline = None  # Lazily injected ETL entrypoint; loaded in _init_etl().
default_sql_folder = None  # Resolved SQL folder name (from profiles/defaults).
default_user = None  # Resolved SQL login (from profiles/defaults).
sql_root = None  # Resolved root path for SQL scripts (profile/config aware).
trigger_powerbi_dataset_refresh = None  # Power BI refresh trigger function (set in _init_etl()).
read_secret_env = None  # Helper to read/decrypt env vars (set in _init_etl()).
logger = logging.getLogger(__name__)  # Module logger (replaced with ETL logger in _init_etl()).
active_profile_name = None  # Track the selected profile name for profile-scoped settings/state.
active_cycle_id = None  # Track the selected cycle id for the current run.

def _resolve_lock_dir() -> Path:
    # Prefer an explicit lock directory when provided by the environment.
    env_override = os.getenv("ETL_LOCK_DIR")
    if env_override:
        return Path(env_override)

    # Walk upward from the repo root to find a shared runtime folder, if present.
    repo_root = Path(__file__).resolve().parents[2]
    for parent in (repo_root, *repo_root.parents):
        runtime_dir = parent / "runtime"
        if runtime_dir.is_dir():
            return runtime_dir / "state"

    # Fallback to the repo-managed logs folder for local/dev use.
    return Path(__file__).resolve().parent / "logs"


LOCK_DIR = _resolve_lock_dir()  # Lock files live under runtime/state when available.


def _normalize_folder_name(name: str) -> str:
    # Replace any characters outside safe filename set with underscores.
    # This keeps lock file names filesystem-friendly across environments.
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def _resolve_runtime_state_dir() -> Path | None:
    # Find a shared runtime/state folder above the repo when present.
    repo_root = Path(__file__).resolve().parents[2]  # Start at the repo root.
    for parent in (repo_root, *repo_root.parents):  # Walk up through all parent folders.
        runtime_dir = parent / "runtime"  # Look for a sibling runtime folder at this level.
        if runtime_dir.is_dir():  # Only accept real folders (ignore missing paths).
            return runtime_dir / "state"  # Use the state subfolder for persisted state files.
    return None  # No runtime folder found anywhere above the repo.


def _resolve_pbi_refresh_counter_path(profile_name: str) -> tuple[Path, Path]:
    # Prefer runtime/state for the live counter, otherwise fall back to the template.
    repo_root = Path(__file__).resolve().parents[2]  # Start from the repo root path.
    template_path = (  # Template file stored with the repo (fallback).
        repo_root / "scripts" / "Setup" / "Pipeline_Setup" / "pbi_refresh_counter.json"
    )
    runtime_state_dir = _resolve_runtime_state_dir()  # Try to find a shared runtime/state folder.
    safe_profile = _normalize_folder_name(profile_name or "default")  # Make the profile name filename-safe.
    if runtime_state_dir is not None:  # If we found runtime/state, use it for the live counter.
        return runtime_state_dir / f"pbi_refresh_counter_{safe_profile}.json", template_path
    return template_path, template_path  # No runtime folder; use the template for both read/write.


def _load_pbi_refresh_policy() -> tuple[int | None, str | None, str | None, str]:
    # Read optional Power BI refresh policy settings from profile, then pipeline_config.json.
    _ensure_etl_init()  # Make sure ETL config is loaded so we know where the config file is.
    try:
        from uhs_etl import etl_setup as _etl_setup  # Import config module that knows config paths.
    except Exception:
        return None, None, None, "UTC"  # If import fails, treat as "no policy" with UTC default.

    profile = getattr(_etl_setup, "selected_profile", None)  # Get the active profile dict (if any).
    policy_keys = {  # Keys that define the refresh policy.
        "pbi_refresh_limit_per_day",
        "pbi_refresh_window_start",
        "pbi_refresh_window_end",
        "pbi_refresh_timezone",
    }
    config = {}  # Default to no policy until we find a valid one.
    if isinstance(profile, dict):  # Only read policy if the profile is a dict.
        policy = profile.get("pbi_refresh_policy") or {}  # Prefer the nested policy block when present.
        if isinstance(policy, dict) and policy:  # Use the nested policy if it has values.
            config = policy
        elif any(key in profile for key in policy_keys):  # Fall back to inline keys on the profile (keys placed directly on the profile, not inside pbi_refresh_policy).
            config = profile

    if not config:
        config_path = getattr(_etl_setup, "pipeline_config_path", None)  # Get the resolved config path.
        if not isinstance(config_path, Path) or not config_path.exists():  # Ensure it is a real file.
            return None, None, None, "UTC"  # Missing config means no policy; keep UTC default.

        try:
            with config_path.open("r", encoding="utf-8") as fh:  # Open the config JSON file.
                config = json.load(fh)  # Parse JSON into a dict.
        except Exception:
            return None, None, None, "UTC"  # Any read/parse error means no policy; keep UTC default.

    limit: int | None = None  # Default is no limit until we successfully parse one.
    raw_limit = config.get("pbi_refresh_limit_per_day")  # Read the raw value from config.
    if isinstance(raw_limit, int):  # If it's already a number, use it.
        limit = raw_limit if raw_limit > 0 else None  # Ignore zero/negative values.
    elif isinstance(raw_limit, str):  # If it's a string, try to parse it safely.
        raw_limit = raw_limit.strip()  # Remove whitespace so " 8 " works.
        if raw_limit.isdigit():  # Only accept pure digits to avoid bad values.
            parsed = int(raw_limit)  # Convert the string to an integer.
            limit = parsed if parsed > 0 else None  # Ignore zero/negative values.

    window_start = config.get("pbi_refresh_window_start")  # Read the raw window start value.
    if not isinstance(window_start, str) or not window_start.strip():  # Blank or non-string means "no window start".
        window_start = None  # Use None to indicate no configured start time.
    else:
        window_start = window_start.strip()  # Normalize by trimming whitespace.

    window_end = config.get("pbi_refresh_window_end")  # Read the raw window end value.
    if not isinstance(window_end, str) or not window_end.strip():  # Blank or non-string means "no window end".
        window_end = None  # Use None to indicate no configured end time.
    else:
        window_end = window_end.strip()  # Normalize by trimming whitespace.

    tz_name = config.get("pbi_refresh_timezone")  # Read the raw timezone value.
    if not isinstance(tz_name, str) or not tz_name.strip():  # Blank or non-string means "use default".
        tz_name = "UTC"  # Default to UTC so behavior is predictable.
    else:
        tz_name = tz_name.strip()  # Normalize by trimming whitespace.

    return limit, window_start, window_end, tz_name  # Return parsed policy values to the caller.


def _load_pbi_semantic_models() -> list[dict]:
    # Read optional semantic model list from the selected profile.
    _ensure_etl_init()  # Make sure ETL config is loaded before reading the profile.
    try:
        from uhs_etl import etl_setup as _etl_setup  # Import config module that exposes the profile.
    except Exception:
        return []  # If import fails, treat as "no models configured".

    profile = getattr(_etl_setup, "selected_profile", None)  # Pull the active profile.
    if not isinstance(profile, dict):  # If no valid profile dict, stop.
        return []  # No profile means no models configured.

    raw_models = profile.get("pbi_semantic_models")  # Read the raw model list from the profile.
    if not isinstance(raw_models, list):  # It must be a list to be valid.
        return []  # Any other type means no valid models.

    models: list[dict] = []  # Collect validated model entries here.
    for item in raw_models:  # Loop over each configured model entry.
        if not isinstance(item, dict):  # Each entry must be a dict.
            continue  # Skip invalid entries.
        workspace_id = item.get("workspace_id")  # Read workspace ID.
        dataset_id = item.get("dataset_id")  # Read dataset ID.
        if not isinstance(workspace_id, str) or not workspace_id.strip():  # Require non-empty workspace ID.
            continue  # Skip entries missing workspace ID.
        if not isinstance(dataset_id, str) or not dataset_id.strip():  # Require non-empty dataset ID.
            continue  # Skip entries missing dataset ID.
        models.append(  # Save a cleaned entry.
            {
                "workspace_id": workspace_id.strip(),  # Normalize whitespace.
                "dataset_id": dataset_id.strip(),  # Normalize whitespace.
                "is_primary": bool(item.get("is_primary")),  # Mark primary if true.
                "name": item.get("name", ""),  # Optional human label for logs.
            }
        )
    return models  # Return the cleaned list (may be empty if nothing valid).


def _load_pbi_refresh_state(state_path: Path, template_path: Path) -> dict:
    # Load the counter state; fall back to template or defaults on errors.
    for path in (state_path, template_path):  # Try the live counter first, then the template.
        try:
            if path.exists():  # Only try to read if the file exists.
                with path.open("r", encoding="utf-8") as fh:  # Open JSON file for reading.
                    payload = json.load(fh)  # Parse JSON into a Python object.
                if isinstance(payload, dict):  # We only accept dictionary-shaped payloads.
                    return payload  # Return the first valid payload we find.
        except Exception:
            continue  # Ignore read/parse errors and try the next path.
    return {"date": "", "count": 0}  # Fallback defaults when no valid file is found.


def _write_pbi_refresh_state(state_path: Path, payload: dict) -> None:
    # Persist the counter state; log failures but do not block the run.
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure the parent folder exists.
        with state_path.open("w", encoding="utf-8") as fh:  # Open the file for writing (overwrite).
            json.dump(payload, fh, indent=2)  # Write the payload as pretty JSON.
    except Exception:
        logger.exception("Failed to write Power BI refresh counter: %s", state_path)  # Log any write errors.


def _parse_hhmm(value: str) -> tuple[int, int] | None:
    # Parse an "HH:MM" string into (hour, minute).
    parts = value.split(":")  # Split the string into hours and minutes.
    if len(parts) != 2:  # We expect exactly two parts.
        return None  # Any other format is invalid.
    if not (parts[0].isdigit() and parts[1].isdigit()):  # Both parts must be numeric.
        return None  # Reject non-numeric values.
    hour = int(parts[0])  # Convert hour string to integer.
    minute = int(parts[1])  # Convert minute string to integer.
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:  # Validate time ranges.
        return None  # Reject invalid clock values.
    return hour, minute  # Return the parsed hour/minute pair.


def _get_tzinfo(tz_name: str):
    # Resolve a timezone name with a UTC fallback.
    try:
        from zoneinfo import ZoneInfo  # Python's built-in timezone database.
    except Exception:
        return datetime.utcnow().astimezone().tzinfo  # Fallback to system timezone if zoneinfo is unavailable.
    try:
        return ZoneInfo(tz_name)  # Look up and return the requested timezone.
    except Exception:
        logger.warning("Unknown timezone '%s'; defaulting to UTC.", tz_name)  # Log a warning for bad names.
        return ZoneInfo("UTC")  # Use UTC if the requested timezone is invalid.


def _reserve_pbi_refresh(limit: int, window_start: str | None, window_end: str | None, tz_name: str) -> bool:
    # Track and enforce the daily refresh limit and spacing when configured.
    profile_name = active_profile_name or "default"  # Use the active profile name, or "default" if none is set.
    state_path, template_path = _resolve_pbi_refresh_counter_path(profile_name)  # Find where the counter should live.
    state = _load_pbi_refresh_state(state_path, template_path)  # Load the last saved counter state.

    tzinfo = _get_tzinfo(tz_name)  # Resolve the timezone used for window calculations.
    now_local = datetime.now(tz=tzinfo)  # Current time in the configured timezone.
    today = now_local.date().isoformat()  # Current date string (YYYY-MM-DD).
    count = state.get("count")  # Read how many refreshes we have used today.
    if not isinstance(count, int):  # Guard against bad data types in the state file.
        count = 0  # Reset to zero if the stored value is invalid.
    last_date = state.get("date") if isinstance(state.get("date"), str) else ""  # Read last stored date safely.

    if last_date != today:  # New day means reset the counter.
        count = 0  # Start the day at zero refreshes.

    if window_start and window_end:  # Only enforce a window if both start and end are provided.
        parsed_start = _parse_hhmm(window_start)  # Convert the start time text into numbers.
        parsed_end = _parse_hhmm(window_end)  # Convert the end time text into numbers.
        if parsed_start is None or parsed_end is None:  # If either time failed to parse, skip window logic.
            logger.warning(
                "Invalid Power BI refresh window '%s' - '%s'; ignoring window.",
                window_start,
                window_end,
            )
        else:
            start_dt = now_local.replace(  # Build today's window start datetime.
                hour=parsed_start[0], minute=parsed_start[1], second=0, microsecond=0
            )
            end_dt = now_local.replace(  # Build today's window end datetime.
                hour=parsed_end[0], minute=parsed_end[1], second=0, microsecond=0
            )
            if end_dt <= start_dt:  # End must be after start to make a valid window.
                logger.warning(
                    "Power BI refresh window end '%s' is not after start '%s'; ignoring window.",
                    window_end,
                    window_start,
                )
            else:
                if now_local < start_dt or now_local > end_dt:  # Skip refreshes outside the window.
                    _write_pbi_refresh_state(state_path, {"date": today, "count": count})  # Persist the unchanged count.
                    logger.info(
                        "ETL succeeded but Power BI refresh skipped: outside window %s-%s (%s).",
                        window_start,
                        window_end,
                        tz_name,
                    )
                    return False  # Returning False tells the caller to skip the refresh this cycle.
                window_seconds = (end_dt - start_dt).total_seconds()  # Total window length in seconds.
                min_interval = window_seconds / limit  # Minimum spacing between refreshes.
                last_refresh_utc = state.get("last_refresh_utc")  # Read the last refresh timestamp from state.
                last_refresh_dt = None  # Default to "unknown" until we parse a value.
                if isinstance(last_refresh_utc, str) and last_refresh_utc:  # Only parse non-empty strings.
                    try:
                        last_refresh_dt = datetime.fromisoformat(last_refresh_utc)  # Parse ISO timestamp.
                    except Exception:
                        last_refresh_dt = None  # Treat invalid timestamps as missing.
                if last_refresh_dt is not None:  # Only enforce spacing if we have a previous refresh time.
                    elapsed = (datetime.utcnow() - last_refresh_dt).total_seconds()  # Seconds since last refresh.
                    if elapsed < min_interval:  # Too soon means we skip this refresh.
                        _write_pbi_refresh_state(  # Persist the unchanged count and last refresh time.
                            state_path,
                            {
                                "date": today,
                                "count": count,
                                "last_refresh_utc": last_refresh_utc,
                            },
                        )
                        logger.info(
                            "ETL succeeded but Power BI refresh skipped: spacing not met "
                            "(elapsed=%ss, min_interval=%ss).",
                            int(elapsed),
                            int(min_interval),
                        )
                        return False  # Returning False tells the caller to skip the refresh.

    if count >= limit:  # If we've used up today's quota, skip the refresh.
        _write_pbi_refresh_state(  # Save the unchanged count and last refresh time.
            state_path,
            {
                "date": today,
                "count": count,
                "last_refresh_utc": state.get("last_refresh_utc", ""),
            },
        )
        logger.warning(
            "ETL succeeded but Power BI refresh skipped: daily limit reached "
            "(count=%s, limit=%s, date=%s).",
            count,
            limit,
            today,
        )
        return False  # Returning False tells the caller to skip the refresh.

    count += 1  # Consume one refresh slot for today.
    _write_pbi_refresh_state(  # Save the updated count and the current refresh time.
        state_path,
        {
            "date": today,
            "count": count,
            "last_refresh_utc": datetime.utcnow().isoformat(timespec="seconds"),
        },
    )
    logger.info(  # Log that this refresh is allowed.
        "Power BI refresh allowed (count=%s/%s for %s).", count, limit, today
    )
    return True  # Returning True tells the caller to proceed with the refresh.


def _init_etl(profile_name: str | None) -> None:
    # Lazily import ETL components and bind them to module-level globals.
    # This avoids import side effects before we pick a profile.
    global run_etl_pipeline, default_sql_folder, default_user, sql_root
    global trigger_powerbi_dataset_refresh, read_secret_env, logger, active_profile_name

    if profile_name:
        # Set the profile name before importing etl_setup so defaults are resolved correctly.
        os.environ["ETL_PROFILE"] = profile_name

    # Import the ETL runner function from the package.
    from uhs_etl import run_etl_pipeline as _run_etl_pipeline
    # Import resolved defaults and the package logger.
    from uhs_etl.etl_setup import (
        default_sql_folder as _default_sql_folder,
        default_user as _default_user,
        logger as _etl_logger,
        sql_root as _sql_root,
    )
    # Import the Power BI refresh trigger helper.
    from uhs_etl.powerbi_refresh import (
        trigger_powerbi_dataset_refresh as _trigger_powerbi_dataset_refresh,
    )
    # Import the env var reader that handles DPAPI decryption.
    from uhs_etl.secure_env import read_secret_env as _read_secret_env

    # Bind the imported functions/config values to the module-level placeholders.
    # This makes them available to the rest of the script after init.
    run_etl_pipeline = _run_etl_pipeline
    default_sql_folder = _default_sql_folder
    default_user = _default_user
    sql_root = _sql_root
    trigger_powerbi_dataset_refresh = _trigger_powerbi_dataset_refresh
    read_secret_env = _read_secret_env
    logger = _etl_logger
    active_profile_name = profile_name or os.environ.get("ETL_PROFILE", "default")  # Pick the explicit profile, or fall back to env/default.


def _ensure_etl_init() -> None:
    # Make sure _init_etl() has been called before accessing ETL globals.
    # This avoids AttributeError/None usage when helper functions are called
    # before main() explicitly initializes the ETL runtime.
    if run_etl_pipeline is None:
        # Use the default profile when none was explicitly provided.
        _init_etl(None)


def _set_cycle_id_for_run(pipeline_name: str) -> str:
    # Generate and register a new cycle id for this ETL run.
    _ensure_etl_init()
    new_cycle_id = str(uuid.uuid4())
    global active_cycle_id
    active_cycle_id = new_cycle_id
    os.environ["ETL_CYCLE_ID"] = new_cycle_id
    try:
        from uhs_etl import etl_setup as _etl_setup
        if hasattr(_etl_setup, "set_cycle_id"):
            _etl_setup.set_cycle_id(new_cycle_id)
    except Exception:
        # Keep running even if we cannot sync the cycle id into the ETL module.
        pass
    logger.info("Cycle id for pipeline '%s' run: %s", pipeline_name, new_cycle_id)
    return new_cycle_id

# What happens when psutil is not available?
# If psutil isn’t available, _is_pid_active() logs a warning and returns True, which means the script assumes the existing lock is valid. 
# This will block any new ETL run unless --force-lock is used (or the lock is manually removed from json file). 
def _is_pid_active(pid: int) -> bool:
    # Check whether a process ID from a lock file still appears to be running.
    # If we cannot verify (missing psutil), assume it is active to be safe.
    _ensure_etl_init()
    try:
        import psutil
    except Exception:
        logger.warning(
            "psutil not available; cannot verify existing lock for pid=%s.", pid
        )
        return True

    try:
        # psutil lets us query process state by PID.
        proc = psutil.Process(pid)
        # Treat zombie as inactive even if the PID technically exists.
        return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
    except Exception:
        # Any lookup error means the PID is not active anymore.
        return False


def _read_lock(lock_path: Path) -> dict:
    # Load lock metadata from disk; returns empty dict if unreadable.
    _ensure_etl_init()
    try:
        # Lock files are JSON payloads written during _acquire_sql_folder_lock().
        with lock_path.open("r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        # Any parsing or I/O error means we treat it as missing/invalid. Returns an empty dictionary so caller handle gracefully finishes without crashing.
        return {}


# If a lock file is left behind and the script treats it as active, new runs that match the stuck lock will refuse to start unless you use --force-lock (or manually delete the lock). The _release_lock()
# helper only runs when the current process exits cleanly; it can’t fix a lock created by a previous crashed process.
def _release_lock(lock_path: Path) -> None:
    # Remove the lock file so future runs are allowed to start.
    _ensure_etl_init()
    try:
        # Only attempt deletion if the file still exists.
        if lock_path.exists():
            # unlink() deletes the file at the given path.
            lock_path.unlink()
    except Exception:
        # Log the failure but do not crash; a stuck lock can be removed manually.
        logger.exception("Failed to remove lock file: %s", lock_path)


def _acquire_sql_folder_lock(force: bool) -> bool:
    # Create a lock file that prevents multiple ETL runs for the same
    # SQL folder + SQL user from overlapping.
    _ensure_etl_init()
    # Ensure the lock directory exists; parents=True creates intermediate folders.
    # exist_ok=True avoids an error if the folder is already there.
    # See LOCK_DIR value at declarations section above.
    LOCK_DIR.mkdir(parents=True, exist_ok=True)
    # These are set in _init_etl(); assert to satisfy type checkers and guard runtime.
    assert default_sql_folder is not None
    assert default_user is not None
    # Normalize folder/user names so they are safe to use in a filename.
    lock_name = _normalize_folder_name(default_sql_folder)
    user_name = _normalize_folder_name(default_user)
    # Build the full lock file path, e.g. logs/etl_lock_<folder>__<user>.json.
    lock_path = LOCK_DIR / f"etl_lock_{lock_name}__{user_name}.json"

    # Payload saved into the lock file so we can inspect who holds the lock.
    payload = {
        "pid": os.getpid(),  # Current process ID so we can check if it's still running.
        "user": getuser(),  # OS username that started this run.
        "host": socket.gethostname(),  # Machine name where the run started.
        "sql_folder": default_sql_folder,  # SQL folder name for this run.
        "sql_root": str(sql_root),  # Full SQL root path for traceability.
        "sql_user": default_user,  # SQL login for this run.
        "started_utc": datetime.utcnow().isoformat(timespec="seconds"),  # Start time in UTC.
    }

    # Try twice: if a stale lock is removed on the first pass, the second
    # attempt can create a fresh lock file.
    for _ in range(2):
        try:
            # Open in exclusive-create mode ("x") so this fails if the file exists.
            with lock_path.open("x", encoding="utf-8") as fh:
                # Write the lock payload so we can inspect who owns the lock.
                json.dump(payload, fh, indent=2)
            # Ensure the lock is removed when this process exits normally e.g. when sys.exit() is called.
            # It calls the registered hook _release_lock
            atexit.register(_release_lock, lock_path)
            return True
        except FileExistsError:
            # Another process already created the lock; inspect it.
            existing = _read_lock(lock_path)
            # Pull the PID out of the lock file (0 means missing/invalid).
            existing_pid = int(existing.get("pid") or 0)
            # If the PID exists AND that process is still running,
            # treat the lock as active (not stale).
            if existing_pid and _is_pid_active(existing_pid):
                # If --force-lock aregument used
                if force:
                    # Force option: remove an active lock and retry.
                    logger.warning(
                        "Force override: removing active lock for SQL folder '%s' "
                        "and user '%s' (pid=%s).",
                        existing.get("sql_folder", default_sql_folder),
                        existing.get("sql_user", default_user),
                        existing_pid,
                    )
                    _release_lock(lock_path)
                    continue
                # No force option: block the run to avoid overlapping ETL.
                logger.error(
                    "Refusing to start: SQL folder '%s' with SQL user '%s' "
                    "is already in use by pid=%s (host=%s, user=%s).",
                    existing.get("sql_folder", default_sql_folder),
                    existing.get("sql_user", default_user),
                    existing_pid,
                    existing.get("host", "unknown"),
                    existing.get("user", "unknown"),
                )
                return False
            # Lock exists but PID is not active; treat as stale and remove.
            logger.warning(
                "Removing stale lock file for SQL folder '%s' (pid=%s).",
                existing.get("sql_folder", default_sql_folder),
                existing_pid or "unknown",
            )
            _release_lock(lock_path)
        except Exception:
            # Unexpected errors creating the lock should abort startup.
            logger.exception("Failed to create lock file: %s", lock_path)
            return False

    # If we got here, we tried and still couldn't create a lock.
    logger.error("Unable to acquire lock for SQL folder '%s'.", default_sql_folder)
    return False


def run_once(
    pipeline_name: str,
    wait_for_pbi: bool = True,
    pbi_timeout_seconds: int = 1800,
    pbi_poll_interval_seconds: int = 60,
) -> bool:
    # Called by main() for each loop iteration (and optional bootstrap run).
    """
    Run a single ETL + optional Power BI refresh cycle for the given pipeline.

    - Logs errors but does NOT raise, so the outer loop can keep going.
    - ETL failure: log and skip Power BI refresh this iteration.
    - Power BI failure/timeout: log and move on to next iteration.

    Inputs:
      pipeline_name: Pipeline key to run (e.g., "delta", "full", "adhoc").
      wait_for_pbi: If True, poll Power BI until refresh completes or times out.
      pbi_timeout_seconds: Max seconds to wait for Power BI refresh completion.
      pbi_poll_interval_seconds: Seconds between Power BI status checks.

    Outputs:
      True if ETL completed successfully, False otherwise.

    Example:
      run_once("delta", wait_for_pbi=True, pbi_timeout_seconds=1800, pbi_poll_interval_seconds=60)
    """
    _ensure_etl_init()
    cycle_id = _set_cycle_id_for_run(pipeline_name)
    logger.info("=== Cycle id: %s ===", cycle_id)
    logger.info("=== Starting ETL pipeline '%s' ===", pipeline_name)

    try:
        # Ensure ETL runner is initialized before calling it.
        assert run_etl_pipeline is not None
        # Run the selected ETL pipeline and capture whether it succeeded.
        etl_success = run_etl_pipeline(
            pipeline_name,
            fail_fast_exit=False,   # don't sys.exit() inside ETL. Tells the ETL runner not to call sys.exit() on failure. Instead, it should return a success flag (True/False) so this wrapper can decide what to do next (log, skip  refresh, continue loop) without killing the whole process.
        )
    except Exception:
        # If ETL crashes, log the stack trace and skip the refresh.
        logger.exception("ETL pipeline '%s' crashed.", pipeline_name)
        return False

    if not etl_success:
        # ETL reported failure (but didn't crash), so skip Power BI refresh.
        logger.error(
            "ETL pipeline '%s' reported failure. Skipping Power BI refresh this cycle.",
            pipeline_name,
        )
        return False

    # If we reached here, the ETL run finished successfully.
    logger.info("ETL pipeline '%s' completed successfully.", pipeline_name)

    # ---- Power BI daily limit (optional) ----
    # Gate the Power BI refresh: if the policy says "not now" we return early,
    # which skips the refresh for this cycle but keeps the ETL loop running.
    pbi_limit, window_start, window_end, tz_name = _load_pbi_refresh_policy()
    if pbi_limit is not None and not _reserve_pbi_refresh(pbi_limit, window_start, window_end, tz_name):
        return True

    # ---- Power BI config from environment ----
    # Read Power BI IDs/secrets from environment variables.
    # Values may be stored as plain text or as DPAPI-protected strings
    # (prefixed with "dpapi:") which are decrypted per user.
    env_vars = [
        "PBI_TENANT_ID",
        "PBI_CLIENT_ID",
        "PBI_CLIENT_SECRET",
    ]
    # pbi_values collects the decrypted/usable values.
    pbi_values: dict[str, str] = {}
    # missing collects env var names that are blank or not set.
    missing: list[str] = []
    # decrypt_errors collects env var names that failed to decrypt/read.
    decrypt_errors: list[str] = []

    # Ensure the helper is initialized before calling it.
    assert read_secret_env is not None
    for name in env_vars:
        # Attempt to read each env var; read_secret_env handles DPAPI if present.
        try:
            value = read_secret_env(name)
        except Exception as exc:
            # Keep track of which variables failed to decrypt/read.
            decrypt_errors.append(f"{name} ({exc})")
            continue
        if not value:
            # Empty or missing value counts as missing.
            missing.append(name)
            continue
        # Store the usable value for the refresh call.
        pbi_values[name] = value

    # If any secrets failed to decrypt/read, skip refresh and log why.
    if decrypt_errors:
        logger.error(
            "ETL succeeded but Power BI refresh skipped this cycle: "
            "failed to read/decrypt env var(s): %s",
            ", ".join(decrypt_errors),
        )
        return True

    # If required env vars are missing/blank, skip refresh and log which ones.
    if missing:
        logger.error(
            "ETL succeeded but Power BI refresh skipped this cycle: "
            "missing env var(s): %s",
            ", ".join(missing),
        )
        return True

    # Load semantic models from the selected profile (or fallback to env vars).
    models = _load_pbi_semantic_models()  # Read the list from the profile config.
    if not models:  # If the profile did not define any models, try the env vars.
        try:
            workspace_id = read_secret_env("PBI_WORKSPACE_ID")  # Read workspace ID from env.
        except Exception:
            workspace_id = ""  # Treat any read error as missing value.
        try:
            dataset_id = read_secret_env("PBI_DATASET_ID")  # Read dataset ID from env.
        except Exception:
            dataset_id = ""  # Treat any read error as missing value.
        if workspace_id and dataset_id:  # Only build a model if both IDs exist.
            models = [
                {
                    "workspace_id": workspace_id,  # Workspace for the refresh.
                    "dataset_id": dataset_id,  # Dataset for the refresh.
                    "is_primary": True,  # Only model available, so it is primary.
                    "name": "env-default",  # Label for logs.
                }
            ]

    if not models:  # Still no models means we cannot refresh anything.
        logger.error(
            "ETL succeeded but Power BI refresh skipped this cycle: "
            "no semantic models configured for profile '%s'.",
            active_profile_name or "default",
        )
        return True  # Exit early so no refresh attempt is made.

    # ---- Trigger Power BI refresh (optionally waiting for completion) ----
    try:
        # Ensure refresh helper is initialized before calling it.
        assert trigger_powerbi_dataset_refresh is not None  # Safety check: refresh function must be set.
        primary_models = [m for m in models if m.get("is_primary")]  # Collect models marked as primary.
        primary_model = primary_models[0] if primary_models else models[0]  # Pick the primary (or first) model.
        secondary_models = [m for m in models if m is not primary_model]  # All other models are secondary.

        # In wait mode, fire secondary refreshes first, then wait on the primary.
        ordered_models = secondary_models + [primary_model] if wait_for_pbi else models

        for model in ordered_models:
            is_primary = model is primary_model
            trigger_powerbi_dataset_refresh(
                tenant_id=pbi_values["PBI_TENANT_ID"],
                client_id=pbi_values["PBI_CLIENT_ID"],
                client_secret=pbi_values["PBI_CLIENT_SECRET"],
                workspace_id=model["workspace_id"],
                dataset_id=model["dataset_id"],
                wait_for_completion=wait_for_pbi and is_primary,
                timeout_seconds=pbi_timeout_seconds,
                poll_interval_seconds=pbi_poll_interval_seconds,
            )
            logger.info(
                "Power BI refresh triggered for dataset '%s' (name=%s, primary=%s, wait=%s).",
                model.get("dataset_id"),
                model.get("name", ""),
                is_primary,
                wait_for_pbi and is_primary,
            )

        logger.info("Power BI refresh cycle finished (success or completed status).")
    except Exception:
        # Acceptable for you: failed OR successful; we log and continue loop
        logger.exception("Power BI dataset refresh failed or timed out this cycle.")
    return True


def main(
    pipeline_name: str = "delta",  # Primary pipeline to run in the loop.
    interval_minutes: int = 1,  # Minutes to sleep between iterations.
    bootstrap_pipeline: str | None = None,  # Optional one-time pipeline before looping.
    run_once_only: bool = False,  # If True, run once and exit (no loop).
    force_lock: bool = False,  # If True, override an existing active lock.
    profile_name: str | None = None,  # Optional profile name from etl_profiles.json.
    wait_for_pbi: bool = True,  # If False, trigger refresh and do not wait.
) -> int:
    """
    Main loop: repeatedly run ETL + PBI refresh, then sleep.

    pipeline_name   e.g. 'delta', 'full', 'adhoc'
    interval_minutes  minutes between cycles, measured AFTER refresh completes.
    bootstrap_pipeline  optional pipeline to run once before looping.
    run_once_only  if True, run a single iteration and exit.
    force_lock  if True, remove an existing active lock (use with care).
    profile_name  optional named profile to override defaults (db/user/sql folder).
    wait_for_pbi  if False, trigger Power BI refresh and continue without waiting.

    Inputs:
      pipeline_name: Pipeline key to run for each loop iteration.
      interval_minutes: Minutes between iterations (sleep occurs after refresh or immediately if not waiting).
      bootstrap_pipeline: Optional pipeline to run once before the loop starts.
      run_once_only: If True, run a single iteration and exit.
      force_lock: If True, override an existing lock (even if active).
      profile_name: Name from etl_profiles.json to select defaults.
      wait_for_pbi: Whether to block until Power BI refresh completes.

    Outputs:
      Exit code integer (0 for normal exit path).

    Example:
      main(pipeline_name="full", interval_minutes=10)
      main(pipeline_name="delta", interval_minutes=5, bootstrap_pipeline="full")
      main(pipeline_name="full", interval_minutes=1, run_once_only=True)
      main(pipeline_name="delta", interval_minutes=5, wait_for_pbi=False)
    """
    # Initialize ETL globals (runner, defaults, logger) using the selected profile.
    _init_etl(profile_name)

    # Convert minutes to seconds for time.sleep().
    interval_seconds = interval_minutes * 60
    # Try to get the lock; if we cannot, exit with a non-zero code.
    if not _acquire_sql_folder_lock(force_lock):
        return 1

    if run_once_only:
        # --once means "run exactly one iteration" and exit afterward.
        if bootstrap_pipeline:
            # Bootstrap is ignored in one-shot mode because we only run one pipeline.
            logger.warning(
                "Ignoring bootstrap pipeline '%s' because --once was set.",
                bootstrap_pipeline,
            )
        logger.info(
            "Running a single iteration for pipeline '%s' and exiting.",
            pipeline_name,
        )
        # Execute a single ETL + optional Power BI refresh cycle.
        once_start = time.perf_counter()
        etl_success = run_once(
            pipeline_name=pipeline_name,
            wait_for_pbi=wait_for_pbi,
            pbi_timeout_seconds=1800,
            pbi_poll_interval_seconds=60,
        )
        duration_s = time.perf_counter() - once_start
        logger.info(
            "Loop iteration summary: pipeline='%s' cycle_id=%s outcome=%s duration=%.2fs.",
            pipeline_name,
            active_cycle_id or "unknown",
            "success" if etl_success else "failed",
            duration_s,
        )
        return 0

    if bootstrap_pipeline:
        # Run a one-time pipeline before entering the main loop (if requested).
        logger.info(
            "Running bootstrap pipeline '%s' once before starting '%s' loop.",
            bootstrap_pipeline,
            pipeline_name,
        )
        # This uses the same Power BI waiting behavior as the main loop.
        bootstrap_start = time.perf_counter()
        bootstrap_success = run_once(
            pipeline_name=bootstrap_pipeline,
            wait_for_pbi=wait_for_pbi,
            pbi_timeout_seconds=1800,
            pbi_poll_interval_seconds=60,
        )
        duration_s = time.perf_counter() - bootstrap_start
        logger.info(
            "Loop iteration summary: pipeline='%s' cycle_id=%s outcome=%s duration=%.2fs.",
            bootstrap_pipeline,
            active_cycle_id or "unknown",
            "success" if bootstrap_success else "failed",
            duration_s,
        )

    # Log the start of the continuous loop and its cadence.
    logger.info(
        "Starting '%s' loop with interval=%d minutes between cycles.",
        pipeline_name,
        interval_minutes,
    )

    # Infinite loop; Task Scheduler starts the process once and it self-perpetuates.
    while True:
        # Record the start time for logging/debugging.
        started = datetime.now().isoformat(timespec="seconds")
        iteration_start = time.perf_counter()
        logger.info(
            "=== Loop iteration for pipeline '%s' (cycle id=%s) started at %s ===",
            pipeline_name,
            active_cycle_id or "unknown",
            started,
        )

        # Run the ETL + Power BI refresh cycle once.
        etl_success = run_once(
            pipeline_name=pipeline_name,
            wait_for_pbi=wait_for_pbi,
            pbi_timeout_seconds=1800,
            pbi_poll_interval_seconds=60,
        )
        duration_s = time.perf_counter() - iteration_start

        # Log that the iteration is done and pause before the next cycle.
        logger.info(
            "Loop iteration summary: pipeline='%s' cycle_id=%s outcome=%s duration=%.2fs. "
            "Sleeping %d seconds before next run.",
            pipeline_name,
            active_cycle_id or "unknown",
            "success" if etl_success else "failed",
            duration_s,
            interval_seconds,
        )
        # Sleep keeps the loop at the requested cadence.
        time.sleep(interval_seconds)


class _LoggingArgumentParser(argparse.ArgumentParser):
    # ArgumentParser that logs errors before exiting.
    def error(self, message: str) -> NoReturn:
        logger.error("Argument parsing error: %s", message)
        self.exit(2, f"{self.prog}: error: {message}\n")


def _preparse_profile(argv: list[str]) -> str | None:
    # Minimal parse to capture --profile before full parsing.
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--profile", default=None)
    args, _ = pre_parser.parse_known_args(argv)
    return args.profile


if __name__ == "__main__":
    # Optional: allow pipeline name + interval override via CLI arguments.
    #
    # Examples:
    #   python RunDeltaLoop.py
    #       -> pipeline='delta', interval=1 mins
    #
    #   python RunDeltaLoop.py full
    #       -> pipeline='full', interval=1 mins
    #
    #   python RunDeltaLoop.py delta 10
    #       -> pipeline='delta', interval=10 mins
    #
    #   python RunDeltaLoop.py adhoc 60
    #       -> pipeline='adhoc', interval=60 mins
    #
    # Optional flags:
    #   --bootstrap PIPELINE
    #       -> run PIPELINE once before looping with the main pipeline
    #   --once
    #       -> run a single iteration and exit (no loop)
    #
    # Examples:
    #   python RunDeltaLoop.py delta 5 --bootstrap full
    #       -> run full once, then loop delta every 5 minutes
    #
    #   python RunDeltaLoop.py full --once
    #       -> run full once, then exit
    pre_profile = _preparse_profile(sys.argv[1:])  # Read profile early so logging is configured.
    _init_etl(pre_profile)  # Initialize ETL logger before full arg parsing.
    # Build the command-line interface for this script.
    parser = _LoggingArgumentParser(description="Run ETL in a loop.")
    parser.add_argument(
        "pipeline",
        nargs="?",
        default="delta",
        help="Pipeline key to run (delta/full/adhoc).",
    )
    # The first positional argument selects the pipeline name.
    parser.add_argument(
        "interval",
        nargs="?",
        type=int,
        default=1,
        help="Minutes between cycles (after refresh completes).",
    )
    # The second positional argument sets the sleep interval in minutes.
    parser.add_argument(
        "--bootstrap",
        dest="bootstrap_pipeline",
        default=None,
        help="Pipeline to run once before the main loop starts.",
    )
    # Optional flag: run one pipeline once before entering the loop.
    parser.add_argument(
        "--profile",
        default=None,
        help="ETL profile name from scripts/Setup/Pipeline_Setup/etl_profiles.json.",
    )
    # Optional flag: choose a named profile that overrides defaults.
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single iteration and exit.",
    )
    # Optional flag: stop after one iteration (no infinite loop).
    parser.add_argument(
        "--force-lock",
        action="store_true",
        help="Override an existing lock for the same SQL folder and user.",
    )
    # Optional flag: remove a lock and continue even if another run is active.
    parser.add_argument(
        "--no-wait-pbi",
        action="store_true",
        help="Trigger Power BI refresh and continue without waiting for completion.",
    )
    # Optional flag: fire-and-forget the Power BI refresh.

    # Parse command-line args into the args object.
    args = parser.parse_args()
    if args.pipeline == "prod":
        if args.once:
            logger.warning(
                "Ignoring --once for 'prod' alias (prod is intended for looping).",
            )
            args.once = False
        if args.bootstrap_pipeline is None:
            args.bootstrap_pipeline = "full"
        args.pipeline = "delta"

    # Exit code is propagated to the scheduler (0 for normal exit path).
    sys.exit(
        main(
            pipeline_name=args.pipeline,  # Pipeline name from CLI.
            interval_minutes=args.interval,  # Minutes between iterations.
            bootstrap_pipeline=args.bootstrap_pipeline,  # Optional one-time bootstrap.
            run_once_only=args.once,  # Single-iteration mode.
            force_lock=args.force_lock,  # Allow overriding an active lock.
            profile_name=args.profile,  # Profile name override.
            wait_for_pbi=not args.no_wait_pbi,  # True means wait; False means fire-and-forget.
        )
    )
