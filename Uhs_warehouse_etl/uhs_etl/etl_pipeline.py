# uhs_etl/etl_pipeline.py

import time  # Sleep between retries and measure elapsed time.
import sys  # Exit codes and process-level behaviors when needed.
import re  # Parse and normalize strings (e.g., SQL file naming).
import uuid  # Generate per-cycle GUIDs when not provided by caller.
from concurrent.futures import ThreadPoolExecutor, as_completed  # Run work in parallel threads.
from pathlib import Path  # Build filesystem paths in a cross-platform way.
from threading import Event  # Signal cancellation across concurrent workers.
from datetime import datetime, timezone  # Timestamp logs and handle UTC conversions.
from time import perf_counter  # High-resolution timer for execution duration.
from tempfile import TemporaryDirectory  # Create temp folders for intermediate outputs.

from typing import Union, Sequence, Optional, Literal, cast  # Type hints and safe casting.
import pyodbc  # ODBC database driver used by Kraken connectors.
import pandas as pd  # DataFrame utilities for SQL results and reporting.

import kraken  # UHS data access wrapper for SQL execution.
# from kraken.support.support import _load_filepaths  # Optional helper (not used).
from kraken.exceptions import DatabaseConnectionError  # Specific error type for retry logic.


from .etl_setup import (
    logger,  # Shared logger instance for ETL messages.
    default_db,  # Default SQL connection alias (from config/profile).
    default_user,  # Default SQL username for connections.
    default_isolationlevel,  # Default transaction isolation level.
    default_untagged_run_mode,  # Default behaviour: How to run files without explicit tags.
    profile_name,  # Active profile name for logging metadata.
    get_cycle_id,  # Active cycle id for grouping a run.
    set_cycle_id,  # Setter used when no cycle id is provided.
    sql_dirs,  # Map of stage names to SQL directory paths.
    PIPELINE_STAGE_SETS,  # Ordered stage lists for each pipeline (delta/full/etc.).
)

# This module orchestrates staged SQL execution through Kraken connectors.
# It provides retry logic for transient connection errors and structured logging
# for both per-file execution and overall pipeline runs.

# Kraken's create_connector expects a specific set of isolation level strings.
IsolationLevel = Literal[
    "SERIALIZABLE",
    "REPEATABLE READ",
    "READ COMMITTED",
    "READ UNCOMMITTED",
    "AUTOCOMMIT",
]

# -----------------------------------
# Helper: connection error detection
#         the helper decides whether an error is retryable
# -----------------------------------
def _is_connection_error(exc: Exception) -> bool:
    """
    Return True if this looks like a transient connection issue
    (TCP, login timeout, SSL, etc.), False otherwise.

    Inputs:
      exc: The exception raised by a connection or query attempt.

    Outputs:
      True when the error appears to be connection-related and retryable;
      False for other error types (syntax, permissions, constraints).

    Example:
      try:
          ...
      except Exception as exc:
          if _is_connection_error(exc):
              ...
    """
    # This helper decides whether we should retry after an error.
    # It only returns True for errors that look like temporary connection problems.
    # Kraken raises a specific connection error type that we treat as retryable.
    if isinstance(exc, DatabaseConnectionError):
        return True

    # pyodbc uses OperationalError / InterfaceError for connection issues.
    if isinstance(exc, (pyodbc.OperationalError, pyodbc.InterfaceError)):
        return True

    # Fallback to string matching for common connection failure signatures.
    msg = str(exc).lower()
    markers = (
        "tcp provider",
        "login timeout",
        "timeout error",
        "prelogin",
        "client unable to establish connection",
        "ssl provider",
        "connection was forcibly closed",
        "could not connect to",
    )
    # If any marker string appears in the error message, treat it as retryable.
    return any(m in msg for m in markers)


def _test_connection(
    stage: str,
    username: str,
    isolationlevel: str,
) -> bool:
    """
    Cheap connection test: create a connector, run SELECT 1, close it.
    Returns True if OK, False if it's a connection-type failure.
    Non-connection errors are treated as fatal and re-raised.

    Inputs:
      stage: Stage name for logging context.
      username: SQL username to use for the test connection.
      isolationlevel: Isolation level used by the connector.

    Outputs:
      True if the connection test succeeds.
      False if a connection-type error occurs.
      Raises for non-connection errors (permissions, syntax, etc.).

    Example:
      if not _test_connection("facts_populate", "sqlkraken", "AUTOCOMMIT"):
          ...
    """
    # This function does a very small "ping" to the database to see if it works.
    # It avoids running any real ETL logic when connectivity is broken.
    connector = None
    try:
        # Create a connector without autoclose so we can explicitly close.
        connector = kraken.create_connector(
            alias=default_db,
            autoclose=False,
            isolation_level=cast(IsolationLevel, isolationlevel),
            username=username,
        )
        # Explicit connect + trivial query for a low-cost validation.
        connector.connect()
        connector.execute("SELECT 1 AS [TestConnection];", query_name="TestConnection")

        # If we get here, the database connection is working.
        logger.info("Stage '%s': connection test succeeded.", stage)
        return True

    except Exception as exc:
        # If it looks like a connection glitch, return False so caller can retry.
        if _is_connection_error(exc):
            logger.warning(
                "Stage '%s': connection test failed: %s",
                stage,
                exc,
            )
            return False

        # Something else (e.g. permissions) - don't hide it
        raise

    finally:
        if connector is not None:
            try:
                # Ensure we release any partially opened connections.
                connector.close_connection()
            except Exception:
                # Don't let cleanup errors hide the original result.
                logger.debug(
                    "Ignoring error while closing connector after connection test.",
                    exc_info=True,
                )
                
def _wrap_file_for_stage(src: Path, stage: str) -> str:
    """
    Read the raw SQL file and wrap it with Kraken metadata + logging + TRY/CATCH
    BEFORE it is passed to kraken.extract_sql.

    RuleName  -> file name without extension
    RuleGroup -> folder name of sql_dirs[stage] (e.g. '04_Dimensions')

    Inputs:
      src: Path to the raw .sql file.
      stage: Stage name used to resolve the SQL directory and RuleGroup.

    Outputs:
      A single SQL string with metadata header + TRY/CATCH wrapper.

    Example:
      wrapped_sql = _wrap_file_for_stage(Path("003_Load_To_Output.sql"), "facts_populate")
    """
    # Load the raw SQL text and trim trailing whitespace.
    raw_sql = src.read_text(encoding="utf-8").strip()
    rule_name = src.stem

    # RuleGroup uses the stage folder name to match logging expectations.
    stage_dir = Path(sql_dirs[stage])
    rule_group = stage_dir.name

    # Escape any single quotes to avoid breaking logging stored procedures.
    safe_rule_name = rule_name.replace("'", "''")
    safe_rule_group = rule_group.replace("'", "''")
    safe_profile_name = (profile_name or "default").replace("'", "''")
    raw_cycle_id = get_cycle_id()
    safe_cycle_id = raw_cycle_id.replace("'", "''") if raw_cycle_id else ""
    cycle_id_value = f"'{safe_cycle_id}'" if safe_cycle_id else "NULL"

    # Kraken metadata + LogExecutionStart establishes logging context per file.
    header = f"""--$Database = {default_db}
--$Dataframe = {rule_name}
--$Split=FALSE
--$Isolation_level = AUTOCOMMIT

EXEC dbo.LogExecutionStart  
     @RuleName    = '{safe_rule_name}',  
     @RuleGroup   = '{safe_rule_group}',
     @ProfileName = '{safe_profile_name}',
     @CycleId     = {cycle_id_value};

BEGIN TRY

"""
    # LogExecutionResult on success and capture error message in CATCH.
    footer = f"""

EXEC dbo.LogExecutionResult  
       @RuleName     = '{safe_rule_name}',  
       @IsSuccess    = 1,  
       @ErrorMessage = NULL,
       @ProfileName  = '{safe_profile_name}',
       @CycleId      = {cycle_id_value};	

END TRY
BEGIN CATCH
    DECLARE @msg NVARCHAR(MAX) = ERROR_MESSAGE();
    EXEC dbo.LogExecutionResult
       @RuleName      = '{safe_rule_name}',  
       @IsSuccess     = 0,  
       @ErrorMessage  = @msg,
       @ProfileName   = '{safe_profile_name}',
       @CycleId       = {cycle_id_value};
END CATCH;
"""

    return header + raw_sql + footer


# Tag pattern like "MyScript[2].sql" -> numeric ordering group "2".
_RUN_TAG_RE = re.compile(r"\[(\d+)\]$")


def _extract_run_tag(path: Path) -> int | None:
    """
    Extract the numeric tag from a filename suffix like "MyScript[2].sql".

    Inputs:
      path: Path object for the SQL file.

    Outputs:
      The integer tag if present, otherwise None.

    Example:
      _extract_run_tag(Path("Something[10].sql")) -> 10
    """
    # Look for a number inside square brackets in the filename (without extension).
    match = _RUN_TAG_RE.search(path.stem)
    if match:
        # Convert the captured number text into a real integer.
        return int(match.group(1))
    # No tag found.
    return None


def _execute_sql_with_new_connector(
    sqlfile,  # SQL file object/path to run.
    stage: str,  # Stage name for logging context.
    username: str,  # SQL user to connect with.
    isolationlevel: str,  # Transaction isolation level for the connection.
    retry_window_seconds: int = 900,   # 15 minutes max retry window.
    retry_interval_seconds: int = 60,  # 60s between connection tests.
    cancel_event: Event | None = None,  # Optional signal to stop retries early.
):
    """
    Execute ONE parsed sqlfile with its own connector.

    - Retries only for connection-type errors, within a time window.
    - No retries for SQL/syntax/constraint errors: these bubble up immediately.
    - If the retry window expires, this sets cancel_event (if provided) and raises.

    Inputs:
      sqlfile: Kraken SQL object (has .sql and optional .df_name).
      stage: Stage name for logging context.
      username: SQL username for the connector.
      isolationlevel: Isolation level for the connector.
      retry_window_seconds: Max retry window for connection errors.
      retry_interval_seconds: Sleep between connection tests during retries.
      cancel_event: Optional Event that, when set, aborts further work.

    Outputs:
      The result returned by kraken connector execute(), or None if cancelled.
      Raises on non-connection errors or when the retry window is exceeded.

    Example:
      _execute_sql_with_new_connector(sqlfile, "facts_populate", "sqlkraken", "AUTOCOMMIT")
    """
    # Pull a friendly name for logging; fall back to "unknown" if missing.
    file_name = getattr(sqlfile, "df_name", "unknown")
    # Start a monotonic timer to measure how long this file takes.
    start_ts = time.monotonic()

    def _single_attempt():
        # Inner helper: try to run the SQL once with a new connector.
        connector = None
        try:
            # Use a fresh connector per file to isolate failures.
            connector = kraken.create_connector(
                alias=default_db,
                autoclose=False,
                isolation_level=cast(IsolationLevel, isolationlevel),
                username=username,
            )
            connector.connect()

            logger.info(
                "Stage '%s' starting file '%s'",
                stage, file_name
            )

            # Run the SQL text as a single statement/batch in Kraken.
            result = connector.execute(sqlfile.sql, query_name=file_name)

            logger.info(
                "Stage '%s' finished file '%s' successfully",
                stage, file_name
            )
            # Return the execution result to the caller.
            return result

        # This runs no matter what happened in the try/except (success or failure)
        # The “ignore error” part is when failures happen while closing the connector e.g.  the connection already dropped, so close_connection() throws
        # This doesn't change the outcome of the SQL run so just log them and continue.
        finally:
            if connector is not None:
                try:
                    # Best-effort cleanup after each attempt.
                    connector.close_connection()
                except Exception:
                    logger.debug(
                        "Ignoring error while closing connector for file '%s'",
                        file_name,
                        exc_info=True,
                    )

    while True:
        # Loop so we can retry on temporary connection errors.
        # Respect cancellation from other parallel tasks.
        if cancel_event is not None and cancel_event.is_set():
            logger.info(
                "Stage '%s' cancellation requested before running file '%s'; skipping.",
                stage,
                file_name,
            )
            return None

        try:
            # Normal path: try to run the SQL once and return its result.
            return _single_attempt()

        except Exception as exc:
            # 1) Non-connection error: do NOT retry
            if not _is_connection_error(exc):
                logger.warning(
                    "Stage '%s' non-connection error on file '%s'; "
                    "no retries will be attempted. Error: %s",
                    stage,
                    file_name,
                    exc,
                )
                # Let run_stage handle cancellation + logging
                raise

            # 2) Connection error: retry within the time window
            elapsed = time.monotonic() - start_ts
            if elapsed >= retry_window_seconds:
                logger.warning(
                    "Stage '%s' connection error on file '%s'; "
                    "retry window %.1fs exceeded (elapsed %.1fs). Giving up.",
                    stage,
                    file_name,
                    retry_window_seconds,
                    elapsed,
                )
                if cancel_event is not None:
                    cancel_event.set()
                raise

            # Loop: wait, test connection, maybe retry
            while True:
                # If another task already failed, stop retrying.
                if cancel_event is not None and cancel_event.is_set():
                    logger.info(
                        "Stage '%s' cancellation requested while waiting to "
                        "retry file '%s'; stopping.",
                        stage,
                        file_name,
                    )
                    return None

                now = time.monotonic()
                # Calculate how much retry time is left in the window.
                remaining = retry_window_seconds - (now - start_ts)
                if remaining <= 0:
                    # We ran out of time to keep retrying this file.
                    logger.warning(
                        "Stage '%s' retry window elapsed while waiting to "
                        "re-test connection for file '%s'.",
                        stage,
                        file_name,
                    )
                    if cancel_event is not None:
                        # Signal other workers to stop if retries are exhausted.
                        cancel_event.set()
                    # Re-raise the original exception so the caller sees failure.
                    raise

                # Choose how long to wait before the next connection test.
                # Never sleep less than 1s, and never exceed the remaining window.
                # - won’t sleep longer than the remaining time,
                # - won’t sleep less than 1 second,
                # - and normally uses retry_interval_seconds when there’s plenty of time left.
                sleep_for = min(retry_interval_seconds, max(1.0, remaining))
                logger.info(
                    "Stage '%s' connection error on file '%s'; will re-test "
                    "connection in %.1fs (elapsed %.1fs / %.1fs).",
                    stage,
                    file_name,
                    sleep_for,
                    now - start_ts,
                    retry_window_seconds,
                )
                # Pause before retrying to avoid hammering the database.
                time.sleep(sleep_for)

                if cancel_event is not None and cancel_event.is_set():
                    # Another worker signaled a stop (e.g., retries exhausted).
                    logger.info(
                        "Stage '%s' cancellation requested while waiting for "
                        "connection test for file '%s'; stopping.",
                        stage,
                        file_name,
                    )
                    # Stop retrying this file and return without a result.
                    return None

                # Test connection only; if OK, break inner loop and re-run SQL
                if _test_connection(stage, username, isolationlevel):
                    logger.info(
                        "Stage '%s' connection test succeeded; re-running file '%s'.",
                        stage,
                        file_name,
                    )
                    break  # exit inner loop -> outer while will call _single_attempt()
                # else: connection still dead; inner loop repeats until window expires

def run_stage(
    stage: str,  # Stage name (maps to a SQL folder).
    username: str = default_user,  # SQL user for connections.
    concurrent: bool = False,  # If True, run files in parallel where allowed.
    isolationlevel: str = default_isolationlevel,  # Transaction isolation level.
    batch_size: int = 30,  # Number of files per batch when running in bulk.
    retry_window_seconds: int = 900,    # 15 mins per file.
    retry_interval_seconds: int = 60,   # 60s between connection tests.
    untagged_run_mode: str = default_untagged_run_mode,  # How to run untagged files.
):
    """
    Execute .sql files for a stage in sequential batches, after wrapping each file
    with Kraken metadata + logging + TRY/CATCH BEFORE kraken.extract_sql.

    - Retries are time-based and only for connection errors.
    - Syntax/logic/constraint errors are not retried.
    - If any file ultimately fails, cancel_event is set and further work is skipped.

    Inputs:
      stage: Stage name (key in sql_dirs).
      username: SQL username for the connector.
      concurrent: If True, run files within each batch in parallel.
      isolationlevel: Isolation level for the connector.
      batch_size: Number of SQL files per batch when not tag-ordered.
      retry_window_seconds: Max retry window for connection errors per file.
      retry_interval_seconds: Sleep between connection tests during retries.
      untagged_run_mode: How to handle untagged files when tags exist ("parallel" or "sequential").

    Outputs:
      Returns the last Kraken execute() result on success; None on failure.

    Example:
      run_stage("facts_populate", concurrent=True, batch_size=5)
    """
    # Track duration and overall outcome for summary logging.
    start_perf = perf_counter()
    outcome = "success"
    last_result = None

    # Shared cancellation signal for parallel tasks within this stage.
    cancel_event = Event()


    # - It looks up the stage name in sql_dirs to find where the SQL files are.
    # - If the stage is configured as a list/tuple, it treats that as an explicit list of file paths and runs exactly those.
    # - Otherwise it treats it as a path:
    # - If it’s a folder, it runs all .sql files in that folder (sorted).
    # - If it’s a single file path, it runs just that one file.
    # So the stage can be configured three ways: a folder of SQL files, a single SQL file, or an explicit list of files.
    try:
        # Resolve the SQL directory (or explicit file list) for the stage.
        stage_root = sql_dirs[stage]

        if isinstance(stage_root, (list, tuple)):
            # Stage can point to explicit file paths.
            source_files = [Path(p) for p in stage_root]
        else:
            stage_root = Path(stage_root)
            if stage_root.is_dir():
                # Normal path: run all .sql files in the directory.
                source_files = sorted(stage_root.glob("*.sql"))
            else:
                # Stage can point directly to a single SQL file.
                source_files = [stage_root]

        if not source_files:
            logger.info("Stage '%s' has no SQL files to run.", stage)
            return None

        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer.")

        # Determine whether any filenames include run tags like [2].
        tag_by_name: dict[str, int | None] = {
            src.stem: _extract_run_tag(src) for src in source_files
        }
        has_tags = any(tag is not None for tag in tag_by_name.values())

        # Validate untagged handling mode early to avoid silent misconfig.
        if untagged_run_mode not in ("parallel", "sequential"):
            raise ValueError("untagged_run_mode must be 'parallel' or 'sequential'.")

        # Wrap SQL files into a temporary folder so raw sources remain unchanged.
        with TemporaryDirectory(prefix=f"{stage}_wrapped_") as tmpdir:
            tmpdir_path = Path(tmpdir)
            wrapped_paths: list[Path] = []

            for src in source_files:
                # Inject metadata/logging wrapper around the raw SQL.
                wrapped_sql = _wrap_file_for_stage(src, stage)
                dest = tmpdir_path / src.name
                dest.write_text(wrapped_sql, encoding="utf-8")
                wrapped_paths.append(dest)

            # Parse wrapped SQL into Kraken sqlfile objects.
            sql_list = kraken.extract_sql(
                filepaths=wrapped_paths,
                username=username
            )

            total = len(sql_list)
            # If there are no SQL statements, there's nothing to execute.
            if total == 0:
                logger.info(
                    "Stage '%s' has no SQL statements to run after wrapping.",
                    stage
                )
                # Return None to signal "no work done" for this stage.
                return None

            def run_sql_batch(batch, batch_num, total_batches, concurrent_flag):
                # Execute one batch of sqlfiles, honoring the concurrency flag.
                nonlocal last_result, outcome
                # Stop early if another batch already signaled cancellation.
                if cancel_event.is_set():
                    outcome = "failed"
                    logger.warning(
                        "Stage '%s' cancelled before starting batch %d/%d.",
                        stage,
                        batch_num,
                        total_batches
                    )
                    return None

                # Log the batch about to run and how many files it contains.
                logger.info(
                    "Stage '%s' running batch %d/%d (%d file(s))",
                    stage, batch_num, total_batches, len(batch)
                )

                # ---------- PARALLEL WITHIN BATCH ----------
                if concurrent_flag and len(batch) > 1:
                    # One worker per file to maximize throughput within the batch.
                    max_workers = len(batch)
                    # ThreadPoolExecutor runs multiple SQL files at the same time.
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        # Submit each SQL file as a separate task and keep a map to its file.
                        future_to_sql = {
                            executor.submit(
                                _execute_sql_with_new_connector,
                                sqlfile,
                                stage,
                                username,
                                isolationlevel,
                                retry_window_seconds,
                                retry_interval_seconds,
                                cancel_event,
                            ): sqlfile
                            for sqlfile in batch
                        }

                        for future in as_completed(future_to_sql):
                            sqlfile = future_to_sql[future]
                            file_name = getattr(sqlfile, "df_name", "unknown")
                            try:
                                # Pull the result of this file's execution.
                                result = future.result()
                                last_result = result
                            except Exception:
                                # Any failure in a parallel task marks the batch failed.
                                outcome = "failed"
                                cancel_event.set()

                                # Best-effort: cancel any not-yet-started work
                                for f in future_to_sql:
                                    if f is not future and not f.done():
                                        f.cancel()

                                # Log which file failed and stop this batch.
                                logger.exception(
                                    "Stage '%s' failed in batch %d on file '%s'",
                                    stage,
                                    batch_num,
                                    file_name
                                )
                                return None
                    # If all tasks finished, return the last result we saw.
                    return last_result

                # ---------- SEQUENTIAL WITHIN BATCH ----------
                for sqlfile in batch:
                    # Run files one-by-one in order (no parallel threads).
                    if cancel_event.is_set():
                        outcome = "failed"
                        logger.warning(
                            "Stage '%s' cancelled before starting file '%s' in batch %d.",
                            stage,
                            getattr(sqlfile, "df_name", "unknown"),
                            batch_num
                        )
                        return None

                    file_name = getattr(sqlfile, "df_name", "unknown")
                    try:
                        # Execute this file and capture its result.
                        result = _execute_sql_with_new_connector(
                            sqlfile,
                            stage,
                            username,
                            isolationlevel,
                            retry_window_seconds=retry_window_seconds,
                            retry_interval_seconds=retry_interval_seconds,
                            cancel_event=cancel_event,
                        )
                        last_result = result
                    except Exception:
                        # Any failure stops the batch and signals cancellation.
                        outcome = "failed"
                        cancel_event.set()
                        logger.exception(
                            "Stage '%s' failed in batch %d on file '%s'",
                            stage,
                            batch_num,
                            file_name
                        )
                        return None
                # All files succeeded; return the last result we saw.
                return last_result

            if has_tags:
                # When tags exist, they override stage-level concurrency/batching.
                logger.info(
                    "Stage '%s' tag ordering active; ignoring stage concurrency and batch_size.",
                    stage
                )
                # Preserve original file order to keep deterministic execution.
                # This keeps the tag handling stable even if filenames are sorted elsewhere.
                order_map = {src.stem: idx for idx, src in enumerate(source_files)}
                def sort_key(sqlfile):
                    # Use the original index; unknown files go to the end.
                    # Look up the file’s original position by its name; if it can’t be found, give it a very large number so it sorts to the end.
                    # - getattr(sqlfile, "df_name", "") gets the file’s name, or "" if missing.
                    # - order_map.get(name, 10**9) returns the original index from order_map, or 1,000,000,000 as a fallback.
                    # - When used as a sort key, smaller numbers come first, so unknown files go last.    
                    return order_map.get(getattr(sqlfile, "df_name", ""), 10**9)

                # Build groups: each numeric tag is its own batch.
                tagged_groups: dict[int, list] = {}
                untagged: list = []

                for sqlfile in sql_list:
                    # Look up the file's tag (if any) based on its name.
                    name = getattr(sqlfile, "df_name", "")
                    tag = tag_by_name.get(name)
                    if tag is None:
                        # No tag found; put it in the untagged bucket.
                        untagged.append(sqlfile)
                    else:
                        # Group files by their numeric tag value.
                        tagged_groups.setdefault(tag, []).append(sqlfile)

                # Each tag runs as a batch; concurrent if multiple files share the tag.
                batches = []
                for tag in sorted(tagged_groups):
                    # Order files within this tag group using the original file order.
                    group = sorted(tagged_groups[tag], key=sort_key)
                    # Add a batch tuple: (files_in_group, run_in_parallel_flag).
                    batches.append((group, len(group) > 1))

                # Untagged files are appended based on the configured mode.
                if untagged:
                    # Keep untagged files in the original order.
                    untagged_sorted = sorted(untagged, key=sort_key)
                    if untagged_run_mode == "parallel":
                        # Run all untagged files together (parallel if >1).
                        batches.append((untagged_sorted, len(untagged_sorted) > 1))
                    else:
                        # Run each untagged file one-by-one (sequential).
                        for sqlfile in untagged_sorted:
                            batches.append(([sqlfile], False))

                # Count how many batches we will run for this stage.
                total_batches = len(batches)
                # Run each batch in order, keeping track of its index for logging.
                for idx, (batch, concurrent_flag) in enumerate(batches, start=1):
                    result = run_sql_batch(batch, idx, total_batches, concurrent_flag)
                    if result is None:
                        # A failure or cancellation occurred; stop the stage.
                        return None
                # All batches completed; return the last successful result.
                return last_result

            # Default batching path when no tags are present.
            total_batches = (total + batch_size - 1) // batch_size

            # Walk through the SQL list in chunks of size batch_size.
            for batch_idx in range(0, total, batch_size):
                # Convert the start index into a human-friendly batch number (1-based).
                batch_num = (batch_idx // batch_size) + 1
                # Slice the list to get just this batch's files.
                batch = sql_list[batch_idx: batch_idx + batch_size]
                # Run the batch (sequential or parallel based on 'concurrent').
                result = run_sql_batch(batch, batch_num, total_batches, concurrent)
                if result is None:
                    # A failure or cancellation occurred; stop the stage.
                    return None

        # Normal success path: return the last SQL execution result.
        return last_result

    except Exception:
        # Any unexpected error marks the whole stage as failed.
        outcome = "failed"
        logger.exception("Stage '%s' failed", stage)
        # Return None to indicate failure to the caller.
        return None

    finally:
        # Always log a summary when the stage ends (success or failure).
        end_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
        # Also capture local time for easier human reference.
        end_local = datetime.now().isoformat(timespec="seconds")
        # Measure total runtime using a high-precision timer.
        duration_s = perf_counter() - start_perf
        logger.info(
            "Stage '%s' finished: outcome=%s | end_utc=%s | end_local=%s | duration=%.2fs",
            stage, outcome, end_utc, end_local, duration_s
        )

def run_etl_pipeline(
    pipeline_name: str,  # Which pipeline to run (e.g., "delta", "full").
    *,
    # * All parameters below must be passed by name (keyword-only).
    stage_sets: dict[str, list] | None = None,  # Optional override for pipeline stages.
    username: str = default_user,  # SQL user for all stages.
    default_concurrent: bool = True,  # Default parallelism when not explicitly specified.
    isolationlevel: str = default_isolationlevel,  # Transaction isolation level.
    batch_size: int = 30,  # Number of files per batch when untagged.
    retry_window_seconds: int = 900,  # Retry window for connection errors.
    retry_interval_seconds: int = 60,  # Sleep time between connection tests.
    fail_fast_exit: bool = True,  # If True, exit process on first failure.
) -> bool:
    """
    Run a named ETL pipeline (e.g. 'full', 'delta') defined as an ordered list
    of stages.

    Each entry in stage_sets[pipeline_name] can be:
      - a string:   "facts_populate"          -> uses default_concurrent
      - a dict:     {"stage": "facts_populate", "concurrent": False}

    Returns True if all stages succeeded, False if any failed.
    If fail_fast_exit=True, calls sys.exit(1) on first failure.

    Inputs:
      pipeline_name: Name of the pipeline key in stage_sets.
      stage_sets: Optional override of PIPELINE_STAGE_SETS.
      username: SQL username for stage execution.
      default_concurrent: Default concurrency for string-only stage entries.
      isolationlevel: Isolation level for the connector.
      batch_size: Number of SQL files per batch when not tag-ordered.
      retry_window_seconds: Max retry window for connection errors per file.
      retry_interval_seconds: Sleep between connection tests during retries.
      fail_fast_exit: If True, exit process on first failure.

    Outputs:
      True when all stages complete successfully, False on failure.
      May raise SystemExit if fail_fast_exit=True and a stage fails.

    Example:
      run_etl_pipeline("delta", batch_size=5, default_concurrent=True)
    """
    # Track overall pipeline duration and outcome.
    pipeline_start_perf = perf_counter()
    outcome = "success"
    failed_stage: str | None = None
    cycle_id = get_cycle_id()
    generated_cycle_id = False

    try:
        if not cycle_id:
            cycle_id = str(uuid.uuid4())
            set_cycle_id(cycle_id)
            generated_cycle_id = True
            logger.info("Auto-generated cycle id for pipeline '%s': %s", pipeline_name, cycle_id)

        # Use the default stage definitions if the caller didn't pass any.
        # Default to the configured stage sets when no override is provided.
        if stage_sets is None:
            stage_sets = PIPELINE_STAGE_SETS

        # Make sure the requested pipeline exists before doing any work.
        # Validate pipeline name early to avoid partial runs.
        if pipeline_name not in stage_sets:
            raise ValueError(
                f"Unknown pipeline_name '{pipeline_name}'. "
                f"Available pipelines: {', '.join(stage_sets.keys())}"
            )

        # Get the ordered list of stages we will run.
        # Pull the ordered stage list for this pipeline.
        stages_in_order = stage_sets[pipeline_name]
        # Log a clear "start" message with how many stages will run.
        logger.info(
            "=== Starting ETL pipeline '%s' (%d stage(s)) ===",
            pipeline_name,
            len(stages_in_order),
        )

        for entry in stages_in_order:
            # Normalise entry -> (stage_name, stage_concurrent)
            if isinstance(entry, str):
                # If the entry is just a string, it is the stage name.
                stage_name = entry
                # Use the default concurrency setting for this stage.
                stage_concurrent = default_concurrent
            elif isinstance(entry, dict):
                # If the entry is a dict, it can override concurrency per stage.
                stage_name = entry.get("stage")
                if not stage_name:
                    # Guard against malformed config (missing stage name).
                    raise ValueError(
                        f"Stage dict in pipeline '{pipeline_name}' is missing 'stage': {entry}"
                    )
                # Use the per-stage concurrency if provided, otherwise default.
                stage_concurrent = entry.get("concurrent", default_concurrent)
            else:
                # Any other type is invalid for a pipeline stage entry.
                raise TypeError(
                    f"Invalid stage entry in pipeline '{pipeline_name}': {entry!r}"
                )

            # Log the start of this stage with its concurrency setting.
            logger.info(
                "=== Starting stage '%s' in pipeline '%s' (concurrent=%s) ===",
                stage_name,
                pipeline_name,
                stage_concurrent,
            )

            # Run the stage and propagate retry/batch settings.
            result = run_stage(
                stage=stage_name,  # Which stage to run.
                username=username,  # SQL user for this stage.
                concurrent=stage_concurrent,  # Per-stage concurrency setting.
                isolationlevel=isolationlevel,  # Transaction isolation level.
                batch_size=batch_size,  # Batch size for untagged files.
                retry_window_seconds=retry_window_seconds,  # Retry window for connection errors.
                retry_interval_seconds=retry_interval_seconds,  # Sleep between retry checks.
            )

            # Convention: run_stage returns None on failure
            if result is None:
                # Mark the pipeline as failed and remember which stage failed.
                outcome = "failed"
                failed_stage = stage_name
                # Log that we're stopping the pipeline early.
                logger.error(
                    "Stage '%s' in pipeline '%s' did not complete successfully. "
                    "Aborting remaining stages.",
                    stage_name,
                    pipeline_name,
                )
                if fail_fast_exit:
                    # Exit the process for scheduler visibility.
                    sys.exit(1)
                # Return False to indicate the pipeline did not succeed.
                return False

        logger.info("=== ETL pipeline '%s' completed successfully ===", pipeline_name)
        return True

    except SystemExit:
        # fail_fast_exit path (sys.exit(1)) still runs finally -> duration log
        raise

    except Exception:
        # Any unexpected error marks the whole pipeline as failed.
        outcome = "failed"
        logger.exception("ETL pipeline '%s' failed unexpectedly.", pipeline_name)
        if fail_fast_exit:
            # Exit the process so schedulers see a non-zero failure.
            sys.exit(1)
        # Return False to signal failure to the caller.
        return False

    finally:
        # Always log a pipeline summary at the end (success or failure).
        duration_s = perf_counter() - pipeline_start_perf
        # Capture UTC and local timestamps for easier troubleshooting.
        end_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
        end_local = datetime.now().isoformat(timespec="seconds")

        if generated_cycle_id:
            set_cycle_id(None)

        if failed_stage:
            # Include the failed stage in the summary when applicable.
            logger.info(
                "=== ETL pipeline '%s' finished: outcome=%s | failed_stage=%s | end_utc=%s | end_local=%s | duration=%.2fs ===",
                pipeline_name,
                outcome,
                failed_stage,
                end_utc,
                end_local,
                duration_s,
            )
        else:
            # Success path (no failed stage to report).
            logger.info(
                "=== ETL pipeline '%s' finished: outcome=%s | end_utc=%s | end_local=%s | duration=%.2fs ===",
                pipeline_name,
                outcome,
                end_utc,
                end_local,
                duration_s,
            )



# Custom usage
#custom_sets = {
#    "minimal": ["dims_unknown_members", "facts_populate"],
#}
