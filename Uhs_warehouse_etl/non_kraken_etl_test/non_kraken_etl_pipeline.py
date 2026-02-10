# non_kraken_etl_test/non_kraken_etl_pipeline.py

import time
import sys
import re
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Event
from datetime import datetime, timezone
from time import perf_counter
from dataclasses import dataclass


from .non_kraken_etl_setup import (
    logger,
    default_user,
    default_isolationlevel,
    default_untagged_run_mode,
    sql_dirs,
    PIPELINE_STAGE_SETS,
)

# This module mirrors the main ETL pipeline but runs "SQL" files as shell
# scripts for orchestration testing without a live database.

@dataclass(frozen=True)
class SimpleSQLFile:
    # Lightweight representation of a file-based command.
    df_name: str
    sql: str
    path: Path

# -----------------------------------
# Helper: connection error detection
# -----------------------------------
def _is_connection_error(exc: Exception) -> bool:
    """
    Return True if this looks like a transient connection issue
    (TCP, login timeout, SSL, etc.), False otherwise.

    Inputs:
      exc: The exception raised by a command execution attempt.

    Outputs:
      Always False in the non-kraken test runner (no DB connections).

    Example:
      if _is_connection_error(exc):
          ...
    """
    # No database is involved in this test harness, so nothing is retryable
    # based on connection state.
    return False


def _test_connection(
    stage: str,
    username: str,
    isolationlevel: str,
) -> bool:
    """
    Cheap connection test placeholder: no-op when running without a database.
    Returns True to allow execution to proceed.

    Inputs:
      stage: Stage name for logging context.
      username: Included for signature parity with the main pipeline.
      isolationlevel: Included for signature parity with the main pipeline.

    Outputs:
      True, because there is no live connection to validate.

    Example:
      if _test_connection("runfolders", "sqlkraken", "AUTOCOMMIT"):
          ...
    """
    # Keep the call site behavior consistent without performing I/O.
    logger.info("Stage '%s': connection test skipped (no database connection).", stage)
    return True


def _extract_sql_files(filepaths: list[Path]) -> list[SimpleSQLFile]:
    """
    Read file contents and build SimpleSQLFile entries.

    Inputs:
      filepaths: List of file paths to read.

    Outputs:
      List of SimpleSQLFile objects with name, contents, and path.

    Example:
      sql_list = _extract_sql_files([Path("Folder1/Test.sql")])
    """
    sql_list: list[SimpleSQLFile] = []
    for path in filepaths:
        # Preserve raw file contents; no wrapping in the test harness.
        sql_text = path.read_text(encoding="utf-8")
        sql_list.append(SimpleSQLFile(df_name=path.stem, sql=sql_text, path=path))
    return sql_list


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
    match = _RUN_TAG_RE.search(path.stem)
    if match:
        return int(match.group(1))
    return None


def _execute_sql_with_new_connector(
    sqlfile,
    stage: str,
    username: str,
    isolationlevel: str,
    retry_window_seconds: int = 900,   # 15 minutes
    retry_interval_seconds: int = 60,  # 60s between connection tests
    cancel_event: Event | None = None,
):
    """
    Execute ONE parsed sqlfile by running its contents as a shell script.

    - Retries only for connection-type errors, within a time window.
    - No retries for SQL/syntax/constraint errors: these bubble up immediately.
    - If the retry window expires, this sets cancel_event (if provided) and raises.

    Inputs:
      sqlfile: SimpleSQLFile with .sql contents to run as a shell command.
      stage: Stage name for logging context.
      username: Included for signature parity with the main pipeline.
      isolationlevel: Included for signature parity with the main pipeline.
      retry_window_seconds: Max retry window for connection errors.
      retry_interval_seconds: Sleep between connection tests during retries.
      cancel_event: Optional Event that, when set, aborts further work.

    Outputs:
      The result returned by subprocess.run(), or None if cancelled/empty.
      Raises on non-connection errors or when the retry window is exceeded.

    Example:
      _execute_sql_with_new_connector(sqlfile, "runfolders", "sqlkraken", "AUTOCOMMIT")
    """
    file_name = getattr(sqlfile, "df_name", "unknown")
    start_ts = time.monotonic()

    def _single_attempt():
        logger.info(
            "Stage '%s' starting file '%s'",
            stage, file_name
        )

        sql_text = getattr(sqlfile, "sql", "")
        if not sql_text.strip():
            # No-op for empty files to keep behavior consistent with SQL runner.
            logger.info(
                "Stage '%s' skipping empty file '%s'",
                stage, file_name
            )
            return None

        # Execute the file contents as a shell command.
        if os.name == "nt":
            result = subprocess.run(sql_text, shell=True, check=True)
        else:
            result = subprocess.run(
                sql_text,
                shell=True,
                executable="/bin/bash",
                check=True,
            )

        logger.info(
            "Stage '%s' finished file '%s' successfully",
            stage, file_name
        )
        return result

    while True:
        # Respect cancellation from other parallel tasks.
        if cancel_event is not None and cancel_event.is_set():
            logger.info(
                "Stage '%s' cancellation requested before running file '%s'; skipping.",
                stage,
                file_name,
            )
            return None

        try:
            # Normal path: try to run the SQL
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
                remaining = retry_window_seconds - (now - start_ts)
                if remaining <= 0:
                    logger.warning(
                        "Stage '%s' retry window elapsed while waiting to "
                        "re-test connection for file '%s'.",
                        stage,
                        file_name,
                    )
                    if cancel_event is not None:
                        cancel_event.set()
                    raise

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
                time.sleep(sleep_for)

                if cancel_event is not None and cancel_event.is_set():
                    logger.info(
                        "Stage '%s' cancellation requested while waiting for "
                        "connection test for file '%s'; stopping.",
                        stage,
                        file_name,
                    )
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
    stage: str, 
    username: str = default_user,
    concurrent: bool = False,
    isolationlevel: str = default_isolationlevel,
    batch_size: int = 10,
    retry_window_seconds: int = 900,    # 15 mins per file
    retry_interval_seconds: int = 60,   # 60s between connection tests
    untagged_run_mode: str = default_untagged_run_mode,
):
    """
    Execute .sql files for a stage in sequential batches, using raw file contents.

    - Retries are time-based and only for connection errors.
    - Syntax/logic/constraint errors are not retried.
    - If any file ultimately fails, cancel_event is set and further work is skipped.

    Inputs:
      stage: Stage name (key in sql_dirs).
      username: Included for signature parity with the main pipeline.
      concurrent: If True, run files within each batch in parallel.
      isolationlevel: Included for signature parity with the main pipeline.
      batch_size: Number of files per batch when not tag-ordered.
      retry_window_seconds: Max retry window for connection errors per file.
      retry_interval_seconds: Sleep between connection tests during retries.
      untagged_run_mode: How to handle untagged files when tags exist ("parallel" or "sequential").

    Outputs:
      Returns the last subprocess result on success; None on failure.

    Example:
      run_stage("runfolders", concurrent=True, batch_size=5)
    """
    # Track duration and overall outcome for summary logging.
    start_perf = perf_counter()
    outcome = "success"
    last_result = None

    # Shared cancellation signal for parallel tasks within this stage.
    cancel_event = Event()

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

        # Build SimpleSQLFile objects from the raw file contents.
        sql_list = _extract_sql_files(source_files)

        total = len(sql_list)
        if total == 0:
            logger.info(
                "Stage '%s' has no SQL statements to run.",
                stage
            )
            return None

        def run_sql_batch(batch, batch_num, total_batches, concurrent_flag):
            # Execute one batch of sqlfiles, honoring the concurrency flag.
            nonlocal last_result, outcome
            if cancel_event.is_set():
                outcome = "failed"
                logger.warning(
                    "Stage '%s' cancelled before starting batch %d/%d.",
                    stage,
                    batch_num,
                    total_batches
                )
                return None

            logger.info(
                "Stage '%s' running batch %d/%d (%d file(s))",
                stage, batch_num, total_batches, len(batch)
            )

            # ---------- PARALLEL WITHIN BATCH ----------
            if concurrent_flag and len(batch) > 1:
                # One worker per file to maximize throughput within the batch.
                max_workers = len(batch)
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                            result = future.result()
                            last_result = result
                        except Exception:
                            outcome = "failed"
                            cancel_event.set()

                            # Best-effort: cancel any not-yet-started work
                            for f in future_to_sql:
                                if f is not future and not f.done():
                                    f.cancel()

                            logger.exception(
                                "Stage '%s' failed in batch %d on file '%s'",
                                stage,
                                batch_num,
                                file_name
                            )
                            return None
                return last_result

            # ---------- SEQUENTIAL WITHIN BATCH ----------
            for sqlfile in batch:
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
                    outcome = "failed"
                    cancel_event.set()
                    logger.exception(
                        "Stage '%s' failed in batch %d on file '%s'",
                        stage,
                        batch_num,
                        file_name
                    )
                    return None
            return last_result

        if has_tags:
            # When tags exist, they override stage-level concurrency/batching.
            logger.info(
                "Stage '%s' tag ordering active; ignoring stage concurrency and batch_size.",
                stage
            )
            # Preserve original file order to keep deterministic execution.
            order_map = {src.stem: idx for idx, src in enumerate(source_files)}
            def sort_key(sqlfile):
                return order_map.get(getattr(sqlfile, "df_name", ""), 10**9)

            # Build groups: each numeric tag is its own batch.
            tagged_groups: dict[int, list] = {}
            untagged: list = []

            for sqlfile in sql_list:
                name = getattr(sqlfile, "df_name", "")
                tag = tag_by_name.get(name)
                if tag is None:
                    untagged.append(sqlfile)
                else:
                    tagged_groups.setdefault(tag, []).append(sqlfile)

            # Each tag runs as a batch; concurrent if multiple files share the tag.
            batches = []
            for tag in sorted(tagged_groups):
                group = sorted(tagged_groups[tag], key=sort_key)
                batches.append((group, len(group) > 1))

            # Untagged files are appended based on the configured mode.
            if untagged:
                untagged_sorted = sorted(untagged, key=sort_key)
                if untagged_run_mode == "parallel":
                    batches.append((untagged_sorted, len(untagged_sorted) > 1))
                else:
                    for sqlfile in untagged_sorted:
                        batches.append(([sqlfile], False))

            total_batches = len(batches)
            for idx, (batch, concurrent_flag) in enumerate(batches, start=1):
                result = run_sql_batch(batch, idx, total_batches, concurrent_flag)
                if result is None:
                    return None
            return last_result

        # Default batching path when no tags are present.
        total_batches = (total + batch_size - 1) // batch_size

        for batch_idx in range(0, total, batch_size):
            batch_num = (batch_idx // batch_size) + 1
            batch = sql_list[batch_idx: batch_idx + batch_size]
            result = run_sql_batch(batch, batch_num, total_batches, concurrent)
            if result is None:
                return None

        return last_result

    except Exception:
        outcome = "failed"
        logger.exception("Stage '%s' failed", stage)
        return None

    finally:
        end_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
        end_local = datetime.now().isoformat(timespec="seconds")
        duration_s = perf_counter() - start_perf
        logger.info(
            "Stage '%s' finished: outcome=%s | end_utc=%s | end_local=%s | duration=%.2fs",
            stage, outcome, end_utc, end_local, duration_s
        )

def run_etl_pipeline(
    pipeline_name: str,
    *,
    stage_sets: dict[str, list] | None = None,
    username: str = default_user,
    default_concurrent: bool = True,
    isolationlevel: str = default_isolationlevel,
    batch_size: int = 10,
    retry_window_seconds: int = 900,
    retry_interval_seconds: int = 60,
    fail_fast_exit: bool = True,
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
      username: Included for signature parity with the main pipeline.
      default_concurrent: Default concurrency for string-only stage entries.
      isolationlevel: Included for signature parity with the main pipeline.
      batch_size: Number of files per batch when not tag-ordered.
      retry_window_seconds: Max retry window for connection errors per file.
      retry_interval_seconds: Sleep between connection tests during retries.
      fail_fast_exit: If True, exit process on first failure.

    Outputs:
      True when all stages complete successfully, False on failure.
      May raise SystemExit if fail_fast_exit=True and a stage fails.

    Example:
      run_etl_pipeline("runfolders", batch_size=5, default_concurrent=True)
    """
    # Track overall pipeline duration and outcome.
    pipeline_start_perf = perf_counter()
    outcome = "success"
    failed_stage: str | None = None

    try:
        # Default to the configured stage sets when no override is provided.
        if stage_sets is None:
            stage_sets = PIPELINE_STAGE_SETS

        # Validate pipeline name early to avoid partial runs.
        if pipeline_name not in stage_sets:
            raise ValueError(
                f"Unknown pipeline_name '{pipeline_name}'. "
                f"Available pipelines: {', '.join(stage_sets.keys())}"
            )

        # Pull the ordered stage list for this pipeline.
        stages_in_order = stage_sets[pipeline_name]
        logger.info(
            "=== Starting ETL pipeline '%s' (%d stage(s)) ===",
            pipeline_name,
            len(stages_in_order),
        )

        for entry in stages_in_order:
            # Normalise entry -> (stage_name, stage_concurrent)
            if isinstance(entry, str):
                stage_name = entry
                stage_concurrent = default_concurrent
            elif isinstance(entry, dict):
                stage_name = entry.get("stage")
                if not stage_name:
                    raise ValueError(
                        f"Stage dict in pipeline '{pipeline_name}' is missing 'stage': {entry}"
                    )
                stage_concurrent = entry.get("concurrent", default_concurrent)
            else:
                raise TypeError(
                    f"Invalid stage entry in pipeline '{pipeline_name}': {entry!r}"
                )

            logger.info(
                "=== Starting stage '%s' in pipeline '%s' (concurrent=%s) ===",
                stage_name,
                pipeline_name,
                stage_concurrent,
            )

            # Run the stage and propagate retry/batch settings.
            result = run_stage(
                stage=stage_name,
                username=username,
                concurrent=stage_concurrent,
                isolationlevel=isolationlevel,
                batch_size=batch_size,
                retry_window_seconds=retry_window_seconds,
                retry_interval_seconds=retry_interval_seconds,
            )

            # Convention: run_stage returns None on failure
            if result is None:
                outcome = "failed"
                failed_stage = stage_name
                logger.error(
                    "Stage '%s' in pipeline '%s' did not complete successfully. "
                    "Aborting remaining stages.",
                    stage_name,
                    pipeline_name,
                )
                if fail_fast_exit:
                    # Exit the process for scheduler visibility.
                    sys.exit(1)
                return False

        logger.info("=== ETL pipeline '%s' completed successfully ===", pipeline_name)
        return True

    except SystemExit:
        # fail_fast_exit path (sys.exit(1)) still runs finally -> duration log
        raise

    except Exception:
        outcome = "failed"
        logger.exception("ETL pipeline '%s' failed unexpectedly.", pipeline_name)
        if fail_fast_exit:
            sys.exit(1)
        return False

    finally:
        duration_s = perf_counter() - pipeline_start_perf
        end_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
        end_local = datetime.now().isoformat(timespec="seconds")

        if failed_stage:
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
