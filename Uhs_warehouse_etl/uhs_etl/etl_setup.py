# uhs_etl/etl_setup.py
# Central configuration, profile loading, pipeline mapping, and logging for ETL runs.

import json  # Read JSON configuration files for profiles and pipelines.
import logging  # Core logging API used across the ETL package.
from logging.handlers import BaseRotatingHandler, NTEventLogHandler, SMTPHandler  # Log handlers: custom file rotation, Windows Event Log, email.
import os  # Environment variables and filesystem checks.
from pathlib import Path  # OS-agnostic filesystem path handling.
import re  # Regular expressions for parsing timestamps from log lines.
import time  # Time computations for log rotation intervals and timestamps.
from typing import TypedDict, Union  # Stronger typing for pipeline stage specs.

# ---- Defaults / config ----
# These defaults control the SQL root, login context, and behavior when
# the orchestration is run without explicit overrides.
_BASE_DEFAULT_DB = "DEFAULTDEV2"  # Default database alias used by the ETL connection.
_BASE_DEFAULT_USER = "sqldefaultuser"  # Default SQL login for ETL execution.
_BASE_DEFAULT_SQL_FOLDER = "SQL_Synapse"  # Active SQL root folder.
_BASE_DEFAULT_EMAIL = 'paul.nasti@uhs.nhs.uk'  # Email target for error notifications.
default_isolationlevel = "AUTOCOMMIT"  # Avoid long-running open transactions.
default_untagged_run_mode = "parallel"  # Default for stages without concurrency tags.
default_log_retention_days = 7  # Rotate main log after N days.
default_log_backup_count = 8  # Keep N archived logs.
default_event_log_level = logging.ERROR  # Minimum level sent to Windows Event Log.

# ---- Resolve repo root and profile config ----
# __file__ = ...\scripts\Uhs_warehouse_etl\uhs_etl\etl_setup.py
_pkg_dir = Path(__file__).resolve().parent          # ...\uhs_etl (package directory for this module)
repo_root = _pkg_dir.parents[2]                     # ...\PythonWinOrc (repository root)

def _resolve_config_path(file_name: str) -> Path:
    # Prefer a shared config folder above the app repo when present.
    for parent in (repo_root, *repo_root.parents):
        candidate = parent / "config" / file_name
        if candidate.is_file():
            return candidate

    # Fall back to repo templates when no external config exists.
    return repo_root / "scripts" / "Setup" / "Pipeline_Setup" / file_name


# Path to the optional profile configuration file used to override defaults.
_profile_config_path = _resolve_config_path("etl_profiles.json")
# Path to the optional pipeline config file used for stage mappings and log path.
pipeline_config_path = _resolve_config_path("pipeline_config.json")


def _load_log_path(config_path: Path, default_path: Path) -> str:
    # Read log_path from pipeline_config.json without relying on the logger.
    if not config_path.exists():
        return str(default_path)
    try:
        with config_path.open("r", encoding="utf-8") as fh:
            config = json.load(fh)
    except Exception:
        return str(default_path)
    raw_log_path = config.get("log_path")
    if isinstance(raw_log_path, str) and raw_log_path.strip():
        return raw_log_path
    return str(default_path)


def _load_profile_config(path: Path) -> tuple[dict, list[str]]:
    # Read profile overrides from JSON and return (profiles_dict, errors_list).
    if not path.exists():
        # Missing file is not fatal; return empty profiles and no errors.
        return {}, []
    try:
        # Open and parse JSON with explicit encoding to avoid platform ambiguity.
        with path.open("r", encoding="utf-8") as fh:
            config = json.load(fh)
    except Exception as exc:
        # Return error string so caller can log warning without crashing.
        return {}, [f"Failed to read profiles file: {exc}"]

    # The expected JSON structure has a top-level "profiles" object.
    profiles = config.get("profiles")
    if not isinstance(profiles, dict):
        # File exists but is not in the expected shape.
        return {}, ["'profiles' missing or not an object in etl_profiles.json."]
    return profiles, []


_profile_errors: list[str] = []  # Collect profile issues to log once logger is available.
_profiles, _profile_errors = _load_profile_config(_profile_config_path)  # Load profile config and errors.
_profile_name = os.environ.get("ETL_PROFILE", "default")  # Profile selection from env, defaulting to "default".
_profile_prefix = f"[{_profile_name}] "  # Prefix used in log output to tag the active profile.
_selected_profile = _profiles.get(_profile_name) if _profiles else None  # Selected profile dict or None.
selected_profile = _selected_profile  # Public alias for other modules.
profile_name = _profile_name  # Public alias for the active profile name string.
_cycle_id = os.environ.get("ETL_CYCLE_ID")  # Optional cycle identifier for grouping an ETL run.
cycle_id = _cycle_id  # Public alias for the active cycle id string (if set).
if _profiles and _selected_profile is None:
    # Profile set was loaded but requested name was not found.
    _profile_errors.append(
        f"Profile '{_profile_name}' not found. Using base defaults."
    )

# Initialize defaults that may be overridden by the selected profile.
default_db = _BASE_DEFAULT_DB
default_user = _BASE_DEFAULT_USER
default_sql_folder = _BASE_DEFAULT_SQL_FOLDER
default_email = _BASE_DEFAULT_EMAIL

if isinstance(_selected_profile, dict):
    # Override defaults when a valid profile dict is present.
    default_db = _selected_profile.get("default_db", default_db)
    default_user = _selected_profile.get("default_user", default_user)
    default_sql_folder = _selected_profile.get("default_sql_folder", default_sql_folder)
    default_email = _selected_profile.get("default_email", default_email)

# ---- Logger setup ----
# Configure once at module-load so every import gets a ready logger.
# Logger name matches the log path for quick identification in outputs.
def _resolve_default_log_path() -> Path:
    # Prefer a shared runtime/logs folder when available above the repo.
    for parent in (repo_root, *repo_root.parents):
        runtime_dir = parent / "runtime"
        if runtime_dir.is_dir():
            return runtime_dir / "logs" / "execution.log"

    # Fallback to repo-managed logs for local/dev use.
    return repo_root / "scripts" / "Uhs_warehouse_etl" / "logs" / "execution.log"


_default_log_path = _resolve_default_log_path()
LOG_PATH = _load_log_path(pipeline_config_path, _default_log_path)


class IntervalRotatingFileHandler(BaseRotatingHandler):
    """
    Rotate the log file after a fixed number of days.
    Archived logs are named with start/end timestamps.
    """

    # Match the leading timestamp on a log line: "YYYY-MM-DD HH:MM:SS,".
    _TS_RE = re.compile(r"^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}),")

    def __init__(self, filename: str, *, interval_days: int, backup_count: int) -> None:
        # Initialize the base handler in append mode and delay file open until first emit.
        super().__init__(filename, mode="a", encoding="utf-8", delay=True)
        if interval_days < 1:
            # Prevent invalid configuration that would never roll over.
            raise ValueError("interval_days must be >= 1")
        if backup_count < 0:
            # Negative backup counts are not meaningful.
            raise ValueError("backup_count must be >= 0")
        self.interval_seconds = interval_days * 86400  # Convert day interval to seconds.
        self.backup_count = backup_count  # How many archived logs to keep.
        self._start_time = self._resolve_start_time()  # Determine initial log start time.
        self._rollover_at = self._start_time + self.interval_seconds  # When to rotate next.

    def _resolve_start_time(self) -> float:
        # Determine start time based on file content or mtime.
        if not os.path.exists(self.baseFilename):
            # If log file does not exist yet, treat start time as now.
            return time.time()
        start_time = self._read_first_timestamp()
        if start_time is not None:
            # Prefer explicit timestamp from first log line.
            return start_time
        # Fall back to file modification time when timestamp is missing.
        return os.path.getmtime(self.baseFilename)

    def _read_first_timestamp(self) -> float | None:
        # Try to extract timestamp from the first line of the log.
        try:
            # Open the file with forgiving decode in case of older encoding issues.
            with open(self.baseFilename, "r", encoding="utf-8", errors="ignore") as fh:
                first_line = fh.readline().strip()
        except Exception:
            # Any I/O error results in no timestamp.
            return None
        if not first_line:
            # Empty file or blank first line.
            return None
        match = self._TS_RE.match(first_line)
        if not match:
            # Line does not start with expected timestamp format.
            return None
        try:
            # Parse timestamp into a struct_time using the log formatter pattern.
            ts = time.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            # Timestamp not parseable even though regex matched.
            return None
        # Convert to seconds since epoch for numeric comparisons.
        return time.mktime(ts)

    def shouldRollover(self, record: logging.LogRecord) -> bool:
        # Decide whether it is time to rotate based on wall-clock time.
        return time.time() >= self._rollover_at

    def doRollover(self) -> None:
        # Rotate the active log file to an archive name.
        if self.stream:
            # Close current stream before renaming to release file handle.
            self.stream.close()
            self.stream = None  # type: ignore[assignment]

        end_time = time.time()  # End timestamp for archive naming.
        archive_path = self._archive_name(self._start_time, end_time)
        if os.path.exists(self.baseFilename):
            # Rename the current log file to the archive name.
            os.rename(self.baseFilename, archive_path)

        # Reset rotation window to start now.
        self._start_time = end_time
        self._rollover_at = self._start_time + self.interval_seconds
        self._cleanup_archives()  # Enforce backup retention.

    def _archive_name(self, start_ts: float, end_ts: float) -> str:
        # Build a unique archive file name containing start/end timestamps.
        log_path = Path(self.baseFilename)
        start_str = time.strftime("%Y%m%d_%H%M%S", time.localtime(start_ts))
        end_str = time.strftime("%Y%m%d_%H%M%S", time.localtime(end_ts))
        candidate = log_path.with_name(
            f"{log_path.stem}_{start_str}-{end_str}{log_path.suffix}"
        )
        if not candidate.exists():
            # Preferred name is available.
            return str(candidate)
        # In the rare case of collision, try suffixes.
        for i in range(1, 1000):
            alt = log_path.with_name(
                f"{log_path.stem}_{start_str}-{end_str}_{i}{log_path.suffix}"
            )
            if not alt.exists():
                return str(alt)
        # As a last resort, return the base candidate even if it exists.
        return str(candidate)

    def _cleanup_archives(self) -> None:
        # Remove oldest archives beyond backup_count.
        if self.backup_count == 0:
            # 0 means keep all archives.
            return
        log_path = Path(self.baseFilename)
        pattern = f"{log_path.stem}_*-*{log_path.suffix}"
        archives = list(log_path.parent.glob(pattern))  # Find rotated log files.
        archives.sort(key=lambda p: p.stat().st_mtime, reverse=True)  # Newest first.
        for old in archives[self.backup_count :]:
            try:
                old.unlink()
            except Exception:
                # Ignore deletion failures to avoid crashing logging.
                pass

logger = logging.getLogger('execution.log')  # Named logger for ETL runtime messages.
logger.setLevel(logging.INFO)  # Default log level for file and console output.
logger.propagate = False  # Don't bubble to root (Jupyter already has a StreamHandler).

# Only configure handlers once (prevents duplicate log lines on re-import).
if not logger.handlers:
    # Write to file and console to support both scheduled and interactive runs.
    log_dir = Path(LOG_PATH).parent
    log_dir.mkdir(parents=True, exist_ok=True)  # Ensure log directory exists.
    file_handler = IntervalRotatingFileHandler(
        LOG_PATH,
        interval_days=default_log_retention_days,
        backup_count=default_log_backup_count,
    )
    console_handler = logging.StreamHandler()  # Console output for interactive runs.

    class _ProfilePrefixFilter(logging.Filter):
        # Inject the profile prefix into every log record for formatting.
        def __init__(self, profile_prefix: str) -> None:
            super().__init__()
            self.profile_prefix = profile_prefix

        def filter(self, record: logging.LogRecord) -> bool:
            record.profile_prefix = self.profile_prefix
            return True

    fmt = '%(asctime)s [%(levelname)s] %(profile_prefix)s%(name)s: %(message)s'
    formatter = logging.Formatter(fmt)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.addFilter(_ProfilePrefixFilter(_profile_prefix))

    # Windows Event Log (optional). Requires a registered source name.
    try:
        event_handler = NTEventLogHandler("UhsWarehouseEtl")
        event_handler.setLevel(default_event_log_level)
        event_handler.setFormatter(formatter)
        logger.addHandler(event_handler)
    except Exception:
        # Use logger.exception so the error is visible in file/console logs.
        logger.exception("Failed to attach Windows Event Log handler.")

    # SMTPHandler sends email notifications on errors for unattended runs.
    # Subject includes logger name and severity for quick triage.
    smtp = SMTPHandler(
        mailhost=('Rhmsmtp1', 25),
        fromaddr=default_email,
        toaddrs=[default_email],
        subject='[ETL Error] %(name)s - %(levelname)s'
    )
    smtp.setLevel(logging.ERROR)  # Only send emails for errors and above.
    smtp.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s in %(filename)s:\n\n%(message)s'
    ))
    logger.addHandler(smtp)

if _profile_errors:
    # Log profile configuration issues after logger is configured.
    joined = "\n- " + "\n- ".join(_profile_errors)
    logger.warning("ETL profile config issues:%s", joined)


def set_cycle_id(value: str | None) -> None:
    # Update the active cycle id (and keep environment in sync).
    global cycle_id
    if value:
        cycle_id = value
        os.environ["ETL_CYCLE_ID"] = value
    else:
        cycle_id = None
        os.environ.pop("ETL_CYCLE_ID", None)


def get_cycle_id() -> str | None:
    # Return the active cycle id (falling back to environment if needed).
    return cycle_id or os.environ.get("ETL_CYCLE_ID")
    

PipelineConfig = tuple[dict, dict, Path | None]  # (sql_dirs, pipeline_sets, sql_root_override)


def _load_pipeline_config(
    config_path: Path, sql_root_path: Path
) -> PipelineConfig:
    # Load and validate pipeline configuration mapping stage names to directories.
    if not config_path.exists():
        # Missing config file means fall back to built-in defaults.
        logger.warning("Pipeline config not found: %s", config_path)
        return {}, {}, None
    try:
        # Parse JSON pipeline config.
        with config_path.open("r", encoding="utf-8") as fh:
            config = json.load(fh)
    except Exception:
        # Log stack trace to help diagnose config parsing errors.
        logger.exception("Failed to load pipeline config: %s", config_path)
        return {}, {}, None

    errors: list[str] = []  # Collect validation errors to report as a warning.
    raw_root = config.get("sql_root_path")  # Optional override for SQL root folder.
    raw_dirs = config.get("sql_dirs")  # Expected to be a dict of stage->relative path.
    raw_sets = config.get("pipeline_stage_sets")  # Expected to be a dict of pipeline->stages.
    root_override: Path | None = None  # Tracks explicit root override from config.
    root_path = sql_root_path  # Default to the profile/sql folder root provided by caller.
    if raw_root is not None:
        if isinstance(raw_root, str):
            if raw_root.strip():
                root_override = Path(raw_root)  # Use explicit root path from config.
                # If override is repo root, append the default SQL folder name.
                if root_override.name == sql_root_path.name:
                    root_path = root_override
                else:
                    root_path = root_override / sql_root_path.name
        else:
            errors.append("sql_root_path must be a string.")
    if not isinstance(raw_dirs, dict):
        errors.append("sql_dirs missing or not an object.")
        raw_dirs = {}
    if not isinstance(raw_sets, dict):
        errors.append("pipeline_stage_sets missing or not an object.")
        raw_sets = {}

    # Resolve relative sql_dirs to absolute paths under the resolved SQL root.
    sql_dirs_loaded = {
        key: (root_path / Path(value))
        for key, value in raw_dirs.items()
        if isinstance(key, str) and isinstance(value, str)
    }
    for key, value in raw_dirs.items():
        if not isinstance(key, str) or not isinstance(value, str):
            errors.append(f"sql_dirs entry must be string->string: {key!r}.")

    pipeline_sets: dict = {}
    for pipeline_name, stages in raw_sets.items():
        if not isinstance(pipeline_name, str):
            errors.append("pipeline_stage_sets keys must be strings.")
            continue
        if not isinstance(stages, list):
            errors.append(f"{pipeline_name}: stages must be a list.")
            continue
        pipeline_sets[pipeline_name] = stages

    if pipeline_sets:
        # Validate that each referenced stage exists in sql_dirs.
        stage_names = set(sql_dirs_loaded.keys())
        for pipeline_name, stages in pipeline_sets.items():
            for item in stages:
                if isinstance(item, str):
                    stage_name = item
                elif isinstance(item, dict):
                    stage_name = item.get("stage")
                    concurrent = item.get("concurrent")
                    if concurrent is not None and not isinstance(concurrent, bool):
                        errors.append(
                            f"{pipeline_name}: concurrent must be true/false for stage "
                            f"{stage_name!r}."
                        )
                else:
                    errors.append(f"{pipeline_name}: stage entry must be string or object.")
                    continue

                if isinstance(stage_name, str):
                    if stage_name not in stage_names:
                        errors.append(
                            f"{pipeline_name}: stage '{stage_name}' not found in sql_dirs."
                        )
                else:
                    errors.append(f"{pipeline_name}: stage name is missing or invalid.")

    if errors:
        # On validation errors, warn and return empty so defaults are used.
        joined = "\n- " + "\n- ".join(errors)
        logger.warning(
            "Pipeline config validation failed; falling back to defaults. Issues:%s",
            joined,
        )
        return {}, {}, root_path if root_override else None

    return sql_dirs_loaded, pipeline_sets, root_path if root_override else None


# ---- Resolve SQL root ----
def _resolve_default_sql_root(sql_folder: str) -> Path:
    # Prefer a sibling "sql" repo when present above the app repo.
    for parent in (repo_root, *repo_root.parents):
        sql_root = parent / "sql"
        if sql_root.is_dir():
            return sql_root / sql_folder

    # Fallback to repo-managed SQL folder for local/dev use.
    return repo_root / sql_folder


# Base SQL folder path, either default or profile override.
_default_sql_root = _resolve_default_sql_root(default_sql_folder)
# Map stage names to their SQL dirs; keys are referenced in PIPELINE_STAGE_SETS.
_sql_dirs_default = {
    "pre_deploy":                   _default_sql_root / "01_Deploy" / "01_PreDeploy",  # Pre-deploy scripts.
    "integration_config":           _default_sql_root / "01_Deploy" / "02_Tables" / "01A_Integration_Config",  # Config tables.
    "integration_cohort":           _default_sql_root / "01_Deploy" / "02_Tables" / "01B_Integration_Cohort",  # Cohort tables.
    "integration_staging":          _default_sql_root / "01_Deploy" / "02_Tables" / "01C_Integration_Staging",  # Staging tables.
    "integration_other":            _default_sql_root / "01_Deploy" / "02_Tables" / "01D_Integration_Other",  # Other tables.
    "dimension":                    _default_sql_root / "01_Deploy" / "02_Tables" / "02_Dimension",  # Dimension tables.
    "fact":                         _default_sql_root / "01_Deploy" / "02_Tables" / "03_Fact",  # Fact tables.
    "pre_fullload":                 _default_sql_root / "03_Load"   / "01_PreLoad" / "01_Populate_IntConfig_Parameter" / "02_Full",  # Full load params.
    "pre_deltaload":                _default_sql_root / "03_Load"   / "01_PreLoad" / "01_Populate_IntConfig_Parameter" / "01_Delta",  # Delta load params.
    "tracking_pre":                 _default_sql_root / "03_Load"   / "01_PreLoad" / "02_Populate_IntConfig_ChangeTracking",  # Change tracking setup.
    "identify_range":               _default_sql_root / "03_Load"   / "02_Staging" / "01_Change_tracking_identify_record_range",  # CT range find.
    "individual_cohorts":           _default_sql_root / "03_Load"   / "02_Staging" / "02_Populate_individual_cohort_tables",  # Individual cohorts.
    "combined_cohorts_primary":     _default_sql_root / "03_Load"   / "02_Staging" / "03A_Populate_combined_cohort_tables_primary",  # Combined cohorts (primary).
    "combined_cohorts_secondary":   _default_sql_root / "03_Load"   / "02_Staging" / "03B_Populate_combined_cohort_tables_secondary",  # Combined cohorts (secondary).
    "staging_populate":             _default_sql_root / "03_Load"   / "02_Staging" / "04_Populate_staging_tables",  # Populate staging tables.
    "master_populate":              _default_sql_root / "03_Load"   / "03_Master" / "01_Populate_master_tables",  # Populate master tables.
    "dims_unknown_members":         _default_sql_root / "03_Load"   / "04A_Dimensions_unknown_members",  # Unknown member rows.
    "dims_populate":                _default_sql_root / "03_Load"   / "04B_Dimensions_populate",  # Populate dimensions.
    "dims_populate_sequential":     _default_sql_root / "03_Load"   / "04C_Dimensions_populate_sequential",  # Sequential dimension steps.
    "facts_inferred":               _default_sql_root / "03_Load"   / "05A_Facts_inferred",  # Inferred facts.
    "facts_populate":               _default_sql_root / "03_Load"   / "05B_Facts_populate",  # Populate facts.
    "facts_deletion":               _default_sql_root / "03_Load"   / "05C_Facts_deletion",  # Fact deletions.
    "stats_postload":               _default_sql_root / "03_Load"   / "06_PostLoad" / "01_Statistics",  # Post-load stats.
    "close_delta":                  _default_sql_root / "03_Load"   / "06_PostLoad" / "02_Change_tracking_close_delta_range",  # Close delta range.
}

class StageSpecDict(TypedDict):
    # Explicit stage definition for steps that must be serialized.
    # "stage" maps to a sql_dirs key; "concurrent" controls parallelism.
    stage: str
    concurrent: bool


StageSpec = Union[str, StageSpecDict]  # Stage entry is either a name or an explicit dict.

# PIPELINE_STAGE_SETS defines the ordered stage lists for each run mode.
# List entries can be strings or dicts with explicit concurrency flags.
_PIPELINE_STAGE_SETS_DEFAULT: dict[str, list[StageSpec]] = {
    # Minimal pipeline used for quick checks / small runs.
    "test": [
        "pre_deltaload",               # Seed delta parameters.
        "tracking_pre",                # Prepare tracking tables.
        # "identify_range",            # Disabled for test runs by default.
        # "individual_cohorts",        # Disabled for test runs by default.
    ],
    # Full load pipeline (includes sequential-only stages where required).
    "full": [
        "pre_fullload",                                                             # Full load parameters.
        "tracking_pre",                                                             # Change tracking prep.
        "identify_range",                                                           # Determine CT range.
        "individual_cohorts",                                                       # Load individual cohort tables.
        "combined_cohorts_primary",                                                 # Load primary combined cohorts.
        "combined_cohorts_secondary",                                               # Load secondary combined cohorts.
        "staging_populate",                                                         # Populate staging tables.
        "master_populate",                                                          # Populate master tables.
        "dims_unknown_members",                                                     # Insert unknown members.
        "dims_populate",                                                            # Populate dimensions.
        {"stage": "dims_populate_sequential",               "concurrent": False},   # Sequential dimension steps.
        {"stage": "facts_inferred",                         "concurrent": False},   # Inferred facts must be serialized.
        "facts_populate",                                                           # Populate facts.
        "facts_deletion",                                                           # Handle deletions.
        {"stage": "stats_postload",                         "concurrent": False},   # Post-load stats serialized.
        {"stage": "close_delta",                            "concurrent": False},   # Close delta range serialized.
    ],
    # Ad-hoc subset focused on downstream population stages.
    "adhoc": [
        {"stage": "dims_populate_sequential",               "concurrent": False},   # Sequential dimension steps.
        {"stage": "facts_inferred",                         "concurrent": False},   # Inferred facts serialized.
        "facts_populate",                                                           # Populate facts.
        "facts_deletion",                                                           # Handle deletions.
        {"stage": "stats_postload",                         "concurrent": False},   # Post-load stats serialized.
        {"stage": "close_delta",                            "concurrent": False},   # Close delta range serialized.
    ],
    # Delta load pipeline (skips some full-load-only stages).
    "delta": [
        "pre_deltaload",                                                            # Delta load parameters.
        "tracking_pre",                                                             # Change tracking prep.
        "identify_range",                                                           # Determine CT range.
        "individual_cohorts",                                                       # Load individual cohort tables.
        "combined_cohorts_primary",                                                 # Load primary combined cohorts.
        "combined_cohorts_secondary",                                               # Load secondary combined cohorts.
        "staging_populate",                                                         # Populate staging tables.
        "master_populate",                                                          # Populate master tables.
        #"dims_unknown_members",                                                    # Intentionally skipped for delta.
        "dims_populate",                                                            # Populate dimensions.
        {"stage": "dims_populate_sequential",               "concurrent": False},   # Sequential dimension steps.
        {"stage": "facts_inferred",                         "concurrent": False},   # Inferred facts serialized.
        "facts_populate",                                                           # Populate facts.
        "facts_deletion",                                                           # Handle deletions.
        {"stage": "stats_postload",                         "concurrent": False},   # Post-load stats serialized.
        {"stage": "close_delta",                            "concurrent": False},   # Close delta range serialized.
    ],
    # Deploy pipeline covers full deployment + load for initial setup.
    "deploy": [
        # ETL Run
        ## Deploy
        ### Pre Depl    oy
        {"stage": "pre_deploy",                             "concurrent": True},    # Pre-deploy scripts.
        ### Create tables
        #### Create Co  nfig tables
        {"stage": "integration_config",                     "concurrent": True},    # Config tables.
        #### Create Cohort tables
        {"stage": "integration_cohort",                     "concurrent": True},    # Cohort tables.
        #### Create Staging tables
        {"stage": "integration_staging",                    "concurrent": True},    # Staging tables.
        #### Create Other tables
        {"stage": "integration_other",                      "concurrent": True},    # Other tables.
        #### Create Dimension tables
        {"stage": "dimension",                              "concurrent": True},    # Dimension tables.
        #### Create Fact tables
        {"stage": "fact",                                   "concurrent": True},    # Fact tables.
        #### Set ETL to Full load and prepare meta tables
        {"stage": "pre_fullload",                           "concurrent": True},    # Full load params.
        #### Set tracking change records
        {"stage": "tracking_pre",                           "concurrent": True},    # Change tracking prep.
        ### Staging
        #### Set change tracking for start and end sequence for __ct tables
        {"stage": "identify_range",                         "concurrent": True},    # CT range.
        #### Populate individual cohort tables
        {"stage": "individual_cohorts",                     "concurrent": True},    # Individual cohorts.
        #### Populate combined cohort tables
        {"stage": "combined_cohorts_primary",               "concurrent": True},    # Combined cohorts primary.
        {"stage": "combined_cohorts_secondary",             "concurrent": True},    # Combined cohorts secondary.
        #### Populate staging table
        {"stage": "staging_populate",                       "concurrent": True},    # Staging tables.
        #### Populate master tables
        {"stage": "master_populate",                        "concurrent": True},    # Master tables.
        #### Populate dimension tables
        {"stage": "dims_unknown_members",                   "concurrent": True},    # Unknown member rows.
        {"stage": "dims_populate",                          "concurrent": True},    # Populate dimensions.
        {"stage": "dims_populate_sequential",               "concurrent": False},   # Sequential dimension steps.
        #### Populate Fact tables
        {"stage": "facts_inferred",                         "concurrent": False},   # Inferred facts serialized.
        {"stage": "facts_populate",                         "concurrent": True},    # Populate facts.
        {"stage": "facts_deletion",                         "concurrent": True},    # Handle deletions.
        ### Post load
        #### PostLoad run statistics
        {"stage": "stats_postload",                         "concurrent": False},   # Post-load stats.
        #### Post load update change tracking
        {"stage": "close_delta",                            "concurrent": False},   # Close delta range.
    ],
}

# Attempt to load overrides from pipeline_config.json.
_sql_dirs_loaded, _pipeline_sets_loaded, _sql_root_loaded = _load_pipeline_config(
    pipeline_config_path, _default_sql_root
)
# Use config-provided SQL root if available and valid.
sql_root = _sql_root_loaded or _default_sql_root
# Use loaded config if valid; otherwise fall back to defaults.
sql_dirs = _sql_dirs_loaded or _sql_dirs_default
PIPELINE_STAGE_SETS = _pipeline_sets_loaded or _PIPELINE_STAGE_SETS_DEFAULT

