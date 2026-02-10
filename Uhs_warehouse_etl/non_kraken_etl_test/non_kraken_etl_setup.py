# non_kraken_etl_test/non_kraken_etl_setup.py

import logging
from logging.handlers import SMTPHandler
from pathlib import Path
from typing import TypedDict, Union

# ---- Defaults / config ----
# These defaults control the SQL root, login context, and behavior when
# the orchestration is run without explicit overrides.
default_db = "KRAKENDEV2"  # Default database used by the ETL connection.
default_user = "sqlkraken"  # Default SQL login for ETL execution.
default_sql_folder = "SQL_Test"  # Test SQL root folder (or SQL_Synapse).
default_email = 'paul.nasti@uhs.nhs.uk'  # Email target for error notifications.
default_isolationlevel = "AUTOCOMMIT"  # Avoid long-running open transactions.
default_untagged_run_mode = "parallel"  # Default for stages without concurrency tags.

# ---- Logger setup ----
# Configure once at module-load so every import gets a ready logger.
# Logger name matches the log path for quick identification in outputs.
logger = logging.getLogger('./logs/testexecution.log')
logger.setLevel(logging.INFO)
logger.propagate = False  # don't bubble to root (Jupyter already has a StreamHandler)

# Only configure handlers once (prevents duplicate log lines on re-import).
if not logger.handlers:
    # Write to file and console to support both scheduled and interactive runs.
    file_handler = logging.FileHandler('./logs/testexecution.log')
    console_handler = logging.StreamHandler()

    fmt = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    formatter = logging.Formatter(fmt)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # SMTPHandler sends email notifications on errors for unattended runs.
    # Subject includes logger name and severity for quick triage.
    smtp = SMTPHandler(
        mailhost=('Rhmsmtp1', 25),
        fromaddr=default_email,
        toaddrs=[default_email],
        subject='[ETL Error] %(name)s – %(levelname)s'
    )
    smtp.setLevel(logging.ERROR)
    smtp.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s in %(filename)s:\n\n%(message)s'
    ))
    logger.addHandler(smtp)
    
# ---- Resolve SQL root ----
# __file__ = ...\scripts\Uhs_warehouse_etl\non_kraken_etl_test\non_kraken_etl_setup.py
# Compute repo_root so this module is location-independent.
_pkg_dir = Path(__file__).resolve().parent          # ...\uhs_etl
repo_root = _pkg_dir.parents[2]                     # ...\KrakenBedrockReplacement
sql_root = repo_root / default_sql_folder           # ...\KrakenBedrockReplacement\SQL_Synapse

# Map stage names to their SQL dirs; keys are referenced in PIPELINE_STAGE_SETS.
sql_dirs = {
    "folder1":                   sql_root / "Folder1",
    "folder2":                   sql_root / "Folder2",
    "folder3":                   sql_root / "Folder3",

}

class StageSpecDict(TypedDict):
    # Explicit stage definition for steps that must be serialized.
    # "stage" maps to a sql_dirs key; "concurrent" controls parallelism.
    stage: str
    concurrent: bool


StageSpec = Union[str, StageSpecDict]

# PIPELINE_STAGE_SETS defines the ordered stage lists for each run mode.
# List entries can be strings or dicts with explicit concurrency flags.
PIPELINE_STAGE_SETS: dict[str, list[StageSpec]] = {
    # Minimal pipeline for quick orchestration checks.
    "runfolder3": [
        "folder3",               
    ],
    # Test pipeline covering multiple folders with mixed concurrency.
    "runfolders": [
        "folder1",
        {"stage": "folder2",                                "concurrent": True},   
        {"stage": "folder3",                                "concurrent": False},          
    ],
}
