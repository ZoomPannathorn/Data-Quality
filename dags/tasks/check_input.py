"""
check_input.py
──────────────
Dataset : movie_ratings.csv
Columns : Unnamed: 0 | movie | year | imdb | metascore | votes

Checking List
  1. Column names remain unchanged  (validated against EXPECTED_COLUMNS)
  2. None of the columns contain null values
     known issue: 'metascore' has 850 nulls out of 1800 rows

Pushes the validated DataFrame to XCom so downstream tasks can use it.
"""

import logging
import pandas as pd
from airflow.exceptions import AirflowFailException

logger = logging.getLogger(__name__)

# ── Expected schema for movie_ratings.csv ─────────────────────────────────────
EXPECTED_COLUMNS: list[str] = [
    "Unnamed: 0",   # original row index
    "movie",
    "year",
    "imdb",
    "metascore",
    "votes",
]


def check_input(input_path: str, **context) -> None:
    """
    Reads movie_ratings.csv, runs both validation checks, and pushes
    results to XCom.

    XCom keys pushed
    -----------------
    has_issue  bool  True when any check failed.
    dataframe  str   JSON-serialised DataFrame (records orient).
    issues     list  Human-readable descriptions of every problem.
    """
    logger.info("Reading input file: %s", input_path)
    df = _read_file(input_path)
    logger.info("Loaded %d rows x %d columns", *df.shape)

    issues: list[str] = []

    # ── Check 1: Column names unchanged ───────────────────────────────────────
    missing = set(EXPECTED_COLUMNS) - set(df.columns)
    extra   = set(df.columns) - set(EXPECTED_COLUMNS)

    if missing:
        issues.append(f"Missing columns: {sorted(missing)}")
        logger.warning("Missing columns: %s", sorted(missing))
    if extra:
        issues.append(f"Unexpected extra columns: {sorted(extra)}")
        logger.warning("Unexpected columns: %s", sorted(extra))

    if not missing and not extra:
        logger.info("Check 1 passed - all column names match expected schema.")

    # ── Check 2: No null values in any column ─────────────────────────────────
    null_counts     = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]

    if not cols_with_nulls.empty:
        for col, cnt in cols_with_nulls.items():
            pct = cnt / len(df) * 100
            msg = f"Column '{col}' has {cnt} null value(s) ({pct:.1f}%)"
            issues.append(msg)
            logger.warning(msg)
    else:
        logger.info("Check 2 passed - no null values found.")

    # ── Push results to XCom ──────────────────────────────────────────────────
    has_issue = bool(issues)
    ti = context["ti"]
    ti.xcom_push(key="has_issue",  value=has_issue)
    ti.xcom_push(key="dataframe",  value=df.to_json(orient="records", date_format="iso"))
    ti.xcom_push(key="issues",     value=issues)

    if has_issue:
        logger.warning("%d issue(s) detected. Routing to Split_Record.", len(issues))
    else:
        logger.info("All checks passed. Routing to Convert_to_parquet.")


def _read_file(path: str) -> pd.DataFrame:
    """Supports .csv, .tsv, .xlsx, .parquet."""
    ext = path.rsplit(".", 1)[-1].lower()
    readers = {
        "csv":     lambda p: pd.read_csv(p),
        "tsv":     lambda p: pd.read_csv(p, sep="\t"),
        "xlsx":    lambda p: pd.read_excel(p),
        "xls":     lambda p: pd.read_excel(p, engine="xlrd"),
        "parquet": lambda p: pd.read_parquet(p),
    }
    if ext not in readers:
        raise AirflowFailException(f"Unsupported file extension: .{ext}")
    return readers[ext](path)
