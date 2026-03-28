"""
split_record.py
───────────────
Dataset : movie_ratings.csv  (1800 rows)
Known issue: 'metascore' column has 850 null values (~47%)

Splits the DataFrame into two subsets:
  - Rows WITH at least one null  → saved to error_output_path
  - Rows that are fully clean    → saved to clean_output_path

The clean DataFrame is pushed to XCom for Convert_to_parquet.
"""

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def split_record(
    error_output_path: str,
    clean_output_path: str,
    **context,
) -> None:
    """
    Reads the DataFrame from XCom, splits on null rows, saves both halves.

    Outputs
    -------
    {error_output_path}/errors_<run_id>.csv   Rows with nulls (metascore missing)
    {clean_output_path}/clean_<run_id>.csv    Fully populated rows

    XCom keys pushed
    ----------------
    clean_dataframe  str   JSON-serialised clean DataFrame.
    clean_file_path  str   Path to the saved clean CSV.
    error_file_path  str   Path to the saved error CSV.
    """
    ti     = context["ti"]
    run_id = context["run_id"]

    # ── Load DataFrame from XCom ──────────────────────────────────────────────
    raw_json: str | None = ti.xcom_pull(task_ids="Check_Input", key="dataframe")
    if not raw_json:
        raise ValueError("No DataFrame found in XCom from Check_Input.")

    df = pd.read_json(raw_json, orient="records")

    # Rename the index column for clarity
    if "Unnamed: 0" in df.columns:
        df = df.rename(columns={"Unnamed: 0": "row_id"})

    logger.info("Loaded DataFrame: %d rows x %d cols", *df.shape)

    # ── Split on null rows ────────────────────────────────────────────────────
    mask_error = df.isnull().any(axis=1)
    df_errors  = df[mask_error].copy()
    df_clean   = df[~mask_error].copy()

    logger.info(
        "Split result -> error rows (nulls in metascore etc.): %d | clean rows: %d",
        len(df_errors), len(df_clean),
    )

    # Log which columns caused the errors
    null_cols = df_errors.isnull().sum()
    null_cols = null_cols[null_cols > 0]
    for col, cnt in null_cols.items():
        logger.info("  Column '%s': %d null(s) in error set", col, cnt)

    # ── Save error records ────────────────────────────────────────────────────
    _ensure_dir(error_output_path)
    error_file = os.path.join(error_output_path, f"errors_{run_id}.csv")
    df_errors.to_csv(error_file, index=False)
    logger.info("Error records saved  -> %s", error_file)

    # ── Save clean records ────────────────────────────────────────────────────
    _ensure_dir(clean_output_path)
    clean_file = os.path.join(clean_output_path, f"clean_{run_id}.csv")
    df_clean.to_csv(clean_file, index=False)
    logger.info("Clean records saved  -> %s", clean_file)

    # ── Push to XCom for Convert_to_parquet ──────────────────────────────────
    ti.xcom_push(
        key="clean_dataframe",
        value=df_clean.to_json(orient="records", date_format="iso"),
    )
    ti.xcom_push(key="clean_file_path", value=clean_file)
    ti.xcom_push(key="error_file_path", value=error_file)


def _ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)
