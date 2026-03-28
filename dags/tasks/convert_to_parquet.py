"""
convert_to_parquet.py
─────────────────────
Converts the (clean) DataFrame to Parquet format.

This task is reached via TWO branches:
  • Directly from Issue_Found when no issues were detected.
  • After Split_Record when issues were found and records were split.

It intelligently reads the DataFrame from whichever upstream task
populated XCom — Split_Record (``clean_dataframe``) or Check_Input
(``dataframe``).
"""

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def convert_to_parquet(parquet_output_path: str, **context) -> None:
    """
    Reads the clean DataFrame from XCom and writes it as a Parquet file.

    Output
    ──────
    ``{parquet_output_path}/output_<run_id>.parquet``
    """
    ti     = context["ti"]
    run_id = context["run_id"]

    # ── Resolve which upstream task provided the DataFrame ────────────────────
    # Priority: Split_Record (clean subset) → Check_Input (full clean frame)
    raw_json: str | None = ti.xcom_pull(
        task_ids="Split_Record", key="clean_dataframe"
    )
    source = "Split_Record"

    if not raw_json:
        raw_json = ti.xcom_pull(task_ids="Check_Input", key="dataframe")
        source   = "Check_Input"

    if not raw_json:
        raise ValueError(
            "No DataFrame found in XCom from either Split_Record or Check_Input."
        )

    logger.info("Reading DataFrame from XCom (source: %s).", source)
    df = pd.read_json(raw_json, orient="records")
    logger.info("DataFrame shape: %d rows × %d cols", *df.shape)

    # ── Write Parquet ─────────────────────────────────────────────────────────
    _ensure_dir(parquet_output_path)
    output_file = os.path.join(parquet_output_path, f"output_{run_id}.parquet")

    df.to_parquet(output_file, index=False, engine="pyarrow", compression="snappy")
    logger.info("Parquet file written to: %s", output_file)

    # ── Push output path for potential downstream tasks ───────────────────────
    ti.xcom_push(key="parquet_file_path", value=output_file)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)
