"""
issue_found.py
──────────────
BranchPythonOperator callable.

Reads the ``has_issue`` flag pushed by Check_Input and returns the
task_id of the next node to execute:

  • has_issue == True  → "Split_Record"
  • has_issue == False → "Convert_to_parquet"
"""

import logging

logger = logging.getLogger(__name__)

BRANCH_ISSUE   = "Split_Record"
BRANCH_NO_ISSUE = "Convert_to_parquet"


def decide_branch(**context) -> str:
    """
    BranchPythonOperator callable.

    Returns
    -------
    str
        The task_id Airflow should execute next.
    """
    ti = context["ti"]
    has_issue: bool = ti.xcom_pull(task_ids="Check_Input", key="has_issue")

    if has_issue is None:
        # XCom missing — treat as an issue to be safe
        logger.warning(
            "XCom key 'has_issue' not found from Check_Input. "
            "Defaulting to '%s'.", BRANCH_ISSUE
        )
        return BRANCH_ISSUE

    issues = ti.xcom_pull(task_ids="Check_Input", key="issues") or []
    if issues:
        logger.info(
            "Issue(s) detected (%d). Routing to '%s'.\n  %s",
            len(issues), BRANCH_ISSUE, "\n  ".join(issues),
        )
    else:
        logger.info("No issues detected. Routing to '%s'.", BRANCH_NO_ISSUE)

    return BRANCH_ISSUE if has_issue else BRANCH_NO_ISSUE
