"""
pipeline_dag.py
───────────────
DAG  : data_validation_pipeline
Input: movie_ratings.csv  (1800 rows, columns: Unnamed:0, movie, year, imdb, metascore, votes)

Flow:
  Start
    └─> Check_Input          PythonOperator       validates columns & nulls
          └─> Issue_Found?   BranchPythonOperator routes on has_issue flag
                ├─(Yes)─> Split_Record           separates error / clean rows
                │               └─> Convert_to_parquet
                └─(No)──────────> Convert_to_parquet
                                        └─> End
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

from tasks.check_input import check_input
from tasks.issue_found import decide_branch
from tasks.split_record import split_record
from tasks.convert_to_parquet import convert_to_parquet

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
    dag_id="data_validation_pipeline",
    default_args=default_args,
    description="Validates movie_ratings.csv, splits on nulls, converts clean data to Parquet.",
    schedule_interval=None,   # triggered manually
    catchup=False,
    tags=["movies", "validation", "parquet"],
) as dag:

    # ── Step 1: Check Input ───────────────────────────────────────────────────
    task_check_input = PythonOperator(
        task_id="Check_Input",
        python_callable=check_input,
        op_kwargs={
            # Default points to movie_ratings.csv mounted in /opt/airflow/data
            "input_path": "{{ dag_run.conf.get('input_path', '/opt/airflow/data/input/movie_ratings.csv') }}",
        },
    )

    # ── Step 2: Branch — Issue Found? ─────────────────────────────────────────
    # BranchPythonOperator reads has_issue from XCom and returns next task_id
    task_issue_found = BranchPythonOperator(
        task_id="Issue_Found",
        python_callable=decide_branch,
    )

    # ── Step 3a: Split Record  (Yes branch — issues detected) ─────────────────
    task_split_record = PythonOperator(
        task_id="Split_Record",
        python_callable=split_record,
        op_kwargs={
            "error_output_path": "{{ dag_run.conf.get('error_output_path', '/opt/airflow/data/errors/') }}",
            "clean_output_path": "{{ dag_run.conf.get('clean_output_path', '/opt/airflow/data/clean/') }}",
        },
    )

    # ── Step 3b: Convert to Parquet  (No branch — data is clean) ─────────────
    task_convert_to_parquet = PythonOperator(
        task_id="Convert_to_parquet",
        python_callable=convert_to_parquet,
        # trigger_rule=ALL_DONE so this task runs after EITHER branch
        trigger_rule="none_failed_min_one_success",
        op_kwargs={
            "parquet_output_path": "{{ dag_run.conf.get('parquet_output_path', '/opt/airflow/data/parquet/') }}",
        },
    )

    # ── Dependency graph ──────────────────────────────────────────────────────
    task_check_input >> task_issue_found
    task_issue_found >> [task_split_record, task_convert_to_parquet]
    task_split_record >> task_convert_to_parquet
