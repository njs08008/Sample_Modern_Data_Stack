from airflow import DAG
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

"""
This script was working earlier in development,
but then I made changes and introduced a bug.
It currently fails to run. Sorry!
"""

# =========================
# DAG CONFIG
# =========================

default_args = {
    "owner": "data_engineering_project",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# =========================
# DAG
# =========================

with DAG(
    dag_id="telemetry_pipeline",
    default_args=default_args,
    description="Telemetry pipeline using SparkSubmitOperator",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["telemetry", "spark", "etl"],
) as dag:

    # -------------------------
    # BRONZE LAYER
    # -------------------------
    bronze = SparkSubmitOperator(
        task_id="bronze_load",
        application="spark_jobs/bronze_load.py",
        name="bronze_load",
        verbose=True,
        conn_id="spark_default",
        conf={
            "spark.master": "local[*]"
        }
    )

    # -------------------------
    # SILVER LAYER
    # -------------------------
    silver = SparkSubmitOperator(
        task_id="silver_transform",
        application="spark_jobs/bronze_to_silver.py",
        name="silver_transform",
        verbose=True,
        conn_id="spark_default",
        conf={
            "spark.master": "local[*]"
        }
    )

    # -------------------------
    # SCD
    # -------------------------
    scd = SparkSubmitOperator(
        task_id="update_dim_user_scd",
        application="spark_jobs/update_dim_user_scd.py",
        name="update_dim_user_scd",
        verbose=True,
        conn_id="spark_default",
        conf={
            "spark.master": "local[*]"
        }
    )

    # -------------------------
    # GOLD LAYER
    # -------------------------
    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="""
    cd dbt_project &&
    dbt run
    """,
)

    # -------------------------
    # DEPENDENCIES
    # -------------------------
    bronze >> silver >> scd >> dbt_run