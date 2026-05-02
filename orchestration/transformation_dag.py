from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

sys.path.append(os.path.dirname(__file__))

import config.central_config as cfg


def dbt_bash(task_id: str, command: str, select: str | None = None, full_refresh: bool = False) -> BashOperator:
    """Builds a dbt CLI command and wraps it in a BashOperator."""
    parts = [
        cfg.DBT_EXECUTABLE,
        command,
        "--project-dir", cfg.DBT_PROJECT_DIR,
        "--profiles-dir", cfg.DBT_PROFILES_DIR,
        "--profile", cfg.DBT_PROFILE,
        "--target", cfg.DBT_TARGET,
    ]
    if select:
        parts += ["--select", select]
    if full_refresh and command == "run":
        parts.append("--full-refresh")

    return BashOperator(
        task_id=task_id,
        bash_command=" ".join(parts),
        env=cfg.DBT_ENV or None,
        append_env=True,
    )


with DAG(
    dag_id="dbt_layers_run",
    schedule=cfg.TRANSFORMATION_SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=cfg.DEFAULT_ARGS,
    tags=["transformation", "dbt", "beejan_ride"],
) as dag:

    # wait_for_ingestion = ExternalTaskSensor(
    #     task_id="wait_for_ingestion",
    #     external_dag_id="airbyte_sync",
    #     external_task_id=None,          # waits for the whole DAG run
    #     mode="reschedule",
    #     poke_interval=cfg.SENSOR_POKE_INTERVAL,
    #     timeout=cfg.SENSOR_TIMEOUT,
    # )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    previous = start

    for layer in cfg.DBT_LAYERS:
        name         = layer["name"]
        command      = layer.get("command", "run")
        select       = layer.get("select")
        run_tests    = layer.get("run_tests", False)
        full_refresh = layer.get("full_refresh", False)

        with TaskGroup(group_id=f"layer_{name}") as tg:
            run_task = dbt_bash(
                task_id=f"dbt_{command}_{name}",
                command=command,
                select=select,
                full_refresh=full_refresh,
            )
            if run_tests and select:
                test_task = dbt_bash(
                    task_id=f"dbt_test_{name}",
                    command="test",
                    select=select,
                )
                run_task >> test_task
                last = test_task
            else:
                last = run_task

        previous >> tg
        previous = last

    previous >> end