from __future__ import annotations
import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

sys.path.append(os.path.dirname(__file__))
import config.central_config as cfg

with DAG(
    dag_id="airbyte_sync_raw_tables",
    schedule=cfg.INGESTION_SCHEDULE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=cfg.DEFAULT_ARGS,
    tags=["ingestion", "airbyte", "beejan_ride"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    sync_tasks   = []
    sensor_tasks = []

    for name, conn_uuid in cfg.AIRBYTE_CONNECTIONS.items():

        trigger = AirbyteTriggerSyncOperator(
            task_id=f"sync_{name}",
            airbyte_conn_id=cfg.AIRBYTE_CONN_ID,
            connection_id=conn_uuid,
            asynchronous=True,          # fires the sync and returns the job_id immediately
        )

        sensor = AirbyteJobSensor(
            task_id=f"wait_{name}",
            airbyte_conn_id=cfg.AIRBYTE_CONN_ID,
            airbyte_job_id=trigger.output,   # XCom job_id from the trigger above
            poke_interval=cfg.SENSOR_POKE_INTERVAL,
            timeout=cfg.SENSOR_TIMEOUT,
            mode=cfg.SENSOR_MODE,            # "reschedule" frees the worker slot while polling
        )

        trigger >> sensor
        sync_tasks.append(trigger)
        sensor_tasks.append(sensor)

    start >> sync_tasks
    sensor_tasks >> end