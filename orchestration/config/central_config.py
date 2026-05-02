from __future__ import annotations
import os
from datetime import timedelta


def _require_env(var: str) -> str:
    """Raise a clear error at import time if a required env var is missing."""
    value = os.getenv(var)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{var}' is not set. "
            f"Add it to your Airflow environment or .env file."
        )
    return value


# dbt binary + project paths (all required, no hardcoded fallbacks)

DBT_EXECUTABLE: str   = _require_env("DBT_EXECUTABLE")
DBT_PROJECT_DIR: str  = _require_env("DBT_PROJECT_DIR")
DBT_PROFILES_DIR: str = _require_env("DBT_PROFILES_DIR")

DBT_PROFILE: str = os.getenv("DBT_PROFILE", "beejan_ride_analytics")
DBT_TARGET: str  = os.getenv("DBT_TARGET",  "dev")

# Optional: extra env vars injected into every BashOperator
DBT_ENV: dict[str, str] = {
    "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
}

# Layer definitions (executed top → bottom)

DBT_LAYERS: list[dict] = [
    {
        "name":         "staging",
        "select":       "staging.*",
        "command":      "run",
        "run_tests":    True,
        "full_refresh": False,
        "description":  "Light renaming and casting of raw source tables",
    },
    {
        "name":         "snapshot",
        "select":       None,
        "command":      "snapshot",
        "run_tests":    False,
        "full_refresh": False,
        "description":  "Capture slowly changing dimensions",
    },
    {
        "name":         "intermediate",
        "select":       "intermediate.*",
        "command":      "run",
        "run_tests":    False,
        "full_refresh": False,
        "description":  "Business logic joins and aggregations",
    },
    {
        "name":         "marts",
        "select":       "marts.*",
        "command":      "run",
        "run_tests":    True,
        "full_refresh": False,
        "description":  "Fact and dimension models consumed by BI tools",
    },
]

# Airflow connection IDs

AIRBYTE_CONN_ID: str = os.getenv("AIRBYTE_CONN_ID", "airbyte_default")

# One entry per Airbyte connection:
#   key   = logical name  → becomes the Airflow task_id suffix
#   value = Airbyte connection UUID
AIRBYTE_CONNECTIONS: dict[str, str] = {
    "raw_table_ingestions": _require_env("AIRBYTE_CONNECTION_UUID_RAW_TABLE_INGESTIONS"),
}

# DAG schedules

INGESTION_SCHEDULE: str = os.getenv("INGESTION_SCHEDULE", "0 2 * * *")  # 02:00 UTC daily
TRANSFORMATION_SCHEDULE = None  # sensor-driven

# Sensor settings

SENSOR_POKE_INTERVAL: float = float(os.getenv("SENSOR_POKE_INTERVAL", "60"))
SENSOR_TIMEOUT: float       = float(os.getenv("SENSOR_TIMEOUT", "7200"))
SENSOR_MODE: str            = os.getenv("SENSOR_MODE", "reschedule")

# Shared Airflow default_args

DEFAULT_ARGS: dict = {
    "owner":            os.getenv("AIRFLOW_OWNER", "data-engineering"),
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          int(os.getenv("AIRFLOW_RETRIES", "1")),
    "retry_delay":      timedelta(minutes=int(os.getenv("AIRFLOW_RETRY_DELAY_MINUTES", "5"))),
}