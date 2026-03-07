{% macro to_utc(column_name, local_timezone='Europe/London') -%}
    -- Converts a local string/datetime to a UTC Timestamp based on the provided zone
    TIMESTAMP(DATETIME(CAST({{ column_name }} AS TIMESTAMP), "{{ local_timezone }}"))
{%- endmacro %}