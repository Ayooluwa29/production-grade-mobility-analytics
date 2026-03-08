{% macro get_duration_minutes(start_col, end_col) -%}
    round(
        cast(
            {{ dbt.datediff(start_col, end_col, 'second') }} / 60.0 as {{ dbt.type_numeric() }}
        ), 2
    )
{%- endmacro %}