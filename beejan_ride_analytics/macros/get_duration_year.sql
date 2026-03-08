{% macro get_duration_years(start_col, end_col) -%}
    -- tenure calculations
    round(
        cast(
            {{ dbt.datediff(start_col, end_col, 'day') }} / 365.25 as {{ dbt.type_numeric() }}
        ), 2
    )
{%- endmacro %}