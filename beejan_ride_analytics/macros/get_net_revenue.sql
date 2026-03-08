{% macro get_net_revenue(fare, fee) %}
    -- Formula: (Fare - Fee) and also Handles NULLs by treating them as 0 and casts to a standard numeric type
    ( 
        coalesce(cast({{ fare }} as {{ dbt.type_numeric() }}), 0) - 
        coalesce(cast({{ fee }} as {{ dbt.type_numeric() }}), 0) 
    )
{% endmacro %}