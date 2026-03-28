SELECT *
FROM 
{{ ref('fact_trips') }}
WHERE duration_minutes < 0