SELECT *
FROM 
{{ ref('fact_payments') }}
WHERE net_revenue < 0