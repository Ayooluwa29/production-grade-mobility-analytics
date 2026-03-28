SELECT *
FROM 
{{ ref('fact_trips') }}
WHERE status = 'completed'
AND
failed_payment_flag = 1