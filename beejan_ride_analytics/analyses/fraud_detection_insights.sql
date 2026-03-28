SELECT
    DATE(created_at)                                             AS date,
    COUNTIF(fraud_detected)                                      AS fraud_cases,
    SUM(CASE WHEN fraud_detected THEN amount ELSE 0 END)         AS amount_at_risk
FROM {{ fct_payments }}
GROUP BY 1
ORDER BY 1 DESC