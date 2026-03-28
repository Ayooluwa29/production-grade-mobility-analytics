SELECT
    payment_provider,
    COUNT(*)                              AS total_payments,
    COUNTIF(payment_status = 'failed')    AS failed_payments,
    ROUND(COUNTIF(payment_status = 'failed') / COUNT(*) * 100, 2) AS failure_rate_pct
FROM {{ fct_payments }}
GROUP BY 1
ORDER BY 3 DESC