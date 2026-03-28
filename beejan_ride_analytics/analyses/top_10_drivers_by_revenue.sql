SELECT
    d.driver_id,
    d.rating,
    d.driver_lifetime_trips,
    SUM(t.net_revenue) AS total_revenue
FROM {{ fct_trips }} t
JOIN {{ dim_drivers_current }} d
ON t.driver_id = d.driver_id
WHERE t.status = 'completed'
GROUP BY 1, 2, 3
ORDER BY 4 DESC
LIMIT 10