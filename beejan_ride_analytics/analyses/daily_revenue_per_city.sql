SELECT
    c.city_name,
    DATE(t.pickup_at)     AS trip_date,
    SUM(t.net_revenue)    AS net_revenue,
    SUM(t.actual_fare)    AS gross_revenue,
    COUNT(t.trip_id)      AS total_trips
FROM ref{{ fct_trips }} t
JOIN {{ dim_cities }} c
ON t.city_id = c.city_id
WHERE t.status = 'completed'
GROUP BY 1, 2
ORDER BY 2 DESC