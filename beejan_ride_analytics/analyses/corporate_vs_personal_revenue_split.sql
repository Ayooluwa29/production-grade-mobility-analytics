SELECT
    rider_type,
    SUM(net_revenue)  AS total_net_revenue,
    COUNT(trip_id)    AS total_trips,
    ROUND(SUM(net_revenue) / SUM(SUM(net_revenue)) OVER () * 100, 2) AS pct
FROM {{ fct_trips }}
WHERE status = 'completed'
GROUP BY 1