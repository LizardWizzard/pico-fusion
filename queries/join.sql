SELECT
    first_value(u.name),
    COUNT(*),
    SUM(package_weight) AS pw
FROM
    deliveries
    JOIN picodata.demo.users u ON deliveries.user_id = u.id
GROUP BY
    deliveries.user_id
ORDER BY
    pw