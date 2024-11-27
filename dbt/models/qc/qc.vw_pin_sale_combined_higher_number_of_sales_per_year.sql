SELECT
    pin,
    year,
    COUNT(*) AS num_sales
FROM {{ ref("default.vw_pin_sale_combined") }}
GROUP BY pin, year
HAVING COUNT(*) > 3
