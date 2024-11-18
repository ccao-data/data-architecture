SELECT
    pin,
    year,
    COUNT(*) AS num_sales
FROM {{ ref("default.vw_pin_sale") }}
GROUP BY pin, year
HAVING COUNT(*) > 3
