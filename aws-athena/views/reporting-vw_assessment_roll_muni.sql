-- Gathers AVs by year, major class, assessment stage, and
-- municipality for reporting

WITH pin_counts AS (
    SELECT
        municipality_name,
        major_class,
        year,
        COUNT(*) AS total_n
    FROM {{ ref('reporting.vw_pin_township_class') }}
    GROUP BY
        municipality_name,
        year,
        major_class
)

-- Add total and median values by municipality
SELECT
    vpvl.year,
    COALESCE(LOWER(vpvl.stage_name), 'mailed') AS stage,
    COALESCE(munis.municipality_name = pin_counts.municipality_name)
        AS municipality_name,
    COALESCE(munis.major_class, pin_counts.major_class) AS class,
    CASE WHEN COUNT(*) IS NULL THEN 0 ELSE COUNT(*) END AS n,
    pin_counts.total_n,
    CAST(COUNT(*) AS DOUBLE)
    / CAST(pin_counts.total_n AS DOUBLE) AS stage_portion,
    SUM(vpvl.bldg) AS bldg_sum,
    CAST(APPROX_PERCENTILE(vpvl.bldg, 0.5) AS INT) AS bldg_median,
    SUM(vpvl.land) AS land_sum,
    CAST(APPROX_PERCENTILE(vpvl.land, 0.5) AS INT) AS land_median,
    SUM(vpvl.tot) AS tot_sum,
    CAST(APPROX_PERCENTILE(vpvl.tot, 0.5) AS INT) AS tot_median
FROM {{ ref('reporting.vw_pin_value_long') }} AS vpvl
LEFT JOIN {{ ref('reporting.vw_pin_township_class') }} AS munis
    ON vpvl.pin = munis.pin
    AND vpvl.year = munis.year
FULL OUTER JOIN pin_counts
    ON munis.municipality_name = pin_counts.municipality_name
    AND munis.major_class = pin_counts.major_class
    AND munis.year = pin_counts.year
WHERE munis.municipality_name IS NOT NULL
GROUP BY
    munis.municipality_name,
    vpvl.year,
    munis.major_class,
    vpvl.stage_name,
    pin_counts.total_n
ORDER BY
    munis.municipality_name,
    vpvl.year,
    vpvl.stage_name,
    munis.major_class
