-- Gathers AVs by year, major class, assessment stage, and
-- municipality for reporting

-- Add total and median values by municipality
SELECT
    vpvl.year,
    LOWER(vpvl.stage_name) AS stage,
    munis.municipality_name,
    munis.major_class AS class,
    AVG(CAST(munis.reassessment_year AS INT)) AS portion_reassessed,
    COUNT(*) AS n,
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
WHERE munis.municipality_name IS NOT NULL
GROUP BY
    munis.municipality_name,
    vpvl.year,
    munis.major_class,
    munis.triad_name,
    vpvl.stage_name,
ORDER BY
    munis.municipality_name,
    vpvl.year,
    vpvl.stage_name,
    munis.major_class
