-- Gathers AVs by year, major class, assessment stage, and
-- township for reporting

-- Add total and median values by township
SELECT
    vpvl.year,
    LOWER(vpvl.stage_name) AS stage,
    leg.cityname AS municipality_name,
    townships.major_class AS class,
    townships.reassessment_year,
    COUNT(*) AS n,
    SUM(vpvl.bldg) AS bldg_sum,
    CAST(APPROX_PERCENTILE(vpvl.bldg, 0.5) AS INT) AS bldg_median,
    SUM(vpvl.land) AS land_sum,
    CAST(APPROX_PERCENTILE(vpvl.land, 0.5) AS INT) AS land_median,
    SUM(vpvl.tot) AS tot_sum,
    CAST(APPROX_PERCENTILE(vpvl.tot, 0.5) AS INT) AS tot_median
FROM {{ ref('reporting.vw_pin_value_long') }} AS vpvl
LEFT JOIN {{ ref('reporting.vw_pin_township_class') }} AS townships
    ON vpvl.pin = townships.pin
    AND vpvl.year = townships.year
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON vpvl.pin = leg.parid
    AND vpvl.year = leg.taxyr
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
WHERE townships.township_name IS NOT NULL
GROUP BY
    leg.cityname,
    vpvl.year,
    townships.major_class,
    vpvl.stage_name,
    townships.reassessment_year
ORDER BY
    leg.cityname,
    vpvl.year,
    vpvl.stage_name,
    townships.major_class
