-- Gathers AVs by year, major class, assessment stage, and
-- township for reporting

-- Add total and median values by township
SELECT
    vpvl.year,
    LOWER(vpvl.stage_name) AS stage,
    tax.tax_municipality_name AS municipality_name,
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
LEFT JOIN {{ ref('location.tax') }} AS tax
    ON SUBSTR(vpvl.pin, 1, 10) = tax.pin10
    AND CASE
        WHEN
            vpvl.year > (SELECT MAX(year) FROM {{ ref('location.tax') }})
            THEN (SELECT MAX(year) FROM {{ ref('location.tax') }})
        ELSE vpvl.year
    END
    = tax.year
WHERE townships.township_name IS NOT NULL
GROUP BY
    tax.tax_municipality_name,
    vpvl.year,
    townships.major_class,
    vpvl.stage_name,
    townships.reassessment_year
ORDER BY
    tax.tax_municipality_name,
    vpvl.year,
    vpvl.stage_name,
    townships.major_class
