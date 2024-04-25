-- Gathers AVs by year, major class, assessment stage, and
-- township for reporting

-- Add total and median values by township
SELECT
    values_by_year.year,
    LOWER(values_by_year.stage_name) AS stage,
    tax.municpality_name,
    townships.major_class AS class,
    townships.reassessment_year,
    COUNT(*) AS n,
    SUM(values_by_year.bldg) AS bldg_sum,
    CAST(APPROX_PERCENTILE(values_by_year.bldg, 0.5) AS INT) AS bldg_median,
    SUM(values_by_year.land) AS land_sum,
    CAST(APPROX_PERCENTILE(values_by_year.land, 0.5) AS INT) AS land_median,
    SUM(values_by_year.tot) AS tot_sum,
    CAST(APPROX_PERCENTILE(values_by_year.tot, 0.5) AS INT) AS tot_median
FROM {{ ref('reporting.vw_pin_value_long') }} AS values_by_year
LEFT JOIN {{ ref('reporting.vw_pin_township_class') }} AS townships
    ON values_by_year.pin = townships.pin
    AND values_by_year.year = townships.year
LEFT JOIN {{ ref('location.tax') }} AS tax
    ON SUBSTR(values_by_year.pin, 1, 10) = tax.pin10
    AND values_by_year.year = tax.year
WHERE townships.township_name IS NOT NULL
GROUP BY
    tax.municpality_name,
    values_by_year.year,
    townships.major_class,
    values_by_year.stage_name,
    townships.reassessment_year
ORDER BY
    tax.municpality_name,
    values_by_year.year,
    values_by_year.stage_name,
    townships.major_class
