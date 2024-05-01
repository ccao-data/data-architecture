-- Gathers AVs by year, major class, assessment stage, and municipality for
-- reporting

/* Ensure every municipality/class/year has a row for every stage through
cross-joining. This is to make sure that combinations that do not yet
exist in iasworld.asmt_all for the current year will exist in the view, but have
largely empty columns. For example: even if no class 4s in the City of Chicago
have been mailed yet for the current assessment year, we would still like an
empty City of Chicago/class 4 row to exist for the mailed stage. */
WITH stages AS (

    SELECT 'mailed' AS stage
    UNION
    SELECT 'assessor certified' AS stage
    UNION
    SELECT 'bor certified' AS stage

),

/* This CTE removes historical PINs in reporting.vw_pin_township_class that are
not in reporting.vw_pin_value_long. We do this to make sure the samples we
derive the numerator and denominator from for stage_portion are the same. These
differences are inherent in the tables that feed these views (iasworld.pardat
and iasworld.asmt_all, respectively) and are data errors - not emblematic of
what portion of a municipality has actually progressed through an assessment
stage.

It does NOT remove PINs from the most recent year of
reporting.vw_pin_township_class since we expect these differences based on how
iasworld.asmt_all is populated through the year as the assessment cycle
progresses.

Starting in 2020 a small number of PINs are present in iasworld.asmt_all for
one or two but not all three stages of assessment when we would expect all three
stages to be present for said PINs. This is also a data error, but is NOT
addressed in this view and leads to a few instances where stage_portion ends up
being less than 1 when it should equal 1. */
trimmed_town_class AS (
    SELECT vptc.*
    FROM {{ ref('reporting.vw_pin_township_class') }} AS vptc
    LEFT JOIN
        (
            SELECT DISTINCT
                pin,
                year
            FROM {{ ref('reporting.vw_pin_value_long') }}
        )
            AS pins
        ON vptc.pin = pins.pin
        AND vptc.year = pins.year
    WHERE pins.pin IS NOT NULL
        OR vptc.year
        = (SELECT MAX(year) FROM {{ ref('reporting.vw_pin_township_class') }})

),

/* Calculate the denominator for the stage_portion column.
reporting.vw_pin_township_class serves as the universe of yearly PINs we expect
to see in reporting.vw_pin_value_long. */
pin_counts AS (
    SELECT
        vptc.municipality_name,
        vptc.major_class,
        vptc.year,
        stages.stage,
        COUNT(*) AS total_n
    FROM trimmed_town_class AS vptc
    CROSS JOIN stages
    WHERE vptc.municipality_name IS NOT NULL
    GROUP BY
        vptc.municipality_name,
        vptc.year,
        vptc.major_class,
        stages.stage
)

-- Calculate total and median values by municipality, as well as the portion of
-- each municipality that has progressed through an assessment stage by class.
SELECT
    pin_counts.year,
    pin_counts.stage,
    pin_counts.municipality_name,
    munis.major_class AS class,
    SUM(CAST(vpvl.pin IS NOT NULL AS INT)) AS n,
    pin_counts.total_n,
    SUM(CAST(vpvl.pin IS NOT NULL AS DOUBLE))
    / CAST(pin_counts.total_n AS DOUBLE) AS stage_portion,
    SUM(vpvl.bldg) AS bldg_sum,
    CAST(APPROX_PERCENTILE(vpvl.bldg, 0.5) AS INT) AS bldg_median,
    SUM(vpvl.land) AS land_sum,
    CAST(APPROX_PERCENTILE(vpvl.land, 0.5) AS INT) AS land_median,
    SUM(vpvl.tot) AS tot_sum,
    CAST(APPROX_PERCENTILE(vpvl.tot, 0.5) AS INT) AS tot_median
FROM pin_counts
LEFT JOIN trimmed_town_class AS munis
    ON pin_counts.municipality_name = munis.municipality_name
    AND pin_counts.major_class = munis.major_class
    AND pin_counts.year = munis.year
LEFT JOIN {{ ref('reporting.vw_pin_value_long') }} AS vpvl
    ON munis.pin = vpvl.pin
    AND munis.year = vpvl.year
    AND pin_counts.stage = LOWER(vpvl.stage_name)
GROUP BY
    pin_counts.municipality_name,
    pin_counts.year,
    munis.major_class,
    pin_counts.stage,
    pin_counts.total_n
ORDER BY
    pin_counts.year DESC,
    pin_counts.municipality_name ASC,
    pin_counts.stage ASC,
    munis.major_class ASC
