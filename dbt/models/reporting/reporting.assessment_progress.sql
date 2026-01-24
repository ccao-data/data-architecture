-- Gathers AVs by year, major class, assessment stage, and municipality for
-- reporting
{{
    config(
        materialized='table'
    )
}}

/* Ensure every municipality/class/year has a row for every stage through
cross-joining. This is to make sure that combinations that do not yet
exist in iasworld.asmt_all for the current year will exist in the view, but have
largely empty columns. For example: even if no class 4s in the City of Chicago
have been mailed yet for the current assessment year, we would still like an
empty City of Chicago/class 4 row to exist for the mailed stage. */
WITH stages AS (

    SELECT
        'MAILED' AS stage_name,
        1 AS stage_num
    UNION
    SELECT
        'ASSESSOR CERTIFIED' AS stage_name,
        2 AS stage_num
    UNION
    SELECT
        'BOR CERTIFIED' AS stage_name,
        3 AS stage_num

),

/* This CTE removes historical PINs in default.vw_pin_universe that are
not in reporting.vw_pin_value_long. We do this to make sure the samples we
derive the numerator and denominator from for pct_pin_w_value_in_group are the
same. These differences are inherent in the tables that feed these views
(iasworld.pardat and iasworld.asmt_all, respectively) and are data errors - not
emblematic of what portion of a municipality has actually progressed through an
assessment stage.

It does NOT remove PINs from the most recent year of
default.vw_pin_universe since we expect differences based on how
iasworld.asmt_all is populated through the year as the assessment cycle
progresses. This means data errors caused by differences between iasworld.pardat
and iasworld.asmt_all won't be addressed in the most recent year. Unfortunately,
we can't know what those errors are (or if they even exist) until asmt_all has
at least one fully complete stage for a given year.

Starting in 2020 a small number of PINs are present in iasworld.asmt_all for
one or two but not all three stages of assessment when we would expect all three
stages to be present for said PINs. This is also a data error, but is NOT
addressed in this view and leads to a few instances where
pct_pin_w_value_in_group ends up being less than 1 when it should equal 1.
16-07-219-029-1032 missing a mailed value but having CCAO and BOR certified
values in 2021 is an example. */
trimmed_geos AS (
    SELECT
        vpu.pin,
        vpu.year,
        ARRAY_JOIN(vpu.combined_municipality_name, ', ') AS municipality_name,
        ARRAY_JOIN(
            vpu.tax_school_elementary_district_name, ', ') AS
        school_elementary_district_name,
        ARRAY_JOIN(
            vpu.tax_school_secondary_district_name, ', ') AS
        school_secondary_district_name,
        ARRAY_JOIN(vpu.tax_school_unified_district_name, ', ')
            AS school_unified_district_name,
        CASE
            WHEN
                vpu.ward_chicago_data_year IS NOT NULL
                THEN REPLACE(vpu.ward_name, 'chicago_', '')
        END
            AS chicago_ward_name
    FROM {{ ref('default.vw_pin_universe') }} AS vpu
    LEFT JOIN
        (
            SELECT DISTINCT
                pin,
                year
            FROM {{ ref('reporting.vw_pin_value_long') }}
        )
            AS pins
        ON vpu.pin = pins.pin
        AND vpu.year = pins.year
    WHERE pins.pin IS NOT NULL
        OR vpu.year
        = (SELECT MAX(year) FROM {{ ref('default.vw_pin_universe') }})

),

-- Cross join our trimmed sample with the stages we defined above
expanded_geos1 AS (
    SELECT
        trimmed_geos.*,
        stages.*
    FROM trimmed_geos
    CROSS JOIN stages
),

-- Join on values by stage from vw_pin_value_long so we can determine which
-- parcels have values for which stages in which years.
expanded_geos2 AS (
    SELECT
        expanded_geos1.*,
        CASE WHEN vpvl.tot IS NOT NULL THEN 1 ELSE 0 END AS has_value,
        vpvl.bldg,
        vpvl.land,
        vpvl.tot
    FROM expanded_geos1
    LEFT JOIN {{ ref('reporting.vw_pin_value_long') }} AS vpvl
        ON expanded_geos1.pin = vpvl.pin
        AND expanded_geos1.year = vpvl.year
        AND expanded_geos1.stage_name = vpvl.stage_name
),

/* Calculate the denominator for the pct_pin_w_value_in_group column.
default.vw_pin_universe serves as the universe of yearly PINs we expect
to see in reporting.vw_pin_value_long. Loop over the different geographies
we track progress for and then stack the output.*/
pin_counts AS (

    {{ assessment_progress_pin_count(
            from = 'expanded_geos2',
            geo_type = 'Municipality',
            column_name = 'municipality_name'
            ) }}
    UNION ALL
    {{ assessment_progress_pin_count(
            from = 'expanded_geos2',
            geo_type = 'Chicago Ward',
            column_name = 'chicago_ward_name'
            ) }}
    UNION ALL
    {{ assessment_progress_pin_count(
            from = 'expanded_geos2',
            geo_type = 'Elementary School District',
            column_name = 'school_elementary_district_name'
            ) }}
    UNION ALL
    {{ assessment_progress_pin_count(
            from = 'expanded_geos2',
            geo_type = 'Secondary School District',
            column_name = 'school_secondary_district_name'
            ) }}
    UNION ALL
    {{ assessment_progress_pin_count(
            from = 'expanded_geos2',
            geo_type = 'Unified School District',
            column_name = 'school_unified_district_name'
            ) }}
)

-- Calculate the portion of each geography that has progressed through an
-- assessment stage by class.
SELECT
    pin_counts.year,
    pin_counts.stage_name,
    pin_counts.stage_num,
    pin_counts.geo_type,
    pin_counts.geo_id,
    pin_counts.num_pin_total,
    pin_counts.num_pin_w_value,
    ROUND(
        CAST(pin_counts.num_pin_w_value AS DOUBLE)
        / CAST(pin_counts.num_pin_total AS DOUBLE), 4
    ) AS pct_pin_w_value,
    pin_counts.bldg_sum,
    pin_counts.bldg_median,
    pin_counts.land_sum,
    pin_counts.land_median,
    pin_counts.tot_sum,
    pin_counts.tot_median
FROM pin_counts
ORDER BY
    pin_counts.year DESC,
    pin_counts.geo_type ASC,
    pin_counts.geo_id ASC,
    pin_counts.stage_name DESC
