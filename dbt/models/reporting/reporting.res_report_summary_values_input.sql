/*
Input table for `reporting.res_report_summary` that produces the raw
characteristic and value data that `res_report_summary` aggregates. We split
these input data out into a separate table to speed up query time for
`res_report_summary`, since otherwise it needs to rerun the query logic below
for every possible geography and reporting group combination.

This table takes model and assessment values from two separate tables and
stacks them. Model and assessment values are gathered independently and
aggregated via a UNION rather than a JOIN, so it's important to keep in mind
that years for model and assessment stages do NOT need to match, i.e. we can
have 2023 model values in the table before there are any 2023 assessment values
to report on.

See `reporting.res_report_summary` for a full description of these data.

Intended to be materialized daily through a GitHub action.
*/

{{
    config(
        materialized='table',
        partitioned_by=['year']
    )
}}

-- AVs and model values
WITH all_fmvs AS (
    SELECT
        ap.meta_pin AS pin,
        ap.year,
        'model' AS assessment_stage,
        ap.pred_pin_final_fmv_round AS total
    FROM {{ source('model', 'assessment_pin') }} AS ap
    INNER JOIN {{ ref('model.final_model') }} AS fm
        ON ap.run_id = fm.run_id
        AND ap.year = fm.year
        AND (
            -- If reassessment year, use different models for different towns
            (
                CONTAINS(fm.township_code_coverage, ap.township_code)
                AND ap.meta_triad_code = fm.triad_code
            )
            -- Otherwise, just use whichever model is "final"
            OR (ap.meta_triad_code != fm.triad_code AND fm.is_final)
        )

    UNION ALL

    SELECT
        pin,
        year,
        stage_name AS assessment_stage,
        tot * 10 AS total
    FROM {{ ref('reporting.vw_pin_value_long') }}
    WHERE year >= '2021'
),

-- Combined SF/MF and condo characteristics
chars AS (
    SELECT
        parid AS pin,
        taxyr AS year,
        MIN(yrblt) AS yrblt,
        SUM(sfla) AS total_bldg_sf
    FROM {{ source('iasworld', 'dweldat') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
    GROUP BY parid, taxyr
    UNION ALL
    SELECT
        pin,
        year,
        char_yrblt AS yrblt,
        char_building_sf AS total_bldg_sf
    FROM {{ ref('default.vw_pin_condo_char') }}
    WHERE NOT is_parking_space
        AND NOT is_common_area
)

-- Join land, chars, and reporting groups to values
SELECT
    fmvs.pin,
    vptc.property_group,
    vptc.class,
    vptc.triad_name AS triad,
    vptc.township_code,
    CONCAT(vptc.township_code, vptc.nbhd) AS townnbhd,
    fmvs.assessment_stage,
    fmvs.total,
    chars.yrblt,
    chars.total_bldg_sf,
    vpl.sf AS total_land_sf,
    fmvs.year
FROM all_fmvs AS fmvs
LEFT JOIN {{ ref('reporting.vw_pin_township_class') }} AS vptc
    ON fmvs.pin = vptc.pin
    AND fmvs.year = vptc.year
INNER JOIN chars
    ON fmvs.pin = chars.pin
    AND fmvs.year = chars.year
LEFT JOIN {{ ref('default.vw_pin_land') }} AS vpl
    ON fmvs.pin = vpl.pin
    AND fmvs.year = vpl.year
WHERE vptc.property_group IS NOT NULL
    AND vptc.triad_name IS NOT NULL
