/*
Aggregates statistics on characteristics, classes, AVs, and sales by
assessment stage, property groups, year, and various geographies.

Essentially, this view takes model and assessment values from two
locations on Athena and stacks them. Model and assessment values are
gathered independently and aggregated via a UNION rather than a JOIN,
so it's important to keep in mind that years for model and assessment
stages do NOT need to match, i.e. we can have 2023 model values in the
view before there are any 2023 assessment values to report on. Sales are
added via a lagged join, so sales_year should always = year - 1. It is
also worth nothing that "model year" has has 1 added to it solely for
the sake of reporting in this view - models with a 'meta_year' value
of 2022 in model.assessment_pin will populate the view with a value of
2023 for 'year'.

Intended to feed glue job 'res_report_summary'.
*/
CREATE OR REPLACE VIEW reporting.vw_res_report_summary AS

-- Valuation class and nbhd from pardat, townships from legdat
-- since pardat has some errors we can't accept for public reporting
WITH town_class AS (
    SELECT
        p.parid,
        p.taxyr,
        triad_name AS triad,
        l.user1 AS township_code,
        CONCAT(l.user1, SUBSTR(p.nbhd, 3, 3)) AS townnbhd,
        p.class,
        CASE WHEN p.class IN ('299', '399') THEN 'CONDO'
            WHEN p.class IN ('211', '212') THEN 'MF'
            WHEN
                p.class IN (
                    '202', '203', '204', '205', '206', '207',
                    '208', '209', '210', '234', '278', '295'
                )
                THEN 'SF'
            ELSE NULL
        END AS property_group,
        CAST(CAST(p.taxyr AS INT) - 1 AS VARCHAR) AS model_join_year
    FROM iasworld.pardat AS p
    LEFT JOIN iasworld.legdat AS l ON p.parid = l.parid AND p.taxyr = l.taxyr
    LEFT JOIN spatial.township AS t ON l.user1 = t.township_code

),

cm4 AS (
    SELECT DISTINCT
        assessment_stage,
        townnbhd AS geography_id,
        year,
        townnbhd_mode
    FROM class_modes
),

cm1 AS (
    SELECT DISTINCT
        assessment_stage,
        township_code AS geography_id,
        year,
        property_group,
        group_town_mode
    FROM class_modes
),

cm2 AS (
    SELECT DISTINCT
        assessment_stage,
        townnbhd AS geography_id,
        year,
        property_group,
        group_townnbhd_mode
    FROM class_modes
)

SELECT
    av.*,
    sale_year,
    sale_min,
    sale_median,
    sale_max,
    sale_n,
    -- Class mode has to be populated based on reporting group
    CASE
        WHEN
            av.geography_type = 'Town' AND av.property_group != 'ALL REGRESSION'
            THEN group_town_mode
        WHEN
            av.geography_type = 'Town' AND av.property_group = 'ALL REGRESSION'
            THEN town_mode
        WHEN
            av.geography_type = 'TownNBHD'
            AND av.property_group != 'ALL REGRESSION'
            THEN group_townnbhd_mode
        WHEN
            av.geography_type = 'TownNBHD'
            AND av.property_group = 'ALL REGRESSION'
            THEN townnbhd_mode
        ELSE NULL
    END AS class_mode

FROM aggregated_values AS av

-- Join sales so that values for a given year can be compared to a complete set of sales from the previous year
LEFT JOIN all_sales
    ON av.geography_id = all_sales.geography_id
    AND CAST(av.year AS INT) = CAST(all_sales.sale_year AS INT) + 1
    AND av.property_group = all_sales.property_group

-- Join on class modes specifically by the columns that were used to generate them
LEFT JOIN
    cm1
    ON av.assessment_stage = cm1.assessment_stage
    AND av.geography_id = cm1.geography_id
    AND av.year = cm1.year
    AND av.property_group = cm1.property_group
LEFT JOIN
    cm2
    ON av.assessment_stage = cm2.assessment_stage
    AND av.geography_id = cm2.geography_id
    AND av.year = cm2.year
    AND av.property_group = cm2.property_group
LEFT JOIN
    cm3
    ON av.assessment_stage = cm3.assessment_stage
    AND av.geography_id = cm3.geography_id
    AND av.year = cm3.year
LEFT JOIN
    cm4
    ON av.assessment_stage = cm4.assessment_stage
    AND av.geography_id = cm4.geography_id
    AND av.year = cm4.year
