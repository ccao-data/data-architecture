/*
Aggregates statistics on characteristics, classes, AVs, and sales by
assessment stage, property groups, year, and various geographies.

This table takes model and assessment values from two locations on Athena and
stacks them. Model and assessment values are gathered independently and
aggregated via a UNION rather than a JOIN, so it's important to keep in mind
that years for model and assessment stages do NOT need to match, i.e. we can
have 2023 model values in the table before there are any 2023 assessment values
to report on. Sales are added via a lagged join, so sales_year should always =
year - 1. It is also worth nothing that "model year" has has 1 added to it
solely for the sake of reporting in this table - models with a 'meta_year' value
of 2022 in model.assessment_pin will populate the table with a value of 2023 for
'year'.

Intended to be materialized daily through a GitHub action.
*/

{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        bucketed_by=['year'],
        bucket_count=1
    )
}}

-- AVs and model values
WITH all_fmvs AS (
    SELECT
        assessment_pin.meta_pin AS pin,
        -- Subtracting one aligns model year with tax year
        CAST(CAST(assessment_pin.year AS INT) - 1 AS VARCHAR) AS year,
        'model' AS assessment_stage,
        assessment_pin.pred_pin_final_fmv_round AS total
    FROM {{ source('model', 'assessment_pin') }}
    WHERE assessment_pin.run_id IN (SELECT run_id FROM model.final_model)

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
),

-- Join land, chars, and reporting groups to values
all_values AS (
    SELECT
        fmvs.pin,
        vptc.property_group,
        vptc.class,
        vptc.triad_name AS triad,
        vptc.township_code,
        CONCAT(vptc.township_code, vptc.nbhd) AS townnbhd,
        fmvs.year,
        fmvs.assessment_stage,
        fmvs.total,
        chars.yrblt,
        chars.total_bldg_sf,
        vpl.sf AS total_land_sf
    FROM all_fmvs AS fmvs
    LEFT JOIN vw_pin_township_class AS vptc
        ON fmvs.pin = vptc.pin
        AND fmvs.year = vptc.year
    INNER JOIN chars
        ON fmvs.pin = chars.pin
        AND fmvs.year = chars.year
    LEFT JOIN default.vw_pin_land AS vpl
        ON fmvs.pin = vpl.pin
        AND fmvs.year = vpl.year
    WHERE vptc.property_group IS NOT NULL
        AND vptc.triad_name IS NOT NULL
),

-- Sales, filtered to exclude outliers and mutlisales
sales AS (
    SELECT
        vwps.sale_price,
        vwps.year AS sale_year,
        tc.property_group,
        tc.township_code,
        vwps.nbhd AS townnbhd
    FROM {{ ref('default.vw_pin_sale') }} AS vwps
    LEFT JOIN town_class AS tc
        ON vwps.pin = tc.parid
        AND vwps.year = tc.taxyr
    WHERE NOT vwps.is_multisale
        AND NOT vwps.sale_filter_is_outlier
        AND NOT vwps.sale_filter_deed_type
        AND NOT vwps.sale_filter_less_than_10k
        AND NOT vwps.sale_filter_same_sale_within_365
        AND tc.property_group IS NOT NULL
        AND tc.triad IS NOT NULL
),

--- AGGREGATE ---

-- Here we aggregate stats on AV and characteristics for each reporting group
-- By township, assessment_stage, and property group
values_town_groups AS (
    SELECT
        triad,
        'Town' AS geography_type,
        property_group,
        assessment_stage,
        township_code AS geography_id,
        year,
        APPROX_PERCENTILE(total, 0.5) AS fmv_median,
        COUNT(*) AS pin_n,
        APPROX_PERCENTILE(total_land_sf, 0.5) AS land_sf_median,
        APPROX_PERCENTILE(total_bldg_sf, 0.5) AS bldg_sf_median,
        APPROX_PERCENTILE(yrblt, 0.5) AS yrblt_median
    FROM all_values
    GROUP BY
        assessment_stage,
        triad,
        township_code,
        year,
        property_group
),

-- By township and assessment stage
values_town_no_groups AS (
    SELECT
        triad,
        'Town' AS geography_type,
        ' REGRESSION' AS property_group,
        assessment_stage,
        township_code AS geography_id,
        year,
        APPROX_PERCENTILE(total, 0.5) AS fmv_median,
        COUNT(*) AS pin_n,
        APPROX_PERCENTILE(total_land_sf, 0.5) AS land_sf_median,
        APPROX_PERCENTILE(total_bldg_sf, 0.5) AS bldg_sf_median,
        APPROX_PERCENTILE(yrblt, 0.5) AS yrblt_median
    FROM all_values
    GROUP BY
        assessment_stage,
        triad,
        township_code,
        year
),

-- By neighborhood, assessment_stage, and property group
values_nbhd_groups AS (
    SELECT
        triad,
        'TownNBHD' AS geography_type,
        property_group,
        assessment_stage,
        townnbhd AS geography_id,
        year,
        APPROX_PERCENTILE(total, 0.5) AS fmv_median,
        COUNT(*) AS pin_n,
        APPROX_PERCENTILE(total_land_sf, 0.5) AS land_sf_median,
        APPROX_PERCENTILE(total_bldg_sf, 0.5) AS bldg_sf_median,
        APPROX_PERCENTILE(yrblt, 0.5) AS yrblt_median
    FROM all_values
    GROUP BY
        assessment_stage,
        triad,
        townnbhd,
        year,
        property_group
),

-- By neighborhood and assessment stage
values_nbhd_no_groups AS (
    SELECT
        triad,
        'TownNBHD' AS geography_type,
        ' REGRESSION' AS property_group,
        assessment_stage,
        townnbhd AS geography_id,
        year,
        APPROX_PERCENTILE(total, 0.5) AS fmv_median,
        COUNT(*) AS pin_n,
        APPROX_PERCENTILE(total_land_sf, 0.5) AS land_sf_median,
        APPROX_PERCENTILE(total_bldg_sf, 0.5) AS bldg_sf_median,
        APPROX_PERCENTILE(yrblt, 0.5) AS yrblt_median
    FROM all_values
    GROUP BY
        assessment_stage,
        triad,
        townnbhd,
        year
),

-- Here we aggregate stats on sales for each reporting group
-- By township and property group
sales_town_groups AS (
    SELECT
        township_code AS geography_id,
        sale_year,
        property_group,
        MIN(sale_price) AS sale_min,
        APPROX_PERCENTILE(sale_price, 0.5) AS sale_median,
        MAX(sale_price) AS sale_max,
        COUNT(*) AS sale_n
    FROM sales
    GROUP BY township_code, sale_year, property_group
),

-- By township
sales_town_no_groups AS (
    SELECT
        township_code AS geography_id,
        sale_year,
        'ALL REGRESSION' AS property_group,
        MIN(sale_price) AS sale_min,
        APPROX_PERCENTILE(sale_price, 0.5) AS sale_median,
        MAX(sale_price) AS sale_max,
        COUNT(*) AS sale_n
    FROM sales
    GROUP BY
        township_code,
        sale_year
),

-- By neighborhood and property group
sales_nbhd_groups AS (
    SELECT
        townnbhd AS geography_id,
        sale_year,
        property_group,
        MIN(sale_price) AS sale_min,
        APPROX_PERCENTILE(sale_price, 0.5) AS sale_median,
        MAX(sale_price) AS sale_max,
        COUNT(*) AS sale_n
    FROM sales
    GROUP BY
        townnbhd,
        sale_year,
        property_group
),

-- By neighborhood
sales_nbhd_no_groups AS (
    SELECT
        townnbhd AS geography_id,
        sale_year,
        'ALL REGRESSION' AS property_group,
        MIN(sale_price) AS sale_min,
        APPROX_PERCENTILE(sale_price, 0.5) AS sale_median,
        MAX(sale_price) AS sale_max,
        COUNT(*) AS sale_n
    FROM sales
    GROUP BY
        townnbhd,
        sale_year
),

-- Stack all the aggregated value stats
aggregated_values AS (
    SELECT * FROM values_town_groups
    UNION ALL
    SELECT * FROM values_town_no_groups
    UNION ALL
    SELECT * FROM values_nbhd_groups
    UNION ALL
    SELECT * FROM values_nbhd_no_groups
),

-- Stack all the aggregated sales stats
all_sales AS (
    SELECT * FROM sales_town_groups
    UNION ALL
    SELECT * FROM sales_town_no_groups
    UNION ALL
    SELECT * FROM sales_nbhd_groups
    UNION ALL
    SELECT * FROM sales_nbhd_no_groups
)

SELECT
    av.*,
    asl.sale_year,
    asl.sale_min,
    asl.sale_median,
    asl.sale_max,
    asl.sale_n,
    -- Class mode has to be populated based on reporting group
    CASE
        WHEN
            av.geography_type = 'Town'
            AND av.property_group != 'ALL REGRESSION'
            THEN cm1.group_town_mode
        WHEN
            av.geography_type = 'Town'
            AND av.property_group = 'ALL REGRESSION'
            THEN cm3.town_mode
        WHEN
            av.geography_type = 'TownNBHD'
            AND av.property_group != 'ALL REGRESSION'
            THEN cm2.group_townnbhd_mode
        WHEN
            av.geography_type = 'TownNBHD'
            AND av.property_group = 'ALL REGRESSION'
            THEN cm4.townnbhd_mode
    END AS class_mode
FROM aggregated_values AS av
-- Join sales so that values for a given year can be compared
-- to a complete set of sales from the previous year
LEFT JOIN all_sales AS asl
    ON av.geography_id = asl.geography_id
    AND CAST(av.year AS INT) = CAST(asl.sale_year AS INT) + 1
    AND av.property_group = asl.property_group
-- Join on class modes specifically by the columns that were
-- used to generate them
LEFT JOIN (
    SELECT DISTINCT
        assessment_stage,
        township_code AS geography_id,
        year,
        property_group,
        group_town_mode
    FROM class_modes
) AS cm1
    ON av.assessment_stage = cm1.assessment_stage
    AND av.geography_id = cm1.geography_id
    AND av.year = cm1.year
    AND av.property_group = cm1.property_group
LEFT JOIN (
    SELECT DISTINCT
        assessment_stage,
        townnbhd AS geography_id,
        year,
        property_group,
        group_townnbhd_mode
    FROM class_modes
) AS cm2
    ON av.assessment_stage = cm2.assessment_stage
    AND av.geography_id = cm2.geography_id
    AND av.year = cm2.year
    AND av.property_group = cm2.property_group
LEFT JOIN (
    SELECT DISTINCT
        assessment_stage,
        township_code AS geography_id,
        year,
        town_mode
    FROM class_modes
) AS cm3
    ON av.assessment_stage = cm3.assessment_stage
    AND av.geography_id = cm3.geography_id
    AND av.year = cm3.year
LEFT JOIN (
    SELECT DISTINCT
        assessment_stage,
        townnbhd AS geography_id,
        year,
        townnbhd_mode
    FROM class_modes
) AS cm4
    ON av.assessment_stage = cm4.assessment_stage
    AND av.geography_id = cm4.geography_id
    AND av.year = cm4.year
