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
-- Valuation class and nbhd from pardat, townships from legdat
-- since pardat has some errors we can't accept for public reporting
WITH town_class AS (
    SELECT
        par.parid,
        par.taxyr,
        town.triad_name AS triad,
        leg.user1 AS township_code,
        CONCAT(leg.user1, SUBSTR(par.nbhd, 3, 3)) AS townnbhd,
        par.class,
        CASE WHEN par.class IN ('299', '399') THEN 'CONDO'
            WHEN par.class IN ('211', '212') THEN 'MF'
            WHEN
                par.class IN (
                    '202', '203', '204', '205', '206', '207',
                    '208', '209', '210', '234', '278', '295'
                )
                THEN 'SF'
        END AS property_group,
        CAST(CAST(par.taxyr AS INT) - 1 AS VARCHAR) AS model_join_year
    FROM {{ ref('iasworld.pardat') }} AS par
    LEFT JOIN {{ ref('iasworld.legdat') }} AS leg
        ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
    LEFT JOIN {{ ref('spatial.township') }} AS town
        ON leg.user1 = town.township_code
),

-- Final model values (Add 1 to model year since '2021' correspond to '2022'
-- mailed values in iasWorld)
model_values AS (
    SELECT
        ap.meta_pin AS parid,
        tc.property_group,
        tc.class,
        tc.triad,
        tc.township_code,
        tc.townnbhd,
        tc.taxyr AS year,
        'model' AS assessment_stage,
        ap.pred_pin_final_fmv_round AS total
    FROM {{ ref('model.assessment_pin') }} AS ap
    LEFT JOIN town_class AS tc
        ON ap.meta_pin = tc.parid
        AND ap.meta_year = tc.model_join_year
    WHERE ap.run_id IN (SELECT run_id FROM model.final_model)
        AND tc.property_group IS NOT NULL
),

-- Values by assessment stages available in iasWorld (not model)
iasworld_values AS (
    SELECT
        aa.parid,
        tc.property_group,
        aa.class,
        tc.triad,
        tc.township_code,
        tc.townnbhd,
        aa.taxyr AS year,
        CASE
            WHEN aa.procname = 'CCAOVALUE' THEN 'mailed'
            WHEN aa.procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN aa.procname = 'BORVALUE' THEN 'bor certified'
        END AS assessment_stage,
        MAX(
            CASE
                WHEN aa.taxyr < '2020' THEN aa.ovrvalasm3
                WHEN aa.taxyr >= '2020' THEN aa.valasm3
            END
        ) * 10 AS total
    FROM {{ ref('iasworld.asmt_all') }} AS aa
    LEFT JOIN town_class AS tc
        ON aa.parid = tc.parid
        AND aa.taxyr = tc.taxyr
    WHERE aa.procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND aa.rolltype != 'RR'
        AND aa.deactivat IS NULL
        AND aa.valclass IS NULL
        AND tc.property_group IS NOT NULL
        AND aa.taxyr >= '2021'
    GROUP BY
        aa.parid,
        aa.taxyr,
        aa.procname,
        tc.property_group,
        CASE
            WHEN aa.procname = 'CCAOVALUE' THEN 'mailed'
            WHEN aa.procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN aa.procname = 'BORVALUE' THEN 'bor certified'
        END,
        aa.class,
        tc.triad,
        tc.township_code,
        tc.townnbhd
),

all_values AS (
    SELECT * FROM model_values
    UNION
    SELECT * FROM iasworld_values
),

-- Count of each class by different reporting groups (property group,
-- assessment stage, town/nbhd)
class_counts AS (
    SELECT
        class,
        assessment_stage,
        township_code,
        townnbhd,
        year,
        property_group,
        COUNT(*) OVER (
            PARTITION BY
                assessment_stage, township_code, year, property_group, class
        ) AS group_town_count,
        COUNT(*) OVER (
            PARTITION BY assessment_stage, townnbhd, year, property_group, class
        ) AS group_townnbhd_count,
        COUNT(*) OVER (
            PARTITION BY assessment_stage, township_code, year, class
        ) AS town_count,
        COUNT(*) OVER (
            PARTITION BY assessment_stage, townnbhd, year, class
        ) AS townnbhd_count
    FROM all_values
),

-- Most common class by reporting group based on class counts
class_modes AS (
    SELECT
        assessment_stage,
        township_code,
        townnbhd,
        year,
        property_group,
        FIRST_VALUE(class) OVER (
            PARTITION BY assessment_stage, township_code, year, property_group
            ORDER BY group_town_count DESC
        ) AS group_town_mode,
        FIRST_VALUE(class) OVER (
            PARTITION BY assessment_stage, townnbhd, year, property_group
            ORDER BY group_townnbhd_count DESC
        ) AS group_townnbhd_mode,
        FIRST_VALUE(class) OVER (
            PARTITION BY assessment_stage, township_code, year
            ORDER BY group_townnbhd_count DESC
        ) AS town_mode,
        FIRST_VALUE(class) OVER (
            PARTITION BY assessment_stage, townnbhd, year
            ORDER BY group_townnbhd_count DESC
        ) AS townnbhd_mode
    FROM class_counts
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
),

-- Aggregate land for all parcels
aggregate_land AS (
    SELECT
        parid,
        taxyr,
        SUM(sf) AS total_land_sf
    FROM {{ ref('iasworld.land') }}
    GROUP BY parid, taxyr
),

-- Combined SF/MF and condo characteristics
chars AS (
    SELECT
        parid,
        taxyr,
        MIN(yrblt) AS yrblt,
        SUM(sfla) AS total_bldg_sf
    FROM {{ ref('iasworld.dweldat') }}
    GROUP BY parid, taxyr
    UNION
    SELECT
        pin AS parid,
        year AS taxyr,
        char_yrblt AS yrblt,
        char_building_sf AS total_bldg_sf
    FROM {{ ref('default.vw_pin_condo_char') }}
    WHERE NOT is_parking_space
        AND NOT is_common_area
),

--- AGGREGATE ---

-- Here we aggregate stats on AV and characteristics for each reporting group
-- By township, assessment_stage, and property group
values_town_groups AS (
    SELECT
        av.triad,
        'Town' AS geography_type,
        av.property_group,
        av.assessment_stage,
        av.township_code AS geography_id,
        av.year,
        APPROX_PERCENTILE(av.total, 0.5) AS fmv_median,
        COUNT(*) AS pin_n,
        APPROX_PERCENTILE(al.total_land_sf, 0.5) AS land_sf_median,
        APPROX_PERCENTILE(ab.total_bldg_sf, 0.5) AS bldg_sf_median,
        APPROX_PERCENTILE(ab.yrblt, 0.5) AS yrblt_median
    FROM all_values AS av
    LEFT JOIN aggregate_land AS al
        ON av.parid = al.parid
        AND av.year = al.taxyr
    INNER JOIN chars AS ab
        ON av.parid = ab.parid
        AND av.year = ab.taxyr
    GROUP BY
        av.assessment_stage,
        av.triad,
        av.township_code,
        av.year,
        av.property_group
),

-- By township and assessment stage
values_town_no_groups AS (
    SELECT
        av.triad,
        'Town' AS geography_type,
        'ALL REGRESSION' AS property_group,
        av.assessment_stage,
        av.township_code AS geography_id,
        av.year,
        APPROX_PERCENTILE(av.total, 0.5) AS fmv_median,
        COUNT(*) AS pin_n,
        APPROX_PERCENTILE(al.total_land_sf, 0.5) AS land_sf_median,
        APPROX_PERCENTILE(ab.total_bldg_sf, 0.5) AS bldg_sf_median,
        APPROX_PERCENTILE(ab.yrblt, 0.5) AS yrblt_median
    FROM all_values AS av
    LEFT JOIN aggregate_land AS al
        ON av.parid = al.parid
        AND av.year = al.taxyr
    INNER JOIN chars AS ab
        ON av.parid = ab.parid
        AND av.year = ab.taxyr
    GROUP BY
        av.assessment_stage,
        av.triad,
        av.township_code,
        av.year
),

-- By neighborhood, assessment_stage, and property group
values_nbhd_groups AS (
    SELECT
        av.triad,
        'TownNBHD' AS geography_type,
        av.property_group,
        av.assessment_stage,
        av.townnbhd AS geography_id,
        av.year,
        APPROX_PERCENTILE(av.total, 0.5) AS fmv_median,
        COUNT(*) AS pin_n,
        APPROX_PERCENTILE(al.total_land_sf, 0.5) AS land_sf_median,
        APPROX_PERCENTILE(ab.total_bldg_sf, 0.5) AS bldg_sf_median,
        APPROX_PERCENTILE(ab.yrblt, 0.5) AS yrblt_median
    FROM all_values AS av
    LEFT JOIN aggregate_land AS al
        ON av.parid = al.parid
        AND av.year = al.taxyr
    INNER JOIN chars AS ab
        ON av.parid = ab.parid
        AND av.year = ab.taxyr
    GROUP BY
        av.assessment_stage,
        av.triad,
        av.townnbhd,
        av.year,
        av.property_group
),

-- By neighborhood and assessment stage
values_nbhd_no_groups AS (
    SELECT
        av.triad,
        'TownNBHD' AS geography_type,
        'ALL REGRESSION' AS property_group,
        av.assessment_stage,
        av.townnbhd AS geography_id,
        av.year,
        APPROX_PERCENTILE(av.total, 0.5) AS fmv_median,
        COUNT(*) AS pin_n,
        APPROX_PERCENTILE(al.total_land_sf, 0.5) AS land_sf_median,
        APPROX_PERCENTILE(ab.total_bldg_sf, 0.5) AS bldg_sf_median,
        APPROX_PERCENTILE(ab.yrblt, 0.5) AS yrblt_median
    FROM all_values AS av
    LEFT JOIN aggregate_land AS al
        ON av.parid = al.parid
        AND av.year = al.taxyr
    INNER JOIN chars AS ab
        ON av.parid = ab.parid
        AND av.year = ab.taxyr
    GROUP BY
        av.assessment_stage,
        av.triad,
        av.townnbhd,
        av.year
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
    UNION
    SELECT * FROM values_town_no_groups
    UNION
    SELECT * FROM values_nbhd_groups
    UNION
    SELECT * FROM values_nbhd_no_groups
),

-- Stack all the aggregated sales stats
all_sales AS (
    SELECT * FROM sales_town_groups
    UNION
    SELECT * FROM sales_town_no_groups
    UNION
    SELECT * FROM sales_nbhd_groups
    UNION
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
