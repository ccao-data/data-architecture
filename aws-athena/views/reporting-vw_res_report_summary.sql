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
*/
CREATE OR REPLACE VIEW reporting.vw_res_report_summary
AS

-- Valuation class and nbhd from pardat, townships from legdat
-- since pardat has some errors we can't accept for public reporting
WITH town_class AS (
    SELECT
        p.parid,
        p.taxyr,
        triad_name AS triad,
        l.user1 AS township_code,
        CONCAT(l.user1, substr(p.nbhd, 3, 3)) AS TownNBHD,
        p.class,
        CASE WHEN p.class in ('299', '399') THEN 'CONDO'
            WHEN p.class in ('211', '212') THEN 'MF'
            WHEN p.class in ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278', '295') THEN 'SF'
            ELSE NULL END AS property_group,
        CAST(CAST(p.taxyr AS INT) - 1 AS VARCHAR) AS model_join_year
    FROM iasworld.pardat p
    LEFT JOIN iasworld.legdat l ON p.parid = l.parid AND p.taxyr = l.taxyr
    LEFT JOIN spatial.township t ON l.user1 = t.township_code

    ),
-- Final model values (Add 1 to model year since '2021' correspond to '2022'
-- mailed values in iasWorld)
model_values AS (
    SELECT
        meta_pin AS parid,
        property_group,
        class,
        triad,
        tc.township_code,
        tc.TownNBHD,
        tc.taxyr as year,
        'model' AS assessment_stage,
        pred_pin_final_fmv_round AS total

    FROM model.assessment_pin ap

    LEFT JOIN town_class tc
        ON ap.meta_pin = tc.parid AND ap.meta_year = tc.model_join_year

    WHERE run_id IN (SELECT run_id FROM model.final_model)
    AND property_group IS NOT NULL
),
-- Values by assessment stages available in iasWorld (not model)
iasworld_values AS (
    SELECT
        aa.parid,
        tc.property_group,
        aa.class,
        triad,
        tc.township_code,
        tc.TownNBHD,
        aa.taxyr as year,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE'  THEN 'bor certified'
            ELSE NULL END AS assessment_stage,
        max(
            CASE
                WHEN aa.taxyr < '2020' THEN ovrvalasm3
                WHEN aa.taxyr >= '2020' THEN valasm3
                ELSE NULL END
            ) * 10 AS total
    FROM iasworld.asmt_all aa

    LEFT JOIN town_class tc ON aa.parid = tc.parid AND aa.taxyr = tc.taxyr

    WHERE procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND rolltype != 'RR'
        AND deactivat IS NULL
        AND valclass IS NULL
        AND property_group IS NOT NULL
        AND aa.taxyr >= '2021'

    GROUP BY
        aa.parid,
        aa.taxyr,
        aa.procname,
        tc.property_group,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE'  THEN 'bor certified'
            ELSE NULL END,
        aa.class,
        triad,
        tc.township_code,
        tc.TownNBHD
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
        TownNBHD,
        year,
        property_group,
        count(*) over(
            PARTITION BY assessment_stage, township_code, year, property_group, class
            ) AS group_town_count,
        count(*) over(
            PARTITION BY assessment_stage, TownNBHD, year, property_group, class
            ) AS group_townnbhd_count,
        count(*) over(
            PARTITION BY assessment_stage, township_code, year, class
            ) AS town_count,
        count(*) over(
            PARTITION BY assessment_stage, TownNBHD, year, class
            ) AS townnbhd_count
    FROM all_values
),
-- Most common class by reporting group based on class counts
class_modes AS (
    SELECT
        assessment_stage,
        township_code,
        TownNBHD,
        year,
        property_group,
        first_value(class) over(
             PARTITION BY assessment_stage, township_code, year, property_group
             ORDER BY group_town_count DESC) AS group_town_mode,
        first_value(class) over(
             PARTITION BY assessment_stage, TownNBHD, year, property_group
             ORDER BY group_townnbhd_count DESC) AS group_townnbhd_mode,
        first_value(class) over(
             PARTITION BY assessment_stage, township_code, year
             ORDER BY group_townnbhd_count DESC) AS town_mode,
        first_value(class) over(
             PARTITION BY assessment_stage, TownNBHD, year
             ORDER BY group_townnbhd_count DESC) AS townnbhd_mode
    FROM class_counts
),
-- Sales, filtered to exclude outliers and mutlisales
sales AS (
    SELECT
        sale_price,
        year AS sale_year,
        property_group,
        vps.township_code,
        vps.nbhd AS TownNBHD
    FROM default.vw_pin_sale vps
    LEFT JOIN town_class tc
        ON vps.pin = tc.parid AND vps.year = tc.taxyr
    WHERE is_multisale = FALSE
    AND NOT sale_filter_is_outlier
),
-- Aggregate land for all parcels
aggregate_land AS (
    SELECT
        parid,
        taxyr,
        SUM(sf) AS total_land_sf
    FROM iasworld.land
    GROUP BY parid, taxyr
),
-- Combined SF/MF and condo characteristics
chars AS (
    SELECT
        parid,
        taxyr,
        MIN(yrblt) AS yrblt,
        SUM(sfla) AS total_bldg_sf
    FROM iasworld.dweldat
    GROUP BY parid, taxyr
    UNION
    SELECT
        pin as parid,
        year as taxyr,
        char_yrblt AS yrblt,
        char_building_sf AS total_bldg_sf
    FROM default.vw_pin_condo_char
    WHERE is_parking_space = FALSE
        AND is_common_area = FALSE
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
        approx_percentile(total, 0.5) AS fmv_median,
        count(*) as pin_n,
        approx_percentile(total_land_sf, 0.5) AS land_sf_median,
        approx_percentile(total_bldg_sf, 0.5) AS bldg_sf_median,
        approx_percentile(yrblt, 0.5) AS yrblt_median

    FROM all_values av

    LEFT JOIN aggregate_land al ON av.parid = al.parid
        AND av.year = al.taxyr
    INNER JOIN chars ab ON av.parid = ab.parid
        AND av.year = ab.taxyr

    GROUP BY assessment_stage, triad, township_code, year, property_group
),
-- By township and assessment stage
values_town_no_groups AS (
    SELECT
        triad,
        'Town' AS geography_type,
        'ALL REGRESSION' AS property_group,
        assessment_stage,
        township_code AS geography_id,
        year,
        approx_percentile(total, 0.5) AS fmv_median,
        count(*) as pin_n,
        approx_percentile(total_land_sf, 0.5) AS land_sf_median,
        approx_percentile(total_bldg_sf, 0.5) AS bldg_sf_median,
        approx_percentile(yrblt, 0.5) AS yrblt_median

    FROM all_values av

    LEFT JOIN aggregate_land al ON av.parid = al.parid
        AND av.year = al.taxyr
    INNER JOIN chars ab ON av.parid = ab.parid
        AND av.year = ab.taxyr

    GROUP BY assessment_stage, triad, township_code, year
),
-- By neighborhood, assessment_stage, and property group
values_nbhd_groups AS (
    SELECT
        triad,
        'TownNBHD' AS geography_type,
        property_group,
        assessment_stage,
        TownNBHD AS geography_id,
        year,
        approx_percentile(total, 0.5) AS fmv_median,
        count(*) as pin_n,
        approx_percentile(total_land_sf, 0.5) AS land_sf_median,
        approx_percentile(total_bldg_sf, 0.5) AS bldg_sf_median,
        approx_percentile(yrblt, 0.5) AS yrblt_median

    FROM all_values av

    LEFT JOIN aggregate_land al ON av.parid = al.parid
        AND av.year = al.taxyr
    INNER JOIN chars ab ON av.parid = ab.parid
        AND av.year = ab.taxyr

    GROUP BY assessment_stage, triad, TownNBHD, year, property_group
),
-- By neighborhood and assessment stage
values_nbhd_no_groups AS (
    SELECT
        triad,
        'TownNBHD' AS geography_type,
        'ALL REGRESSION' AS property_group,
        assessment_stage,
        townNBHD AS geography_id,
        year,
        approx_percentile(total, 0.5) AS fmv_median,
        count(*) as pin_n,
        approx_percentile(total_land_sf, 0.5) AS land_sf_median,
        approx_percentile(total_bldg_sf, 0.5) AS bldg_sf_median,
        approx_percentile(yrblt, 0.5) AS yrblt_median

    FROM all_values av

    LEFT JOIN aggregate_land al ON av.parid = al.parid
        AND av.year = al.taxyr
    INNER JOIN chars ab ON av.parid = ab.parid
        AND av.year = ab.taxyr

    GROUP BY assessment_stage, triad, TownNBHD, year
),
-- Here we aggregate stats on sales for each reporting group
-- By township and property group
sales_town_groups AS (
    SELECT
        township_code AS geography_id,
        sale_year,
        property_group,
        min(sale_price) AS sale_min,
        approx_percentile(sale_price, 0.5) AS sale_median,
        max(sale_price) AS sale_max,
        count(*) AS sale_n

    FROM sales
    GROUP BY township_code, sale_year, property_group
),
-- By township
sales_town_no_groups AS (
    SELECT
        township_code AS geography_id,
        sale_year,
        'ALL REGRESSION' AS property_group,
        min(sale_price) AS sale_min,
        approx_percentile(sale_price, 0.5) AS sale_median,
        max(sale_price) AS sale_max,
        count(*) AS sale_n

    FROM sales
    GROUP BY township_code, sale_year
),
-- By neighborhood and property group
sales_nbhd_groups AS (
    SELECT
        TownNBHD AS geography_id,
        sale_year,
        property_group,
        min(sale_price) AS sale_min,
        approx_percentile(sale_price, 0.5) AS sale_median,
        max(sale_price) AS sale_max,
        count(*) AS sale_n

    FROM sales
    GROUP BY TownNBHD, sale_year, property_group
),
-- By neighborhood
sales_nbhd_no_groups AS (
    SELECT
        TownNBHD AS geography_id,
        sale_year,
        'ALL REGRESSION' AS property_group,
        min(sale_price) AS sale_min,
        approx_percentile(sale_price, 0.5) AS sale_median,
        max(sale_price) AS sale_max,
        count(*) AS sale_n

    FROM sales
    GROUP BY TownNBHD, sale_year
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
    sale_year,
    sale_min,
    sale_median,
    sale_max,
    sale_n,
    -- Class mode has to be populated based on reporting group
    CASE
        WHEN av.geography_type = 'Town' AND av.property_group != 'ALL REGRESSION' THEN group_town_mode
        WHEN av.geography_type = 'Town' AND av.property_group = 'ALL REGRESSION' THEN town_mode
        WHEN av.geography_type = 'TownNBHD' AND av.property_group != 'ALL REGRESSION' THEN group_townnbhd_mode
        WHEN av.geography_type = 'TownNBHD' AND av.property_group = 'ALL REGRESSION' THEN townnbhd_mode
        ELSE NULL
        END AS class_mode

FROM aggregated_values av

-- Join sales so that values for a given year can be compared to a complete set of sales from the previous year
LEFT JOIN all_sales ON av.geography_id = all_sales.geography_id
    AND CAST(av.year AS INT) = CAST(all_sales.sale_year AS INT) + 1
    AND av.property_group = all_sales.property_group

-- Join on class modes specifically by the columns that were used to generate them
LEFT JOIN (SELECT DISTINCT assessment_stage, township_code AS geography_id, year, property_group, group_town_mode FROM class_modes) cm1
    ON av.assessment_stage = cm1.assessment_stage
    AND av.geography_id = cm1.geography_id
    AND av.year = cm1.year
    AND av.property_group = cm1.property_group
LEFT JOIN (SELECT DISTINCT assessment_stage, TownNBHD AS geography_id, year, property_group, group_townnbhd_mode FROM class_modes) cm2
    ON av.assessment_stage = cm2.assessment_stage
    AND av.geography_id = cm2.geography_id
    AND av.year = cm2.year
    AND av.property_group = cm2.property_group
LEFT JOIN (SELECT DISTINCT assessment_stage, township_code AS geography_id, year, town_mode FROM class_modes) cm3
    ON av.assessment_stage = cm3.assessment_stage
    AND av.geography_id = cm3.geography_id
    AND av.year = cm3.year
LEFT JOIN (SELECT DISTINCT assessment_stage, TownNBHD AS geography_id, year, townnbhd_mode FROM class_modes) cm4
    ON av.assessment_stage = cm4.assessment_stage
    AND av.geography_id = cm4.geography_id
    AND av.year = cm4.year
