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
THIS VIEW NEEDS TO BE UPDATED WITH FINAL MODEL RUN IDs EACH YEAR
*/
CREATE OR REPLACE VIEW reporting.vw_res_report_summary
AS

-- Valuation class and nbhd from pardat
WITH classes AS (
    SELECT
        parid,
        taxyr,
        nbhd,
        class,
        CASE WHEN class in ('299', '399') THEN 'CONDO'
            when class in ('211', '212') THEN 'MF'
            when class in ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278', '295') THEN 'SF'
            ELSE NULL END AS property_group
    FROM iasworld.pardat
    ),
-- Townships from legdat since pardat has some errors we can't accept for public reporting
townships AS (
    SELECT
        parid,
        taxyr,
        substr(TAXDIST, 1, 2) AS township_code,
        triad_name AS triad
    FROM iasworld.legdat

    LEFT JOIN spatial.township
        ON substr(TAXDIST, 1, 2) = township_code
),
-- Final model values (Add 1 to model year since '2021' correspond to '2022' mailed values in iasWorld)
model_values AS (
    SELECT
        meta_pin AS parid,
        property_group,
        class,
        triad,
        townships.township_code,
        CONCAT(townships.township_code, substr(nbhd, 3, 3)) AS TownNBHD,
        CAST(CAST(meta_year AS INT) + 1 AS VARCHAR) AS year,
        'model' AS assessment_stage,
        pred_pin_final_fmv_round AS total

    FROM model.assessment_pin

    LEFT JOIN classes
        ON assessment_pin.meta_pin = classes.parid AND CAST(assessment_pin.meta_year AS INT) + 1 = CAST(classes.taxyr AS INT)
    LEFT JOIN townships
        ON assessment_pin.meta_pin = townships.parid AND CAST(assessment_pin.meta_year AS INT) + 1 = CAST(townships.taxyr AS INT)

    WHERE run_id IN (
        '2023-03-14-clever-damani', '2023-03-15-clever-kyra', --- 2023 models
        '2022-04-26-beautiful-dan', '2022-04-27-keen-gabe'  --- 2022 models
        )
        AND property_group IS NOT NULL
),
-- Values by assessment stages available in iasWorld (not model)
iasworld_values AS (
    SELECT
        asmt_all.parid,
        property_group,
        asmt_all.class,
        triad,
        townships.township_code,
        CONCAT(townships.township_code, substr(nbhd, 3, 3)) AS TownNBHD,
        asmt_all.taxyr as year,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE'  THEN 'bor certified'
            ELSE NULL END AS assessment_stage,
        max(
            CASE
                WHEN asmt_all.taxyr < '2020' THEN ovrvalasm3
                WHEN asmt_all.taxyr >= '2020' THEN valasm3
                ELSE NULL END
            ) * 10 AS total
    FROM iasworld.asmt_all

    LEFT JOIN classes
        ON asmt_all.parid = classes.parid AND asmt_all.taxyr = classes.taxyr
    LEFT JOIN townships
        ON asmt_all.parid = townships.parid AND asmt_all.taxyr = townships.taxyr

    WHERE (valclass IS null OR asmt_all.taxyr < '2020')
    AND procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
    AND property_group IS NOT NULL
    AND asmt_all.taxyr >= '2021'

    GROUP BY
        asmt_all.parid,
        asmt_all.taxyr,
        procname,
        property_group,
        CASE
            WHEN procname = 'CCAOVALUE' THEN 'mailed'
            WHEN procname = 'CCAOFINAL' THEN 'assessor certified'
            WHEN procname = 'BORVALUE'  THEN 'bor certified'
            ELSE NULL END,
        asmt_all.class,
        triad,
        townships.township_code,
        CONCAT(townships.township_code, substr(nbhd, 3, 3))
        ),
all_values AS (
    SELECT * FROM model_values
    UNION
    SELECT * FROM iasworld_values
),
-- Count of each class by different reporting groups (property group, assessment stage, town/nbhd)
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
        townships.township_code,
        CONCAT(townships.township_code, substr(nbhd, 3, 3)) AS TownNBHD
    FROM default.vw_pin_sale vps
    LEFT JOIN classes
        ON vps.pin = classes.parid AND vps.year = classes.taxyr
    LEFT JOIN townships
        ON vps.pin = townships.parid AND vps.year = townships.taxyr
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
