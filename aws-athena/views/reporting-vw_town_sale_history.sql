CREATE OR REPLACE VIEW reporting.vw_town_sale_history
AS
-- Add township name
WITH town_names AS (
    SELECT
        triad_name AS triad_code,
        township_code
    FROM spatial.township
    ),
-- Recode classes into modeling groups and filter sales
classes AS (

SELECT
    sale_price,
    year AS sale_year,
    CASE WHEN class in ('299', '399') THEN 'CONDO'
    when class in ('211', '212') THEN 'MF'
    when class in ('202', '203', '204', '205', '206', '207', '208', '209', '210', '234', '278', '295') THEN 'SF'
    ELSE NULL END AS property_group,
    vw_pin_sale.township_code AS geography_id,
    triad_code
FROM default.vw_pin_sale
LEFT JOIN town_names
    ON vw_pin_sale.township_code = town_names.township_code
WHERE is_multisale = FALSE
AND sale_price_log10 BETWEEN sale_filter_lower_limit AND sale_filter_upper_limit

),
-- Aggregate by modeling group, town
groups AS (

SELECT
    triad_code,
    'Town' AS geography_type,
    property_group,
    geography_id,
    sale_year,
    approx_percentile(sale_price, 0.5) AS sale_median,
    count(*) AS sale_n
FROM classes
WHERE property_group IS NOT null
GROUP BY sale_year, property_group, geography_id, triad_code

),
-- Aggregate by town
no_groups AS (

SELECT
    triad_code,
    'Town' AS geography_type,
    'ALL REGRESSION' AS property_group,
    geography_id,
    sale_year,
    approx_percentile(sale_price, 0.5) AS sale_median,
    count(*) AS sale_n
FROM classes
WHERE property_group IS NOT null
GROUP BY sale_year, geography_id, triad_code

)

SELECT * FROM groups
UNION
SELECT * FROM no_groups