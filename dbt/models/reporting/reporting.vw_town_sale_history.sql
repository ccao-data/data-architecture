-- Aggregate sales by assessment stage, property groups, year, and township

-- Recode classes into modeling groups and filter sales
WITH classes AS (
    SELECT
        vwps.sale_price,
        vwps.year AS sale_year,
        town_names.property_group,
        vwps.township_code AS geography_id,
        town_names.triad_code
    FROM {{ ref('default.vw_pin_sale') }} AS vwps
    LEFT JOIN {{ ref('reporting.vw_pin_township_class') }} AS town_names
        ON vwps.pin = town_names.pin
        AND vwps.year = town_names.year
    WHERE NOT vwps.is_multisale
        AND NOT vwps.sale_filter_is_outlier
        AND NOT vwps.sale_filter_deed_type
        AND NOT vwps.sale_filter_less_than_10k
        AND NOT vwps.sale_filter_same_sale_within_365
),

-- Aggregate by modeling group, town
groups AS (
    SELECT
        triad_code,
        'Town' AS geography_type,
        property_group,
        geography_id,
        sale_year,
        APPROX_PERCENTILE(sale_price, 0.5) AS sale_median,
        COUNT(*) AS sale_n
    FROM classes
    WHERE property_group IS NOT NULL
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
        APPROX_PERCENTILE(sale_price, 0.5) AS sale_median,
        COUNT(*) AS sale_n
    FROM classes
    WHERE property_group IS NOT NULL
    GROUP BY sale_year, geography_id, triad_code
)

SELECT * FROM groups
UNION
SELECT * FROM no_groups
