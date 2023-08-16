-- Aggregate sales by assessment stage, property groups, year, and township

-- Add township name
WITH town_names AS (
    SELECT
        triad_name AS triad_code,
        township_code
    FROM {{ ref('spatial.township') }}
),

-- Recode classes into modeling groups and filter sales
classes AS (
    SELECT
        vwps.sale_price,
        vwps.year AS sale_year,
        CASE WHEN vwps.class IN ('299', '399') THEN 'CONDO'
            WHEN vwps.class IN ('211', '212') THEN 'MF'
            WHEN vwps.class IN (
                    '202', '203', '204', '205', '206', '207',
                    '208', '209', '210', '234', '278', '295'
                )
                THEN 'SF'
        END AS property_group,
        vwps.township_code AS geography_id,
        town_names.triad_code
    FROM {{ ref('default.vw_pin_sale') }} AS vwps
    LEFT JOIN town_names
        ON vwps.township_code = town_names.township_code
    WHERE NOT vwps.is_multisale
        AND NOT vwps.sale_filter_is_outlier
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
