-- Gather parcel-level geographies and join land, sales, and class groupings
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

    SELECT 'MAILED' AS stage_name
    UNION
    SELECT 'ASSESSOR CERTIFIED' AS stage_name
    UNION
    SELECT 'BOR CERTIFIED' AS stage_name

),

uni AS (
    SELECT
        vw_pin_universe.*,
        stages.*
    FROM {{ ref('default.vw_pin_universe') }}
    CROSS JOIN stages
)

SELECT
    CAST(sales.sale_price AS DOUBLE) AS sale_price,
    uni.year,
    uni.stage_name,
    uni.class,
    CAST(vals.tot_mv AS DOUBLE) AS tot_mv,
    CAST(vals.tot_mv AS DOUBLE) / CAST(sales.sale_price AS DOUBLE) AS ratio,
    'Cook' AS county,
    uni.triad_name AS triad,
    uni.township_name AS township,
    uni.nbhd_code AS nbhd,
    uni.tax_code,
    uni.zip_code,
    uni.chicago_community_area_name AS community_area,
    uni.census_place_geoid AS census_place,
    uni.census_tract_geoid AS census_tract,
    uni.census_congressional_district_geoid
        AS
        census_congressional_district,
    uni.census_zcta_geoid AS census_zcta,
    uni.cook_board_of_review_district_num AS cook_board_of_review_district,
    uni.cook_commissioner_district_num AS cook_commissioner_district,
    uni.cook_judicial_district_num AS cook_judicial_district,
    uni.ward_num,
    uni.chicago_police_district_num AS police_district,
    uni.school_elementary_district_geoid AS school_elementary_district,
    uni.school_secondary_district_geoid AS school_secondary_district,
    uni.school_unified_district_geoid AS school_unified_district,
    ARRAY_JOIN(uni.tax_municipality_name, ', ') AS tax_municipality,
    ARRAY_JOIN(uni.tax_park_district_name, ', ') AS tax_park_district,
    ARRAY_JOIN(uni.tax_library_district_name, ', ') AS tax_library_district,
    ARRAY_JOIN(uni.tax_fire_protection_district_name, ', ')
        AS tax_fire_protection_district,
    ARRAY_JOIN(uni.tax_community_college_district_name, ', ')
        AS
        tax_community_college_district,
    ARRAY_JOIN(uni.tax_sanitation_district_name, ', ')
        AS tax_sanitation_district,
    ARRAY_JOIN(uni.tax_special_service_area_name, ', ')
        AS tax_special_service_area,
    ARRAY_JOIN(uni.tax_tif_district_name, ', ') AS tax_tif_district,
    uni.econ_central_business_district_num AS central_business_district,
    uni.census_data_year,
    uni.cook_board_of_review_district_data_year,
    uni.cook_commissioner_district_data_year,
    uni.cook_judicial_district_data_year,
    COALESCE(
        uni.ward_chicago_data_year, uni.ward_evanston_data_year) AS
    ward_data_year,
    uni.chicago_community_area_data_year AS community_area_data_year,
    uni.chicago_police_district_data_year AS police_district_data_year,
    uni.econ_central_business_district_data_year
        AS
        central_business_district_data_year,
    uni.school_data_year,
    uni.tax_data_year,
    'no_group' AS no_group,
    class_dict.major_class_type AS major_class,
    class_dict.modeling_group,
    CASE WHEN class_dict.major_class_code = '2' THEN 'RES' ELSE 'OTHER' END
        AS res_other
FROM uni
LEFT JOIN
    {{ ref('reporting.vw_pin_value_long') }} AS vals
    ON uni.pin = vals.pin
    AND uni.year = vals.year
    AND uni.stage_name = vals.stage_name
LEFT JOIN {{ ref('ccao.class_dict') }}
    ON uni.class = class_dict.class_code
LEFT JOIN {{ ref('default.vw_pin_sale') }} AS sales
    ON uni.pin = sales.pin
    AND uni.year = sales.year
    AND NOT sales.is_multisale
    AND NOT sales.sale_filter_deed_type
    AND NOT sales.sale_filter_less_than_10k
    AND NOT sales.sale_filter_same_sale_within_365
WHERE uni.year >= '2020'
    AND uni.year IN ('2022', '2023') AND uni.class IN ('278', '597')
