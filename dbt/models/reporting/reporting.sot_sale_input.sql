-- This script gathers parcel-level geographies and joins them to sales and
-- class groupings. Its sole purpose is to feed reporting.sot_sale,
-- and should not be used otherwise.

{{
    config(
        materialized='table',
        partitioned_by=['year']
    )
}}

-- Gather parcel-level land and yrblt
WITH sf AS (
    SELECT
        pin,
        year,
        SUM(char_bldg_sf) AS char_bldg_sf,
        SUM(char_land_sf) AS char_land_sf,
        ARBITRARY(char_yrblt) AS char_yrblt
    FROM {{ ref('default.vw_card_res_char') }}
    GROUP BY pin, year
)

SELECT
    sales.doc_no,
    -- Code outlier sale prices as NULL so they won't be part of aggregated sale
    -- stats, but we can count the number of outliers
    CASE WHEN sales.sv_is_outlier THEN NULL ELSE sales.sale_price END
        AS sale_price,
    COALESCE(sales.sv_is_outlier, FALSE) AS sale_is_outlier,
    CASE WHEN sf.char_bldg_sf > 0
            THEN
            CAST(sales.sale_price / sf.char_bldg_sf AS DOUBLE)
    END AS sale_price_per_sf,
    CAST(sf.char_bldg_sf AS INT) AS sale_char_bldg_sf,
    CAST(sf.char_land_sf AS INT) AS sale_char_land_sf,
    CAST(sf.char_yrblt AS INT) AS sale_char_yrblt,
    uni.class,
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
        AS res_other,
    uni.year
FROM {{ ref('default.vw_pin_universe') }} AS uni
LEFT JOIN {{ ref('default.vw_pin_sale') }} AS sales
    ON uni.pin = sales.pin
    AND uni.year = sales.year
    AND NOT sales.is_multisale
    AND NOT sales.sale_filter_deed_type
    AND NOT sales.sale_filter_less_than_10k
    AND NOT sales.sale_filter_same_sale_within_365
LEFT JOIN sf
    ON uni.pin = sf.pin
    AND uni.year = sf.year
    -- Don't join characteristics onto outliers
    AND NOT COALESCE(sales.sv_is_outlier, FALSE)
LEFT JOIN {{ ref('ccao.class_dict') }}
    ON uni.class = class_dict.class_code
-- Temporary limit on feeder table to avoid GitHub runner memory issues.
WHERE uni.year = '2023'
