{{
    config(
        materialized='table'
    )
}}

-- Gather parcel-level geographies and join taxes, exemptions, and class
-- groupings
SELECT
    tax.year,
    tax.av_clerk,
    tax.tax_bill_total,
    CASE WHEN tax.exe_homeowner = 0 THEN NULL ELSE tax.exe_homeowner END
        AS exe_homeowner,
    CASE WHEN tax.exe_senior = 0 THEN NULL ELSE tax.exe_senior END
        AS exe_senior,
    CASE WHEN tax.exe_freeze = 0 THEN NULL ELSE tax.exe_freeze END
        AS exe_freeze,
    CASE
        WHEN tax.exe_longtime_homeowner = 0 THEN NULL ELSE
            tax.exe_longtime_homeowner
    END AS exe_longtime_homeowner,
    CASE WHEN tax.exe_disabled = 0 THEN NULL ELSE tax.exe_disabled END
        AS exe_disabled,
    CASE
        WHEN tax.exe_vet_returning = 0 THEN NULL ELSE tax.exe_vet_returning
    END AS exe_vet_returning,
    CASE WHEN tax.exe_vet_dis_lt50 = 0 THEN NULL ELSE tax.exe_vet_dis_lt50 END
        AS exe_vet_dis_lt50,
    CASE
        WHEN tax.exe_vet_dis_50_69 = 0 THEN NULL ELSE tax.exe_vet_dis_50_69
    END AS exe_vet_dis_50_69,
    CASE WHEN tax.exe_vet_dis_ge70 = 0 THEN NULL ELSE tax.exe_vet_dis_ge70 END
        AS exe_vet_dis_ge70,
    CASE WHEN tax.exe_abate = 0 THEN NULL ELSE tax.exe_abate END AS exe_abate,
    tcd.tax_code_rate,
    eqf.eq_factor_tentative,
    eqf.eq_factor_final,
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
    uni.tax_municipality_name AS tax_municipality,
    uni.tax_park_district_name AS tax_park_district,
    uni.tax_library_district_name AS tax_library_district,
    uni.tax_fire_protection_district_name AS tax_fire_protection_district,
    uni.tax_community_college_district_name
        AS
        tax_community_college_district,
    uni.tax_sanitation_district_name AS tax_sanitation_district,
    uni.tax_special_service_area_name AS tax_special_service_area,
    uni.tax_tif_district_name AS tax_tif_district,
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
    class_dict.modeling_group
FROM {{ ref('default.vw_pin_universe') }} AS uni
INNER JOIN {{ source('tax', 'pin') }} AS tax
    ON uni.pin = tax.pin
    AND uni.year = tax.year
INNER JOIN {{ source('tax', 'eq_factor') }} AS eqf
    ON uni.year = eqf.year
INNER JOIN {{ source('tax', 'tax_code') }} AS tcd
    ON tax.tax_code_num = tcd.tax_code_num
    AND tax.year = tcd.year
INNER JOIN {{ ref('ccao.class_dict') }}
    ON uni.class = class_dict.class_code
