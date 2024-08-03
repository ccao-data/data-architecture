-- This script gathers parcel-level geographies and joins them to values, tax
-- amounts, exemptions and class groupings. Its sole purpose is to feed
-- reporting.sot_taxes_and_exemptions, and should not be used otherwise.
{{
    config(
        materialized='table',
        partitioned_by=['year']
    )
}}

-- Gather unique tax codes and rates
WITH tcd AS (
    SELECT DISTINCT
        tax_code_num,
        tax_code_rate,
        year
    FROM {{ source('tax', 'tax_code') }}
)

SELECT
    uni.pin,
    tax.av_clerk AS tax_av,
    tax.tax_bill_total,
    -- Setting exemptions with values of 0 allows us to count the number of
    -- exemptions more easily and doesn't skew stats.
    CASE WHEN tax.exe_homeowner = 0 THEN NULL ELSE tax.exe_homeowner END
        AS tax_exe_homeowner,
    CASE WHEN tax.exe_senior = 0 THEN NULL ELSE tax.exe_senior END
        AS tax_exe_senior,
    CASE WHEN tax.exe_freeze = 0 THEN NULL ELSE tax.exe_freeze END
        AS tax_exe_freeze,
    CASE
        WHEN tax.exe_longtime_homeowner = 0 THEN NULL ELSE
            tax.exe_longtime_homeowner
    END AS tax_exe_longtime_homeowner,
    CASE WHEN tax.exe_disabled = 0 THEN NULL ELSE tax.exe_disabled END
        AS tax_exe_disabled,
    CASE
        WHEN tax.exe_vet_returning = 0 THEN NULL ELSE tax.exe_vet_returning
    END AS tax_exe_vet_returning,
    CASE WHEN tax.exe_vet_dis_lt50 = 0 THEN NULL ELSE tax.exe_vet_dis_lt50 END
        AS tax_exe_vet_dis_lt50,
    CASE
        WHEN tax.exe_vet_dis_50_69 = 0 THEN NULL ELSE tax.exe_vet_dis_50_69
    END AS tax_exe_vet_dis_50_69,
    CASE WHEN tax.exe_vet_dis_ge70 = 0 THEN NULL ELSE tax.exe_vet_dis_ge70 END
        AS tax_exe_vet_dis_ge70,
    CASE WHEN tax.exe_abate = 0 THEN NULL ELSE tax.exe_abate END
        AS tax_exe_abate,
    CASE
        WHEN tax.exe_homeowner + tax.exe_senior + tax.exe_freeze
            + tax.exe_longtime_homeowner + tax.exe_disabled
            + tax.exe_vet_returning + tax.exe_vet_dis_lt50
            + tax.exe_vet_dis_50_69 + tax.exe_vet_dis_ge70 + tax.exe_abate = 0
            THEN NULL ELSE
            tax.exe_homeowner + tax.exe_senior + tax.exe_freeze
            + tax.exe_longtime_homeowner + tax.exe_disabled
            + tax.exe_vet_returning + tax.exe_vet_dis_lt50
            + tax.exe_vet_dis_50_69 + tax.exe_vet_dis_ge70 + tax.exe_abate
    END AS tax_exe_total,
    tcd.tax_code_rate AS tax_rate,
    eqf.eq_factor_tentative AS tax_eq_factor_tentative,
    eqf.eq_factor_final AS tax_eq_factor_final,
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
    tax.year
FROM {{ ref('default.vw_pin_universe') }} AS uni
INNER JOIN {{ source('tax', 'pin') }} AS tax
    ON uni.pin = tax.pin
    AND uni.year = tax.year
INNER JOIN {{ source('tax', 'eq_factor') }} AS eqf
    ON uni.year = eqf.year
INNER JOIN tcd
    ON tax.tax_code_num = tcd.tax_code_num
    AND tax.year = tcd.year
INNER JOIN {{ ref('ccao.class_dict') }}
    ON uni.class = class_dict.class_code
-- Temporary limit on feeder table to avoid GitHub runner memory issues.
WHERE uni.class = '206'
