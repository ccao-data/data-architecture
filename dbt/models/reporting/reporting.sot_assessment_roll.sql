-- Gather parcel-level geographies and join land, sales, and class groupings
WITH counts AS (
    SELECT
        year,
        COUNT(*) AS size
    FROM default.vw_pin_universe
    GROUP BY year
)

SELECT
    CAST(vals.tot AS INT) AS tot,
    CAST(vals.bldg AS INT) AS bldg,
    CAST(vals.land AS INT) AS land,
    CASE
        WHEN
            MOD(CAST(vals.year AS INT), 3) = 0
            AND uni.triad_name = 'North'
            THEN TRUE
        WHEN
            MOD(CAST(vals.year AS INT), 3) = 1
            AND uni.triad_name = 'South'
            THEN TRUE
        WHEN
            MOD(CAST(vals.year AS INT), 3) = 2
            AND uni.triad_name = 'City'
            THEN TRUE
        ELSE FALSE
    END AS reassessment_year,
    vals.class,
    vals.stage_name,
    vals.year,
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
    class_dict.modeling_group,
    counts.size
FROM default.vw_pin_universe AS uni
LEFT JOIN reporting.vw_pin_value_long AS vals
    ON uni.pin = vals.pin
    AND uni.year = vals.year
LEFT JOIN ccao.class_dict
    ON vals.class = class_dict.class_code
LEFT JOIN counts
    ON uni.year = counts.year
