-- Gather parcel-level land and yrblt
WITH sf AS (
    SELECT
        pin,
        year,
        SUM(char_bldg_sf) AS char_bldg_sf,
        SUM(char_land_sf) AS char_land_sf,
        ARBITRARY(char_yrblt) AS char_yrblt
    FROM default.vw_card_res_char
    GROUP BY pin, year
)

-- Gather parcel-level geographies and join land, sales, and class groupings
SELECT
    sales.doc_no,
    sales.sale_price,
    CASE WHEN sf.char_bldg_sf > 0
            THEN
            CAST(sales.sale_price / sf.char_bldg_sf AS DOUBLE)
    END AS price_per_sf,
    CAST(sf.char_bldg_sf AS INT) AS char_bldg_sf,
    CAST(sf.char_land_sf AS INT) AS char_land_sf,
    CAST(sf.char_yrblt AS INT) AS char_yrblt,
    CAST(hist.oneyr_pri_mailed_bldg AS DOUBLE) AS oneyr_pri_mailed_bldg,
    CAST(hist.oneyr_pri_mailed_land AS DOUBLE) AS oneyr_pri_mailed_land,
    CAST(hist.oneyr_pri_mailed_tot AS DOUBLE) AS oneyr_pri_mailed_tot,
    uni.year,
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
FROM vw_pin_universe AS uni
LEFT JOIN sf
    ON uni.pin = sf.pin
    AND uni.year = sf.year
LEFT JOIN ccao.class_dict
    ON uni.class = class_dict.class_code
LEFT JOIN default.vw_pin_history AS hist
    ON uni.pin = hist.pin
    AND uni.year = hist.year
LEFT JOIN vw_pin_sale AS sales
    ON uni.pin = sales.pin
    AND uni.year = sales.year
    AND NOT sales.is_multisale
    AND NOT sales.sale_filter_deed_type
    AND NOT sales.sale_filter_less_than_10k
    AND NOT sales.sale_filter_same_sale_within_365
