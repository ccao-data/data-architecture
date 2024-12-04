/*
View containing cleaned, filled data for residential and condo modeling. This
view functions as a pre-cursor to the separate residential and condo modeling
input views with the goal of sharing as much code as possible for the sake of
consistency. Missing data is filled with the following steps:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021 as long as the data isn't something which frequently changes
2. Current data is filled BACKWARD to account for missing historical data.
   Again, this is only for things unlikely to change

WARNING: This is a very heavy view. Don't use it for anything other than making
extracts for modeling
*/
WITH uni AS (

    SELECT
        -- Main PIN-level attribute data from iasWorld
        par.parid AS pin,
        SUBSTR(par.parid, 1, 10) AS pin10,
        par.taxyr AS year,
        REGEXP_REPLACE(par.class, '[^[:alnum:]]', '') AS class,
        twn.triad_name,
        twn.triad_code,
        twn.township_name,
        leg.user1 AS township_code,
        REGEXP_REPLACE(par.nbhd, '([^0-9])', '') AS nbhd_code,
        leg.taxdist AS tax_code,
        NULLIF(leg.zip1, '00000') AS zip_code,

        -- Centroid of each PIN from county parcel files
        sp.lon,
        sp.lat,
        sp.x_3435,
        sp.y_3435,

        -- Features based on the shape of the parcel boundary
        sp.shp_parcel_centroid_dist_ft_sd,
        sp.shp_parcel_edge_len_ft_sd,
        sp.shp_parcel_interior_angle_sd,
        sp.shp_parcel_mrr_area_ratio,
        sp.shp_parcel_mrr_side_ratio,
        sp.shp_parcel_num_vertices

    FROM {{ source('iasworld', 'pardat') }} AS par
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
        ON par.parid = leg.parid
        AND par.taxyr = leg.taxyr
        AND leg.cur = 'Y'
        AND leg.deactivat IS NULL
    LEFT JOIN {{ source('spatial', 'parcel') }} AS sp
        ON SUBSTR(par.parid, 1, 10) = sp.pin10
        AND par.taxyr = sp.year
    LEFT JOIN {{ source('spatial', 'township') }} AS twn
        ON leg.user1 = CAST(twn.township_code AS VARCHAR)
    WHERE par.cur = 'Y'
        AND par.deactivat IS NULL
),

acs5 AS (
    SELECT *
    FROM {{ ref('census.vw_acs5_stat') }}
    WHERE geography = 'tract'
),

/* This CTAS uses location.census_2020 rather than joining onto a specific year
from location.census because we need to join 2020 PUMA geometry to *all*
parcels, not just those that existed in 2020 (or, in our case, 2022 since we
don't have 2020 PUMA shapefiles). This is specific to the IHS data since it
exists for many years but uses static geography. */
housing_index AS (
    SELECT
        puma.pin10,
        ihs.year,
        -- This is quarterly data and needs to be averaged annually
        AVG(CAST(ihs.ihs_index AS DOUBLE)) AS ihs_avg_year_index
    FROM {{ source('other', 'ihs_index') }} AS ihs
    LEFT JOIN
        (SELECT DISTINCT
            pin10,
            census_puma_geoid
        FROM {{ ref('location.census_2020') }}) AS puma
        ON ihs.geoid = puma.census_puma_geoid
    GROUP BY puma.pin10, ihs.year
),

distressed_communities_index AS (
    SELECT
        zcta.pin10,
        zcta.year,
        dci.dci
    FROM {{ source('other', 'dci') }} AS dci
    LEFT JOIN {{ ref('location.census') }} AS zcta
        ON dci.geoid = zcta.census_zcta_geoid
        -- DCI is only available for one year, so we join to census geoids for
        -- all years after that
        AND dci.year <= zcta.year
),

affordability_risk_index AS (
    SELECT
        tract.pin10,
        ari.ari_score AS ari,
        tract.year
    FROM {{ source('other', 'ari') }} AS ari
    LEFT JOIN {{ ref('location.census_acs5') }} AS tract
        ON ari.geoid = tract.census_acs5_tract_geoid
        -- ARI is only available for one year, so we join to census geoids for
        -- all years after that
        AND ari.year <= tract.year
),

tax_bill_amount AS (
    SELECT
        pardat.parid AS pin,
        pardat.taxyr AS year,
        pin.tax_bill_total AS tot_tax_amt,
        tax_code.tax_code_rate AS tax_rate
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN {{ source('tax', 'pin') }} AS pin
        ON pardat.parid = pin.pin
        AND (
            CASE WHEN pardat.taxyr > (SELECT MAX(year) FROM tax.pin)
                    THEN (SELECT MAX(year) FROM tax.pin)
                ELSE pardat.taxyr
            END = pin.year
        )
    LEFT JOIN (
        SELECT DISTINCT
            year,
            tax_code_num,
            tax_code_rate
        FROM {{ source('tax', 'tax_code') }}
    ) AS tax_code
        ON pin.tax_code_num = tax_code.tax_code_num
        AND pin.year = tax_code.year
    WHERE pin.pin IS NOT NULL
        AND pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
),

school_district_ratings AS (
    SELECT
        district_geoid,
        district_type,
        AVG(rating) AS school_district_avg_rating,
        COUNT(*) AS num_schools_in_district
    FROM {{ source('other', 'great_schools_rating') }}
    GROUP BY district_geoid, district_type
),

exemption_features AS (
    SELECT
        year,
        pin,
        SIGN(exe_homeowner) AS ccao_is_active_exe_homeowner,
        act AS ccao_n_years_exe_homeowner
    FROM (
        SELECT
            *,
            SUM(SIGN(exe_homeowner))
                OVER (PARTITION BY pin, grp ORDER BY year)
                AS act
        FROM (
            SELECT
                *,
                SUM(no_exe) OVER (PARTITION BY pin ORDER BY year) AS grp
            FROM (
                SELECT
                    *,
                    CASE WHEN exe_homeowner = 0 THEN 1 ELSE 0 END AS no_exe
                FROM {{ source('tax', 'pin') }}
            )
        )
    )
)

SELECT
    uni.pin AS meta_pin,
    uni.pin10 AS meta_pin10,
    uni.year AS meta_year,
    uni.class AS meta_class,
    uni.triad_name AS meta_triad_name,
    uni.triad_code AS meta_triad_code,
    uni.township_name AS meta_township_name,
    uni.township_code AS meta_township_code,
    uni.nbhd_code AS meta_nbhd_code,
    uni.tax_code AS meta_tax_code,

    -- PIN AV history for use in reporting and aggregate stats generation
    hist.mailed_bldg AS meta_mailed_bldg,
    hist.mailed_land AS meta_mailed_land,
    hist.mailed_tot AS meta_mailed_tot,
    hist.certified_bldg AS meta_certified_bldg,
    hist.certified_land AS meta_certified_land,
    hist.certified_tot AS meta_certified_tot,
    hist.board_bldg AS meta_board_bldg,
    hist.board_land AS meta_board_land,
    hist.board_tot AS meta_board_tot,
    hist.oneyr_pri_board_bldg AS meta_1yr_pri_board_bldg,
    hist.oneyr_pri_board_land AS meta_1yr_pri_board_land,
    hist.oneyr_pri_board_tot AS meta_1yr_pri_board_tot,
    hist.twoyr_pri_board_bldg AS meta_2yr_pri_board_bldg,
    hist.twoyr_pri_board_land AS meta_2yr_pri_board_land,
    hist.twoyr_pri_board_tot AS meta_2yr_pri_board_tot,

    -- Individual PIN-level address/location
    vwpa.prop_address_full AS loc_property_address,
    vwpa.prop_address_city_name AS loc_property_city,
    vwpa.prop_address_state AS loc_property_state,
    vwpa.prop_address_zipcode_1 AS loc_property_zip,
    uni.lon AS loc_longitude,
    uni.lat AS loc_latitude,
    uni.x_3435 AS loc_x_3435,
    uni.y_3435 AS loc_y_3435,

    -- PIN location data for aggregation and spatial joins
    vwlf.census_puma_geoid AS loc_census_puma_geoid,
    vwlf.census_tract_geoid AS loc_census_tract_geoid,
    vwlf.census_data_year AS loc_census_data_year,
    vwlf.census_acs5_puma_geoid AS loc_census_acs5_puma_geoid,
    vwlf.census_acs5_tract_geoid AS loc_census_acs5_tract_geoid,
    vwlf.census_acs5_data_year AS loc_census_acs5_data_year,
    vwlf.tax_municipality_name AS loc_tax_municipality_name,
    vwlf.ward_num AS loc_ward_num,
    vwlf.chicago_community_area_name AS loc_chicago_community_area_name,

    -- Location data used for spatial fixed effects
    vwlf.school_elementary_district_geoid
        AS loc_school_elementary_district_geoid,
    vwlf.school_secondary_district_geoid
        AS loc_school_secondary_district_geoid,
    vwlf.school_unified_district_geoid AS loc_school_unified_district_geoid,
    vwlf.tax_special_service_area_num AS loc_tax_special_service_area_num,
    vwlf.tax_tif_district_num AS loc_tax_tif_district_num,
    vwlf.misc_subdivision_id AS loc_misc_subdivision_id,

    -- Environmental and access data
    vwlf.env_flood_fema_sfha AS loc_env_flood_fema_sfha,
    vwlf.env_flood_fs_factor AS loc_env_flood_fs_factor,
    vwlf.env_flood_fs_risk_direction AS loc_env_flood_fs_risk_direction,
    vwlf.env_ohare_noise_contour_no_buffer_bool
        AS loc_env_ohare_noise_contour_no_buffer_bool,
    vwlf.env_airport_noise_dnl AS loc_env_airport_noise_dnl,
    vwlf.access_cmap_walk_nta_score AS loc_access_cmap_walk_nta_score,
    vwlf.access_cmap_walk_total_score AS loc_access_cmap_walk_total_score,

    -- PIN proximity count variables
    vwpf.num_pin_in_half_mile AS prox_num_pin_in_half_mile,
    vwpf.num_bus_stop_in_half_mile AS prox_num_bus_stop_in_half_mile,
    vwpf.num_foreclosure_per_1000_pin_past_5_years
        AS prox_num_foreclosure_per_1000_pin_past_5_years,
    vwpf.num_school_in_half_mile AS prox_num_school_in_half_mile,
    vwpf.num_school_with_rating_in_half_mile
        AS prox_num_school_with_rating_in_half_mile,
    vwpf.avg_school_rating_in_half_mile
        AS prox_avg_school_rating_in_half_mile,

    -- PIN proximity distance variables
    vwpf.airport_dnl_total AS prox_airport_dnl_total, --new
    vwpf.nearest_bike_trail_dist_ft AS prox_nearest_bike_trail_dist_ft,
    vwpf.nearest_cemetery_dist_ft AS prox_nearest_cemetery_dist_ft,
    vwpf.nearest_cta_route_dist_ft AS prox_nearest_cta_route_dist_ft,
    vwpf.nearest_cta_stop_dist_ft AS prox_nearest_cta_stop_dist_ft,
    vwpf.nearest_golf_course_dist_ft AS prox_nearest_golf_course_dist_ft,
    vwpf.nearest_grocery_store_dist_ft AS prox_nearest_grocery_store_dist_ft,
    vwpf.nearest_hospital_dist_ft AS prox_nearest_hospital_dist_ft,
    vwpf.lake_michigan_dist_ft AS prox_lake_michigan_dist_ft,
    vwpf.nearest_major_road_dist_ft AS prox_nearest_major_road_dist_ft,
    vwpf.nearest_metra_route_dist_ft AS prox_nearest_metra_route_dist_ft,
    vwpf.nearest_metra_stop_dist_ft AS prox_nearest_metra_stop_dist_ft,
    vwpf.nearest_new_construction_dist_ft
        AS prox_nearest_new_construction_dist_ft,
    vwpf.nearest_park_dist_ft AS prox_nearest_park_dist_ft,
    vwpf.nearest_railroad_dist_ft AS prox_nearest_railroad_dist_ft,
    vwpf.nearest_road_arterial_daily_traffic
        AS prox_nearest_road_arterial_daily_traffic,
    vwpf.nearest_road_arterial_dist_ft AS prox_nearest_road_arterial_dist_ft,
    vwpf.nearest_road_arterial_lanes AS prox_nearest_road_arterial_lanes,
    vwpf.nearest_road_arterial_speed_limit
        AS prox_nearest_road_arterial_speed_limit,
    vwpf.nearest_road_arterial_surface_type
        AS prox_nearest_road_arterial_surface_type,
    vwpf.nearest_road_collector_daily_traffic
        AS prox_nearest_road_collector_daily_traffic,
    vwpf.nearest_road_collector_dist_ft AS prox_nearest_road_collector_dist_ft,
    vwpf.nearest_road_collector_lanes AS prox_nearest_road_collector_lanes,
    vwpf.nearest_road_collector_speed_limit
        AS prox_nearest_road_collector_speed_limit,
    vwpf.nearest_road_collector_surface_type
        AS prox_nearest_road_collector_surface_type,
    vwpf.nearest_road_highway_daily_traffic
        AS prox_nearest_road_highway_daily_traffic,
    vwpf.nearest_road_highway_dist_ft AS prox_nearest_road_highway_dist_ft,
    vwpf.nearest_road_highway_lanes AS prox_nearest_road_highway_lanes,
    vwpf.nearest_road_highway_speed_limit
        AS prox_nearest_road_highway_speed_limit,
    vwpf.nearest_road_highway_surface_type
        AS prox_nearest_road_highway_surface_type,
    vwpf.nearest_secondary_road_dist_ft AS prox_nearest_secondary_road_dist_ft,
    vwpf.nearest_stadium_dist_ft AS prox_nearest_stadium_dist_ft,
    vwpf.nearest_university_dist_ft AS prox_nearest_university_dist_ft,
    vwpf.nearest_vacant_land_dist_ft AS prox_nearest_vacant_land_dist_ft,
    vwpf.nearest_water_dist_ft AS prox_nearest_water_dist_ft,

    -- Parcel shape features
    uni.shp_parcel_centroid_dist_ft_sd,
    uni.shp_parcel_edge_len_ft_sd,
    uni.shp_parcel_interior_angle_sd,
    uni.shp_parcel_mrr_area_ratio,
    uni.shp_parcel_mrr_side_ratio,
    uni.shp_parcel_num_vertices,

    -- ACS5 census data
    acs5.count_sex_total AS acs5_count_sex_total,
    acs5.percent_age_children AS acs5_percent_age_children,
    acs5.percent_age_senior AS acs5_percent_age_senior,
    acs5.median_age_total AS acs5_median_age_total,
    acs5.percent_mobility_no_move AS acs5_percent_mobility_no_move,
    acs5.percent_mobility_moved_in_county
        AS acs5_percent_mobility_moved_in_county,
    acs5.percent_mobility_moved_from_other_state
        AS acs5_percent_mobility_moved_from_other_state,
    acs5.percent_household_family_married
        AS acs5_percent_household_family_married,
    acs5.percent_household_nonfamily_alone
        AS acs5_percent_household_nonfamily_alone,
    acs5.percent_education_high_school
        AS acs5_percent_education_high_school,
    acs5.percent_education_bachelor AS acs5_percent_education_bachelor,
    acs5.percent_education_graduate AS acs5_percent_education_graduate,
    acs5.percent_income_below_poverty_level
        AS acs5_percent_income_below_poverty_level,
    acs5.median_income_household_past_year
        AS acs5_median_income_household_past_year,
    acs5.median_income_per_capita_past_year
        AS acs5_median_income_per_capita_past_year,
    acs5.percent_income_household_received_snap_past_year
        AS acs5_percent_income_household_received_snap_past_year,
    acs5.percent_employment_unemployed
        AS acs5_percent_employment_unemployed,
    acs5.median_household_total_occupied_year_built
        AS acs5_median_household_total_occupied_year_built,
    acs5.median_household_renter_occupied_gross_rent
        AS acs5_median_household_renter_occupied_gross_rent,
    acs5.median_household_owner_occupied_value
        AS acs5_median_household_owner_occupied_value,
    acs5.percent_household_owner_occupied
        AS acs5_percent_household_owner_occupied,
    acs5.percent_household_total_occupied_w_sel_cond
        AS acs5_percent_household_total_occupied_w_sel_cond,

    -- Institute for Housing Studies data
    housing_index.ihs_avg_year_index AS other_ihs_avg_year_index,
    -- Distressed Community Index data
    distressed_communities_index.dci
        AS other_distressed_community_index,
    -- Affordability Risk Index data
    affordability_risk_index.ari
        AS other_affordability_risk_index,
    tbill.tot_tax_amt AS other_tax_bill_amount_total,
    tbill.tax_rate AS other_tax_bill_rate,

    sdre.school_district_avg_rating
        AS other_school_district_elementary_avg_rating,
    sdrs.school_district_avg_rating
        AS other_school_district_secondary_avg_rating,

    -- Exemption features
    exemption_features.ccao_is_active_exe_homeowner,
    exemption_features.ccao_n_years_exe_homeowner,

    -- Corner lot indicator, only filled after 2014 since that's
    -- when OpenStreetMap data begins
    CASE
        WHEN uni.year >= '2014'
            THEN COALESCE(lot.is_corner_lot, FALSE)
    END AS ccao_is_corner_lot,

    -- PIN nearest neighbors, used for filling missing data
    vwpf.nearest_neighbor_1_pin10,
    vwpf.nearest_neighbor_1_dist_ft,
    vwpf.nearest_neighbor_2_pin10,
    vwpf.nearest_neighbor_2_dist_ft,
    vwpf.nearest_neighbor_3_pin10,
    vwpf.nearest_neighbor_3_dist_ft

FROM uni
LEFT JOIN {{ ref('location.vw_pin10_location_fill') }} AS vwlf
    ON uni.pin10 = vwlf.pin10
    AND uni.year = vwlf.year
LEFT JOIN {{ ref('proximity.vw_pin10_proximity_fill') }} AS vwpf
    ON uni.pin10 = vwpf.pin10
    AND uni.year = vwpf.year
LEFT JOIN {{ ref('default.vw_pin_address') }} AS vwpa
    ON uni.pin = vwpa.pin
    AND uni.year = vwpa.year
LEFT JOIN {{ ref('default.vw_pin_history') }} AS hist
    ON uni.pin = hist.pin
    AND uni.year = hist.year
LEFT JOIN acs5
    ON vwlf.census_acs5_tract_geoid = acs5.geoid
    AND vwlf.year = acs5.year
LEFT JOIN housing_index
    ON uni.pin10 = housing_index.pin10
    AND uni.year = housing_index.year
LEFT JOIN distressed_communities_index
    ON uni.pin10 = distressed_communities_index.pin10
    AND uni.year
    = distressed_communities_index.year
LEFT JOIN affordability_risk_index
    ON uni.pin10 = affordability_risk_index.pin10
    AND uni.year = affordability_risk_index.year
LEFT JOIN tax_bill_amount AS tbill
    ON uni.pin = tbill.pin
    AND uni.year = tbill.year
    -- The two following joins need to include year if we get more than
    -- one year of school ratings data.
LEFT JOIN
    (
        SELECT *
        FROM school_district_ratings
        WHERE district_type = 'elementary'
    ) AS sdre
    ON vwlf.school_elementary_district_geoid = sdre.district_geoid
LEFT JOIN
    (
        SELECT *
        FROM school_district_ratings
        WHERE district_type = 'secondary'
    ) AS sdrs
    ON vwlf.school_secondary_district_geoid = sdrs.district_geoid
LEFT JOIN exemption_features
    ON uni.pin = exemption_features.pin
    AND uni.year = exemption_features.year
LEFT JOIN {{ ref('default.vw_pin_status') }} AS lot
    ON uni.pin = lot.pin
    AND uni.year = lot.year
