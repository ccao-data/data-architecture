-- Return all model predictors for all years to use in populating a SELECT
-- query, with an optional tablename indicating the table that the predictor
-- columns should be selected from.
--
-- If the `exclude` parameter is set to a string or a list of strings representing
-- one or more predictors, the macro will omit those predictors from the
-- output. This is useful in cases where you plan to select a predictor
-- elsewhere in your query, and you don't want this macro to collide with that
-- selection.
--
-- If the `alias_prefix` parameter is set to a string, it will alias each
-- predictor with the specified prefix prepended to the predictor name.
-- For example, `alias_prefix='shap'` will return column aliases following the
-- pattern "shap_<predictor_name>", e.g. "shap_char_bldg_sf".
--
-- This macro is currently only used by PINVAL views, so it only includes
-- predictors for PINVAL-eligible years.
--
-- We do have to add predictors to this list manually whenever we add them to
-- the model, but we have a data integrity test on PINVAL views that should
-- alert us if we ever fall out of sync with the model.
--
-- Never remove predictors from this list, only add them. Outdated
-- predictors are most likely necessary to support reports for prior
-- assessment years.
{% macro all_predictors(tablename=None, exclude=None, alias_prefix=None) %}
    {#-
        List all predictor column names so we can iterate them and optionally
        exclude them
    -#}
    {%- set predictors = [
        "meta_township_code",
        "meta_nbhd_code",
        "meta_sale_count_past_n_years",
        "char_yrblt",
        "char_air",
        "char_apts",
        "char_attic_fnsh",
        "char_attic_type",
        "char_beds",
        "char_bldg_sf",
        "char_bsmt",
        "char_bsmt_fin",
        "char_class",
        "char_ext_wall",
        "char_fbath",
        "char_frpl",
        "char_gar1_att",
        "char_gar1_cnst",
        "char_gar1_size",
        "char_hbath",
        "char_land_sf",
        "char_heat",
        "char_ncu",
        "char_porch",
        "char_roof_cnst",
        "char_rooms",
        "char_tp_dsgn",
        "char_type_resd",
        "char_recent_renovation",
        "loc_longitude",
        "loc_latitude",
        "loc_census_tract_geoid",
        "loc_env_flood_fs_factor",
        "loc_env_airport_noise_dnl",
        "loc_school_elementary_district_geoid",
        "loc_school_secondary_district_geoid",
        "loc_access_cmap_walk_nta_score",
        "loc_access_cmap_walk_total_score",
        "loc_tax_municipality_name",
        "prox_num_pin_in_half_mile",
        "prox_num_bus_stop_in_half_mile",
        "prox_num_foreclosure_per_1000_pin_past_5_years",
        "prox_num_school_in_half_mile",
        "prox_num_school_with_rating_in_half_mile",
        "prox_avg_school_rating_in_half_mile",
        "prox_airport_dnl_total",
        "prox_nearest_bike_trail_dist_ft",
        "prox_nearest_cemetery_dist_ft",
        "prox_nearest_cta_route_dist_ft",
        "prox_nearest_cta_stop_dist_ft",
        "prox_nearest_hospital_dist_ft",
        "prox_lake_michigan_dist_ft",
        "prox_nearest_major_road_dist_ft",
        "prox_nearest_metra_route_dist_ft",
        "prox_nearest_metra_stop_dist_ft",
        "prox_nearest_park_dist_ft",
        "prox_nearest_railroad_dist_ft",
        "prox_nearest_secondary_road_dist_ft",
        "prox_nearest_university_dist_ft",
        "prox_nearest_vacant_land_dist_ft",
        "prox_nearest_water_dist_ft",
        "prox_nearest_golf_course_dist_ft",
        "prox_nearest_road_highway_dist_ft",
        "prox_nearest_road_arterial_dist_ft",
        "prox_nearest_road_collector_dist_ft",
        "prox_nearest_road_arterial_daily_traffic",
        "prox_nearest_road_collector_daily_traffic",
        "prox_nearest_new_construction_dist_ft",
        "prox_nearest_stadium_dist_ft",
        "acs5_percent_age_children",
        "acs5_percent_age_senior",
        "acs5_median_age_total",
        "acs5_percent_mobility_no_move",
        "acs5_percent_mobility_moved_from_other_state",
        "acs5_percent_household_family_married",
        "acs5_percent_household_nonfamily_alone",
        "acs5_percent_education_high_school",
        "acs5_percent_education_bachelor",
        "acs5_percent_education_graduate",
        "acs5_percent_income_below_poverty_level",
        "acs5_median_income_household_past_year",
        "acs5_median_income_per_capita_past_year",
        "acs5_percent_income_household_received_snap_past_year",
        "acs5_percent_employment_unemployed",
        "acs5_median_household_total_occupied_year_built",
        "acs5_median_household_renter_occupied_gross_rent",
        "acs5_percent_household_owner_occupied",
        "acs5_percent_household_total_occupied_w_sel_cond",
        "acs5_percent_mobility_moved_in_county",
        "other_tax_bill_rate",
        "other_school_district_elementary_avg_rating",
        "other_school_district_secondary_avg_rating",
        "ccao_is_active_exe_homeowner",
        "ccao_is_corner_lot",
        "ccao_n_years_exe_homeowner",
        "time_sale_year",
        "time_sale_day",
        "time_sale_quarter_of_year",
        "time_sale_month_of_year",
        "time_sale_day_of_year",
        "time_sale_day_of_month",
        "time_sale_day_of_week",
        "time_sale_post_covid",
        "shp_parcel_centroid_dist_ft_sd",
        "shp_parcel_edge_len_ft_sd",
        "shp_parcel_interior_angle_sd",
        "shp_parcel_mrr_area_ratio",
        "shp_parcel_mrr_side_ratio",
        "shp_parcel_num_vertices",
    ] -%}

    {#- Handle exclude input: None, string, or list -#}
    {%- if exclude is none -%} {%- set exclude_list = [] -%}
    {%- elif exclude is string -%} {%- set exclude_list = [exclude] -%}
    {%- else -%} {%- set exclude_list = exclude -%}
    {%- endif -%}

    {#- Raise error if any exclude value isn't in predictors -#}
    {%- for excluded in exclude_list -%}
        {%- if excluded not in predictors -%}
            {{
                exceptions.raise_compiler_error(
                    "Excluded predictor '"
                    ~ excluded
                    ~ "' not found in predictor list for all_predictors macro."
                )
            }}
        {%- endif -%}
    {%- endfor -%}

    {#- Remove exclude values from predictor list -#}
    {%- set predictors_to_use = predictors | reject("in", exclude_list) | list -%}

    {#- Add tablename prefix if a tablename is provided -#}
    {%- set tablename_prefix = tablename ~ "." if tablename else "" -%}

    {#- Build select list -#}
    {%- for predictor in predictors_to_use -%}
        {{ tablename_prefix }}{{ predictor }}
        {%- if alias_prefix %} as {{ alias_prefix }}_{{ predictor }}{% endif %}
        {%- if not loop.last %},{% endif -%}
    {%- endfor -%}
{% endmacro %}
