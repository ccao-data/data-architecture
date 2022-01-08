-- View to convert raw ACS5 variables into useable statistics
CREATE OR REPLACE VIEW census.vw_acs5_stat AS
WITH distinct_years AS (
    SELECT DISTINCT year
    FROM spatial.parcel
),
acs5_forward_fill AS (
    SELECT fill_years.filled_year, fill_data.*
    FROM (
        SELECT dy.year AS filled_year, MAX(df.year) AS fill_year
        FROM census.acs5 df
        CROSS JOIN distinct_years dy
        WHERE dy.year >= df.year
        GROUP BY dy.year
    ) fill_years
    LEFT JOIN census.acs5 fill_data
      ON fill_years.fill_year = fill_data.year
)
SELECT
    filled_year AS year,
    year AS acs5_data_year,
    geoid,
    geography,

    -- Sex
    B01001_001E AS count_sex_total,
    B01001_002E AS count_sex_male,
    B01001_026E AS count_sex_female,
    B01001_002E / B01001_001E AS percent_sex_male,
    B01001_026E / B01001_001E AS percent_sex_female,

    -- All school-age children (less than or equal to 19 years old)
    B01001_003E + B01001_004E + B01001_005E + B01001_006E + B01001_007E +
    B01001_027E + B01001_028E + B01001_029E + B01001_030E + B01001_031E AS count_age_children,
    (B01001_003E + B01001_004E + B01001_005E + B01001_006E + B01001_007E +
    B01001_027E + B01001_028E + B01001_029E + B01001_030E + B01001_031E) / B01001_001E AS percent_age_children,

    -- Seniors 65+, all genders
    B01001_020E + B01001_021E + B01001_022E + B01001_023E + B01001_024E + B01001_025E +
    B01001_044E + B01001_045E + B01001_046E + B01001_046E + B01001_048E + B01001_049E AS count_age_senior,
    (B01001_020E + B01001_021E + B01001_022E + B01001_023E + B01001_024E + B01001_025E +
    B01001_044E + B01001_045E + B01001_046E + B01001_046E + B01001_048E + B01001_049E) / B01001_001E AS percent_age_senior,

    -- Median age w/ gender breakout
    B01002_001E AS median_age_total,
    B01002_002E AS median_age_male,
    B01002_003E AS median_age_female,

    -- Race and hisp/latino origin breakout
    B02001_001E AS count_race_total,
    B02001_002E AS count_race_white,
    B02001_003E AS count_race_black,
    B03003_003E AS count_race_hisp,
    B02001_004E AS count_race_aian,
    B02001_005E AS count_race_asian,
    B02001_006E AS count_race_nhpi,
    B02001_007E + B02001_008E + B02001_009E + B02001_010E AS count_race_other,

    B02001_002E / B02001_001E AS percent_race_white,
    B02001_003E / B02001_001E AS percent_race_black,
    B03003_003E / B02001_001E AS percent_race_hisp,
    B02001_004E / B02001_001E AS percent_race_aian,
    B02001_005E / B02001_001E AS percent_race_asian,
    B02001_006E / B02001_001E AS percent_race_nhpi,
    (B02001_007E + B02001_008E + B02001_009E + B02001_010E) / B02001_001E AS percent_race_other,

    -- Movement and geographic mobility (only people older than 1 year)
    B07003_001E AS count_mobility_total,
    B07003_004E AS count_mobility_no_move,
    B07003_007E AS count_mobility_moved_in_county,
    B07003_010E AS count_mobility_moved_in_state,
    B07003_013E AS count_mobility_moved_from_other_state,
    B07003_016E AS count_mobility_moved_from_abroad,

    B07003_004E / B07003_001E AS percent_mobility_no_move,
    B07003_007E / B07003_001E AS percent_mobility_moved_in_county,
    B07003_010E / B07003_001E AS percent_mobility_moved_in_state,
    B07003_013E / B07003_001E AS percent_mobility_moved_from_other_state,
    B07003_016E / B07003_001E AS percent_mobility_moved_from_abroad,

    -- Household type
    B11001_001E AS count_household_total,
    B11001_002E AS count_household_family_total,
    B11001_003E AS count_household_family_married,
    B11001_004E AS count_household_family_other,
    B11001_008E AS count_household_nonfamily_alone,
    B11001_009E AS count_household_nonfamily_not_alone,

    B11001_003E / B11001_001E AS percent_household_family_married,
    B11001_008E / B11001_001E AS percent_household_nonfamily_alone,
    B11001_009E / B11001_001E AS percent_household_nonfamily_not_alone,

    -- Educational attainment for people older than 25
    B15002_001E AS count_education_total,
    B15002_011E + B15002_028E AS count_education_high_school,
    B15002_014E + B15002_031E AS count_education_associate,
    B15002_015E + B15002_032E AS count_education_bachelor,
    B15002_016E + B15002_017E + B15002_018E +
    B15002_033E + B15002_034E + B15002_035E AS count_education_graduate,

    (B15002_011E + B15002_028E) / B15002_001E AS percent_education_high_school,
    (B15002_014E + B15002_031E) / B15002_001E AS percent_education_associate,
    (B15002_015E + B15002_032E) / B15002_001E AS percent_education_bachelor,
    (B15002_016E + B15002_017E + B15002_018E +
    B15002_033E + B15002_034E + B15002_035E) / B15002_001E AS percent_education_graduate,

    -- Poverty, employment, and income
    B17001_001E AS count_income_total_poverty_level,
    B17001_002E AS count_income_below_poverty_level,
    B17001_031E AS count_income_above_poverty_level,

    B17001_002E / B17001_001E AS percent_income_below_poverty_level,
    B17001_031E / B17001_001E AS percent_income_above_poverty_level,

    B19013_001E AS median_income_household_past_year,
    B19301_001E AS median_income_per_capita_past_year,
    B22003_002E AS count_income_household_received_snap_past_year,
    B22003_002E / B22003_001E AS percent_income_household_received_snap_past_year,

    -- Only for population 16 and older
    B23025_003E AS count_employment_in_civilian_labor_force,
    B23025_005E AS count_employment_unemployed,
    B23025_007E AS count_employment_not_in_labor_force,
    B23025_005E / B23025_003E AS percent_employment_unemployed,

    -- Household stats
    B25003_001E AS count_household_total_occupied,
    B25003_002E AS count_household_owner_occupied,
    B25003_003E AS count_household_renter_occupied,
    NULLIF(B25037_001E, 0.0) AS median_household_total_occupied_year_built,
    NULLIF(B25037_002E, 0.0) AS median_household_owner_occupied_year_built,
    NULLIF(B25037_003E, 0.0) AS median_household_renter_occupied_year_built,
    B25064_001E AS median_household_renter_occupied_gross_rent,
    B25077_001E AS median_household_owner_occupied_value,

    B25003_002E / B25003_001E AS percent_household_owner_occupied,
    B25003_003E / B25003_001E AS percent_household_renter_occupied,

    -- Selected conditions, including:
    --   incomplete plumbing or kitchens
    --   overcrowding
    --   30% or more of the household income spent on rent or monthly owner costs
    B25123_003E AS count_household_owner_occupied_1_sel_cond,
    B25123_004E AS count_household_owner_occupied_2_sel_cond,
    B25123_005E AS count_household_owner_occupied_3_sel_cond,
    B25123_006E AS count_household_owner_occupied_4_sel_cond,
    B25123_009E AS count_household_renter_occupied_1_sel_cond,
    B25123_010E AS count_household_renter_occupied_2_sel_cond,
    B25123_011E AS count_household_renter_occupied_3_sel_cond,
    B25123_012E AS count_household_renter_occupied_4_sel_cond,

    (B25123_003E + B25123_004E + B25123_005E + B25123_006E +
    B25123_009E + B25123_010E + B25123_011E + B25123_012E) / B25123_001E AS percent_household_total_occupied_w_sel_cond,
    (B25123_003E + B25123_004E + B25123_005E + B25123_006E) / B25123_002E AS percent_household_owner_occupied_w_sel_cond,
    (B25123_009E + B25123_010E + B25123_011E + B25123_012E) / B25123_008E AS percent_household_renter_occupied_w_sel_cond

FROM acs5_forward_fill