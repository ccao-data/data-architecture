-- View to convert raw ACS5 variables into useable statistics
CREATE OR REPLACE VIEW census.vw_acs5_stat AS
WITH distinct_years AS (
    SELECT DISTINCT year
    FROM spatial.parcel
),

acs5_forward_fill AS (
    SELECT
        fill_years.filled_year,
        fill_data.*
    FROM (
        SELECT
            dy.year AS filled_year,
            MAX(df.year) AS fill_year
        FROM census.acs5 AS df
        CROSS JOIN distinct_years AS dy
        WHERE dy.year >= df.year
        GROUP BY dy.year
    ) AS fill_years
    LEFT JOIN census.acs5 AS fill_data
        ON fill_years.fill_year = fill_data.year
)

SELECT
    filled_year AS year,
    year AS acs5_data_year,
    geoid,
    geography,

    -- Sex
    b01001_001e AS count_sex_total,
    b01001_002e AS count_sex_male,
    b01001_026e AS count_sex_female,
    b01001_002e / b01001_001e AS percent_sex_male,
    b01001_026e / b01001_001e AS percent_sex_female,

    -- All school-age children (less than or equal to 19 years old)
    b01001_003e + b01001_004e + b01001_005e + b01001_006e + b01001_007e
    + b01001_027e + b01001_028e + b01001_029e + b01001_030e
    + b01001_031e AS count_age_children,
    (
        b01001_003e + b01001_004e + b01001_005e + b01001_006e + b01001_007e
        + b01001_027e + b01001_028e + b01001_029e + b01001_030e + b01001_031e
    ) / b01001_001e AS percent_age_children,

    -- Seniors 65+, all genders
    b01001_020e + b01001_021e + b01001_022e + b01001_023e + b01001_024e
    + b01001_025e + b01001_044e + b01001_045e + b01001_046e + b01001_046e
    + b01001_048e + b01001_049e AS count_age_senior,
    (
        b01001_020e + b01001_021e + b01001_022e + b01001_023e + b01001_024e
        + b01001_025e + b01001_044e + b01001_045e + b01001_046e + b01001_046e
        + b01001_048e + b01001_049e
    ) / b01001_001e AS percent_age_senior,

    -- Median age w/ gender breakout
    b01002_001e AS median_age_total,
    b01002_002e AS median_age_male,
    b01002_003e AS median_age_female,

    -- Race and hisp/latino origin breakout
    b02001_001e AS count_race_total,
    b02001_002e AS count_race_white,
    b02001_003e AS count_race_black,
    b03003_003e AS count_race_hisp,
    b02001_004e AS count_race_aian,
    b02001_005e AS count_race_asian,
    b02001_006e AS count_race_nhpi,
    b02001_007e + b02001_008e + b02001_009e + b02001_010e AS count_race_other,

    b02001_002e / b02001_001e AS percent_race_white,
    b02001_003e / b02001_001e AS percent_race_black,
    b03003_003e / b02001_001e AS percent_race_hisp,
    b02001_004e / b02001_001e AS percent_race_aian,
    b02001_005e / b02001_001e AS percent_race_asian,
    b02001_006e / b02001_001e AS percent_race_nhpi,
    (b02001_007e + b02001_008e + b02001_009e + b02001_010e)
    / b02001_001e AS percent_race_other,

    -- Movement and geographic mobility (only people older than 1 year)
    b07003_001e AS count_mobility_total,
    b07003_004e AS count_mobility_no_move,
    b07003_007e AS count_mobility_moved_in_county,
    b07003_010e AS count_mobility_moved_in_state,
    b07003_013e AS count_mobility_moved_from_other_state,
    b07003_016e AS count_mobility_moved_from_abroad,

    b07003_004e / b07003_001e AS percent_mobility_no_move,
    b07003_007e / b07003_001e AS percent_mobility_moved_in_county,
    b07003_010e / b07003_001e AS percent_mobility_moved_in_state,
    b07003_013e / b07003_001e AS percent_mobility_moved_from_other_state,
    b07003_016e / b07003_001e AS percent_mobility_moved_from_abroad,

    -- Household type
    b11001_001e AS count_household_total,
    b11001_002e AS count_household_family_total,
    b11001_003e AS count_household_family_married,
    b11001_004e AS count_household_family_other,
    b11001_008e AS count_household_nonfamily_alone,
    b11001_009e AS count_household_nonfamily_not_alone,

    b11001_003e / b11001_001e AS percent_household_family_married,
    b11001_008e / b11001_001e AS percent_household_nonfamily_alone,
    b11001_009e / b11001_001e AS percent_household_nonfamily_not_alone,

    -- Educational attainment for people older than 25
    b15002_001e AS count_education_total,
    b15002_011e + b15002_028e AS count_education_high_school,
    b15002_014e + b15002_031e AS count_education_associate,
    b15002_015e + b15002_032e AS count_education_bachelor,
    b15002_016e + b15002_017e + b15002_018e
    + b15002_033e + b15002_034e + b15002_035e AS count_education_graduate,

    (b15002_011e + b15002_028e) / b15002_001e AS percent_education_high_school,
    (b15002_014e + b15002_031e) / b15002_001e AS percent_education_associate,
    (b15002_015e + b15002_032e) / b15002_001e AS percent_education_bachelor,
    (
        b15002_016e + b15002_017e + b15002_018e
        + b15002_033e + b15002_034e + b15002_035e
    ) / b15002_001e AS percent_education_graduate,

    -- Poverty, employment, and income
    b17001_001e AS count_income_total_poverty_level,
    b17001_002e AS count_income_below_poverty_level,
    b17001_031e AS count_income_above_poverty_level,

    b17001_002e / b17001_001e AS percent_income_below_poverty_level,
    b17001_031e / b17001_001e AS percent_income_above_poverty_level,

    b19013_001e AS median_income_household_past_year,
    b19301_001e AS median_income_per_capita_past_year,
    b22003_002e AS count_income_household_received_snap_past_year,
    b22003_002e
    / b22003_001e AS percent_income_household_received_snap_past_year,

    -- Only for population 16 and older
    b23025_003e AS count_employment_in_civilian_labor_force,
    b23025_005e AS count_employment_unemployed,
    b23025_007e AS count_employment_not_in_labor_force,
    b23025_005e / b23025_003e AS percent_employment_unemployed,

    -- Household stats
    b25003_001e AS count_household_total_occupied,
    b25003_002e AS count_household_owner_occupied,
    b25003_003e AS count_household_renter_occupied,
    NULLIF(b25037_001e, 0.0) AS median_household_total_occupied_year_built,
    NULLIF(b25037_002e, 0.0) AS median_household_owner_occupied_year_built,
    NULLIF(b25037_003e, 0.0) AS median_household_renter_occupied_year_built,
    b25064_001e AS median_household_renter_occupied_gross_rent,
    b25077_001e AS median_household_owner_occupied_value,

    b25003_002e / b25003_001e AS percent_household_owner_occupied,
    b25003_003e / b25003_001e AS percent_household_renter_occupied,

    -- Selected conditions, including:
    --   incomplete plumbing or kitchens
    --   overcrowding
    --   30% or more of the household income spent on rent or monthly costs
    b25123_003e AS count_household_owner_occupied_1_sel_cond,
    b25123_004e AS count_household_owner_occupied_2_sel_cond,
    b25123_005e AS count_household_owner_occupied_3_sel_cond,
    b25123_006e AS count_household_owner_occupied_4_sel_cond,
    b25123_009e AS count_household_renter_occupied_1_sel_cond,
    b25123_010e AS count_household_renter_occupied_2_sel_cond,
    b25123_011e AS count_household_renter_occupied_3_sel_cond,
    b25123_012e AS count_household_renter_occupied_4_sel_cond,
    (
        b25123_003e + b25123_004e + b25123_005e + b25123_006e
        + b25123_009e + b25123_010e + b25123_011e + b25123_012e
    ) / b25123_001e AS percent_household_total_occupied_w_sel_cond,
    (b25123_003e + b25123_004e + b25123_005e + b25123_006e)
    / b25123_002e AS percent_household_owner_occupied_w_sel_cond,
    (b25123_009e + b25123_010e + b25123_011e + b25123_012e)
    / b25123_008e AS percent_household_renter_occupied_w_sel_cond

FROM acs5_forward_fill
