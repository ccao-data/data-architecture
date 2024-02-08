/*
View containing cleaned, filled data for residential condo modeling. Missing
data is filled with the following steps:

1. All historical data is filled FORWARD in time, i.e. data from 2020 fills
   2021 as long as the data isn't something which frequently changes
2. Current data is filled BACKWARD to account for missing historical data.
   Again, this is only for things unlikely to change

This view is "materialized" (made into a table) daily in order to improve
query performance and reduce data queried by Athena. The materialization
is triggered by sqoop-bot (runs after Sqoop grabs iasWorld data)
*/
{{
    config(
        materialized='table',
        partitioned_by=['year'],
        bucketed_by=['meta_pin'],
        bucket_count=1
    )
}}

WITH uni AS (
    SELECT
        vpsi.*,
        -- Exemptions data is usually missing for the 1 or 2 years prior
        -- to the lien date, so we need to fill missing values w/ down up fill
        -- This assumes that people currently receiving exemptions keep them
        COALESCE(
            vpsi.ccao_is_active_exe_homeowner,
            LAG(vpsi.ccao_is_active_exe_homeowner)
                IGNORE NULLS
                OVER (PARTITION BY vpsi.meta_pin ORDER BY vpsi.meta_year)
        ) AS ccao_is_active_exe_homeowner,
        COALESCE(
            vpsi.ccao_n_years_exe_homeowner,
            LAG(vpsi.ccao_n_years_exe_homeowner)
                IGNORE NULLS
                OVER (PARTITION BY vpsi.meta_pin ORDER BY vpsi.meta_year)
        ) AS ccao_n_years_exe_homeowner
    FROM {{ ref('model.vw_pin_shared_input') }} AS vpsi
    WHERE vpsi.meta_class IN ('299', '399')
),

sqft_percentiles AS (
    SELECT
        ch.year,
        leg.user1 AS township_code,
        CAST(APPROX_PERCENTILE(ch.char_land_sf, 0.95) AS INT)
            AS char_land_sf_95_percentile
    FROM {{ ref('default.vw_pin_condo_char') }} AS ch
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
        ON ch.pin = leg.parid
        AND ch.year = leg.taxyr
        AND leg.cur = 'Y'
        AND leg.deactivat IS NULL
    GROUP BY ch.year, leg.user1
)

SELECT
    uni.*,

    CASE
        WHEN ch.is_parking_space = TRUE
            OR ch.is_common_area = TRUE THEN 'NONLIVABLE'
        ELSE 'CONDO'
    END AS meta_modeling_group,

    -- Proration fields. Buildings can be split over multiple PINs, with each
    -- PIN owning a percentage of a building. For residential buildings, if
    -- a proration rate is NULL or 0, it's almost always actually 1
    ch.tieback_key_pin AS meta_tieback_key_pin,
    ch.tieback_proration_rate AS meta_tieback_proration_rate,
    COALESCE(ch.tieback_proration_rate < 1.0, FALSE) AS ind_pin_is_prorated,
    ch.card_proration_rate AS meta_card_proration_rate,

    -- Multicard/multi-landline related fields. Each PIN can have more than
    -- one improvement/card AND/OR more than one attached landline
    ch.card AS meta_card_num,
    ch.lline AS meta_lline_num,
    ch.pin_is_multilline AS ind_pin_is_multilline,
    ch.pin_num_lline AS meta_pin_num_lline,
    ch.pin_is_multiland AS ind_pin_is_multiland,
    ch.pin_num_landlines AS meta_pin_num_landlines,
    ch.cdu AS meta_cdu,

    -- Property characteristics from iasWorld
    ch.char_yrblt,
    NULLIF(ch.char_land_sf, 0.0) AS char_land_sf,
    ch.char_building_pins,
    ch.char_building_pins - ch.char_building_non_units AS char_building_units,
    ch.char_building_non_units,
    ch.bldg_is_mixed_use AS char_bldg_is_mixed_use,

    -- Property characteristics from MLS/valuations
    ch.char_building_sf,
    ch.char_unit_sf,
    ch.char_bedrooms,
    ch.char_half_baths,
    ch.char_full_baths,

    -- Land and lot size indicators
    sp.char_land_sf_95_percentile,
    COALESCE(
        ch.char_land_sf >= sp.char_land_sf_95_percentile,
        FALSE
    ) AS ind_land_gte_95_percentile,
    uni.meta_year AS year
FROM uni
LEFT JOIN {{ ref('default.vw_pin_condo_char') }} AS ch
    ON uni.meta_pin = ch.pin
    AND uni.meta_year = ch.year
LEFT JOIN sqft_percentiles AS sp
    ON uni.meta_year = sp.year
    AND uni.meta_township_code = sp.township_code
