-- Goal of this view is to catch condo PINs with suspicious characteristics for
-- review.

-- Grab township and triad names
WITH towns AS (
    SELECT DISTINCT
        township_code,
        township_name,
        triad_name
    FROM {{ source('spatial', 'township') }}
),

-- Construct all the flags to detect units for review
flags AS (
    SELECT
        {{ insert_hyphens("chars.pin", 2, 4, 7, 10) }} AS pin,
        {{ insert_hyphens("chars.pin10", 2, 4, 7) }} AS pin10,
        chars.year,
        towns.township_name,
        towns.triad_name,
        chars.char_half_baths,
        chars.char_full_baths,
        chars.char_bedrooms,
        chars.char_unit_sf,
        chars.char_building_sf,
        chars.char_building_pins
        - chars.char_building_non_units AS char_building_livable_units,
        chars.char_yrblt,
        COALESCE(chars.char_half_baths > 2, FALSE) AS flag_half_baths,
        COALESCE(chars.char_full_baths > 4, FALSE) AS flag_full_baths,
        COALESCE(chars.char_bedrooms > 4, FALSE) AS flag_bedrooms,
        COALESCE(
            chars.char_unit_sf NOT BETWEEN 500 AND 3000
            OR chars.char_unit_sf > chars.char_building_sf,
            FALSE
        ) AS flag_unit_sf,
        COALESCE(chars.char_unit_sf > chars.char_building_sf, FALSE)
            AS flag_unit_sf_gt_bldg,
        COALESCE(
            chars.char_building_sf NOT BETWEEN 2500 AND 500000,
            FALSE
        ) AS flag_building_sf,
        COALESCE(
            SUM(chars.char_unit_sf) OVER (PARTITION BY chars.pin10, chars.year)
            > chars.char_building_sf,
            FALSE
        ) AS flag_unit_sf_sum,
        -- Count the number of different values for a building's square footage,
        -- including NULLSs
        COALESCE(
            DENSE_RANK()
                OVER (
                    PARTITION BY chars.pin10, chars.year
                    ORDER BY COALESCE(chars.char_building_sf, -1) ASC
                )
            + DENSE_RANK()
                OVER (
                    PARTITION BY chars.pin10, chars.year
                    ORDER BY COALESCE(chars.char_building_sf, -1) DESC
                )
            - 1 > 1,
            FALSE
        ) AS flag_diff_bldg_sf,
        COALESCE(
            chars.char_building_pins - chars.char_building_non_units = 0, FALSE
        )
            AS flag_no_livable_units,
        COALESCE(
            chars.char_yrblt NOT BETWEEN 1880 AND YEAR(CURRENT_DATE), FALSE
        )
            AS flag_yrblt
    FROM {{ ref('default.vw_pin_condo_char') }} AS chars
    LEFT JOIN towns
        ON chars.township_code = towns.township_code
),

-- Gather flags together for cumulative output
comments AS (
    SELECT
        *,
        NULLIF(CONCAT_WS(
            ', ',
            CASE WHEN flag_half_baths THEN 'More than 2 Half Baths' END,
            CASE WHEN flag_full_baths THEN 'More than 4 Full Baths' END,
            CASE WHEN flag_bedrooms THEN 'More than 4 Bedrooms' END,
            CASE
                WHEN flag_unit_sf THEN 'Unit SF not between 300 and 5,000'
            END,
            CASE
                WHEN flag_unit_sf_gt_bldg THEN 'Unit SF exceeds building SF'
            END,
            CASE
                WHEN
                    flag_building_sf
                    THEN 'Building SF not between 2,500 and 500,000'
            END,
            CASE
                WHEN
                    flag_unit_sf_sum
                    THEN 'Combined Unit SF for all units in PIN10 exceeds Building SF' -- noqa: LT05
            END,
            CASE
                WHEN
                    flag_diff_bldg_sf
                    THEN 'Multiple Building SF values for same PIN10'
            END,
            CASE
                WHEN flag_no_livable_units THEN 'Building has no livable units'
            END,
            CASE
                WHEN
                    flag_yrblt
                    THEN CONCAT(
                        'Year Built not between 1880 and ',
                        CAST(YEAR(CURRENT_DATE) AS VARCHAR)
                    )
            END
        ), '') AS flag_comments
    FROM flags
)

SELECT * FROM comments WHERE flag_comments IS NOT NULL
