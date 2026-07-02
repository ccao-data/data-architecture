-- Goal of this view is to catch condo PINs with suspicious characteristics for
-- review.

-- Constrct all the flags to detect units for review
WITH flags AS (
    SELECT
        pin,
        pin10,
        year,
        township_code,
        char_half_baths,
        char_full_baths,
        char_bedrooms,
        char_unit_sf,
        char_building_sf,
        char_building_pins
        - char_building_non_units AS char_building_livable_units,
        char_yrblt,
        COALESCE(char_half_baths > 2, FALSE) AS flag_half_baths,
        COALESCE(char_full_baths > 4, FALSE) AS flag_full_baths,
        COALESCE(char_bedrooms > 4, FALSE) AS flag_bedrooms,
        COALESCE(
            char_unit_sf NOT BETWEEN 500 AND 3000
            OR char_unit_sf > char_building_sf,
            FALSE
        ) AS flag_unit_sf,
        COALESCE(
            char_building_sf NOT BETWEEN 2500 AND 500000,
            FALSE
        ) AS flag_building_sf,
        COALESCE(
            SUM(char_unit_sf) OVER (PARTITION BY pin10, year)
            > char_building_sf,
            FALSE
        ) AS flag_unit_sf_sum,
        -- Count the number of different values for a building's square footage,
        -- including NULLSs
        COALESCE(
            DENSE_RANK()
                OVER (
                    PARTITION BY pin10, year
                    ORDER BY COALESCE(char_building_sf, -1) ASC
                )
            + DENSE_RANK()
                OVER (
                    PARTITION BY pin10, year
                    ORDER BY COALESCE(char_building_sf, -1) DESC
                )
            - 1 > 1,
            FALSE
        ) AS flag_diff_bldg_sf,
        COALESCE(char_building_pins - char_building_non_units = 0, FALSE)
            AS flag_no_livable_units,
        COALESCE(char_yrblt NOT BETWEEN 1880 AND YEAR(CURRENT_DATE), FALSE)
            AS flag_yrblt
    FROM {{ ref('default.vw_pin_condo_char') }}
    WHERE year = (SELECT MAX(year) FROM {{ ref('default.vw_pin_condo_char') }})
),

-- Gather flags together for cummulative output
comments AS (
    SELECT
        *,
        NULLIF(CONCAT_WS(
            ', ',
            CASE WHEN flag_half_baths THEN 'More than 3 Half Baths' END,
            CASE WHEN flag_full_baths THEN 'More than 4 Full Baths' END,
            CASE WHEN flag_bedrooms THEN 'More than 4 Bedrooms' END,
            CASE
                WHEN flag_unit_sf THEN 'Unit SF not between 200 and 10,000'
            END,
            CASE
                WHEN
                    flag_building_sf
                    THEN 'Building SF not between 2,500 and 500,000'
            END,
            CASE
                WHEN flag_unit_sf_sum THEN 'Combined Unit SF for all units in PIN10 exceeds Building SF'
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
