-- A view to properly aggregate land square footage (sf) at the PIN level.
-- Parcels can have multiple land lines in iasworld.land, and the correct
-- aggregation method depends on whether those lines represent independent
-- land areas (→ sum) or portions of the same land area (→ top line only).
--
-- Aggregation logic summary:
--
--   1. SPLIT CLASS (infl1 = '35'): These parcels have their land value
--      split across multiple lines, each representing a share of the same
--      total area. We take the top line sf only (sf_top), not the sum.
--
--   2. MULTIPLE NON-NULL INFLU/ALLOCPCT WITH IDENTICAL sf VALUES: When
--      more than one land line carries an influence/allocation factor AND
--      all lines share the same sf, the repeated sf values represent the
--      same land area measured multiple times. We take sf_top to avoid
--      double-counting.
--
--   3. ALL OTHER CASES: Land lines represent distinct portions of the
--      parcel. We sum all line sf values (sf_sum).
--
-- The "influence/allocation" field used to detect case 2 changed in 2024:
--   - Prior to 2024: iasworld.land.influ
--   - 2024 and later: iasworld.land.allocpct  (a new field that supports
--     more granular prorations, e.g. fractional percentages needed for
--     condos, rather than whole-number percentages in influ)
--
-- Only active, non-deactivated land lines are included (cur = 'Y' and
-- deactivat IS NULL). The "top line" is the land line with the lowest lline
-- value among active lines, which may not be lline = 1 if line 1 was
-- deactivated.
WITH total_influ AS (
    SELECT
        land.parid,
        land.taxyr,
        land.lline,

        -- Total number of active land lines for this PIN/year combination.
        COUNT(*)
            OVER (PARTITION BY land.parid, land.taxyr)
            AS num_landlines,

        -- Sum of sf across all active land lines. Used when lines represent
        -- distinct land areas that should be added together.
        SUM(land.sf) OVER (PARTITION BY land.parid, land.taxyr) AS sf_sum,

        -- sf of the top (lowest lline) active land line. Used when multiple
        -- lines describe the same land area and summing would overcount.
        FIRST_VALUE(land.sf)
            OVER (PARTITION BY land.parid, land.taxyr ORDER BY land.lline)
            AS sf_top,

        -- Count of land lines that carry a non-null influence/allocation
        -- factor for this PIN/year. A value > 1 means multiple lines have
        -- been assigned an explicit influence or allocation, which (combined
        -- with identical sf values) signals that the lines represent the same
        -- land area rather than additive areas.
        --
        -- The relevant field changed in 2024:
        --   < 2024: influ   — whole-number influence factor
        --   >= 2024: allocpct — fractional allocation percentage, introduced
        --            to support condos and other split-class properties that
        --            require sub-1% precision
        SUM(
            CASE
                WHEN
                    (
                        (land.influ IS NULL AND land.taxyr < '2024')
                        OR (land.allocpct IS NULL AND land.taxyr >= '2024')
                    )
                    THEN 0
                ELSE 1
            END
        )
            OVER (PARTITION BY land.parid, land.taxyr)
            AS non_null_influ,

        -- Parcels with infl1 = '35' are "split class" properties whose land
        -- value has been explicitly split across lines. For these, always use
        -- sf_top regardless of non_null_influ or sf uniformity, because each
        -- line's sf already reflects only that line's share of the parcel.
        COALESCE(land.infl1 = '35', FALSE) AS split_class,

        -- Max and min sf across all active lines, used together to check
        -- whether all land lines report the same square footage (max = min).
        -- When combined with non_null_influ > 1, uniform sf is the signal
        -- that lines are duplicative rather than additive.
        MAX(land.sf) OVER (PARTITION BY land.parid, land.taxyr) AS max_sf,
        MIN(land.sf) OVER (PARTITION BY land.parid, land.taxyr) AS min_sf,

        -- The lowest active lline value for this PIN/year. This is our
        -- "top line" anchor. We filter to this line in the outer SELECT so
        -- that each PIN/year produces exactly one output row, and we use
        -- sf_top (FIRST_VALUE ordered by lline) consistently with it.
        -- Note: lline 1 may have been deactivated, so top_line is not
        -- always 1.
        MIN(land.lline) OVER (PARTITION BY land.parid, land.taxyr) AS top_line
    FROM {{ source('iasworld', 'land') }} AS land
    WHERE
        land.cur = 'Y'
        AND land.deactivat IS NULL
)

SELECT
    total_influ.parid AS pin,
    total_influ.taxyr AS year,
    total_influ.num_landlines,
    -- Final sf aggregation. See the top-of-file comment for full logic.
    --
    --   • sf_top  → split-class parcels, or parcels where multiple lines
    --               carry an influence/allocation factor and all lines have
    --               the same sf (indicating duplicative, not additive, lines)
    --   • sf_sum  → all other parcels, where lines represent distinct land
    --               areas that should be summed
    CASE
        WHEN
            (
                total_influ.non_null_influ > 1
                AND total_influ.max_sf = total_influ.min_sf
            )
            OR total_influ.split_class
            THEN total_influ.sf_top
        ELSE total_influ.sf_sum
    END AS sf
FROM total_influ
-- Keep only the top (lowest active lline) row per PIN/year so the output
-- is one record per parcel per year.
WHERE total_influ.lline = total_influ.top_line
