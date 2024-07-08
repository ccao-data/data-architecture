-- A view to properly aggregate land square footage at the PIN level. Parcels
-- can have multiple land lines that are sometimes summed or ignored.
WITH total_allocpct AS (
    SELECT
        land.parid,
        land.taxyr,
        land.lline,
        COUNT(*)
            OVER (PARTITION BY land.parid, land.taxyr)
            AS num_landlines,
        SUM(land.sf) OVER (PARTITION BY land.parid, land.taxyr) AS sf_sum,
        -- We explicitly want to take the top line land sf if we're only taking
        -- one line.
        FIRST_VALUE(land.sf)
            OVER (PARTITION BY land.parid, land.taxyr ORDER BY land.lline)
            AS sf_top,
        SUM(CASE WHEN land.allocpct IS NULL THEN 0 ELSE 1 END)
            OVER (PARTITION BY land.parid, land.taxyr)
            AS non_null_allocpct,
        -- Split class indicator can override the rest of the allocpctence
        -- factor logic
        COALESCE(land.infl1 = '35', FALSE) AS split_class,
        MAX(land.sf) OVER (PARTITION BY land.parid, land.taxyr) AS max_sf,
        MIN(land.sf) OVER (PARTITION BY land.parid, land.taxyr) AS min_sf,
        -- When the first landline for a pin is deactived we should take the
        -- minimum value of lline as the top line.
        MIN(land.lline) OVER (PARTITION BY land.parid, land.taxyr) AS top_line
    FROM {{ source('iasworld', 'land') }} AS land
    WHERE
        land.cur = 'Y'
        AND land.deactivat IS NULL
)

SELECT
    total_allocpct.parid AS pin,
    total_allocpct.taxyr AS year,
    total_allocpct.num_landlines,
    CASE
        -- When there are multiple non-null values for allocpct across land
        -- lines and all sf values are the same, we choose the topline land sf,
        -- otherwise we sum land sf.
        WHEN
            (
                total_allocpct.non_null_allocpct > 1
                AND total_allocpct.max_sf = total_allocpct.min_sf
            )
            OR total_allocpct.split_class
            THEN total_allocpct.sf_top
        ELSE total_allocpct.sf_sum
    END AS sf
FROM total_allocpct
WHERE total_allocpct.lline = total_allocpct.top_line
