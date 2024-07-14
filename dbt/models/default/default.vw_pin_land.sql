-- A view to properly aggregate land square footage at the PIN level. Parcels
-- can have multiple land lines that are sometimes summed or ignored.
WITH total_influ AS (
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
        SUM(
            CASE
                WHEN
                    (
                        /*The new field we got in 2024 for land prorations,
                        ALLOCPCT. This field reduces the land value by the
                        percent entered and can provide more granular
                        prorations (needed for condos) as it can go further
                        than whole percentages.

                        So, ahead of this assessment, we updated the split
                        class properties to move the percentage attributable to
                        each from the Influ field to the new ALLOCPCT field.*/
                        (land.influ IS NULL AND land.taxyr < '2024')
                        OR (land.allocpct IS NULL AND land.taxyr >= '2024')
                    )
                    THEN 0
                ELSE 1
            END
        )
            OVER (PARTITION BY land.parid, land.taxyr)
            AS non_null_influ,
        -- Split class indicator can override the rest of the influence
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
    total_influ.parid AS pin,
    total_influ.taxyr AS year,
    total_influ.num_landlines,
    CASE
        -- When there are multiple non-null values for influ across land lines
        -- and all sf values are the same, we choose the topline land sf,
        -- otherwise we sum land sf.
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
WHERE total_influ.lline = total_influ.top_line
