-- CTE that adds lagged sums of total AV by geography
WITH muni_progress AS (

    SELECT
        *,
        LAG(tot_sum)
            OVER (
                PARTITION BY
                    geo_id
                ORDER BY year ASC, stage_num ASC
            )
            AS tot_sum_lag
    FROM {{ ref('reporting.vw_assessment_progress') }}
    WHERE geo_type = 'Municipality'

)

-- Use lagged values to calculate change in total AV by geography
SELECT
    *,
    CASE WHEN tot_sum_lag IN (0, NULL) THEN NULL ELSE
            CAST(tot_sum - tot_sum_lag AS DOUBLE)
            / CAST(tot_sum_lag AS DOUBLE)
    END AS delta_pct_av
FROM muni_progress
