-- View to collect PIN-level exemptions from exdet. *Only* PINs with exemptions
-- are included in this view.
--
-- This view is long, meaning a PIN can have multiple rows in a year, one for
-- each exemption that the PIN has in that year. For a wide version of the view,
-- see `default.vw_pin_exe`.

-- Gather PIN-level exemptions from the exdet table. Exdet contains a row for
-- each exemption applied to a PIN in a year. There are some duplicates due
-- to data errors, so we start with a raw CTE that we can then aggregate in
-- the final view
WITH exe_raw AS (
    SELECT
        det.parid AS pin,
        det.taxyr AS year,
        CASE WHEN det.excode IN ('DP', 'C-DP', 'DPHE') THEN 'exe_disabled'
            WHEN det.excode IN ('SF', 'C-SF') THEN 'exe_freeze'
            WHEN det.excode IN ('HO', 'C-HO') THEN 'exe_homeowner'
            WHEN
                det.excode IN ('LT', 'C-LT', 'LT1', 'LT2')
                THEN 'exe_longtime_homeowner'
            WHEN det.excode IN ('SR', 'C-SR', 'SC', 'SCHE')
                THEN 'exe_senior'
            WHEN det.excode = 'MUNI' THEN 'exe_muni_built'
            WHEN
                det.excode IN ('DV1', 'C-DV1', 'DV0', 'C-DV0', 'DV-1')
                THEN 'exe_vet_dis_lt50'
            WHEN det.excode IN ('DV2', 'C-DV2', 'DV-2') THEN 'exe_vet_dis_50_69'
            WHEN det.excode IN ('DV3', 'DV3-M', 'DV-3') THEN 'exe_vet_dis_ge70'
            WHEN det.excode IN ('DV4', 'DV4-M', 'DV-4') THEN 'exe_vet_dis_100'
            WHEN
                det.excode IN ('RTV', 'C-RTV', 'RDV1', 'RV1', 'RDV2')
                THEN 'exe_vet_returning'
            WHEN det.excode = 'WW2' THEN 'exe_wwii'
        END AS exemption_type,
        CAST(det.apother AS INT) AS exemption_amount,
        COALESCE(UPPER(SUBSTR(admn.user126, 1, 1)) = 'Y', FALSE) AS is_cofe,
        DATE_PARSE(SUBSTR(admn.udate9, 1, 10), '%Y-%m-%d') AS cofe_date
    FROM {{ source('iasworld', 'exdet') }} AS det
    -- Ensure only approved exemptions are pulled
    INNER JOIN {{ source('iasworld', 'exadmn') }} AS admn
        ON det.parid = admn.parid
        AND det.caseno = admn.caseno
        AND det.taxyr = admn.taxyr
        AND det.excode = admn.excode
        AND admn.cur = 'Y'
        AND admn.deactivat IS NULL
        AND admn.exstat = 'A'
    -- Ensure we are only pulling valid exemption codes
    INNER JOIN {{ source('iasworld', 'excode') }} AS code
        ON det.excode = code.excode
        AND det.taxyr = code.taxyr
        AND code.cur = 'Y'
        AND code.deactivat IS NULL
    WHERE det.deactivat IS NULL
        AND det.cur = 'Y'
        -- There are some exemptions with missing amounts in the data.
        -- These are probably data errors, but we should filter them out
        -- regardless
        AND COALESCE(CAST(det.apother AS INT), 0) > 0
),

-- There are currently some unexpected dupes in the exemption data right
-- now. We eventually expect these to get resolved by the data owners, but
-- in the meantime, we deduplicate based on PIN/year/CofE/exemption type, and
-- prioritize exemptions in those groups that have the latest CofE date and/or
-- have the highest exemption amount
exe_group_ranked AS (
    SELECT
        pin,
        year,
        exemption_type,
        exemption_amount,
        is_cofe,
        cofe_date,
        ROW_NUMBER() OVER (
            PARTITION BY pin, year, exemption_type, is_cofe
            ORDER BY
                -- If multiple exemptions in a group are CofEs, choose the most
                -- recent one
                cofe_date DESC NULLS LAST,
                -- If multiple exemptions have the same CofE date (or no date),
                -- choose the one with the highest exemption amount. This
                -- choice is arbitrary but it makes the deduplication
                -- deterministic
                exemption_amount DESC NULLS LAST
        ) AS rank
    FROM exe_raw
)

SELECT
    pin,
    year,
    exemption_type,
    exemption_amount,
    is_cofe,
    cofe_date
FROM exe_group_ranked
WHERE rank = 1
