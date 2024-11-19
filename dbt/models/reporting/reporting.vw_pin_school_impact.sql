-- Script that returns the 10 PINs with the highest AV per school district
-- taxing agency, by year.
WITH ranking AS (
    SELECT
        info.agency_name,
        info.agency_num,
        RANK() OVER (
            PARTITION BY
                info.agency_name,
                pin.year
            ORDER BY pin.av_board DESC
        ) AS av_board_rank,
        pin.pin,
        pin.class,
        pin.av_mailed,
        pin.av_certified,
        pin.av_board,
        CAST((pin.av_board * fact.eq_factor_final) - (
            pin.exe_homeowner
            + pin.exe_senior
            + pin.exe_freeze
            + pin.exe_longtime_homeowner
            + pin.exe_disabled
            + pin.exe_vet_returning
            + pin.exe_vet_dis_lt50
            + pin.exe_vet_dis_50_69
            + pin.exe_vet_dis_ge70
            + pin.exe_abate
        ) AS INT) AS taxable_eav,
        pin.tax_bill_total,
        RANK() OVER (
            PARTITION BY
                info.agency_name,
                pin.year
            ORDER BY pin.tax_bill_total DESC
        ) AS bill_rank,
        code.tax_code_num,
        code.tax_code_rate,
        pin.year
    FROM {{ source('tax', 'pin') }} AS pin
    LEFT JOIN {{ source('tax', 'tax_code') }} AS code
        ON pin.tax_code_num = code.tax_code_num
        AND pin.year = code.year
    INNER JOIN
        {{ source('tax', 'agency_info') }} AS info
        ON code.agency_num = info.agency_num
    LEFT JOIN {{ source('tax', 'eq_factor') }} AS fact ON pin.year = fact.year
    WHERE info.major_type = 'SCHOOL'
        -- Class 0 PINs have 0 AV but can lead to huge ties if there are many in
        -- a district with few PINs
        AND pin.class != '0'
),

-- We need to use array_agg for this CTE since some parcels can be in multiple
-- SSAs
tif_ssa AS (
    SELECT
        pin.pin,
        pin.year,
        info.minor_type,
        ARRAY_AGG(info.agency_name) AS agency_name
    FROM {{ source('tax', 'pin') }} AS pin
    LEFT JOIN {{ source('tax', 'tax_code') }} AS code
        ON pin.tax_code_num = code.tax_code_num
        AND pin.year = code.year
    INNER JOIN
        {{ source('tax', 'agency_info') }} AS info
        ON code.agency_num = info.agency_num
    GROUP BY pin.pin, pin.year, info.minor_type
)

SELECT
    ranking.*,
    tif.agency_name AS tif,
    ssa.agency_name AS ssa
FROM ranking
LEFT JOIN tif_ssa AS tif
    ON ranking.pin = tif.pin
    AND ranking.year = tif.year
    AND tif.minor_type = 'TIF'
LEFT JOIN tif_ssa AS ssa
    ON ranking.pin = ssa.pin
    AND ranking.year = ssa.year
    AND ssa.minor_type = 'SSA'
WHERE ranking.av_board_rank <= 10
ORDER BY
    ranking.year,
    ranking.agency_num,
    ranking.av_board_rank
