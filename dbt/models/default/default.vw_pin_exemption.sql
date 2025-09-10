-- View to collect pin-level exemptions
SELECT
    det.parid AS pin,
    det.taxyr AS year,
    det.excode AS exemption_code,
    code.descr AS exemption_description,
    CASE WHEN det.excode LIKE '%DP%' THEN 'exe_disabled'
        WHEN det.excode LIKE '%SF%' THEN 'exe_freeze'
        WHEN det.excode LIKE '%HO%' THEN 'exe_homeowner'
        WHEN det.excode LIKE '%LT%' THEN 'exe_longtime_homeowner'
        WHEN det.excode LIKE '%SR%' THEN 'exe_senior'
        WHEN det.excode = 'MUNI' THEN 'exe_muni_built'
        WHEN det.excode LIKE '%DV1%' THEN 'exe_vet_dis_lt50'
        WHEN det.excode LIKE '%DV2%' THEN 'exe_vet_dis_50_69'
        WHEN det.excode LIKE '%DV3%' THEN 'exe_vet_dis_ge70'
        WHEN det.excode LIKE '%DV4%' THEN 'exe_vet_dis_100'
        WHEN det.excode LIKE '%RTV%' THEN 'exe_vet_returning'
        WHEN det.excode = 'WW2' THEN 'exe_wwii'
    END AS ptax_exe,
    CAST(det.apother AS INT) AS exemption_amount
FROM {{ source('iasworld', 'exdet') }} AS det
LEFT JOIN {{ source('iasworld', 'excode') }} AS code
    ON det.excode = code.excode
    AND det.taxyr = code.taxyr
    AND code.cur = 'Y'
    AND code.deactivat IS NULL
WHERE det.deactivat IS NULL
    AND det.cur = 'Y'
