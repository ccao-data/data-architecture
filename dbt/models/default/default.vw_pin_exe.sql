-- View to collect pin-level exemptions
{% set exes = [
    'exe_disabled', 'exe_freeze', 'exe_homeowner',
    'exe_longtime_homeowner', 'exe_senior', 'exe_muni_built', 'exe_vet_dis_lt50',
    'exe_vet_dis_50_69', 'exe_vet_dis_ge70', 'exe_vet_dis_100',
    'exe_vet_returning', 'exe_wwii'
] %}

WITH long AS (
    SELECT
        det.parid AS pin,
        det.taxyr AS year,
        CASE WHEN det.excode IN ('DP', 'C-DP', 'DPHE') THEN 'exe_disabled'
            WHEN det.excode IN ('SF', 'C-SF') THEN 'exe_freeze'
            WHEN det.excode IN ('HO', 'C-HO') THEN 'exe_homeowner'
            WHEN
                det.excode IN ('LT', 'C-LT', 'LT1', 'LT2')
                THEN 'exe_longtime_homeowner'
            WHEN det.excode IN ('SR', 'C-SR', 'SC', 'SCHEM', 'SCHE', 'SCHE')
                THEN 'exe_senior'
            WHEN det.excode = 'MUNI' THEN 'exe_muni_built'
            WHEN det.excode IN ('DV1', 'C-DV1') THEN 'exe_vet_dis_lt50'
            WHEN det.excode IN ('DV2', 'C-DV2') THEN 'exe_vet_dis_50_69'
            WHEN det.excode IN ('DV3', 'DV3-M', 'DV30') THEN 'exe_vet_dis_ge70'
            WHEN det.excode IN ('DV4', 'DV4-M') THEN 'exe_vet_dis_100'
            WHEN det.excode IN ('RTV', 'C-RTV') THEN 'exe_vet_returning'
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
)

SELECT
    pin,
    year,
{%- for exe_ in exes %}
    CAST(SUM(CASE
        WHEN ptax_exe = '{{ exe_ }}' THEN exemption_amount ELSE 0
        END) AS INT)
        AS {{ exe_ }}{%- if not loop.last -%},{%- endif -%}
{% endfor %}
FROM long
GROUP BY pin, year
