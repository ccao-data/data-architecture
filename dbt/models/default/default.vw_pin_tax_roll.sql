-- View to collect pin-level exemptions and taxable EAV
{% set exes = [
    'exe_disabled', 'exe_freeze', 'exe_homeowner',
    'exe_longtime_homeowner', 'exe_senior', 'exe_muni_built', 'exe_vet_dis_lt50',
    'exe_vet_dis_50_69', 'exe_vet_dis_ge70', 'exe_vet_dis_100',
    'exe_vet_returning', 'exe_wwii'
] %}

-- pin-level exemptions from the exdet table, universe of pins is pardat
WITH long AS (
    SELECT
        par.parid AS pin,
        par.taxyr AS year,
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
        END AS ptax_exe,
        CAST(det.apother AS INT) AS exemption_amount
    FROM {{ source('iasworld', 'pardat') }} AS par
    LEFT JOIN {{ source('iasworld', 'exdet') }} AS det
        ON par.parid = det.parid
        AND par.taxyr = det.taxyr
        AND det.cur = 'Y'
        AND det.deactivat IS NULL
    WHERE par.deactivat IS NULL
        AND par.cur = 'Y'
        AND par.taxyr BETWEEN '2021' AND '2024'
),

-- pin-level taxable AV and final EAV by stage
asmt AS (
    SELECT
        parid AS pin,
        taxyr AS year,
        MAX(
            CASE WHEN
                    procname = 'CCAOVALUE'
                    AND COALESCE(tot30, 0) + COALESCE(tot31, 0) > 0
                    THEN tot32
                WHEN
                    procname = 'CCAOVALUE'
                    AND COALESCE(tot30, 0) + COALESCE(tot31, 0) = 0
                    THEN tot13
            END
        ) AS mailed_taxable_av,
        MAX(
            CASE WHEN
                    procname = 'CCAOFINAL'
                    AND COALESCE(tot30, 0) + COALESCE(tot31, 0) > 0
                    THEN tot32
                WHEN
                    procname = 'CCAOFINAL'
                    AND COALESCE(tot30, 0) + COALESCE(tot31, 0) = 0
                    THEN tot13
            END
        ) AS certified_taxable_av,
        MAX(
            CASE WHEN
                    procname = 'BORVALUE'
                    AND COALESCE(tot30, 0) + COALESCE(tot31, 0) > 0
                    THEN tot32
                WHEN
                    procname = 'BORVALUE'
                    AND COALESCE(tot30, 0) + COALESCE(tot31, 0) = 0
                    THEN tot13
            END
        ) AS board_taxable_av,
        MAX(CASE WHEN procname = 'CCAOVALUE' THEN tot51 END) AS mailed_eav,
        MAX(CASE WHEN procname = 'CCAOFINAL' THEN tot51 END)
            AS certified_eav,
        MAX(CASE WHEN procname = 'BORVALUE' THEN tot51 END) AS board_eav
    FROM {{ source('iasworld', 'asmt_all') }}
    WHERE rolltype != 'RR'
        AND deactivat IS NULL
        AND procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND valclass IS NULL
        -- Class 999 are test pins
        AND class NOT IN ('999')
    GROUP BY parid, taxyr

),

-- Widen the exemptions
wide AS (
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
)

-- Join exemptions and EAVs
SELECT
    wide.*,
    asmt.mailed_taxable_av,
    asmt.mailed_eav,
    asmt.certified_taxable_av,
    asmt.certified_eav,
    asmt.board_taxable_av,
    asmt.board_eav
FROM wide
LEFT JOIN asmt ON wide.pin = asmt.pin AND wide.year = asmt.year
