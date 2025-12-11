-- View to collect pin-level exemptions and taxable EAV.
-- Unlike default.vw_pin_exe, all PINs, regardless of exemptions, are included.

-- PIN-level taxable AV and final EAV by stage. We use arbitrary aggregation
-- because there are known duplicates in the asmt_all table.
WITH asmt AS (
    SELECT
        parid AS pin,
        taxyr AS year,
        ARBITRARY(CASE WHEN procname = 'CCAOVALUE' THEN tot13 END)
            AS mailed_taxable_av,
        ARBITRARY(CASE WHEN procname = 'CCAOFINAL' THEN tot13 END)
            AS certified_taxable_av,
        ARBITRARY(CASE WHEN procname = 'BORVALUE' THEN tot13 END)
            AS board_taxable_av,
        ARBITRARY(CASE WHEN procname = 'FINALEAV' THEN tot51 END) AS final_eav
    FROM {{ source('iasworld', 'asmt_all') }}
    WHERE rolltype != 'RR'
        AND deactivat IS NULL
        AND procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
        AND valclass IS NULL
        -- Class 999 are test pins
        AND class NOT IN ('999')
    GROUP BY parid, taxyr

)

-- Join exemptions and EAVs
SELECT
    asmt.pin,
    asmt.year,
    wide.exe_disabled,
    wide.exe_freeze,
    wide.exe_homeowner,
    wide.exe_longtime_homeowner,
    wide.exe_senior,
    wide.exe_muni_built,
    wide.exe_vet_dis_lt50,
    wide.exe_vet_dis_50_69,
    wide.exe_vet_dis_ge70,
    wide.exe_vet_dis_100,
    wide.exe_vet_returning,
    wide.exe_wwii,
    asmt.mailed_taxable_av,
    asmt.certified_taxable_av,
    asmt.board_taxable_av,
    asmt.final_eav
FROM asmt
LEFT JOIN {{ ref('default.vw_pin_exe') }} AS wide
    ON asmt.pin = wide.pin
    AND asmt.year = wide.year
