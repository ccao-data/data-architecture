-- View to collect pin-level exemptions and taxable EAV

-- pin-level taxable AV and final EAV by stage
WITH asmt AS (
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
    asmt.mailed_eav,
    asmt.certified_taxable_av,
    asmt.certified_eav,
    asmt.board_taxable_av,
    asmt.board_eav
FROM asmt
LEFT JOIN {{ ref('default.vw_pin_exe') }} AS wide
    ON asmt.pin = wide.pin
    AND asmt.year = wide.year
