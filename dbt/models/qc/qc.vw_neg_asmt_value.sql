SELECT
    legdat.taxyr,
    legdat.parid,
    legdat.user1 AS township_code,
    legdat.taxdist,
    asmt.class,
    asmt.valclass,
    asmt.who,
    asmt.wen,
    asmt.val01,
    asmt.val02,
    asmt.val03,
    asmt.val04,
    asmt.val05,
    asmt.val06,
    asmt.val07,
    asmt.val08,
    asmt.val09,
    asmt.val10,
    asmt.val11,
    asmt.val12,
    asmt.val13,
    asmt.val14,
    asmt.val15,
    asmt.val16,
    asmt.val17,
    asmt.val18,
    asmt.val19,
    asmt.val20,
    asmt.val21,
    asmt.val22,
    asmt.val23,
    asmt.val24,
    asmt.val25,
    asmt.val26,
    asmt.val27,
    asmt.val28,
    asmt.val29,
    asmt.val30,
    asmt.val31,
    asmt.val32,
    asmt.val33,
    asmt.val34,
    asmt.val35,
    asmt.val36,
    asmt.val37,
    asmt.val38,
    asmt.val39,
    asmt.val40,
    asmt.val41,
    asmt.val42,
    asmt.val43,
    asmt.val44,
    asmt.val45,
    asmt.val46,
    asmt.val47,
    asmt.val49,
    asmt.val50,
    asmt.val51,
    asmt.val52,
    asmt.val53,
    asmt.val54,
    asmt.val55,
    asmt.val56,
    asmt.val57,
    asmt.val58,
    asmt.val59,
    asmt.val60,
    asmt.valapr1,
    asmt.valapr2,
    asmt.valapr3,
    asmt.valasm1,
    asmt.valasm2,
    asmt.valasm3
FROM (
    -- This is intended to replicate the ASMT table since we don't
    -- actually have access to it and ASMT_ALL is just the union of
    -- ASMT and ASMT_HIST
    SELECT *
    FROM {{ source('iasworld', 'asmt_all') }}
    EXCEPT
    SELECT *
    FROM {{ source('iasworld', 'asmt_hist') }}
) AS asmt
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON asmt.taxyr = legdat.taxyr
    AND asmt.parid = legdat.parid
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
WHERE asmt.cur = 'Y'
    AND asmt.deactivat IS NULL
