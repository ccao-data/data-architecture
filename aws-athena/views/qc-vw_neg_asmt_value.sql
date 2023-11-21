SELECT
    legdat.taxyr,
    legdat.parid,
    legdat.user1 AS township_code,
    legdat.taxdist,
    pardat.class AS class,
    asmt_all.class AS class_asmt_all,
    asmt_all.valclass,
    asmt_all.val01,
    asmt_all.val02,
    asmt_all.val03,
    asmt_all.val04,
    asmt_all.val05,
    asmt_all.val06,
    asmt_all.val07,
    asmt_all.val08,
    asmt_all.val09,
    asmt_all.val10,
    asmt_all.val11,
    asmt_all.val12,
    asmt_all.val13,
    asmt_all.val14,
    asmt_all.val15,
    asmt_all.val16,
    asmt_all.val17,
    asmt_all.val18,
    asmt_all.val19,
    asmt_all.val20,
    asmt_all.val21,
    asmt_all.val22,
    asmt_all.val23,
    asmt_all.val24,
    asmt_all.val25,
    asmt_all.val26,
    asmt_all.val27,
    asmt_all.val28,
    asmt_all.val29,
    asmt_all.val30,
    asmt_all.val31,
    asmt_all.val32,
    asmt_all.val33,
    asmt_all.val34,
    asmt_all.val35,
    asmt_all.val36,
    asmt_all.val37,
    asmt_all.val38,
    asmt_all.val39,
    asmt_all.val40,
    asmt_all.val41,
    asmt_all.val42,
    asmt_all.val43,
    asmt_all.val44,
    asmt_all.val45,
    asmt_all.val46,
    asmt_all.val47,
    asmt_all.val48,
    asmt_all.val49,
    asmt_all.val50,
    asmt_all.val51,
    asmt_all.val52,
    asmt_all.val53,
    asmt_all.val54,
    asmt_all.val55,
    asmt_all.val56,
    asmt_all.val57,
    asmt_all.val58,
    asmt_all.val59,
    asmt_all.val60,
    asmt_all.valapr1,
    asmt_all.valapr2,
    asmt_all.valapr3,
    asmt_all.valasm1,
    asmt_all.valasm2,
    asmt_all.valasm3
FROM {{ source('iasworld', 'legdat') }} AS legdat
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON legdat.parid = pardat.parid
    AND legdat.taxyr = pardat.taxyr
LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt_all
    ON legdat.parid = asmt_all.parid
    AND legdat.taxyr = asmt_all.taxyr
WHERE legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    AND asmt_all.procname IN ('CCAOVALUE', 'CCAOFINAL', 'BORVALUE')
    AND asmt_all.rolltype != 'RR'
    AND asmt_all.deactivat IS NULL
    AND asmt_all.valclass IS NULL
    -- AND asmt_all.cur == 'Y'  Original query also contains this condition
