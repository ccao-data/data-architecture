SELECT
    parid,
    class,
    valclass,
    val01,
    val02,
    val03,
    val04,
    val05,
    val06,
    val07,
    val08,
    val09,
    val10,
    val11,
    val12,
    val13,
    val16,
    val17,
    val18,
    val19,
    val20,
    val21,
    val22,
    val23,
    val24,
    val25,
    val26,
    val27,
    val28,
    val29,
    val30,
    val31,
    val32,
    val33,
    val34,
    val35,
    val36,
    val37,
    val38,
    val39,
    val40,
    val41,
    val42,
    val43,
    val44,
    val45,
    val46,
    val47,
    val48,
    val49,
    val50,
    val51,
    val52,
    val53,
    val54,
    val55,
    val56,
    val57,
    val58,
    val59,
    val60,
    valapr1,
    valapr2,
    valapr3,
    valasm1,
    valasm2,
    valasm3
FROM {{ ref('qc.vw_neg_asmt_value') }}
WHERE township_code = '{{ var("qc_report_town_code") }}'
    AND CAST(taxyr AS INT)
    BETWEEN {{ var('test_qc_year_start') }} AND {{ var('test_qc_year_end') }}
    AND (
        val01 < 0
        OR val02 < 0
        OR val03 < 0
        OR val04 < 0
        OR val05 < 0
        OR val06 < 0
        OR val07 < 0
        OR val08 < 0
        OR val09 < 0
        OR val10 < 0
        OR val11 < 0
        OR val12 < 0
        OR val13 < 0
        OR val16 < 0
        OR val17 < 0
        OR val18 < 0
        OR val19 < 0
        OR val20 < 0
        OR val21 < 0
        OR val22 < 0
        OR val23 < 0
        OR val24 < 0
        OR val25 < 0
        OR val26 < 0
        OR val27 < 0
        OR val28 < 0
        OR val29 < 0
        OR val30 < 0
        OR val31 < 0
        OR val32 < 0
        OR val33 < 0
        OR val34 < 0
        OR val35 < 0
        OR val36 < 0
        OR val37 < 0
        OR val38 < 0
        OR val39 < 0
        OR val40 < 0
        OR val41 < 0
        OR val42 < 0
        OR val43 < 0
        OR val44 < 0
        OR val45 < 0
        OR val46 < 0
        OR val47 < 0
        OR val48 < 0
        OR val49 < 0
        OR val50 < 0
        OR val51 < 0
        OR val52 < 0
        OR val53 < 0
        OR val54 < 0
        OR val55 < 0
        OR val56 < 0
        OR val57 < 0
        OR val58 < 0
        OR val59 < 0
        OR val60 < 0
        OR valapr1 < 0
        OR valapr2 < 0
        OR valapr3 < 0
        OR valasm1 < 0
        OR valasm2 < 0
        OR valasm3 < 0
    )
