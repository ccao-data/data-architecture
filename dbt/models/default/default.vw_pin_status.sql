SELECT
    pdat.parid AS pin,
    pdat.class,
    pdat.taxyr AS year,
    colo.is_corner_lot,
    vptc.ahsap AS is_ahsap,
    COALESCE(vpe.pin IS NOT NULL, FALSE) AS is_exempt,
    COALESCE(pin.tax_bill_total = 0, FALSE) AS is_zero_bill,
    vpcc.is_parking_space,
    vpcc.parking_space_flag_reason,
    vpcc.is_common_area,
    SUBSTR(pdat.parid, 11, 1) = '8' AS is_leasehold,
    oby.user16 AS oby_cdu,
    cdat.user16 AS com_cdu,
    ddat.cdu AS dwel_cdu,
    pdat.note2 AS note,
    ptst.test_type AS weirdness
FROM iasworld.pardat AS pdat
LEFT JOIN spatial.corner AS colo
    ON SUBSTR(pdat.parid, 1, 10) = colo.pin10
    -- Corner lot indicator, only filled after 2014 since that's
    -- when OpenStreetMap data begins
    AND pdat.taxyr = colo.year
LEFT JOIN reporting.vw_pin_township_class AS vptc
    ON pdat.parid = vptc.pin
    AND pdat.taxyr = vptc.year
LEFT JOIN default.vw_pin_exempt AS vpe
    ON pdat.parid = vpe.pin
    AND pdat.taxyr = vpe.year
LEFT JOIN tax.pin
    ON pdat.parid = pin.pin
    AND pdat.taxyr = pin.year
LEFT JOIN default.vw_pin_condo_char AS vpcc
    ON pdat.parid = vpcc.pin
    AND pdat.taxyr = vpcc.year
LEFT JOIN iasworld.oby
    ON pdat.parid = oby.parid
    AND pdat.taxyr = oby.taxyr
LEFT JOIN iasworld.comdat AS cdat
    ON pdat.parid = cdat.parid
    AND pdat.taxyr = cdat.taxyr
LEFT JOIN iasworld.dweldat AS ddat
    ON pdat.parid = ddat.parid
    AND pdat.taxyr = ddat.taxyr
LEFT JOIN ccao.pin_test AS ptst
    ON pdat.parid = ptst.pin
    AND pdat.taxyr = ptst.year
