SELECT
    hist.pin,
    hist.year,
    hist.class,
    hist.township_name,
    hist.oneyr_pri_certified_tot,
    hist.mailed_class,
    hist.mailed_tot,
    hist.mailed_tot > hist.oneyr_pri_certified_tot * 1.2 AS large_mail_increase,
    hist.certified_class,
    hist.certified_tot,
    hist.certified_tot > hist.mailed_tot * 1.2 AS large_certified_increase,
    hist.board_class,
    hist.board_tot,
    hist.board_tot > hist.certified_tot * 1.2 AS large_board_increase
FROM {{ ref('default.vw_pin_history') }} AS hist
INNER JOIN {{ ref('reporting.vw_pin_township_class') }} AS ahsap
    ON hist.pin = ahsap.pin AND hist.year = ahsap.year
WHERE ahsap.is_ahsap
    AND (
        hist.mailed_tot > hist.oneyr_pri_certified_tot * 1.2
        OR hist.certified_tot > hist.mailed_tot * 1.2
        OR hist.board_tot > hist.certified_tot * 1.2
    )
