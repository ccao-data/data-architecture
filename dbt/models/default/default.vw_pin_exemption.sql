SELECT
    det.parid AS pin,
    det.taxyr AS year,
    det.excode AS exemption_code,
    code.descr AS exemption_description,
    det.apother AS exemption_amount
FROM {{ source('iasworld', 'exdet') }} AS det
LEFT JOIN {{ source('iasworld', 'excode') }} AS code
    ON det.excode = code.excode
    AND det.taxyr = code.taxyr
    AND code.cur = 'Y'
    AND code.deactivat IS NULL
WHERE det.deactivat IS NULL
    AND det.cur = 'Y'
