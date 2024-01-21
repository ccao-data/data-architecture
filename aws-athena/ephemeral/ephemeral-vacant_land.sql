{{
    config(materialized='ephemeral')
}}

SELECT
    parcel.pin10,
    parcel.x_3435,
    parcel.y_3435,
    parcel.year
FROM {{ source('spatial', 'parcel') }} AS parcel
INNER JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON parcel.pin10 = SUBSTR(pardat.parid, 1, 10)
    AND parcel.year = pardat.year
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
    AND pardat.class = '100'
