-- Source of truth view for PIN address, both legal and mailing

WITH mail AS (
    SELECT
        NULLIF(
            REGEXP_REPLACE(CONCAT_WS(' ', mail1, mail2), '\s+', ' '), ''
        ) AS mail_address_name,
        NULLIF(
            REGEXP_REPLACE(CONCAT_WS(' ', maddr1, maddr2), '\s+', ' '), ''
        ) AS mail_address_full,
        mcityname AS mail_address_city_name,
        mstatecode AS mail_address_state,
        NULLIF(mzip1, '00000') AS mail_address_zipcode_1,
        NULLIF(mzip2, '0000') AS mail_address_zipcode_2,
        mailseq,
        MAX(mailseq) OVER (PARTITION BY parid, taxyr) AS newest
    FROM {{ source('iasworld', 'maildat') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
)

SELECT
    -- Main PIN-level attribute data from iasWorld
    par.parid AS pin,
    SUBSTR(par.parid, 1, 10) AS pin10,
    par.taxyr AS year,

    -- PIN legal address from LEGDAT
    leg.adrpre AS prop_address_prefix,
    CASE
        WHEN leg.adrno != 0 THEN leg.adrno
    END AS prop_address_street_number,
    leg.adrdir AS prop_address_street_dir,
    leg.adrstr AS prop_address_street_name,
    leg.adrsuf AS prop_address_suffix_1,
    leg.adrsuf2 AS prop_address_suffix_2,
    leg.unitdesc AS prop_address_unit_prefix,
    leg.unitno AS prop_address_unit_number,
    NULLIF(CONCAT_WS(
        ' ',
        leg.adrpre, CAST(
            CASE WHEN leg.adrno != 0 THEN leg.adrno END AS VARCHAR
        ),
        leg.adrdir, leg.adrstr, leg.adrsuf,
        leg.unitdesc, leg.unitno
    ), '') AS prop_address_full,
    leg.cityname AS prop_address_city_name,
    leg.statecode AS prop_address_state,
    NULLIF(leg.zip1, '00000') AS prop_address_zipcode_1,
    NULLIF(leg.zip2, '0000') AS prop_address_zipcode_2,

    -- PIN owner address from OWNDAT
    NULLIF(CONCAT_WS(
        ' ',
        own.own1, own.own2
    ), '') AS owner_address_name,
    CASE WHEN NULLIF(own.addr1, '') IS NOT NULL THEN own.addr1
        WHEN NULLIF(own.addr2, '') IS NOT NULL THEN own.addr2
        ELSE NULLIF(CONCAT_WS(
                ' ',
                CAST(own.adrno AS VARCHAR),
                own.adrdir, own.adrstr, own.adrsuf,
                own.unitdesc, own.unitno
            ), '')
    END AS owner_address_full,
    own.cityname AS owner_address_city_name,
    own.statecode AS owner_address_state,
    NULLIF(own.zip1, '00000') AS owner_address_zipcode_1,
    NULLIF(own.zip2, '0000') AS owner_address_zipcode_2,

    -- PIN mailing address from MAILDAT
    mail.mail_address_name,
    mail.mail_address_full,
    mail.mail_address_city_name,
    mail.mail_address_state,
    mail.mail_address_zipcode_1,
    mail.mail_address_zipcode_2

FROM {{ source('iasworld', 'pardat') }} AS par
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON par.parid = leg.parid
    AND par.taxyr = leg.taxyr
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'owndat') }} AS own
    ON par.parid = own.parid
    AND par.taxyr = own.taxyr
    AND own.cur = 'Y'
    AND own.deactivat IS NULL
LEFT JOIN mail
    ON par.parid = mail.parid
    AND par.taxyr = mail.taxyr
    AND mail.mailseq = mail.newest
WHERE par.cur = 'Y'
    AND par.deactivat IS NULL
    -- Remove any parcels with non-numeric characters
    -- or that are not 14 characters long
    AND REGEXP_COUNT(par.parid, '[a-zA-Z]') = 0
    AND LENGTH(par.parid) = 14
    -- Class 999 are test pins
    AND par.class NOT IN ('999')
