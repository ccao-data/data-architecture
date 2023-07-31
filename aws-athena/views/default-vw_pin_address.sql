-- Source of truth view for PIN address, both legal and mailing
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

    -- PIN mailing address from OWNDAT
    NULLIF(CONCAT_WS(
        ' ',
        own.own1, own.own2
    ), '') AS mail_address_name,
    CASE WHEN NULLIF(own.addr1, '') IS NOT NULL THEN own.addr1
        WHEN NULLIF(own.addr2, '') IS NOT NULL THEN own.addr2
        ELSE NULLIF(CONCAT_WS(
                ' ',
                CAST(own.adrno AS VARCHAR),
                own.adrdir, own.adrstr, own.adrsuf,
                own.unitdesc, own.unitno
            ), '')
    END AS mail_address_full,
    own.cityname AS mail_address_city_name,
    own.statecode AS mail_address_state,
    NULLIF(own.zip1, '00000') AS mail_address_zipcode_1,
    NULLIF(own.zip2, '0000') AS mail_address_zipcode_2

FROM iasworld.pardat AS par
LEFT JOIN iasworld.legdat AS leg
    ON par.parid = leg.parid
    AND par.taxyr = leg.taxyr
LEFT JOIN iasworld.owndat AS own
    ON par.parid = own.parid
    AND par.taxyr = own.taxyr
