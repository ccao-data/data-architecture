-- View containing RPIE codes and parcel physical and mailing addresses

-- physical parcel addresses
WITH parcel_addressess AS (
    SELECT
        parid AS pin,
        CAST(CAST(taxyr AS INT) + 1 AS VARCHAR) AS rpie_year,
        -- PIN mailing address from OWNDAT
        NULLIF(CONCAT_WS(
            ' ',
            CAST(adrno AS VARCHAR),
            adrdir,
            adrstr,
            adrsuf
        ), '') AS property_address,
        unitno AS property_apt_no,
        cityname AS property_city,
        'IL' AS property_state,
        CONCAT_WS(
            '-',
            NULLIF(zip1, '00000'),
            NULLIF(zip2, '0000')
        ) AS property_zip,
        class,
        SUBSTR(class, 1, 1) AS major_class,
        CASE
            WHEN SUBSTR(
                    nbhd, 1, 2) IN (
                    '70', '71', '72', '73',
                    '74', '75', '76', '77'
                )
                THEN 'City'
            WHEN SUBSTR(
                    nbhd, 1, 2) IN (
                    '10', '16', '17', '18',
                    '20', '22', '23', '24',
                    '25', '26', '29', '35', '38'
                )
                THEN 'North'
            WHEN SUBSTR(
                    nbhd, 1, 2) IN (
                    '11', '12', '13', '14',
                    '15', '19', '21', '27',
                    '28', '30', '31', '32',
                    '33', '34', '36', '37', '39'
                )
                THEN 'South'
        END AS tri
    FROM {{ ref('iasworld.pardat') }}
),

-- parcel mailing addresses
owner_addressess AS (
    SELECT
        parid AS pin,
        CAST(CAST(taxyr AS INT) + 1 AS VARCHAR) AS rpie_year,
        -- PIN mailing address from OWNDAT
        NULLIF(CONCAT_WS(
            ' ',
            own1, own2
        ), '') AS mailing_name,
        NULLIF(careof, '') AS mailing_care_of,
        CASE
            WHEN NULLIF(addr1, '') IS NOT NULL THEN addr1
            WHEN NULLIF(addr2, '') IS NOT NULL THEN addr2
            ELSE NULLIF(CONCAT_WS(
                    ' ',
                    CAST(adrno AS VARCHAR),
                    adrdir, adrstr, adrsuf,
                    unitdesc, unitno
                ), '')
        END AS mailing_address,
        cityname AS mailing_city,
        statecode AS mailing_state,
        CONCAT_WS(
            '-',
            NULLIF(zip1, '00000'),
            NULLIF(zip2, '0000')
        ) AS mailing_zip
    FROM {{ ref('iasworld.owndat') }}
),

-- RPIE pin/code combinations
pin_codes AS (
    SELECT
        pin_codes.pin,
        SUBSTR(pin_codes.pin, 1, 2)
        || '-'
        || SUBSTR(pin_codes.pin, 3, 2)
        || '-'
        || SUBSTR(pin_codes.pin, 5, 3)
        || '-'
        || SUBSTR(pin_codes.pin, 8, 3)
        || '-'
        || SUBSTR(pin_codes.pin, 11, 4) AS rpie_pin,
        pin_codes.year AS rpie_year,
        pin_codes.rpie_code
    FROM {{ ref('rpie.pin_codes') }} AS pin_codes
)

SELECT
    pc.*,
    pa.property_address,
    pa.property_apt_no,
    pa.property_city,
    pa.property_state,
    pa.property_zip,
    oa.mailing_name,
    oa.mailing_care_of,
    oa.mailing_address,
    oa.mailing_city,
    oa.mailing_state,
    oa.mailing_zip,
    pa.class,
    pa.major_class,
    pa.tri
FROM parcel_addressess AS pa
LEFT JOIN owner_addressess AS oa
    ON pa.pin = oa.pin
    AND pa.rpie_year = oa.rpie_year
LEFT JOIN pin_codes AS pc
    ON pa.pin = pc.pin
    AND pa.rpie_year = pc.rpie_year
WHERE pa.rpie_year >= '2019'
