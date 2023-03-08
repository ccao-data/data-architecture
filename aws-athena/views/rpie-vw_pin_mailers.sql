-- View containing RPIE codes and parcel physical and mailing addresses
CREATE OR replace VIEW rpie.vw_pin_mailers
AS
-- physical parcel addresses
WITH parcel_addressess AS (
  SELECT

    parid AS pin,
    Cast(Cast(taxyr AS INT) + 1 AS VARCHAR) AS rpie_year,
    -- PIN mailing address from OWNDAT
    NULLIF(CONCAT_WS(
      ' ',
      CAST(adrno AS varchar),
      adrdir,
      adrstr,
      adrsuf), '') AS property_address,
    unitno AS property_apt_no,
    cityname AS property_city,
    'IL' AS property_state,
    CONCAT_WS(
      '-',
      NULLIF(zip1, '00000'),
      NULLIF(zip2, '0000')
      ) AS property_zip,
    class,
    Substr(class, 1, 1) AS major_class,
    CASE
      WHEN Substr(nbhd, 1, 2) IN ( '70', '71', '72', '73',
      '74', '75', '76', '77' )
      THEN 'City'
      WHEN Substr(nbhd, 1, 2) IN ( '10', '16', '17', '18',
      '20', '22', '23', '24',
      '25', '26', '29', '35', '38' )
      THEN 'North'
      WHEN Substr(nbhd, 1, 2) IN ( '11', '12', '13', '14',
      '15', '19', '21', '27',
      '28', '30', '31', '32',
      '33', '34', '36', '37', '39' )
      THEN 'South'
      ELSE NULL
    END AS tri

  FROM iasworld.pardat
),
-- parcel mailing addresses
owner_addressess AS (
  SELECT

    parid AS pin,
    Cast(Cast(taxyr AS INT) + 1 AS VARCHAR) AS rpie_year,
    -- PIN mailing address from OWNDAT
    NULLIF(CONCAT_WS(
      ' ',
      own1, own2
      ), '') AS mailing_name,
    NULLIF(careof, '') AS mailing_care_of,
    CASE
      WHEN NULLIF(addr1, '') IS NOT NULL THEN addr1
      WHEN NULLIF(addr2, '') IS NOT NULL THEN addr2
      ELSE NULLIF(CONCAT_WS(' ',
      CAST(adrno AS varchar),
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

  FROM iasworld.owndat
),
-- RPIE pin/code combinations
pin_codes AS (
  SELECT

    pin,
    Substr(pin_codes.pin, 1, 2)
      || '-'
      || Substr(pin_codes.pin, 3, 2)
      || '-'
      || Substr(pin_codes.pin, 5, 3)
      || '-'
      || Substr(pin_codes.pin, 8, 3)
      || '-'
      || Substr(pin_codes.pin, 11, 4) AS rpie_pin,
    pin_codes.year AS rpie_year,
    pin_codes.rpie_code

FROM rpie.pin_codes
)

SELECT

  pin_codes.*,
  property_address,
  property_apt_no,
  property_city,
  property_state,
  property_zip,
  mailing_name,
  mailing_care_of,
  mailing_address,
  mailing_city,
  mailing_state,
  mailing_zip,
  class,
  major_class,
  tri

FROM parcel_addressess
LEFT JOIN owner_addressess
ON parcel_addressess.pin = owner_addressess.pin
AND parcel_addressess.rpie_year = owner_addressess.rpie_year
LEFT JOIN pin_codes
ON parcel_addressess.pin = pin_codes.pin
AND parcel_addressess.rpie_year = pin_codes.rpie_year
WHERE parcel_addressess.rpie_year >= '2019'