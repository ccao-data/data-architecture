 -- View containing RPIE codes and parcel physical and mailing addresses
CREATE OR replace VIEW rpie.vw_pin_mailers
AS
  -- physical parcel addresses
  WITH parcel_addressess
       AS (SELECT parid                                   AS pin,
                  Cast(Cast(taxyr AS INT) + 1 AS VARCHAR) AS rpie_year,
                  Cast(adrno AS VARCHAR)
                  || CASE
                       WHEN adrdir IS NOT NULL THEN ' '
                                                    || adrdir
                       ELSE ''
                     END
                  || ' '
                  || adrstr
                  || ' '
                  || adrsuf                               AS property_address,
                  unitno                                  AS property_apt_no,
                  cityname                                AS property_CITY,
                  'IL'                                    AS property_STATE,
                  zip1
                  || CASE
                       WHEN zip2 IS NOT NULL
                            AND zip2 != '0000' THEN '-'
                                                    || zip2
                       ELSE ''
                     END                                  AS property_ZIP,
                  Substr(class, 1, 1)                     AS major_class,
                  CASE
                    WHEN Substr(nbhd, 1, 2) IN ( '70', '71', '72', '73',
                                                 '74', '75', '76', '77' ) THEN
                    'City'
                    WHEN Substr(nbhd, 1, 2) IN ( '10', '16', '17', '18',
                                                 '20', '22', '23', '24',
                                                 '25', '26', '29', '35', '38' )
                  THEN
                    'North'
                    WHEN Substr(nbhd, 1, 2) IN ( '11', '12', '13', '14',
                                                 '15', '19', '21', '27',
                                                 '28', '30', '31', '32',
                                                 '33', '34', '36', '37', '39' )
                  THEN
                    'South'
                    ELSE NULL
                  END                                     AS tri
           FROM   iasworld.pardat),
       -- parcel mailing addresses
       owner_addressess
       AS (SELECT parid                                   AS pin,
                  Cast(Cast(taxyr AS INT) + 1 AS VARCHAR) AS rpie_year,
                  own1
                  || CASE
                       WHEN own2 IS NOT NULL THEN ' '
                                                  || own2
                       ELSE ''
                     END                                  AS mailing_NAME,
                  CASE
                    WHEN careof IS NOT NULL THEN careof
                    ELSE ''
                  END                                     AS mailing_care_of,
                  CASE
                    WHEN addr1 IS NOT NULL THEN addr1
                                                || ( CASE
                    WHEN addr2 IS NOT NULL THEN ' '
                                                || addr2
                    ELSE ''
                                                     END )
                    ELSE Cast(adrno AS VARCHAR)
                         || CASE
                              WHEN adrdir IS NOT NULL THEN adrdir
                                                           || ' '
                              ELSE ''
                            END
                         || ' '
                         || adrstr
                         || ' '
                         || adrsuf
                  END                                     AS mailing_address,
                  cityname                                AS mailing_city,
                  statecode                               AS mailing_state,
                  CASE
                    WHEN zip1 IS NOT NULL
                         AND zip1 != '00000' THEN zip1
                  END
                  || CASE
                       WHEN zip2 IS NOT NULL
                            AND zip2 != '0000' THEN '-'
                                                    || zip2
                       ELSE ''
                     END                                  AS mailing_zip
           FROM   iasworld.owndat),
           -- RPIE pin/code combinations
           pin_codes
           AS (SELECT pin,
         Substr(pin_codes.pin, 1, 2)
         || '-'
         || Substr(pin_codes.pin, 3, 2)
         || '-'
         || Substr(pin_codes.pin, 5, 3)
         || '-'
         || Substr(pin_codes.pin, 8, 3)
         || '-'
         || Substr(pin_codes.pin, 11, 4) AS rpie_pin,
         pin_codes.year                  AS rpie_year,
         pin_codes.rpie_code FROM  rpie.pin_codes)

         SELECT pin_codes.*,
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
         major_class,
         tri
  from parcel_addressess
         left join owner_addressess
                ON parcel_addressess.pin = owner_addressess.pin
                   AND parcel_addressess.rpie_year = owner_addressess.rpie_year
        left join pin_codes
                ON parcel_addressess.pin = pin_codes.pin
                   AND parcel_addressess.rpie_year = pin_codes.rpie_year
where parcel_addressess.rpie_year >= '2019'