 -- View containing RPIE codes and parcel physical and mailing addresses
CREATE OR replace VIEW rpie.vw_pin_mailers
AS
  -- physical parcel addresses
  WITH parcel_address
       AS (SELECT parid                                   AS pin,
                  Cast(Cast(taxyr AS INT) + 1 AS VARCHAR) AS year,
                  Cast(adrno AS VARCHAR)
                  || CASE
                       WHEN adrdir IS NOT NULL THEN ' '
                                                    || adrdir
                       ELSE ''
                     END
                  || ' '
                  || adrstr
                  || ' '
                  || adrsuf                               AS PAR_ADDR,
                  unitno                                  AS PAR_UNIT,
                  cityname                                AS PAR_CITY,
                  'IL'                                    AS PAR_STATE,
                  zip1
                  || CASE
                       WHEN zip2 IS NOT NULL
                            AND zip2 != '0000' THEN '-'
                                                    || zip2
                       ELSE ''
                     END                                  AS PAR_ZIP
           FROM   iasworld.pardat),
           -- parcel mailing addresses
       owner_address
       AS (SELECT parid                                   AS pin,
                  Cast(Cast(taxyr AS INT) + 1 AS VARCHAR) AS year,
                  own1
                  || CASE
                       WHEN own2 IS NOT NULL THEN ' '
                                                  || own2
                       ELSE ''
                     END                                  AS OWN_NAME,
                  CASE
                    WHEN careof IS NOT NULL THEN careof
                    ELSE ''
                  END                                     AS OWN_CAREOF,
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
                         || adrstr
                         || adrsuf
                  END                                     AS OWN_ADR,
                  cityname                                AS own_city,
                  statecode                               AS own_state,
                  CASE
                    WHEN zip1 IS NOT NULL
                         AND zip1 != '00000' THEN zip1
                  END
                  || CASE
                       WHEN zip2 IS NOT NULL
                            AND zip2 != '0000' THEN '-'
                                                    || zip2
                       ELSE ''
                     END                                  AS own_zip
           FROM   iasworld.owndat)
  SELECT pin_codes.pin,
         pin_codes.year AS rpie_year,
         pin_codes.rpie_code,
         par_addr,
         par_unit,
         par_city,
         par_state,
         par_zip,
         own_name,
         own_careof,
         own_adr,
         own_city,
         own_state,
         own_zip
  FROM   rpie.pin_codes
         left join parcel_address
                ON pin_codes.pin = parcel_address.pin
                   AND pin_codes.year = parcel_address.year
         left join owner_address
                ON pin_codes.pin = owner_address.pin
                   AND pin_codes.year = owner_address.year