/* This view compiles data necessary to populate appeals worksheets for IC analysts.
It works in conjuction with XXX.R to output individual worksheets for each case. */

CREATE OR replace VIEW other.vw_appeal_worksheets
AS
WITH hv AS ( -- Historic values for 2022 and 2021
    SELECT
        parid,
        -- Mailed values
        Max(
            CASE WHEN procname = 'CCAOVALUE' THEN valapr3 ELSE NULL END
            ) AS "2022 1st Pass Value",
        -- Assessor certified values
        Max(
            CASE WHEN procname = 'CCAOFINAL' THEN valapr3 ELSE NULL END
            ) AS "2021 CCAO Final Value",
        -- Board certified values
        Max(
            CASE WHEN procname = 'BORVALUE' THEN valapr3 ELSE NULL END
            ) AS "2021 BOR Final Value"
    FROM iasworld.asmt_all
    WHERE
        ((procname IN ('CCAOFINAL', 'BORVALUE') AND taxyr = '2021')
        OR
        (procname = 'CCAOVALUE' AND taxyr = '2022'))
        AND valclass IS NULL
    GROUP BY parid
),
pin_count AS ( -- Aggregated pin counts, land SF, and MVs by case number
    SELECT
        caseno,
        Sum(hv."2022 1st pass value") AS "2022 1st Pass Value",
        Sum(hv."2021 ccao final value") AS "2021 CCAO Final Value",
        Sum(hv."2021 bor final value") AS "2021 BOR Final Value",
        Array_agg(
                Concat_ws( -- Formatting PIN
                    '-',
                    Substr(htpar.parid, 1, 2),
                    Substr(htpar.parid, 3, 2),
                    Substr(htpar.parid, 5, 3),
                    Substr(htpar.parid, 8, 3),
                    Substr(htpar.parid, 11, 4)
                )
        ) AS pins,
        Count(*) AS num_pins,
        Sum(sf) AS "Total Land SF"
    FROM iasworld.htpar

    LEFT JOIN iasworld.land ON htpar.parid = land.parid AND htpar.taxyr = land.taxyr
    LEFT JOIN hv ON htpar.parid = hv.parid
    GROUP BY caseno
)

SELECT
    htpar.caseno AS "Case No.",
    pin_count.num_pins AS "PINs in Case",
    pin_count."2022 1st pass value",
    pin_count."2021 ccao final value",
    pin_count."2021 bor final value",
    htpar.propreduct AS "Petitioner's Requested Value",
    pin_count."total land sf",
    htpar.user38 AS "Appeal Type",
    htpar.user19 AS "Lack of Uniformity",
    htpar.user20 AS "Fire Damage",
    htpar.user21 AS "Bldg No Longer Exists",
    htpar.user22 AS "Overvaluation",
    htpar.user23 AS "Prop Description Error",
    htpar.user24 AS "Vacancy",
    htpar.user25 AS "Bldg is Uninhabited",
    Concat_ws( -- Formatting class
        '-',
        Substr(pardat.class, 1, 1),
        Substr(pardat.class, 2, 2)
        ) AS "Primary PIN Class(es)",
    Concat_ws( -- Formatting PIN
            '-',
            Substr(htpar.parid, 1, 2),
            Substr(htpar.parid, 3, 2),
            Substr(htpar.parid, 5, 3),
            Substr(htpar.parid, 8, 3),
            Substr(htpar.parid, 11, 4)
            ) AS "Primary PIN",
    Array_join(
        Array_remove(
            pin_count.pins,
            Concat_ws( -- Formatting PIN
                '-',
                Substr(htpar.parid, 1, 2),
                Substr(htpar.parid, 3, 2),
                Substr(htpar.parid, 5, 3),
                Substr(htpar.parid, 8, 3),
                Substr(htpar.parid, 11, 4)
                )
            ),
        ', ') AS "Related PIN(s)",
    Nullif(
        Concat_ws( -- Formatting address
            ' ',
            Cast(legdat.adrno AS VARCHAR),
            legdat.adrdir,
            legdat.adrstr,
            legdat.adrsuf
            ),
        ''
    ) AS "Situs Address (Primary PIN) 1",
    Nullif(Concat(legdat.cityname, ', IL'), '') AS "Situs Address (Primary PIN) 2",
    Substr(legdat.taxdist, 1, 2) AS township

FROM iasworld.htpar

LEFT JOIN iasworld.pardat ON htpar.parid = pardat.parid AND htpar.taxyr = pardat.taxyr
LEFT JOIN iasworld.legdat ON htpar.parid = legdat.parid AND htpar.taxyr = legdat.taxyr
LEFT JOIN pin_count ON htpar.caseno = pin_count.caseno

/* Limit output to 2022, exclude condos & non-IC appeals,
choose a primary PIN based on "Petitioner's Requested Value" column */
WHERE htpar.taxyr = '2022'
AND pardat.class != '299'
AND htpar.user38 NOT IN ('RS', 'IN')
AND htpar.propreduct IS NOT NULL
