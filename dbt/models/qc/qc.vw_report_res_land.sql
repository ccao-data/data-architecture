WITH base AS (
    SELECT
        aprval.reascd AS reascd,
        aprval.revcode AS revcode,
        aprval.who AS aprval_who,
        aprval.wen AS aprval_wen,
        aprval.wencalc AS aprval_wencalc,
        aprval.whocalc AS aprval_whocalc,
        aprval.spcflg AS spcflg,
        asmt.valasm1 AS land_av,
        asmt.valasm2 AS bldg_av,
        asmt.valasm3 AS tot_av,
        asmt.tot30 AS hie_incentive_amount,
        asmt_prev.wen AS asmt_all_wen,
        asmt_prev.wencalc AS asmt_all_wencalc,
        asmt_prev.who AS asmt_all_who,
        asmt_prev.whocalc AS asmt_all_whocalc,
        asmt_prev.valasm1 AS board_land_av_prev,
        asmt_prev.valasm2 AS board_bldg_av_prev,
        asmt_prev.valasm3 AS board_tot_av_prev,
        asmt.valasm1 - asmt_prev.valasm1 AS land_av_diff,
        CASE
            WHEN asmt_prev.valasm1 = 0 OR asmt_prev.valasm1 IS NULL THEN NULL
            ELSE (asmt.valasm1 - asmt_prev.valasm1) / asmt_prev.valasm1
        END AS land_av_pct_diff,
        asmt.valasm3 - asmt_prev.valasm3 AS tot_av_diff,
        land.lline AS lline,
        land.ltype AS ltype,
        land.code AS code,
        land.class AS land_class,
        land.sf AS land_sf,
        land.brate AS land_brate,
        land_nbhd_rate.land_rate_per_sqft AS nbhd_land_rate,
        legdat.user1 AS township_code,
        legdat.taxdist AS taxdist,
        oby.user10 AS hie_incentive_year,
        owndat.own1 AS owner_name,
        pardat.parid AS pin,
        pardat.taxyr AS taxyr,
        pardat.tieback AS tieback,
        pardat.class AS class,
        pardat.nbhd AS nbhd_code,
        pardat_prev.class AS class_prev
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY parid, taxyr ORDER BY parid) AS _rn
            FROM {{ source('iasworld', 'asmt_all') }}
            WHERE cur = 'Y'
                AND valclass IS NULL
        )
        WHERE _rn = 1
    ) AS asmt
        ON pardat.parid = asmt.parid
        AND pardat.taxyr = asmt.taxyr
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY parid, taxyr ORDER BY parid) AS _rn
            FROM {{ source('iasworld', 'aprval') }}
        )
        WHERE _rn = 1
    ) AS aprval
        ON pardat.parid = aprval.parid
        AND pardat.taxyr = aprval.taxyr
    LEFT JOIN {{ source('iasworld', 'land') }} AS land
        ON pardat.parid = land.parid
        AND pardat.taxyr = land.taxyr
    LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
        ON pardat.parid = owndat.parid
        AND pardat.taxyr = owndat.taxyr
    LEFT JOIN (
        SELECT
            parid,
            taxyr,
            array_join(array_agg(DISTINCT user10), ', ') AS user10
        FROM {{ source('iasworld', 'oby') }}
        GROUP BY parid, taxyr
    ) AS oby
        ON pardat.parid = oby.parid
        AND pardat.taxyr = oby.taxyr
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY parid, taxyr ORDER BY parid) AS _rn
            FROM {{ source('iasworld', 'asmt_all') }}
            WHERE cur = 'Y'
                AND valclass IS NULL
        )
        WHERE _rn = 1
    ) AS asmt_prev
        ON pardat.parid = asmt_prev.parid
        AND CAST(pardat.taxyr AS INT) = CAST(asmt_prev.taxyr AS INT) + 1
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY parid, taxyr ORDER BY parid) AS _rn
            FROM {{ source('iasworld', 'pardat') }}
        )
        WHERE _rn = 1
    ) AS pardat_prev
        ON pardat.parid = pardat_prev.parid
        AND CAST(pardat.taxyr AS INT) = CAST(pardat_prev.taxyr AS INT) + 1
    LEFT JOIN {{ source('ccao', 'land_nbhd_rate') }} AS land_nbhd_rate
        ON pardat.nbhd = land_nbhd_rate.town_nbhd
        AND pardat.taxyr = land_nbhd_rate.year
        AND pardat.class = land_nbhd_rate.class
    WHERE pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
        AND TRY_CAST(pardat.class AS INT) BETWEEN 100 AND 299
        AND (land.brate IS NULL OR land.brate > 0)
),

base_with_sd AS (
    SELECT
        *,
        avg(land_av_pct_diff) OVER (PARTITION BY township_code) AS land_av_pct_diff_mean,
        stddev(land_av_pct_diff) OVER (PARTITION BY township_code) AS land_av_pct_diff_sd,
        avg(land_sf) OVER (PARTITION BY township_code) AS land_sf_mean,
        stddev(land_sf) OVER (PARTITION BY township_code) AS land_sf_sd
    FROM base
),

base_with_tests AS (
    SELECT
    pin,
    taxyr,
    filter(
        ARRAY[
            CASE WHEN class IN ('239', '224') AND ltype != 'A'
                THEN '❌ Error: Farmland must have land type A (Acre)' END,
            CASE WHEN class IN ('239', '224') AND code != '900'
                THEN '❌ Error: Farmland must have land code 900' END,
            CASE WHEN ltype = 'A' AND class NOT IN ('239', '224')
                THEN '❌ Error: Land type A must be farmland (class 239 or 224)' END,
            CASE WHEN code = '900' AND class NOT IN ('239', '224')
                THEN '❌ Error: Land code 900 must be farmland (class 239 or 224)' END,
            CASE WHEN ltype = 'G' AND class NOT IN ('EX', 'RR')
                THEN '❌ Error: Land type G must have class EX or RR' END,
            CASE WHEN code = '0'
                THEN '❌ Error: Land code 0 should not exist' END,
            CASE WHEN code = '21' AND land_av > 1
                THEN '❌ Error: Land code 21 (Common Area) must have land AV <= 1' END,
            CASE WHEN code = '56' AND land_av <= 1
                THEN '❌ Error: Land code 56 (Unbuildable) must not have land AV <= 1' END,
            CASE WHEN land_class NOT IN ('100', '200', '239', '241', '500')
                THEN '❌ Error: Land class must be 100, 200, 239, 241, or 500' END,
            CASE WHEN land_brate > 0 AND land_brate != nbhd_land_rate
                THEN '❌ Error: Land rate must match neighborhood land rate' END,
            CASE WHEN class IN ('239', '224') AND land_class = '200'
                THEN '🔍 Check: Review class 200 homestead land on farmland PINs' END,
            CASE WHEN class IN ('239', '224') AND land_class = '500'
                THEN '🔍 Check: Send industrial land on farmland PINs to Manager of Industrial' END,
            CASE WHEN code IN ('500', '600', 'EX')
                THEN '🔍 Check: Review land codes 500, 600, and EX' END,
            CASE WHEN ABS(land_av_pct_diff - land_av_pct_diff_mean) > 2 * land_av_pct_diff_sd
                THEN '🔍 Check: Review big increases in land AV' END,
            CASE WHEN ABS(land_sf - land_sf_mean) > 2 * land_sf_sd
                THEN '🔍 Check: Review values for PINs with largest land in the township' END,
            CASE WHEN land_av_diff < 0
                THEN '🔍 Check: Review land value that is decreasing' END
        ],
        x -> x IS NOT NULL
    ) AS tests,
    tieback,
    township_code,
    class,
    class_prev,
    owner_name,
    board_land_av_prev,
    board_bldg_av_prev,
    board_tot_av_prev,
    asmt_all_wen,
    asmt_all_wencalc,
    asmt_all_who,
    asmt_all_whocalc,
    land_av,
    bldg_av,
    tot_av,
    hie_incentive_amount,
    hie_incentive_year,
    land_av_diff,
    land_av_pct_diff,
    land_av_pct_diff_mean,
    land_av_pct_diff_sd,
    tot_av_diff,
    reascd,
    nbhd_code,
    taxdist,
    aprval_who,
    aprval_wen,
    aprval_wencalc,
    aprval_whocalc,
    spcflg,
    lline,
    ltype,
    code,
    land_class,
    land_sf,
    land_sf_mean,
    land_sf_sd,
    land_brate,
    nbhd_land_rate
FROM base_with_sd
)

SELECT *
FROM base_with_tests
WHERE CARDINALITY(tests) > 0
