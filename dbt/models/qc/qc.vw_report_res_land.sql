WITH base AS (
    SELECT
        aprval.reascd,
        aprval.revcode,
        aprval.who AS aprval_who,
        aprval.wen AS aprval_wen,
        aprval.wencalc AS aprval_wencalc,
        aprval.whocalc AS aprval_whocalc,
        aprval.spcflg,
        asmt.valasm1 AS land_av,
        asmt.valasm2 AS bldg_av,
        asmt.valasm3 AS tot_av,
        asmt.tot30 AS hie_incentive_amount,
        asmt_prev.wen AS asmt_prev_wen,
        asmt_prev.wencalc AS asmt_prev_wencalc,
        asmt_prev.who AS asmt_prev_who,
        asmt_prev.whocalc AS asmt_prev_whocalc,
        asmt_prev.valasm1 AS board_land_av_prev,
        asmt_prev.valasm2 AS board_bldg_av_prev,
        asmt_prev.valasm3 AS board_tot_av_prev,
        asmt.valasm1 - asmt_prev.valasm1 AS land_av_diff,
        (asmt.valasm1 - asmt_prev.valasm1)
        / NULLIF(asmt_prev.valasm1, 0) AS land_av_pct_diff,
        asmt.valasm3 - asmt_prev.valasm3 AS tot_av_diff,
        land.lline,
        land.ltype,
        land.code,
        land.class AS land_class,
        land.sf AS land_sf,
        land.brate AS land_brate,
        legdat.user1 AS township_code,
        legdat.taxdist,
        oby.user10 AS hie_incentive_year,
        owndat.own1 AS owner_name,
        pardat.parid AS pin,
        pardat.taxyr,
        pardat.tieback,
        pardat.class,
        pardat.nbhd AS nbhd_code,
        asmt_prev.class AS class_prev
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
        ON pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
        AND legdat.cur = 'Y'
    INNER JOIN (
        -- Replicates ASMT: ASMT_ALL is the union of ASMT and ASMT_HIST
        SELECT *
        FROM {{ source('iasworld', 'asmt_all') }}
        EXCEPT
        SELECT *
        FROM {{ source('iasworld', 'asmt_hist') }}
    ) AS asmt
        ON pardat.parid = asmt.parid
        AND pardat.taxyr = asmt.taxyr
        AND asmt.cur = 'Y'
        AND asmt.valclass IS NULL
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER()
                    OVER (PARTITION BY parid, taxyr ORDER BY parid)
                    AS _rn
            FROM {{ source('iasworld', 'aprval') }}
            WHERE cur = 'Y'
        )
        WHERE _rn = 1
    ) AS aprval
        ON pardat.parid = aprval.parid
        AND pardat.taxyr = aprval.taxyr
    LEFT JOIN {{ source('iasworld', 'land') }} AS land
        ON pardat.parid = land.parid
        AND pardat.taxyr = land.taxyr
        AND land.cur = 'Y'
    LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
        ON pardat.parid = owndat.parid
        AND pardat.taxyr = owndat.taxyr
        AND owndat.cur = 'Y'
    LEFT JOIN (
        SELECT
            parid,
            taxyr,
            ARRAY_JOIN(ARRAY_AGG(DISTINCT user10), ', ') AS user10
        FROM {{ source('iasworld', 'oby') }}
        WHERE cur = 'Y'
        GROUP BY parid, taxyr
    ) AS oby
        ON pardat.parid = oby.parid
        AND pardat.taxyr = oby.taxyr
    LEFT JOIN {{ source('iasworld', 'asmt_all') }} AS asmt_prev
        ON pardat.parid = asmt_prev.parid
        AND CAST(pardat.taxyr AS INT) = CAST(asmt_prev.taxyr AS INT) + 1
        AND asmt_prev.cur = 'Y'
        AND asmt_prev.valclass IS NULL
        AND asmt_prev.rolltype = 'RP'
    WHERE pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
        AND SUBSTR(pardat.class, 1, 1) IN ('1', '2')
),

base_with_sd AS (
    SELECT
        *,
        AVG(land_av_pct_diff)
            OVER (PARTITION BY township_code, taxyr)
            AS land_av_pct_diff_mean,
        STDDEV(land_av_pct_diff)
            OVER (PARTITION BY township_code, taxyr)
            AS land_av_pct_diff_sd,
        AVG(land_sf) OVER (PARTITION BY township_code, taxyr) AS land_sf_mean,
        STDDEV(land_sf) OVER (PARTITION BY township_code, taxyr) AS land_sf_sd
    FROM base
)

SELECT
    pin,
    taxyr,
    FILTER(
        ARRAY[
            CASE WHEN class IN ('239', '224') AND ltype != 'A'
                    THEN '❌ Error: Farmland must have land type A (Acre)'
            END,
            CASE WHEN class IN ('239', '224') AND code != '900'
                    THEN '❌ Error: Farmland must have land code 900'
            END,
            CASE WHEN ltype = 'A' AND class NOT IN ('239', '224')
                    THEN '❌ Error: Land type A must be farmland'
                    || ' (class 239 or 224)'
            END,
            CASE WHEN code = '900' AND class NOT IN ('239', '224')
                    THEN '❌ Error: Land code 900 must be farmland'
                    || ' (class 239 or 224)'
            END,
            CASE WHEN ltype = 'G' AND class NOT IN ('EX', 'RR')
                    THEN '❌ Error: Land type G must have class EX or RR'
            END,
            CASE WHEN code = '0'
                    THEN '❌ Error: Land code 0 should not exist'
            END,
            CASE WHEN code = '21' AND land_av > 1
                    THEN '❌ Error: Land code 21 (Common Area)'
                    || ' must have land AV <= 1'
            END,
            CASE WHEN code = '56' AND land_av <= 1
                    THEN '❌ Error: Land code 56 (Unbuildable)'
                    || ' must not have land AV <= 1'
            END,
            CASE WHEN land_class NOT IN ('100', '200', '239', '241', '500')
                    THEN '❌ Error: Land class must be 100, 200,'
                    || ' 239, 241, or 500'
            END,
            CASE WHEN class IN ('239', '224') AND land_class = '200'
                    THEN '🔍 Check: Review class 200 homestead'
                    || ' land on farmland PINs'
            END,
            CASE WHEN class IN ('239', '224') AND land_class = '500'
                    THEN '🔍 Check: Send industrial land on farmland'
                    || ' PINs to Manager of Industrial'
            END,
            CASE WHEN code IN ('500', '600', 'EX')
                    THEN '🔍 Check: Review land codes 500, 600, and EX'
            END,
            CASE WHEN (land_av_pct_diff - land_av_pct_diff_mean)
                    > 2 * land_av_pct_diff_sd
                    THEN '🔍 Check: Review big increases in land AV'
            END,
            CASE WHEN (land_sf - land_sf_mean) > 2 * land_sf_sd
                    THEN '🔍 Check: Review values for PINs with'
                    || ' largest land in the township'
            END,
            CASE WHEN land_av_diff <= 0
                    THEN '🔍 Check: Review land value that is decreasing'
                    || ' or has zero-percent change'
            END
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
    asmt_prev_wen,
    asmt_prev_wencalc,
    asmt_prev_who,
    asmt_prev_whocalc,
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
    land_brate
FROM base_with_sd
