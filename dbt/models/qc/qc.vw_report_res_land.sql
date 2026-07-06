WITH base AS (
    SELECT
        pardat.parid AS pin,
        pardat.tieback AS tieback,
        legdat.user1 AS township_code,
        pardat.class AS class,
        pardat_prev.class AS class_prev,
        owndat.own1 AS owner_name,
        asmt_prev.valasm1 AS board_land_av_prev,
        asmt_prev.valasm2 AS board_bldg_av_prev,
        asmt_prev.valasm3 AS board_tot_av_prev,
        asmt.wen AS asmt_all_wen,
        asmt.wencalc AS asmt_all_wencalc,
        asmt.who AS asmt_all_who,
        asmt.whocalc AS asmt_all_whocalc,
        asmt.valasm1 AS land_av,
        asmt.valasm2 AS bldg_av,
        asmt.valasm3 AS tot_av,
        asmt.tot30 AS hie_incentive_amount,
        oby.user10 AS hie_incentive_year,
        asmt.valasm1 - asmt_prev.valasm1 AS land_av_diff,
        CASE
            WHEN asmt_prev.valasm1 = 0 OR asmt_prev.valasm1 IS NULL THEN NULL
            ELSE (asmt.valasm1 - asmt_prev.valasm1) / asmt_prev.valasm1
        END AS land_av_pct_diff,
        asmt.valasm3 - asmt_prev.valasm3 AS tot_av_diff,
        aprval.reascd AS reascd,
        pardat.nbhd AS nbhd_code,
        legdat.taxdist AS taxdist,
        aprval.who AS aprval_who,
        aprval.wen AS aprval_wen,
        aprval.wencalc AS aprval_wencalc,
        aprval.whocalc AS aprval_whocalc,
        aprval.spcflg AS spcflg,
        land.lline AS lline,
        land.ltype AS ltype,
        land.code AS code,
        land.class AS land_class,
        land.sf AS land_sf,
        land.brate AS land_brate,
        land_nbhd_rate.land_rate_per_sqft AS nbhd_land_rate
    FROM {{ source('iasworld', 'pardat') }} AS pardat
    LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
        ON pardat.jur = legdat.jur
        AND pardat.parid = legdat.parid
        AND pardat.taxyr = legdat.taxyr
        AND legdat.cur = 'Y'
        AND legdat.deactivat IS NULL
    LEFT JOIN (
        SELECT *
        FROM {{ source('iasworld', 'asmt_all') }}
        WHERE procname = 'BORVALUE'
            AND rolltype != 'RR'
            AND deactivat IS NULL
            AND valclass IS NULL
            AND class NOT IN ('999')
    ) AS asmt
        ON pardat.jur = asmt.jur
        AND pardat.parid = asmt.parid
        AND pardat.taxyr = asmt.taxyr
    LEFT JOIN {{ source('iasworld', 'aprval') }} AS aprval
        ON pardat.jur = aprval.jur
        AND pardat.parid = aprval.parid
        AND pardat.taxyr = aprval.taxyr
        AND aprval.cur = 'Y'
        AND aprval.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'land') }} AS land
        ON pardat.jur = land.jur
        AND pardat.parid = land.parid
        AND pardat.taxyr = land.taxyr
        AND land.cur = 'Y'
        AND land.deactivat IS NULL
    LEFT JOIN {{ source('iasworld', 'owndat') }} AS owndat
        ON pardat.jur = owndat.jur
        AND pardat.parid = owndat.parid
        AND pardat.taxyr = owndat.taxyr
        AND owndat.cur = 'Y'
        AND owndat.deactivat IS NULL
    LEFT JOIN (
        SELECT
            MIN(jur) AS jur,
            parid,
            taxyr,
            array_join(array_agg(DISTINCT user10), ', ') AS user10
        FROM {{ source('iasworld', 'oby') }}
        WHERE cur = 'Y' AND deactivat IS NULL
        GROUP BY parid, taxyr
    ) AS oby
        ON pardat.jur = oby.jur
        AND pardat.parid = oby.parid
        AND pardat.taxyr = oby.taxyr
    LEFT JOIN (
        SELECT *
        FROM {{ source('iasworld', 'asmt_all') }}
        WHERE procname = 'BORVALUE'
            AND rolltype != 'RR'
            AND deactivat IS NULL
            AND valclass IS NULL
            AND class NOT IN ('999')
    ) AS asmt_prev
        ON pardat.jur = asmt_prev.jur
        AND pardat.parid = asmt_prev.parid
        AND CAST(pardat.taxyr AS INT) = CAST(asmt_prev.taxyr AS INT) + 1
    LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat_prev
        ON pardat.jur = pardat_prev.jur
        AND pardat.parid = pardat_prev.parid
        AND CAST(pardat.taxyr AS INT) = CAST(pardat_prev.taxyr AS INT) + 1
        AND pardat_prev.cur = 'Y'
        AND pardat_prev.deactivat IS NULL
    LEFT JOIN {{ source('ccao', 'land_nbhd_rate') }} AS land_nbhd_rate
        ON pardat.nbhd = land_nbhd_rate.town_nbhd
        AND pardat.taxyr = land_nbhd_rate.year
        AND pardat.class = land_nbhd_rate.class
    WHERE pardat.cur = 'Y'
        AND pardat.deactivat IS NULL
),

base_with_sd AS (
    SELECT
        *,
        stddev(land_av_pct_diff) OVER (PARTITION BY township_code) AS land_av_pct_diff_sd,
        stddev(land_sf) OVER (PARTITION BY township_code) AS land_sf_sd
    FROM base
)

SELECT
    pin,
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
            CASE WHEN land_brate != nbhd_land_rate
                THEN '❌ Error: Land rate must match neighborhood land rate' END,
            CASE WHEN class IN ('239', '224') AND land_class = '200'
                THEN '🔍 Check: Review class 200 homestead land on farmland PINs' END,
            CASE WHEN class IN ('239', '224') AND land_class = '500'
                THEN '🔍 Check: Send industrial land on farmland PINs to Manager of Industrial' END,
            CASE WHEN code IN ('500', '600', 'EX')
                THEN '🔍 Check: Review land codes 500, 600, and EX' END,
            -- TK: Activates once land_av_pct_diff_sd is computed
            CASE WHEN land_av_pct_diff_sd > 1
                THEN '🔍 Check: Review big increases in land AV' END,
            -- TK: Activates once land_sf_sd is computed
            CASE WHEN land_sf_sd > 1
                THEN '🔍 Check: Review values for PINs with largest land in the township' END,
            CASE WHEN land_av_diff <= 0
                THEN '🔍 Check: Review land value that is flat or decreasing' END
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
    land_sf_sd,
    land_brate,
    nbhd_land_rate
FROM base_with_sd
