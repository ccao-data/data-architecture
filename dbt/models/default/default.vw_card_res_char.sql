-- View to standardize residential property characteristics for use in
-- modeling and reporting
WITH multicodes AS (
    SELECT
        parid,
        taxyr,
        COALESCE(COUNT(*) > 1, FALSE) AS pin_is_multicard,
        COUNT(*) AS pin_num_cards
    FROM {{ source('iasworld', 'dweldat') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
    GROUP BY parid, taxyr
),

pools AS (
    SELECT
        parid,
        taxyr,
        TRUE AS in_ground_pool
    FROM {{ source('iasworld', 'oby') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
        AND code = '297'
        AND user1 = '13' -- This is our best understanding of a pool indicator
    GROUP BY parid, taxyr
)

SELECT
    dwel.parid AS pin,
    SUBSTR(dwel.parid, 1, 10) AS pin10,
    dwel.taxyr AS year,
    dwel.card,
    dwel.seq,
    dwel.who AS updated_by,
    DATE_PARSE(dwel.wen, '%Y-%m-%d %H:%i:%s.%f') AS updated_at,

    -- PIN information
    -- 218, 219, 236, 241 classes added to DWELDAT
    dwel.class,
    leg.user1 AS township_code,
    dwel.cdu,
    par.tieback AS tieback_key_pin,
    CASE
        WHEN par.tiebldgpct IS NOT NULL THEN par.tiebldgpct / 100.0
        ELSE 1.0
    END AS tieback_proration_rate,
    CAST(dwel.user24 AS DOUBLE) / 100.0 AS card_proration_rate,
    multicodes.pin_is_multicard,
    multicodes.pin_num_cards,
    COALESCE(aggregate_land.num_landlines > 1, FALSE) AS pin_is_multiland,
    aggregate_land.num_landlines AS pin_num_landlines,

    -- Continuous variables
    dwel.yrblt AS char_yrblt,
    dwel.sfla AS char_bldg_sf,
    aggregate_land.sf AS char_land_sf,
    dwel.rmbed AS char_beds,
    dwel.rmtot AS char_rooms,
    dwel.fixbath AS char_fbath,
    dwel.fixhalf AS char_hbath,
    dwel.wbfp_o AS char_frpl,

    -- New numeric encoding compared to AS/400
    dwel.stories AS char_type_resd,
    dwel.grade AS char_cnst_qlty,
    dwel.user14 AS char_apts,
    dwel.user4 AS char_tp_dsgn,
    dwel.user6 AS char_attic_fnsh,
    dwel.user31 AS char_gar1_att,
    dwel.user32 AS char_gar1_area,

    -- Same numeric encoding as AS/400
    dwel.user33 AS char_gar1_size,
    dwel.user34 AS char_gar1_cnst,
    dwel.attic AS char_attic_type,
    dwel.bsmt AS char_bsmt,
    dwel.extwall AS char_ext_wall,
    dwel.heat AS char_heat,
    dwel.user1 AS char_repair_cnd,
    dwel.user12 AS char_bsmt_fin,
    dwel.user13 AS char_roof_cnst,
    dwel.class AS char_class,
    dwel.user15 AS char_use,
    dwel.user17 AS char_age, -- Deprecated, use yrblt
    dwel.user2 AS char_site,
    dwel.user20 AS char_ncu,
    dwel.user3 AS char_renovation,

    -- Indicate a change from 0 or NULL to 1 for renovation
    -- within the last 3 years. Needs to be partioned by card as well as pin.
    COALESCE((
        dwel.user3 = '1'
        AND LAG(dwel.user3)
            OVER (
                PARTITION BY dwel.parid, dwel.card
                ORDER BY dwel.taxyr
            )
        != '1'
    )
    OR (LAG(dwel.user3)
        OVER (
            PARTITION BY dwel.parid, dwel.card
            ORDER BY dwel.taxyr
        )
    = '1'
    AND LAG(dwel.user3, 2)
        OVER (
            PARTITION BY dwel.parid, dwel.card
            ORDER BY dwel.taxyr
        )
    != '1')
    OR (LAG(dwel.user3, 2)
        OVER (
            PARTITION BY dwel.parid, dwel.card
            ORDER BY dwel.taxyr
        )
    = '1'
    AND LAG(dwel.user3, 3)
        OVER (
            PARTITION BY dwel.parid, dwel.card
            ORDER BY dwel.taxyr
        )
    != '1'), FALSE) AS char_recent_renovation,
    dwel.user30 AS char_porch,
    dwel.user7 AS char_air,
    dwel.user5 AS char_tp_plan,

    COALESCE(pools.in_ground_pool, FALSE) AS char_in_ground_pool

FROM {{ source('iasworld', 'pardat') }} AS par
INNER JOIN {{ source('iasworld', 'dweldat') }} AS dwel
    ON par.parid = dwel.parid
    AND par.taxyr = dwel.taxyr
    AND dwel.cur = 'Y'
    AND dwel.deactivat IS NULL
LEFT JOIN multicodes
    ON dwel.parid = multicodes.parid
    AND dwel.taxyr = multicodes.taxyr
LEFT JOIN {{ ref('default.vw_pin_land') }} AS aggregate_land
    ON dwel.parid = aggregate_land.pin
    AND dwel.taxyr = aggregate_land.year
LEFT JOIN {{ source('iasworld', 'legdat') }} AS leg
    ON dwel.parid = leg.parid
    AND dwel.taxyr = leg.taxyr
    AND leg.cur = 'Y'
    AND leg.deactivat IS NULL
LEFT JOIN pools
    ON dwel.parid = pools.parid
    AND dwel.taxyr = pools.taxyr
WHERE par.cur = 'Y'
    AND par.deactivat IS NULL
    AND par.class NOT IN ('999')
