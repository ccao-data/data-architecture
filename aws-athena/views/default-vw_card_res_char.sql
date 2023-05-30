-- View to standardize residential property characteristics for use in
-- modeling and reporting
CREATE OR REPLACE VIEW default.vw_card_res_char AS
WITH multicodes AS (
    SELECT
        parid,
        taxyr,
        COALESCE(COUNT(*) > 1, FALSE) AS pin_is_multicard,
        COUNT(*) AS pin_num_cards
    FROM iasworld.dweldat
    GROUP BY parid, taxyr
),

aggregate_land AS (
    SELECT
        parid,
        taxyr,
        COALESCE(COUNT(*) > 1, FALSE) AS pin_is_multiland,
        COUNT(*) AS pin_num_landlines,
        SUM(sf) AS total_land_sf
    FROM iasworld.land
    GROUP BY parid, taxyr
),

townships AS (
    SELECT
        parid,
        taxyr,
        user1 AS township_code
    FROM iasworld.legdat
)

SELECT
    dweldat.parid AS pin,
    SUBSTR(dweldat.parid, 1, 10) AS pin10,
    dweldat.taxyr AS year,
    card,
    dweldat.seq,
    dweldat.who AS updated_by,
    DATE_PARSE(dweldat.wen, '%Y-%m-%d %H:%i:%s.%f') AS updated_at,

    -- PIN information
    -- 218, 219, 236, 241 classes added to DWELDAT
    dweldat.class,
    township_code,
    cdu,
    pardat.tieback AS tieback_key_pin,
    CASE
        WHEN pardat.tiebldgpct IS NOT NULL THEN pardat.tiebldgpct / 100.0
        ELSE 1.0
    END AS tieback_proration_rate,
    CAST(dweldat.user24 AS DOUBLE) / 100.0 AS card_protation_rate,
    pin_is_multicard,
    pin_num_cards,
    pin_is_multiland,
    pin_num_landlines,

    -- Continuous variables
    yrblt AS char_yrblt,
    sfla AS char_bldg_sf,
    total_land_sf AS char_land_sf,
    rmbed AS char_beds,
    rmtot AS char_rooms,
    fixbath AS char_fbath,
    fixhalf AS char_hbath,
    wbfp_o AS char_frpl,

    -- New numeric encoding compared to AS/400
    stories AS char_type_resd,
    grade AS char_cnst_qlty,
    dweldat.user14 AS char_apts,
    dweldat.user4 AS char_tp_dsgn,
    dweldat.user6 AS char_attic_fnsh,
    dweldat.user31 AS char_gar1_att,
    dweldat.user32 AS char_gar1_area,

    -- Same numeric encoding as AS/400
    dweldat.user33 AS char_gar1_size,
    dweldat.user34 AS char_gar1_cnst,
    attic AS char_attic_type,
    bsmt AS char_bsmt,
    extwall AS char_ext_wall,
    heat AS char_heat,
    dweldat.user1 AS char_repair_cnd,
    dweldat.user12 AS char_bsmt_fin,
    dweldat.user13 AS char_roof_cnst,
    dweldat.user15 AS char_use,
    dweldat.user17 AS char_age, -- Deprecated, use yrblt
    dweldat.user2 AS char_site,
    dweldat.user20 AS char_ncu,
    dweldat.user3 AS char_renovation,

    -- Indicate a change from 0 or NULL to 1 for renovation
    -- within the last 3 years
    COALESCE((
        dweldat.user3 = '1'
        AND LAG(dweldat.user3)
            OVER (
                PARTITION BY dweldat.parid
                ORDER BY dweldat.parid, dweldat.taxyr
            )
        != '1'
    )
    OR (LAG(dweldat.user3)
        OVER (
            PARTITION BY dweldat.parid
            ORDER BY dweldat.parid, dweldat.taxyr
        )
    = '1'
    AND LAG(dweldat.user3, 2)
        OVER (
            PARTITION BY dweldat.parid
            ORDER BY dweldat.parid, dweldat.taxyr
        )
    != '1')
    OR (LAG(dweldat.user3, 2)
        OVER (
            PARTITION BY dweldat.parid
            ORDER BY dweldat.parid, dweldat.taxyr
        )
    = '1'
    AND LAG(dweldat.user3, 3)
        OVER (
            PARTITION BY dweldat.parid
            ORDER BY dweldat.parid, dweldat.taxyr
        )
    != '1'), FALSE) AS char_recent_renovation,
    dweldat.user30 AS char_porch,
    dweldat.user7 AS char_air,
    dweldat.user5 AS char_tp_plan

FROM iasworld.dweldat
LEFT JOIN iasworld.pardat
    ON dweldat.parid = pardat.parid
    AND dweldat.taxyr = pardat.taxyr
LEFT JOIN multicodes
    ON dweldat.parid = multicodes.parid
    AND dweldat.taxyr = multicodes.taxyr
LEFT JOIN aggregate_land
    ON dweldat.parid = aggregate_land.parid
    AND dweldat.taxyr = aggregate_land.taxyr
LEFT JOIN townships
    ON dweldat.parid = townships.parid
    AND dweldat.taxyr = townships.taxyr
