-- View to standardize residential property characteristics for use in
-- modeling and reporting
CREATE OR REPLACE VIEW default.vw_card_res_char AS
WITH multicodes AS (
    SELECT
        parid,
        taxyr,
        CASE
            WHEN COUNT(*) > 1 THEN true
            ELSE false
        END AS pin_is_multicard,
        COUNT(*) AS pin_num_cards
    FROM iasworld.dweldat
    GROUP BY parid, taxyr
),
aggregate_land AS (
    SELECT
        parid,
        taxyr,
        CASE
            WHEN COUNT(*) > 1 THEN true
            ELSE false
        END AS pin_is_multiland,
        COUNT(*) AS pin_num_landlines,
        SUM(sf) AS total_land_sf
    FROM iasworld.land
    GROUP BY parid, taxyr
),
townships AS (
    SELECT
        parid,
        taxyr,
        substr(TAXDIST, 1, 2) AS township_code
    FROM iasworld.legdat
)
SELECT
    dweldat.parid AS pin,
    SUBSTR(dweldat.parid, 1, 10) AS pin10,
    dweldat.taxyr AS year,
    card,
    seq,
    who AS updated_by,
    date_parse(wen, '%Y-%m-%d %H:%i:%s.%f') AS updated_at,

    -- PIN information
    class, -- 218, 219, 236, 241 classes added to DWELDAT
    township_code,
    cdu,
    pin_is_multicard,
    pin_num_cards,
    pin_is_multiland,
    pin_num_landlines,

    -- New variables
    yrblt AS char_yrblt,

    -- Continuous variables
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
    user14 AS char_apts,
    user4 AS char_tp_dsgn,
    user6 AS char_attic_fnsh,
    user31 AS char_gar1_att,
    user32 AS char_gar1_area,

    -- Same numeric encoding as AS/400
    user33 AS char_gar1_size,
    user34 AS char_gar1_cnst,
    attic AS char_attic_type,
    bsmt AS char_bsmt,
    extwall AS char_ext_wall,
    heat AS char_heat,
    user1 AS char_repair_cnd,
    user12 AS char_bsmt_fin,
    user13 AS char_roof_cnst,
    user15 AS char_use,
    user17 AS char_age, -- Deprecated, use yrblt
    user2 AS char_site,
    user20 AS char_ncu,
    user3 AS char_renovation,

    -- Indicate a change from 0 or NULL to 1 for renovation
    -- within the last 3 years
    CASE
        WHEN (user3 = '1' AND
            Lag(user3)
                over(
                    PARTITION BY dweldat.parid
                    ORDER BY dweldat.parid, dweldat.taxyr) != '1') OR
            (Lag(user3)
                over(
                    PARTITION BY dweldat.parid
                    ORDER BY dweldat.parid, dweldat.taxyr) = '1' AND
            Lag(user3, 2)
                over(
                    PARTITION BY dweldat.parid
                    ORDER BY dweldat.parid, dweldat.taxyr) != '1') OR
            (Lag(user3, 2)
                over(
                    PARTITION BY dweldat.parid
                    ORDER BY dweldat.parid, dweldat.taxyr) = '1' AND
            Lag(user3, 3)
                over(
                    PARTITION BY dweldat.parid
                    ORDER BY dweldat.parid, dweldat.taxyr) != '1')
        THEN true
        ELSE false
    END AS char_recent_renovation,

    user30 AS char_porch,
    user7 AS char_air,
    user5 AS char_tp_plan

    -- To investigate later:
    -- plumval
    -- atticval
    -- wbfpval
    -- subtval
    -- user38
    -- user23
    -- convbldg

FROM iasworld.dweldat
LEFT JOIN multicodes
    ON dweldat.parid = multicodes.parid
    AND dweldat.taxyr = multicodes.taxyr
LEFT JOIN aggregate_land
    ON dweldat.parid = aggregate_land.parid
    AND dweldat.taxyr = aggregate_land.taxyr
LEFT JOIN townships
    ON dweldat.parid = townships.parid
    AND dweldat.taxyr = townships.taxyr