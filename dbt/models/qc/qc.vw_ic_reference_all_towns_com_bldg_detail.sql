SELECT
    pardat.jur,
    pardat.taxyr,
    pardat.parid,
    {{ insert_hyphens("pardat.class", 1) }} AS pardat_class,
    {{ insert_hyphens("pardat.tieback", 2, 4, 7, 10) }} AS tieback,
    comdat.bldnum,
    comdat.class,
    CONCAT(
        COALESCE(comdat.user12, ''),
        CASE WHEN comdat.user12 IS NOT NULL THEN ':' ELSE '' END,
        COALESCE(major_subclass.description, '')
    ) AS major_subclass,
    CONCAT(
        COALESCE(comdat.user13, ''),
        CASE WHEN comdat.user13 IS NOT NULL THEN ':' ELSE '' END,
        COALESCE(minor_subclass.description, '')
    ) AS minor_subclass,
    comdat.ovrrcnld,
    comdat.user24 AS proration_percent,
    comdat.user16 AS alt_cdu,
    comdat.card,
    comdat.mktadj,
    comdat.convbldg,
    comdat.yrblt,
    comdat.effyr,
    comdat.chgrsn,
    comdat.userval4,
    comdat.external_calc_rcnld,
    comdat.external_occpct,
    comdat.external_propct,
    comdat.external_rcnld,
    comdat.calc_meth,
    comdat.imprname,
    comdat.user20 AS gross_building_area,
    comdat.user26 AS gross_building_description,
    comdat.units,
    comdat.user28 AS net_rentable_area,
    comdat.user15 AS basement_area,
    comdat.user1 AS stories,
    comdat.parkcover AS parking_spaces,
    comdat.user3 AS studios,
    comdat.user7 AS one_beds,
    comdat.user8 AS two_beds,
    comdat.user9 AS three_beds,
    comdat.user27 AS four_beds,
    comdat.user42 AS mobile_home_pads,
    comdat.user43 AS comm_sf,
    comdat.user44 AS car_wash,
    comdat.user45 AS food_op_rental,
    comdat.user46 AS auto_service_bays,
    comdat.user47 AS idph_license_no,
    comdat.user48 AS ceiling_height,
    comdat.user50 AS agg_units,
    comdat.user51 AS agg_gross_bldg_area,
    comdat.user52 AS agg_net_rentable_area,
    comdat.user54 AS agg_studios,
    comdat.user55 AS agg_one_beds,
    comdat.user56 AS agg_two_beds,
    comdat.user57 AS agg_three_beds,
    comdat.user58 AS agg_four_beds,
    comdat.user59 AS agg_mobile_home_pads,
    comdat.user60 AS agg_comm_sf,
    comdat.user61 AS agg_car_wash,
    comdat.user62 AS agg_food_op_rental,
    comdat.user63 AS agg_auto_service_bays,
    comdat.user64 AS agg_idph_license_no,
    comdat.user65 AS agg_ceiling_height,
    comdat.note1,
    comdat.note2,
    comdat.exmppct,
    CONCAT(
        COALESCE(comdat.user29, ''),
        CASE WHEN comdat.user29 IS NOT NULL THEN ':' ELSE '' END,
        COALESCE(model_group.description, '')
    ) AS model_group,
    legdat.user1 AS township_code
FROM {{ source('iasworld', 'comdat') }} AS comdat
LEFT JOIN {{ source('iasworld', 'legdat') }} AS legdat
    ON comdat.parid = legdat.parid
    AND comdat.taxyr = legdat.taxyr
    AND legdat.cur = 'Y'
    AND legdat.deactivat IS NULL
LEFT JOIN {{ source('iasworld', 'pardat') }} AS pardat
    ON comdat.parid = pardat.parid
    AND comdat.taxyr = pardat.taxyr
    AND pardat.cur = 'Y'
    AND pardat.deactivat IS NULL
LEFT JOIN {{ ref('ccao.commercial_major_subclass') }} AS major_subclass
    ON comdat.user12 = major_subclass.code
LEFT JOIN {{ ref('ccao.commercial_minor_subclass') }} AS minor_subclass
    ON comdat.user13 = minor_subclass.code
LEFT JOIN {{ ref('ccao.commercial_model_group') }} AS model_group
    ON comdat.user29 = model_group.code
WHERE comdat.cur = 'Y'
    AND comdat.deactivat IS NULL
    AND pardat.class NOT BETWEEN '200' AND '299'
