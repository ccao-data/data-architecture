SELECT
    flag.meta_sale_document_num AS doc_no,
    flag.run_id,
    (
        group_mean.group_size >= param.min_group_thresh
    ) AS meets_group_threshold,
    param.min_group_thresh,
    flag."group" AS group_id,
    group_mean.group_size,
    -- These columns are not yet deployed to production flags
    -- flag.sv_price_deviation,
    -- flag.sv_price_per_sqft_deviation,
    flag.meta_sale_price_original AS sale_price,
    flag.sv_is_outlier,
    flag.ptax_flag_original AS ptax_flag,
    flag.sv_is_ptax_outlier,
    flag.sv_outlier_reason1,
    flag.sv_outlier_reason2,
    flag.sv_outlier_reason3,
    pin_sale.pin,
    pin_sale.year,
    pin_sale.sale_date,
    p_uni.triad_code,
    p_uni.class
FROM 'z_dev_miwagne_sale'.'flag' AS flag
LEFT JOIN 'z_dev_miwagne_sale'.'group_mean' AS group_mean
    ON flag.run_id = group_mean.run_id
    AND flag."group" = group_mean."group"
LEFT JOIN 'z_dev_miwagne_sale'.'parameter' AS param
    ON flag.run_id = param.run_id
LEFT JOIN {{ ref('default.vw_pin_sale') }} AS pin_sale
    ON flag.meta_sale_document_num = pin_sale.doc_no
LEFT JOIN {{ ref('default.vw_pin_universe') }} AS p_uni
    ON pin_sale.pin = p_uni.pin
    AND pin_sale.year = p_uni.year
