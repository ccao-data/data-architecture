SELECT
    flag.meta_sale_document_num,
    flag.run_id,
    (
        group_mean.group_size >= param.min_group_thresh
    ) AS meets_group_threshold,
    flag."group",
    group_mean.group_size,
    -- flag.sv_price_deviation,
    -- flag.sv_price_per_sqft_deviation,
    flag.meta_sale_price_original,
    flag.sv_is_outlier,
    flag.ptax_flag_original,
    flag.sv_is_ptax_outlier,
    flag.sv_outlier_reason1,
    flag.sv_outlier_reason2,
    flag.sv_outlier_reason3,
    pin_sale.pin,
    pin_sale.year,
    pin_sale.sale_date,
    p_uni.triad_code,
    p_uni.class
FROM {{ source('sale', 'flag') }} AS flag
LEFT JOIN {{ source('sale', 'group_mean') }} AS group_mean
    ON flag.run_id = group_mean.run_id
    AND flag."group" = group_mean."group"
LEFT JOIN {{ ref('default.vw_pin_sale') }} AS pin_sale
    ON pin_sale.doc_no = flag.meta_sale_document_num
LEFT JOIN {{ ref('default.vw_pin_universe') }} AS p_uni
    ON p_uni.pin = pin_sale.pin
    AND p_uni.year = pin_sale.year
