SELECT
    flag.meta_sale_document_num,
    flag.run_id,
    (gm.group_size >= param.min_group_thresh) AS meets_group_threshold,
    gm."group",
    gm.group_size,
    flag.sv_price_deviation,
    flag.sv_price_per_sqft_deviation,
    flag.meta_sale_price_original,
    flag.sv_is_outlier,
    flag.ptax_flag_original,
    flag.sv_is_ptax_outlier,
    flag.sv_outlier_reason1,
    flag.sv_outlier_reason2,
    flag.sv_outlier_reason3
FROM z_dev_miwagne_sale.flag AS flag
LEFT JOIN z_dev_miwagne_sale.group_mean AS gm
    ON flag.run_id = gm.run_id
    AND flag."group" = gm."group"
LEFT JOIN z_dev_miwagne_sale.parameter AS param
    ON flag.run_id = param.run_id
