WITH ias_sales AS (
    SELECT 
        salekey,
        NULLIF(REPLACE(instruno, 'D', ''), '') AS instruno_clean
    FROM {{'iasworld', 'sales'}}
    WHERE cur = 'Y'
    AND deactivat IS NULL
)

SELECT 
    ias_sales.salekey,
    sf.sv_is_outlier,
    sf.sv_outlier_type,
    CAST(NULL AS VARCHAR) AS analyst_override, -- Should we assume this has been created already in ias world and pull this column from iasworld.sale?
    sf.run_id
FROM ias_sales 
JOIN {{ ref('sale.flag') }} sf ON ias_sales.instruno_clean = sf.meta_sale_document_num;
