WITH ias_sales AS (
    SELECT 
        salekey,
        NULLIF(REPLACE(instruno, 'D', ''), '') AS instruno_clean
    FROM {{ source('iasworld', 'sales') }}
    WHERE cur = 'Y'
    AND deactivat IS NULL
)

SELECT 
    ias_sales.salekey,
    sf.sv_is_outlier,
    sf.sv_outlier_type,
    sf.run_id
FROM ias_sales 
INNER JOIN {{ source('sale', 'flag') }} sf ON ias_sales.instruno_clean = sf.meta_sale_document_num;
