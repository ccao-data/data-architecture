WITH ias_sales AS (
    SELECT
        salekey,
        NULLIF(REPLACE(instruno, 'D', ''), '') AS doc_no
    FROM {{ source('iasworld', 'sales') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
)

SELECT
    ias_sales.salekey,
    sf.sv_is_outlier,
    sf.sv_outlier_reason1,
    sf.sv_outlier_reason2,
    sf.sv_outlier_reason3,
    sf.run_id
FROM ias_sales
INNER JOIN {{ ref('sale.vw_flag') }} AS sf
    ON ias_sales.doc_no = sf.doc_no
