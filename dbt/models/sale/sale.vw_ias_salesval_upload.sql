WITH ias_sales AS (
    SELECT
        salekey,
        NULLIF(REPLACE(instruno, 'D', ''), '') AS instruno_clean
    FROM {{ source('iasworld', 'sales') }}
    WHERE cur = 'Y'
        AND deactivat IS NULL
),

max_version AS (
    SELECT
        meta_sale_document_num,
        MAX(version) AS max_version
    FROM "z_ci_0002-update-outlier-column-structure-w-iasworld-2024-update_sale"."new_prod_data"
    GROUP BY meta_sale_document_num
)

SELECT
    ias_sales.salekey,
    sf.sv_is_outlier,
    sf.sv_outlier_reason1,
    sf.sv_outlier_reason2,
    sf.sv_outlier_reason3,
    sf.run_id
FROM ias_sales
INNER JOIN "z_ci_0002-update-outlier-column-structure-w-iasworld-2024-update_sale"."new_prod_data" AS sf
    ON ias_sales.instruno_clean = sf.meta_sale_document_num
INNER JOIN max_version AS mv
    ON sf.meta_sale_document_num = mv.meta_sale_document_num
    AND sf.version = mv.max_version;
