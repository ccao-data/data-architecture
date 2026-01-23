-- View that derives the most recent version of flags for each sale in the
-- `sale.flag` table, which uses its `version` column as a type 2
-- slowly changing dimension
SELECT
    sf.meta_sale_document_num AS doc_no,
    sf.sv_is_outlier,
    sf.sv_is_ptax_outlier,
    sf.sv_is_heuristic_outlier,
    sf.sv_outlier_reason1,
    sf.sv_outlier_reason2,
    sf.sv_outlier_reason3,
    sf.run_id,
    sf.version
FROM {{ source('sale', 'flag') }} AS sf
INNER JOIN (
    SELECT
        meta_sale_document_num,
        MAX(version) AS max_version
    FROM {{ source('sale', 'flag') }}
    GROUP BY meta_sale_document_num
) AS mv
    ON sf.meta_sale_document_num = mv.meta_sale_document_num
    AND sf.version = mv.max_version
