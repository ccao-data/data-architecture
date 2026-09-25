SELECT
    *,
    'dweldat' AS source_table
FROM {{ ref('qc.vw_report_iasworld_test_dweldat') }}
UNION ALL
SELECT
    *,
    'dweldat' AS source_table
FROM {{ ref('qc.vw_report_iasworld_test_dweldat_agg') }}
UNION ALL
SELECT
    *,
    'owndat' AS source_table
FROM {{ ref('qc.vw_report_iasworld_test_owndat') }}
UNION ALL
SELECT
    *,
    'pardat' AS source_table
FROM {{ ref('qc.vw_report_iasworld_test_pardat') }}
UNION ALL
SELECT
    *,
    'sales' AS source_table
FROM {{ ref('qc.vw_report_iasworld_test_sales') }}
