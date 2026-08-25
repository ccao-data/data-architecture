SELECT *, 'pardat' AS source_table FROM {{ ref('qc.vw_report_iasworld_test_pardat') }}
UNION ALL
SELECT *, 'owndat' AS source_table FROM {{ ref('qc.vw_report_iasworld_test_owndat') }}
