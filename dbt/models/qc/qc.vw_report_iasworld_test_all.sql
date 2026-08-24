SELECT * FROM {{ ref('qc.vw_report_iasworld_test_pardat') }}
UNION ALL
SELECT * FROM {{ ref('qc.vw_report_iasworld_test_owndat') }}
