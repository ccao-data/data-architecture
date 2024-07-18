SELECT *
FROM {{ ref('qc.vw_iasworld_asmt_all_with_prior_year_values') }}
WHERE class != luc
