/*
Input table for `reporting.res_report_summary` that produces the raw sales data
that `res_report_summary` aggregates. We split these input data out into a
separate table to speed up query time for `res_report_summary`, since otherwise
it needs to rerun the query logic below for every possible geography and
reporting group combination.

See `reporting.res_report_summary` for a full description of these data.

Intended to be materialized daily through a GitHub action.
*/

{{
    config(
        materialized='table',
        partitioned_by=['sale_year']
    )
}}

-- Sales, filtered to exclude outliers and mutlisales
SELECT
    vwps.pin,
    vwps.doc_no,
    vwps.sale_price,
    tc.property_group,
    tc.township_code,
    vwps.nbhd AS townnbhd,
    vwps.year AS sale_year
FROM {{ ref('default.vw_pin_sale') }} AS vwps
LEFT JOIN {{ ref('reporting.vw_pin_township_class') }} AS tc
    ON vwps.pin = tc.pin
    AND vwps.year = tc.year
WHERE NOT vwps.is_multisale
    AND NOT vwps.sale_filter_is_outlier
    AND NOT vwps.sale_filter_deed_type
    AND NOT vwps.sale_filter_less_than_10k
    AND NOT vwps.sale_filter_same_sale_within_365
    AND tc.property_group IS NOT NULL
    AND tc.triad_name IS NOT NULL
