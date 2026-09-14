/*
Aggregates statistics on characteristics, classes, AVs, and sales by
assessment stage, property groups, year, and various geographies.

This table takes model and assessment values from two locations on Athena and
stacks them. Model and assessment values are gathered independently and
aggregated via a UNION rather than a JOIN, so it's important to keep in mind
that years for model and assessment stages do NOT need to match, i.e. we can
have 2023 model values in the table before there are any 2023 assessment values
to report on. Sales are added via a lagged join, so sales_year should always =
year - 1. It is also worth nothing that "model year" has has 1 added to it
solely for the sake of reporting in this table - models with a 'meta_year' value
of 2022 in model.assessment_pin will populate the table with a value of 2023 for
'year'.

Intended to be materialized daily through a GitHub action.
*/

{{
    config(
        materialized='table',
        table_type='hive',
        format='parquet',
        write_compression='zstd',
        bucketed_by=['year'],
        bucket_count=1
    )
}}

-- Assign input tables to CTEs for ease of reference in macros
WITH all_values AS (
    SELECT * FROM {{ ref('reporting.res_report_summary_values_input') }}
),

all_sales AS (
    SELECT * FROM {{ ref('reporting.res_report_summary_sales_input') }}
),

-- Aggregate and stack stats on AV and characteristics for each reporting group
aggregated_values AS (
    -- By township, assessment_stage, and property group
    {{ res_report_summarize_values(
        from = 'all_values', geo_type = 'Town', prop_group = True
        ) }}
    UNION ALL
    -- By township and assessment stage
    {{ res_report_summarize_values(
        from = 'all_values', geo_type = 'Town', prop_group = False
        ) }}
    UNION ALL
    -- By neighborhood, assessment_stage, and property group
    {{ res_report_summarize_values(
        from = 'all_values', geo_type = 'TownNBHD', prop_group = True
        ) }}
    UNION ALL
    -- By neighborhood and assessment stage
    {{ res_report_summarize_values(
        from = 'all_values', geo_type = 'TownNBHD', prop_group = False
        ) }}
),

-- Aggregate and stack stats on sales for each reporting group
aggregated_sales AS (
    -- By township and property group
    {{ res_report_summarize_sales(
        from = 'all_sales', geo_type = 'Town', prop_group = True
        ) }}
    UNION ALL
    -- By township
    {{ res_report_summarize_sales(
        from = 'all_sales', geo_type = 'Town', prop_group = False
        ) }}
    UNION ALL
    -- By neighborhood and property group
    {{ res_report_summarize_sales(
        from = 'all_sales', geo_type = 'TownNBHD', prop_group = True
        ) }}
    UNION ALL
    -- By neighborhood
    {{ res_report_summarize_sales(
        from = 'all_sales', geo_type = 'TownNBHD', prop_group = False
        ) }}
)

SELECT
    av.*,
    asl.sale_year,
    asl.sale_min,
    asl.sale_median,
    asl.sale_max,
    asl.sale_n
FROM aggregated_values AS av
-- Join sales so that values for a given year can be compared
-- to a complete set of sales from the previous year
LEFT JOIN aggregated_sales AS asl
    ON av.geography_id = asl.geography_id
    AND CAST(av.year AS INT) = CAST(asl.sale_year AS INT) + 1
    AND av.property_group = asl.property_group
