{{ config(materialized='ephemeral') }}

select * from reporting.res_report_summary
