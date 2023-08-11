{{ config(materialized='ephemeral') }}

select * from reporting.ratio_stats
