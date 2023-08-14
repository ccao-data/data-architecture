{{ config(materialized='ephemeral') }}

select * from proximity.crosswalk_year_fill
