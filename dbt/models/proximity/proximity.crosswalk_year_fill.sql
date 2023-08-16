{{ config(materialized='ephemeral', alias='crosswalk_year_fill') }}

select * from proximity.crosswalk_year_fill
