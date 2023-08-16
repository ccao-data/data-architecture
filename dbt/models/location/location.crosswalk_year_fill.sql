{{ config(materialized='ephemeral', alias='crosswalk_year_fill') }}

select * from location.crosswalk_year_fill
