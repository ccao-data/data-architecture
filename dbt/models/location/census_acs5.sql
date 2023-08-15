{{ config(materialized='ephemeral') }}

select * from location.census_acs5
