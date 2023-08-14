{{ config(materialized='ephemeral') }}

select * from census.acs5
